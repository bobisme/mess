//! [`EventStore`]: the DX-first facade — `load` / `append` / `command` with
//! bounded, jittered optimistic retry.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use mess_core::{Actor, Aggregate, CodecError, CommandError, Decide, Event};

use crate::backend::{AppendError, Backend, RecordToAppend, SubscribeBackend};
use crate::cache::StateCache;
use crate::retry::RetryPolicy;
use crate::snapshot::{
    BlobPtr, SnapshotRef, SnapshotStore, Snapshottable, StateCodecError,
    StoredSnapshot, interim_stream_id,
};
use crate::subscription::Subscription;
use crate::version::Version;

/// The default page size for the [`load`](EventStore::load) replay loop.
pub const DEFAULT_PAGE_SIZE: usize = 1_000;

/// Infrastructure-level store failure — the `S` in
/// [`mess_core::CommandError`].
///
/// This is deliberately *not* a domain rejection (that is the aggregate's
/// [`Decide::Rejection`], the `R`) and *not* a conflict-exhaustion (that is
/// the dedicated [`CommandError::Conflict`] variant). It carries the things
/// that can go wrong in the plumbing: the backend engine failed, a payload
/// would not encode/decode, or a stored event's bytes were corrupt.
#[derive(Debug, thiserror::Error)]
pub enum StoreError<E> {
    /// The storage engine failed.
    #[error("backend error: {0}")]
    Backend(#[source] E),
    /// An event payload failed to encode or decode.
    #[error("codec error: {0}")]
    Codec(#[source] CodecError),
    /// An aggregate **state** blob failed to (de)serialize for the interim
    /// snapshot store. Distinct from [`Codec`](StoreError::Codec), which is
    /// about event payloads. A `load`-side failure here never reaches a
    /// caller who uses the base [`load`](EventStore::load): it can only arise
    /// on the snapshot-accelerated path, which treats it as a reason to fall
    /// back to full replay.
    #[error("state codec error: {0}")]
    State(#[source] StateCodecError),
}

/// The outcome of an **authored** command
/// ([`command_as`](EventStore::command_as) /
/// [`command_cached_as`](EventStore::command_cached_as)): either the command's
/// declared [`Actor`] stream diverged from the stream it was dispatched to — a
/// caller bug caught before the log is touched — or the underlying command
/// round produced an ordinary [`CommandError`].
///
/// It deliberately wraps [`CommandError`] rather than adding a variant to it:
/// an actor/stream divergence is a *dispatch precondition*, not one of the
/// three command-round outcomes (`Domain` / `Conflict` / `Store`), and callers
/// of the plain [`command`](EventStore::command) never have to widen their
/// match to account for it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthoredCommandError<R, S> {
    /// The command declared actor stream `declared` (via
    /// [`Actor::actor_stream`]) but was dispatched to `stream`. A
    /// self-referential invariant's echoed actor id diverged from the stream
    /// key it implies; the store refuses to write rather than let the two
    /// disagree. Nothing was appended.
    ActorMismatch {
        /// The stream the command declared it is authored against.
        declared: String,
        /// The stream it was actually dispatched to.
        stream:   String,
    },
    /// The command round itself failed (a domain rejection, conflict
    /// exhaustion, or store plumbing) — see [`CommandError`].
    Command(CommandError<R, S>),
}

impl<R, S> From<CommandError<R, S>> for AuthoredCommandError<R, S> {
    fn from(e: CommandError<R, S>) -> Self { AuthoredCommandError::Command(e) }
}

impl<R: std::fmt::Display, S: std::fmt::Display> std::fmt::Display
    for AuthoredCommandError<R, S>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AuthoredCommandError::ActorMismatch { declared, stream } => write!(
                f,
                "actor/stream divergence: command is authored against \
                 {declared:?} but was dispatched to stream {stream:?}"
            ),
            AuthoredCommandError::Command(e) => write!(f, "{e}"),
        }
    }
}

impl<R, S> std::error::Error for AuthoredCommandError<R, S>
where
    R: std::error::Error + 'static,
    S: std::error::Error + 'static,
{
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            AuthoredCommandError::ActorMismatch { .. } => None,
            AuthoredCommandError::Command(e) => Some(e),
        }
    }
}

impl<E> From<CodecError> for StoreError<E> {
    fn from(e: CodecError) -> Self { StoreError::Codec(e) }
}

impl<E> From<StateCodecError> for StoreError<E> {
    fn from(e: StateCodecError) -> Self { StoreError::State(e) }
}

/// Result of replaying a stream through [`Aggregate::apply`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Loaded<A> {
    /// The folded aggregate state.
    pub state:           A,
    /// The version the stream was at when loaded; feed this straight back to
    /// [`append`](EventStore::append) as the expected version.
    pub version:         Version,
    /// How many events were replayed to build `state`.
    pub events_replayed: usize,
}

/// Result of a successful [`append`](EventStore::append) or
/// [`command`](EventStore::command).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Commit {
    /// The stream's version after the commit.
    pub version:              Version,
    /// Global position of the last event written, or `None` when nothing was
    /// appended (e.g. a command that decided zero events).
    pub last_global_position: Option<u64>,
    /// How many events were appended.
    pub events_appended:      usize,
    /// How many optimistic attempts [`command`](EventStore::command) needed
    /// (always 1 for a direct [`append`](EventStore::append)).
    pub attempts:             u32,
}

/// Observability counters for the snapshot-accelerated load path — the deploy
/// story of `docs/spec/05-fold-certificates.md` §9.
///
/// A `fold_version` bump is a deploy-time event: every snapshot written by the
/// old binary is now stale and must be rebuilt. This counter makes that
/// rebuild wave **observable** — an operator watching `invalidated()` climb
/// right after a deploy is watching the snapshot store re-warm itself, exactly
/// once per stale stream (each stale snapshot is rebuilt by full replay and
/// then *replaced* with a fresh one carrying the new `fold_version`, so it is
/// counted at most once).
///
/// Shared across clones of an [`EventStore`] (the counter lives behind an
/// `Arc`), so a read on any clone observes increments from all of them — the
/// same clone-is-share contract as the [`StateCache`].
#[derive(Debug, Clone, Default)]
pub struct SnapshotMetrics {
    invalidated: Arc<AtomicU64>,
}

impl SnapshotMetrics {
    /// How many stored snapshots have been invalidated by a `fold_version`
    /// mismatch and rebuilt by full replay (§9). This is the
    /// deploy-observability counter: it climbs once per stale stream after
    /// a `fold_version` bump and then stops, because the rebuilt state is
    /// persisted with the new version.
    #[must_use]
    pub fn invalidated(&self) -> u64 {
        self.invalidated.load(Ordering::Relaxed)
    }

    /// Record one `fold_version`-mismatch invalidation.
    fn record_invalidation(&self) {
        self.invalidated.fetch_add(1, Ordering::Relaxed);
    }
}

/// The outcome of consulting the snapshot store on the accelerated load path.
///
/// Separating "no snapshot to use" from "a snapshot existed but its
/// `fold_version` is stale" is what lets
/// [`load_cached`](EventStore::load_cached) treat the deploy case specially:
/// count it, and *replace* the stale snapshot after the rebuild — without
/// turning an ordinary snapshot-less load into an (unwanted) implicit
/// `save_snapshot`.
enum SnapshotOutcome<A> {
    /// A valid snapshot was found and folded forward; this is the answer.
    Used(Loaded<A>),
    /// No usable snapshot for a reason that is *not* a fold bump — absent,
    /// past-head, an undecodable/garbled record, or a corrupt blob. The load
    /// falls back to a plain full replay and writes nothing back.
    NoSnapshot,
    /// A snapshot existed but its `fold_version` no longer matches the current
    /// [`Snapshottable::FOLD_VERSION`] (§9). The load rebuilds by full replay,
    /// counts the invalidation, and replaces the stale snapshot.
    Invalidated,
}

/// The event-store facade over any [`Backend`].
///
/// Cloning is cheap when the backend is cheap to clone (e.g. an `Arc`-backed
/// handle); each clone shares the same backend, retry configuration, and — when
/// enabled — the same hot-aggregate [`StateCache`] (its entries live behind an
/// `Arc`), so warm state is shared across clones and across concurrent writers.
#[derive(Debug, Clone)]
pub struct EventStore<B> {
    backend:   B,
    policy:    RetryPolicy,
    page_size: usize,
    /// Hot-aggregate state cache (doc-02). Disabled by default, so the base
    /// [`load`](Self::load)/[`command`](Self::command) behavior — and every
    /// existing test — is unchanged until a caller opts in with
    /// [`with_cache_capacity`](Self::with_cache_capacity).
    cache:     StateCache,
    /// Snapshot-path observability (the `fold_version` invalidation counter,
    /// §9). Shared across clones via its internal `Arc`.
    metrics:   SnapshotMetrics,
}

impl<B: Backend> EventStore<B> {
    /// Wrap `backend` with the default retry policy and page size, and **no**
    /// state cache (the cache is opt-in via
    /// [`with_cache_capacity`](Self::with_cache_capacity)).
    pub fn new(backend: B) -> Self {
        Self {
            backend,
            policy: RetryPolicy::default(),
            page_size: DEFAULT_PAGE_SIZE,
            cache: StateCache::disabled(),
            metrics: SnapshotMetrics::default(),
        }
    }

    /// Enable the hot-aggregate [`StateCache`] with room for `capacity`
    /// streams.
    ///
    /// This is the on-switch for the warm command/read path
    /// ([`command_cached`](Self::command_cached) /
    /// [`load_hot`](Self::load_hot)). Off (the default) those methods still
    /// work — they simply run the cache-miss fallthrough on every call,
    /// which is the *identical* code path, so the cache changes
    /// performance, never results.
    #[must_use]
    pub fn with_cache_capacity(mut self, capacity: usize) -> Self {
        self.cache = StateCache::with_capacity(capacity);
        self
    }

    /// Install a pre-built [`StateCache`] (e.g. a shared or explicitly disabled
    /// one). See [`with_cache_capacity`](Self::with_cache_capacity) for the
    /// common case.
    #[must_use]
    pub fn with_cache(mut self, cache: StateCache) -> Self {
        self.cache = cache;
        self
    }

    /// Borrow the hot-aggregate state cache (for inspection / tests).
    #[must_use]
    pub fn cache(&self) -> &StateCache { &self.cache }

    /// Borrow the snapshot-path observability metrics — chiefly the
    /// `fold_version` invalidation counter that makes the deploy-time rebuild
    /// wave visible (§9). Shared across clones.
    #[must_use]
    pub fn snapshot_metrics(&self) -> &SnapshotMetrics { &self.metrics }

    /// Replace the optimistic-retry policy.
    #[must_use]
    pub fn with_retry_policy(mut self, policy: RetryPolicy) -> Self {
        self.policy = policy;
        self
    }

    /// Set the optimistic-retry budget, keeping the default backoff.
    #[must_use]
    pub fn with_max_attempts(mut self, attempts: u32) -> Self {
        self.policy.max_attempts = attempts.max(1);
        self
    }

    /// Set the page size for the [`load`](EventStore::load) replay loop
    /// (clamped to at least 1).
    #[must_use]
    pub fn with_page_size(mut self, page_size: usize) -> Self {
        self.page_size = page_size.max(1);
        self
    }

    /// The configured retry policy.
    #[must_use]
    pub fn retry_policy(&self) -> RetryPolicy { self.policy }

    /// Borrow the underlying backend.
    pub fn backend(&self) -> &B { &self.backend }

    /// Load an aggregate by replaying its whole stream through
    /// [`Aggregate::apply`].
    ///
    /// This pages through the backend in `page_size` chunks, advancing a
    /// cursor until the stream is exhausted — so there is no ceiling on stream
    /// length at the API level. (Backend-native paging lands in Phase 2; the
    /// facade just loops.)
    pub async fn load<A: Aggregate>(
        &self,
        stream_id: &str,
    ) -> Result<Loaded<A>, StoreError<B::Error>> {
        let mut state = A::default();
        let mut version = Version::NoStream;
        let mut events_replayed = 0;
        loop {
            let page = self
                .backend
                .read_stream(stream_id, version, self.page_size)
                .await
                .map_err(StoreError::Backend)?;
            let page_len = page.len();
            for rec in &page {
                let event =
                    <A::Event as Event>::decode(&rec.message_type, &rec.data)?;
                state.apply(&event);
                version = Version::At(rec.stream_position);
            }
            events_replayed += page_len;
            // A short page (or an empty one) means we've reached the end.
            if page_len < self.page_size {
                break;
            }
        }
        Ok(Loaded { state, version, events_replayed })
    }

    /// Append `events` to `stream_id` at an exact `expected` version.
    ///
    /// A version conflict surfaces as
    /// [`AppendError::Conflict`](crate::backend::AppendError::Conflict); this
    /// is the raw, no-retry primitive that [`command`](EventStore::command)
    /// drives. `expected` for a brand-new stream is [`Version::NoStream`].
    pub async fn append<E: Event>(
        &self,
        stream_id: &str,
        expected: Version,
        events: &[E],
    ) -> Result<Commit, AppendError<StoreError<B::Error>>> {
        let records = encode_events(events).map_err(|e| {
            AppendError::Backend(StoreError::<B::Error>::Codec(e))
        })?;
        match self.backend.append_batch(stream_id, expected, &records).await {
            Ok(appended) => Ok(Commit {
                version:              appended.version,
                last_global_position: (!events.is_empty())
                    .then_some(appended.last_global_position),
                events_appended:      events.len(),
                attempts:             1,
            }),
            Err(AppendError::Conflict { expected, actual }) => {
                Err(AppendError::Conflict { expected, actual })
            }
            Err(AppendError::Backend(e)) => {
                Err(AppendError::Backend(StoreError::Backend(e)))
            }
        }
    }

    /// The north-star call: `load → decide → append(expected version)` with
    /// bounded, jittered optimistic retry.
    ///
    /// On a version conflict the stream moved under us, so we back off
    /// (jittered) and re-run the whole round against the fresh state. When the
    /// [`RetryPolicy`] budget is exhausted this returns the distinct
    /// [`CommandError::Conflict`] variant carrying the attempt count; a
    /// business-rule rejection is [`CommandError::Domain`]; plumbing failures
    /// are [`CommandError::Store`].
    ///
    /// `C: Clone` because `decide` consumes the command and a retry needs it
    /// again.
    pub async fn command<A, C>(
        &self,
        stream_id: &str,
        cmd: C,
    ) -> Result<
        Commit,
        CommandError<<A as Decide<C>>::Rejection, StoreError<B::Error>>,
    >
    where
        A: Aggregate + Decide<C>,
        C: Clone,
    {
        let mut attempt: u32 = 0;
        loop {
            attempt += 1;
            let loaded =
                self.load::<A>(stream_id).await.map_err(CommandError::Store)?;
            let events = loaded
                .state
                .decide(cmd.clone())
                .map_err(CommandError::Domain)?;
            if events.is_empty() {
                return Ok(Commit {
                    version:              loaded.version,
                    last_global_position: None,
                    events_appended:      0,
                    attempts:             attempt,
                });
            }
            let records = encode_events(&events)
                .map_err(|e| CommandError::Store(StoreError::Codec(e)))?;
            match self
                .backend
                .append_batch(stream_id, loaded.version, &records)
                .await
            {
                Ok(appended) => {
                    return Ok(Commit {
                        version:              appended.version,
                        last_global_position: Some(
                            appended.last_global_position,
                        ),
                        events_appended:      events.len(),
                        attempts:             attempt,
                    });
                }
                Err(AppendError::Conflict { .. }) => {
                    if attempt >= self.policy.max_attempts {
                        return Err(CommandError::Conflict {
                            stream:   stream_id.to_string(),
                            attempts: attempt,
                        });
                    }
                    let backoff = self.policy.backoff_for(attempt);
                    if !backoff.is_zero() {
                        tokio::time::sleep(backoff).await;
                    }
                    // Lost the race; reload and re-decide against fresh state.
                }
                Err(AppendError::Backend(e)) => {
                    return Err(CommandError::Store(StoreError::Backend(e)));
                }
            }
        }
    }

    /// Like [`command`](Self::command), but for a command that declares its own
    /// [`Actor`] stream: the store checks the command's declared actor stream
    /// equals `stream_id` **before** loading or deciding, so a self-referential
    /// invariant's echoed actor id can never silently diverge from the stream
    /// it is committed to (see [`Actor`] for the full rationale).
    ///
    /// A divergence is a **caller bug**, not a runtime condition: it trips a
    /// `debug_assert` in debug builds (a loud failure in tests) and, in every
    /// build, returns [`AuthoredCommandError::ActorMismatch`] *before the log
    /// is touched* — never a partial write. A command whose declared stream
    /// matches delegates verbatim to [`command`](Self::command), so the
    /// optimistic retry, the returned [`Commit`], and the error taxonomy are
    /// identical; the only addition is the up-front dispatch check.
    pub async fn command_as<A, C>(
        &self,
        stream_id: &str,
        cmd: C,
    ) -> Result<
        Commit,
        AuthoredCommandError<<A as Decide<C>>::Rejection, StoreError<B::Error>>,
    >
    where
        A: Aggregate + Decide<C>,
        C: Clone + Actor,
    {
        check_actor(&cmd, stream_id)?;
        Ok(self.command::<A, C>(stream_id, cmd).await?)
    }
}

impl<B: SubscribeBackend> EventStore<B> {
    /// The current committed global watermark: the exclusive end of the
    /// committed global-position sequence — every global position `< watermark`
    /// is committed and visible to
    /// [`backend().read_global`](crate::backend::Backend::read_global), and it
    /// is the count of committed events. Monotone non-decreasing while the
    /// store is live.
    pub async fn watermark(&self) -> Result<u64, StoreError<B::Error>> {
        self.backend.watermark().await.map_err(StoreError::Backend)
    }

    /// Await until the committed watermark passes global position `pos` — i.e.
    /// until `pos` is committed and visible to
    /// [`backend().read_global`](crate::backend::Backend::read_global).
    /// Resolves immediately if already past.
    ///
    /// This is the **event-bounded** barrier a read model wants after issuing a
    /// write: pass the write's [`Commit::last_global_position`] and await it to
    /// know the projection can now observe that write — woken by the commit
    /// that crosses it, never by polling. Dropping the future is safe and
    /// never wedges the committer (see [`Subscription`]'s module docs).
    pub async fn await_past(
        &self,
        pos: u64,
    ) -> Result<(), StoreError<B::Error>> {
        self.backend
            .await_watermark_past(pos)
            .await
            .map_err(StoreError::Backend)
    }
}

impl<B: SubscribeBackend + Clone> EventStore<B> {
    /// Open a catch-up → live-tail [`Subscription`] over the global event
    /// stream, starting at global position `from` (`None` starts at 0, the
    /// whole log).
    ///
    /// The returned handle first **replays committed history** from `from` a
    /// page at a time (page size = this store's
    /// [`page_size`](Self::with_page_size)), then switches to a **live tail**
    /// driven by commit notification — the durable watermark, not busy polling.
    /// Pull records in global order with
    /// [`Subscription::next_batch`] / [`Subscription::next`]; delivery is
    /// gap-free and in ascending global position. See [`Subscription`] for the
    /// full delivery, cancellation, and drop contract.
    ///
    /// The subscription holds its own cheap clone of the backend handle, so it
    /// is independent of this `EventStore` and of every other subscriber: a
    /// store supports one writer and many concurrent subscriptions.
    #[must_use]
    pub fn subscribe(&self, from: Option<u64>) -> Subscription<B> {
        Subscription::new(
            self.backend.clone(),
            from.unwrap_or(0),
            self.page_size,
        )
    }
}

impl<B: SnapshotStore> EventStore<B> {
    /// Fold the current stream into aggregate state and persist a snapshot
    /// (state blob + [`SnapshotRef`]) to the interim keyspace.
    ///
    /// The state is built by a full replay ([`load`](Self::load)); the
    /// snapshot's [`fold_version`](SnapshotRef::fold_version) is stamped from
    /// [`Snapshottable::FOLD_VERSION`] so a later deploy that bumps it
    /// invalidates this snapshot. Returns the [`SnapshotRef`] that was stored.
    ///
    /// This is the interim, throwaway store. Correctness of a later
    /// [`load_cached`](Self::load_cached) does **not** rest on this blob being
    /// trustworthy — it rests on the snapshot-equivalence law (see the
    /// `snapshot_law` test); the Phase 5 fold certificate is what will make the
    /// stored blob *verifiable*.
    pub async fn save_snapshot<A: Snapshottable>(
        &self,
        stream_id: &str,
    ) -> Result<SnapshotRef, StoreError<B::Error>> {
        let loaded = self.load::<A>(stream_id).await?;
        self.persist_snapshot::<A>(stream_id, &loaded.state, loaded.version)
            .await
    }

    /// Persist a snapshot for `stream_id` from an **already-folded** `state`
    /// covering `version`, stamping the current
    /// [`Snapshottable::FOLD_VERSION`].
    ///
    /// Factored out of [`save_snapshot`](Self::save_snapshot) so the
    /// invalidation-on-deploy path can *replace* a stale snapshot from the
    /// state it just rebuilt — without paying for a second full replay.
    async fn persist_snapshot<A: Snapshottable>(
        &self,
        stream_id: &str,
        state: &A,
        version: Version,
    ) -> Result<SnapshotRef, StoreError<B::Error>> {
        let state_blob = state.encode_state()?;

        let covers_empty_prefix = version == Version::NoStream;
        let stream_version = version.position().unwrap_or(0);
        let interim_id = interim_stream_id(stream_id);

        let snapshot_ref = SnapshotRef {
            stream_id: interim_id,
            stream_version,
            fold_version: A::FOLD_VERSION,
            covers_empty_prefix,
            // Reserved until the Phase 5 fold-chain machinery lands.
            event_prefix_hash: None,
            state_hash: None,
            snapshot_ptr: BlobPtr(interim_id),
        };

        self.backend
            .save_snapshot(
                stream_id,
                StoredSnapshot {
                    snapshot_ref: snapshot_ref.clone(),
                    state_blob,
                },
            )
            .await
            .map_err(StoreError::Backend)?;

        Ok(snapshot_ref)
    }

    /// Load an aggregate, transparently using `snapshot + tail` when a valid
    /// snapshot exists and falling back to full replay otherwise.
    ///
    /// The result is a [`Loaded<A>`] **byte-identical** to what
    /// [`load`](Self::load) would return — the only difference is *how* the
    /// state was reconstructed. That equivalence is the whole point (and the
    /// acceptance law): user code sees the same `Loaded<A>` shape whether the
    /// engine underneath does full replay today or snapshot-accelerated replay
    /// tomorrow, so `load`'s and `command`'s signatures never change. This is
    /// the interim spelling; when the Phase 4 engine makes snapshots always-on
    /// it folds into `load` itself with no change to this contract.
    ///
    /// A snapshot is used only when it is **valid**:
    /// - its [`fold_version`](SnapshotRef::fold_version) equals
    ///   [`Snapshottable::FOLD_VERSION`] (else it is a stale fold — §9), and
    /// - it does not claim to summarize past the stream head
    ///   (`SnapshotBeyondHead`), and
    /// - its state blob deserializes.
    ///
    /// Any failure of these is silently treated as "no usable snapshot" and the
    /// load degrades to a correct full replay.
    ///
    /// # Invalidation on deploy (§9)
    ///
    /// The one case handled specially is a **stale `fold_version`**: a snapshot
    /// written by a prior binary whose fold this one no longer implements. That
    /// is the deploy story. The stale snapshot is *never used*; the state is
    /// rebuilt by full replay; the invalidation is counted in
    /// [`snapshot_metrics`](Self::snapshot_metrics); and the stale record is
    /// **replaced** with a fresh snapshot stamped with the current
    /// `fold_version`, so a following load of the same stream is fast again and
    /// the invalidation is counted **exactly once** per stale stream. The
    /// replace is best-effort: the rebuilt state is already correct and
    /// returned regardless of whether the write-back succeeds.
    pub async fn load_cached<A: Snapshottable>(
        &self,
        stream_id: &str,
    ) -> Result<Loaded<A>, StoreError<B::Error>> {
        match self.try_load_from_snapshot::<A>(stream_id).await? {
            SnapshotOutcome::Used(loaded) => Ok(loaded),
            // No usable snapshot for a non-deploy reason: full replay is always
            // correct, and we deliberately do NOT write a snapshot back (an
            // ordinary snapshot-less load must not become an implicit save).
            SnapshotOutcome::NoSnapshot => self.load::<A>(stream_id).await,
            // Stale fold: count it, rebuild by full replay, then replace the
            // stale snapshot so the next load is fast and this is counted once.
            SnapshotOutcome::Invalidated => {
                self.metrics.record_invalidation();
                let loaded = self.load::<A>(stream_id).await?;
                // Best-effort replace: a failed write-back never fails a load
                // that already holds the correct state — the snapshot store is
                // throwaway (correctness comes from the log), and a miss just
                // means the next load re-invalidates and retries the replace.
                let _ = self
                    .persist_snapshot::<A>(
                        stream_id,
                        &loaded.state,
                        loaded.version,
                    )
                    .await;
                Ok(loaded)
            }
        }
    }

    /// Consult the snapshot store on the accelerated path. Distinguishes a
    /// deploy-time [`Invalidated`](SnapshotOutcome::Invalidated) snapshot
    /// (stale `fold_version`) from an ordinary
    /// [`NoSnapshot`](SnapshotOutcome::NoSnapshot) miss, so the caller can
    /// count and replace the former. Only a genuine backend error
    /// short-circuits with `Err`.
    async fn try_load_from_snapshot<A: Snapshottable>(
        &self,
        stream_id: &str,
    ) -> Result<SnapshotOutcome<A>, StoreError<B::Error>> {
        let Some(stored) = self
            .backend
            .load_snapshot(stream_id)
            .await
            .map_err(StoreError::Backend)?
        else {
            return Ok(SnapshotOutcome::NoSnapshot);
        };
        let snap = &stored.snapshot_ref;

        // Stale fold: the snapshot summarizes a fold this binary no longer
        // implements (a `fold_version` bump on deploy, or an old record whose
        // version could not be recovered and decoded to a non-matching value —
        // §9). Rebuild by full replay and replace; never surfaced as an error.
        if snap.fold_version != A::FOLD_VERSION {
            return Ok(SnapshotOutcome::Invalidated);
        }

        // Where does the tail resume? An empty-prefix snapshot summarizes
        // nothing, so the whole log is the tail; otherwise resume strictly
        // after the last summarized index.
        let resume_from = if snap.covers_empty_prefix {
            Version::NoStream
        } else {
            // Guard against a snapshot that claims to be past the head
            // (`SnapshotBeyondHead`, §7): fall back rather than trust it.
            let head = self
                .backend
                .head(stream_id)
                .await
                .map_err(StoreError::Backend)?;
            match head.position() {
                Some(head_idx) if snap.stream_version <= head_idx => {
                    Version::At(snap.stream_version)
                }
                _ => return Ok(SnapshotOutcome::NoSnapshot),
            }
        };

        // Deserialize the snapshotted state; a bad blob is another reason to
        // fall back rather than fail.
        let Ok(state) = A::decode_state(&stored.state_blob) else {
            return Ok(SnapshotOutcome::NoSnapshot);
        };

        // Fold the tail on top of the snapshot state.
        let (state, version, tail_len) =
            self.replay_tail::<A>(stream_id, state, resume_from).await?;

        Ok(SnapshotOutcome::Used(Loaded {
            state,
            version,
            events_replayed: tail_len,
        }))
    }

    /// Page through `stream_id` strictly after `from`, folding each event into
    /// `state`. Returns the folded state, the final version, and how many
    /// events were replayed — the same paging loop [`load`](Self::load) uses.
    async fn replay_tail<A: Aggregate>(
        &self,
        stream_id: &str,
        mut state: A,
        from: Version,
    ) -> Result<(A, Version, usize), StoreError<B::Error>> {
        let mut version = from;
        let mut replayed = 0;
        loop {
            let page = self
                .backend
                .read_stream(stream_id, version, self.page_size)
                .await
                .map_err(StoreError::Backend)?;
            let page_len = page.len();
            for rec in &page {
                let event =
                    <A::Event as Event>::decode(&rec.message_type, &rec.data)?;
                state.apply(&event);
                version = Version::At(rec.stream_position);
            }
            replayed += page_len;
            if page_len < self.page_size {
                break;
            }
        }
        Ok((state, version, replayed))
    }

    /// The warm-path north star: `version-check → decide → append` with **no
    /// replay and no fold** when the aggregate is cached, a write-through fold
    /// on success, and O(events-you-lost-the-race-to) **delta catch-up** on a
    /// version conflict.
    ///
    /// Semantically identical to [`command`](Self::command) — same optimistic
    /// retry, same [`Commit`], same error taxonomy. The only difference is
    /// *cost*:
    ///
    /// - **Warm hit:** start from the cached `(version, state)` and go straight
    ///   to `decide → append`. The version is proven by the append itself (the
    ///   check it does anyway), so the warm path reads **zero events**.
    /// - **Success:** the caller already holds the events it wrote, so it folds
    ///   them into the cached state (write-through) — no re-read, no
    ///   invalidation protocol.
    /// - **Conflict:** fetch only the events appended since the cached version
    ///   and fold them in (via [`replay_tail`](Self::replay_tail)), then retry
    ///   — so a lost race costs O(events lost), not a full reload.
    /// - **Miss / cache disabled:** fall through to the snapshot + tail
    ///   [`load_cached`](Self::load_cached) — the *same* code path either way,
    ///   so the cache changes performance, never results.
    ///
    /// `A: Clone` because the write-through fold and the delta catch-up build a
    /// new cached state from the one in hand; `A: Snapshottable` because the
    /// miss path is the snapshot-accelerated load.
    ///
    /// # Backoff under contention
    ///
    /// The delta catch-up makes a conflict retry *cheap* — but that cheapness
    /// is a double-edged sword under genuine multi-writer contention on one
    /// hot stream: cheap retries collide back-to-back, where the uncached
    /// full reload incidentally spaces writers out. So `command_cached`
    /// needs a [`RetryPolicy`](crate::RetryPolicy) with **real jittered
    /// backoff** (the [default](crate::RetryPolicy::default)) under
    /// contention; a `no_backoff` policy is for deterministic
    /// single-threaded tests only and can let a thundering herd starve one
    /// writer into conflict exhaustion. The backoff is still honored on
    /// every conflict here regardless.
    pub async fn command_cached<A, C>(
        &self,
        stream_id: &str,
        cmd: C,
    ) -> Result<
        Commit,
        CommandError<<A as Decide<C>>::Rejection, StoreError<B::Error>>,
    >
    where
        A: Snapshottable + Decide<C> + Clone,
        C: Clone,
    {
        let mut attempt: u32 = 0;
        // Warm hit -> start from cached (version, state), reading nothing.
        // Miss / off -> `None`, filled by the snapshot + tail load below.
        // INVARIANT maintained throughout the loop: `state` is folded to
        // exactly `version`, so a delta catch-up from `version` is correct.
        let mut current: Option<(Version, A)> = self.cache.get::<A>(stream_id);
        loop {
            attempt += 1;
            let (version, state) = match current.take() {
                Some(warm) => warm,
                None => {
                    let loaded = self
                        .load_cached::<A>(stream_id)
                        .await
                        .map_err(CommandError::Store)?;
                    (loaded.version, loaded.state)
                }
            };
            let events =
                state.decide(cmd.clone()).map_err(CommandError::Domain)?;
            if events.is_empty() {
                // Nothing written, so no fresh version proof — but record the
                // state we hold so a following command stays warm.
                self.cache.put::<A>(stream_id, version, state);
                return Ok(Commit {
                    version,
                    last_global_position: None,
                    events_appended: 0,
                    attempts: attempt,
                });
            }
            let records = encode_events(&events)
                .map_err(|e| CommandError::Store(StoreError::Codec(e)))?;
            match self.backend.append_batch(stream_id, version, &records).await
            {
                Ok(appended) => {
                    // Write-through fold: fold the events we just wrote into
                    // the cached state instead of
                    // re-reading them.
                    let mut folded = state;
                    for e in &events {
                        folded.apply(e);
                    }
                    self.cache.put::<A>(stream_id, appended.version, folded);
                    return Ok(Commit {
                        version:              appended.version,
                        last_global_position: Some(
                            appended.last_global_position,
                        ),
                        events_appended:      events.len(),
                        attempts:             attempt,
                    });
                }
                Err(AppendError::Conflict { .. }) => {
                    if attempt >= self.policy.max_attempts {
                        // Give up: our optimistic state lost the race for good;
                        // drop it so the next caller reloads clean.
                        self.cache.invalidate(stream_id);
                        return Err(CommandError::Conflict {
                            stream:   stream_id.to_string(),
                            attempts: attempt,
                        });
                    }
                    let backoff = self.policy.backoff_for(attempt);
                    if !backoff.is_zero() {
                        tokio::time::sleep(backoff).await;
                    }
                    // Delta catch-up: fold ONLY the events appended since our
                    // stale `version` — O(events lost), not a full reload —
                    // then retry against the caught-up
                    // state.
                    let (caught, caught_version, _delta) = self
                        .replay_tail::<A>(stream_id, state, version)
                        .await
                        .map_err(CommandError::Store)?;
                    self.cache.put::<A>(
                        stream_id,
                        caught_version,
                        caught.clone(),
                    );
                    current = Some((caught_version, caught));
                }
                Err(AppendError::Backend(e)) => {
                    return Err(CommandError::Store(StoreError::Backend(e)));
                }
            }
        }
    }

    /// The warm-path twin of [`command_as`](Self::command_as): an authored
    /// command on the cached [`command_cached`](Self::command_cached) path.
    ///
    /// Identical cost model and semantics to
    /// [`command_cached`](Self::command_cached), with the same up-front
    /// actor/stream dispatch check as [`command_as`](Self::command_as): a
    /// divergence trips a `debug_assert` and returns
    /// [`AuthoredCommandError::ActorMismatch`] before any cache lookup, load,
    /// or append.
    pub async fn command_cached_as<A, C>(
        &self,
        stream_id: &str,
        cmd: C,
    ) -> Result<
        Commit,
        AuthoredCommandError<<A as Decide<C>>::Rejection, StoreError<B::Error>>,
    >
    where
        A: Snapshottable + Decide<C> + Clone,
        C: Clone + Actor,
    {
        check_actor(&cmd, stream_id)?;
        Ok(self.command_cached::<A, C>(stream_id, cmd).await?)
    }

    /// The warm-path read: return the hot aggregate for `stream_id`, folding at
    /// most the delta since the cache last saw it.
    ///
    /// - **Warm hit:** one [`head`](Backend::head) check (cheap metadata,
    ///   **zero event reads**). If the stream has not moved, the cached state
    ///   is returned untouched; if it has, only the delta events are folded in
    ///   and the cache is refreshed.
    /// - **Miss / cache disabled:** the snapshot + tail
    ///   [`load_cached`](Self::load_cached), then the result warms the cache —
    ///   the same code path as an uncached [`load_cached`](Self::load_cached).
    ///
    /// [`Loaded::events_replayed`] counts only what *this* call folded: `0` on
    /// a fully-warm hit, the delta length on a catch-up, the tail length on
    /// a miss — so a test can prove the warm read touched no events. The
    /// returned state is always byte-identical to what [`load`](Self::load)
    /// would yield.
    pub async fn load_hot<A: Snapshottable + Clone>(
        &self,
        stream_id: &str,
    ) -> Result<Loaded<A>, StoreError<B::Error>> {
        if let Some((version, state)) = self.cache.get::<A>(stream_id) {
            let head = self
                .backend
                .head(stream_id)
                .await
                .map_err(StoreError::Backend)?;
            if head == version {
                // Fully warm: nothing new to fold, no events read.
                return Ok(Loaded { state, version, events_replayed: 0 });
            }
            // Behind: fold only the delta since the cached version.
            let (caught, caught_version, delta) =
                self.replay_tail::<A>(stream_id, state, version).await?;
            self.cache.put::<A>(stream_id, caught_version, caught.clone());
            return Ok(Loaded {
                state:           caught,
                version:         caught_version,
                events_replayed: delta,
            });
        }
        // Miss / disabled: snapshot + tail, then warm the cache.
        let loaded = self.load_cached::<A>(stream_id).await?;
        self.cache.put::<A>(stream_id, loaded.version, loaded.state.clone());
        Ok(loaded)
    }
}

/// Enforce the [`Actor`] dispatch precondition: the stream a command declares
/// it is authored against must equal the stream it is being dispatched to.
///
/// A mismatch is a caller bug, so it trips a `debug_assert` (loud in tests /
/// debug builds) and, in every build, is returned as
/// [`AuthoredCommandError::ActorMismatch`] so a release binary refuses the
/// write instead of committing an event to the wrong stream. Shared by
/// [`EventStore::command_as`] and [`EventStore::command_cached_as`].
fn check_actor<C: Actor, R, S>(
    cmd: &C,
    stream_id: &str,
) -> Result<(), AuthoredCommandError<R, S>> {
    let declared = cmd.actor_stream();
    debug_assert_eq!(
        declared.as_str(),
        stream_id,
        "actor/stream divergence: command authored against {declared:?} but \
         dispatched to stream {stream_id:?}"
    );
    if declared == stream_id {
        Ok(())
    } else {
        Err(AuthoredCommandError::ActorMismatch {
            declared,
            stream: stream_id.to_string(),
        })
    }
}

/// Encode a slice of events into backend append records.
fn encode_events<E: Event>(
    events: &[E],
) -> Result<Vec<RecordToAppend>, CodecError> {
    events
        .iter()
        .map(|e| {
            Ok(RecordToAppend {
                message_type: e.name().to_string(),
                data:         e.encode()?,
            })
        })
        .collect()
}
