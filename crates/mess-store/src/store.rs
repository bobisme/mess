//! [`EventStore`]: the DX-first facade — `load` / `append` / `command` with
//! bounded, jittered optimistic retry.

use mess_core::{Aggregate, CodecError, CommandError, Decide, Event};

use crate::backend::{AppendError, Backend, RecordToAppend};
use crate::cache::StateCache;
use crate::retry::RetryPolicy;
use crate::snapshot::{
    BlobPtr, SnapshotRef, SnapshotStore, Snapshottable, StateCodecError,
    StoredSnapshot, interim_stream_id,
};
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

impl<E> From<CodecError> for StoreError<E> {
    fn from(e: CodecError) -> Self {
        StoreError::Codec(e)
    }
}

impl<E> From<StateCodecError> for StoreError<E> {
    fn from(e: StateCodecError) -> Self {
        StoreError::State(e)
    }
}

/// Result of replaying a stream through [`Aggregate::apply`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Loaded<A> {
    /// The folded aggregate state.
    pub state: A,
    /// The version the stream was at when loaded; feed this straight back to
    /// [`append`](EventStore::append) as the expected version.
    pub version: Version,
    /// How many events were replayed to build `state`.
    pub events_replayed: usize,
}

/// Result of a successful [`append`](EventStore::append) or
/// [`command`](EventStore::command).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Commit {
    /// The stream's version after the commit.
    pub version: Version,
    /// Global position of the last event written, or `None` when nothing was
    /// appended (e.g. a command that decided zero events).
    pub last_global_position: Option<u64>,
    /// How many events were appended.
    pub events_appended: usize,
    /// How many optimistic attempts [`command`](EventStore::command) needed
    /// (always 1 for a direct [`append`](EventStore::append)).
    pub attempts: u32,
}

/// The event-store facade over any [`Backend`].
///
/// Cloning is cheap when the backend is cheap to clone (e.g. an `Arc`-backed
/// handle); each clone shares the same backend, retry configuration, and — when
/// enabled — the same hot-aggregate [`StateCache`] (its entries live behind an
/// `Arc`), so warm state is shared across clones and across concurrent writers.
#[derive(Debug, Clone)]
pub struct EventStore<B> {
    backend: B,
    policy: RetryPolicy,
    page_size: usize,
    /// Hot-aggregate state cache (doc-02). Disabled by default, so the base
    /// [`load`](Self::load)/[`command`](Self::command) behavior — and every
    /// existing test — is unchanged until a caller opts in with
    /// [`with_cache_capacity`](Self::with_cache_capacity).
    cache: StateCache,
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
        }
    }

    /// Enable the hot-aggregate [`StateCache`] with room for `capacity` streams.
    ///
    /// This is the on-switch for the warm command/read path
    /// ([`command_cached`](Self::command_cached) / [`load_hot`](Self::load_hot)).
    /// Off (the default) those methods still work — they simply run the
    /// cache-miss fallthrough on every call, which is the *identical* code path,
    /// so the cache changes performance, never results.
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
    pub fn cache(&self) -> &StateCache {
        &self.cache
    }

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
    pub fn retry_policy(&self) -> RetryPolicy {
        self.policy
    }

    /// Borrow the underlying backend.
    pub fn backend(&self) -> &B {
        &self.backend
    }

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
                version: appended.version,
                last_global_position: (!events.is_empty())
                    .then_some(appended.last_global_position),
                events_appended: events.len(),
                attempts: 1,
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
                    version: loaded.version,
                    last_global_position: None,
                    events_appended: 0,
                    attempts: attempt,
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
                        version: appended.version,
                        last_global_position: Some(
                            appended.last_global_position,
                        ),
                        events_appended: events.len(),
                        attempts: attempt,
                    });
                }
                Err(AppendError::Conflict { .. }) => {
                    if attempt >= self.policy.max_attempts {
                        return Err(CommandError::Conflict {
                            stream: stream_id.to_string(),
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
        let state_blob = loaded.state.encode_state()?;

        let covers_empty_prefix = loaded.version == Version::NoStream;
        let stream_version = loaded.version.position().unwrap_or(0);
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
    pub async fn load_cached<A: Snapshottable>(
        &self,
        stream_id: &str,
    ) -> Result<Loaded<A>, StoreError<B::Error>> {
        if let Some(loaded) =
            self.try_load_from_snapshot::<A>(stream_id).await?
        {
            return Ok(loaded);
        }
        // No usable snapshot: full replay is always correct.
        self.load::<A>(stream_id).await
    }

    /// Attempt the snapshot-accelerated path; `Ok(None)` means "no usable
    /// snapshot, fall back to full replay". Only a genuine backend error
    /// short-circuits with `Err`.
    async fn try_load_from_snapshot<A: Snapshottable>(
        &self,
        stream_id: &str,
    ) -> Result<Option<Loaded<A>>, StoreError<B::Error>> {
        let Some(stored) = self
            .backend
            .load_snapshot(stream_id)
            .await
            .map_err(StoreError::Backend)?
        else {
            return Ok(None);
        };
        let snap = &stored.snapshot_ref;

        // Stale fold: the snapshot summarizes a fold this binary no longer
        // implements. Invalidate and rebuild by full replay (§9).
        if snap.fold_version != A::FOLD_VERSION {
            return Ok(None);
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
                _ => return Ok(None),
            }
        };

        // Deserialize the snapshotted state; a bad blob is another reason to
        // fall back rather than fail.
        let Ok(state) = A::decode_state(&stored.state_blob) else {
            return Ok(None);
        };

        // Fold the tail on top of the snapshot state.
        let (state, version, tail_len) =
            self.replay_tail::<A>(stream_id, state, resume_from).await?;

        Ok(Some(Loaded { state, version, events_replayed: tail_len }))
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
    ///   and fold them in (via [`replay_tail`](Self::replay_tail)), then retry —
    ///   so a lost race costs O(events lost), not a full reload.
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
    /// The delta catch-up makes a conflict retry *cheap* — but that cheapness is
    /// a double-edged sword under genuine multi-writer contention on one hot
    /// stream: cheap retries collide back-to-back, where the uncached full
    /// reload incidentally spaces writers out. So `command_cached` needs a
    /// [`RetryPolicy`](crate::RetryPolicy) with **real jittered backoff** (the
    /// [default](crate::RetryPolicy::default)) under contention; a
    /// `no_backoff` policy is for deterministic single-threaded tests only and
    /// can let a thundering herd starve one writer into conflict exhaustion.
    /// The backoff is still honored on every conflict here regardless.
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
                    // Write-through fold: fold the events we just wrote into the
                    // cached state instead of re-reading them.
                    let mut folded = state;
                    for e in &events {
                        folded.apply(e);
                    }
                    self.cache.put::<A>(stream_id, appended.version, folded);
                    return Ok(Commit {
                        version: appended.version,
                        last_global_position: Some(
                            appended.last_global_position,
                        ),
                        events_appended: events.len(),
                        attempts: attempt,
                    });
                }
                Err(AppendError::Conflict { .. }) => {
                    if attempt >= self.policy.max_attempts {
                        // Give up: our optimistic state lost the race for good;
                        // drop it so the next caller reloads clean.
                        self.cache.invalidate(stream_id);
                        return Err(CommandError::Conflict {
                            stream: stream_id.to_string(),
                            attempts: attempt,
                        });
                    }
                    let backoff = self.policy.backoff_for(attempt);
                    if !backoff.is_zero() {
                        tokio::time::sleep(backoff).await;
                    }
                    // Delta catch-up: fold ONLY the events appended since our
                    // stale `version` — O(events lost), not a full reload — then
                    // retry against the caught-up state.
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

    /// The warm-path read: return the hot aggregate for `stream_id`, folding at
    /// most the delta since the cache last saw it.
    ///
    /// - **Warm hit:** one [`head`](Backend::head) check (cheap metadata,
    ///   **zero event reads**). If the stream has not moved, the cached state is
    ///   returned untouched; if it has, only the delta events are folded in and
    ///   the cache is refreshed.
    /// - **Miss / cache disabled:** the snapshot + tail
    ///   [`load_cached`](Self::load_cached), then the result warms the cache —
    ///   the same code path as an uncached [`load_cached`](Self::load_cached).
    ///
    /// [`Loaded::events_replayed`] counts only what *this* call folded: `0` on a
    /// fully-warm hit, the delta length on a catch-up, the tail length on a
    /// miss — so a test can prove the warm read touched no events. The returned
    /// state is always byte-identical to what [`load`](Self::load) would yield.
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
                state: caught,
                version: caught_version,
                events_replayed: delta,
            });
        }
        // Miss / disabled: snapshot + tail, then warm the cache.
        let loaded = self.load_cached::<A>(stream_id).await?;
        self.cache.put::<A>(stream_id, loaded.version, loaded.state.clone());
        Ok(loaded)
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
                data: e.encode()?,
            })
        })
        .collect()
}
