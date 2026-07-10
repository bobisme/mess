//! The composed production [`Backend`]: `mess-log` (durability, ordering,
//! recovery) + `mess-index` (hot [`ActiveIndex`], sealed corpus, fjall meta
//! tables) behind the [`EventStore`](crate::EventStore) facade.
//!
//! This is the Phase 4 "engine swap": the facade's default backend moves off
//! the interim in-memory [`MockBackend`](crate::mock) onto the real log +
//! index stack, with **zero** changes to the [`Backend`] trait or the facade
//! above it.
//!
//! # How the pieces compose
//!
//! - **Durability spine** — every non-empty
//!   [`append_batch`](Backend::append_batch) is a single-stream batch handed to
//!   a `mess-log` [`Committer`] running on a `mess-log` [`RealRuntime`]. The
//!   committer assigns dense, event-counted global positions (A1), does group
//!   commit under the configured [`Durability`], and advances the durable
//!   watermark. Because the [`Backend`] trait is async (tokio) and the
//!   committer is driven by `mess-log`'s own `block_on`/OS-thread runtime, the
//!   append crosses the seam via
//!   [`spawn_blocking`](tokio::task::spawn_blocking) — the minimal adapter
//!   between the two executors.
//! - **Exact-version gate** — a per-stream sharded async mutex ([`AppendGate`],
//!   bn-1s0) serialises the check-head → append → apply critical section **per
//!   stream**, so two writers that loaded the same [`Version`] on the *same*
//!   stream genuinely race and exactly one wins with a
//!   [`AppendError::Conflict`], while writers on *different* streams no longer
//!   queue behind one store-wide lock. (The committer still serialises the
//!   durable write itself across all streams; the gate's job is only to make
//!   the version check atomic with the append, per stream.)
//! - **Hot reads** — committed batches are applied to a `mess-index`
//!   [`ActiveIndex`] via `apply_committed`, and their payloads are written into
//!   the record book, **only after the committer acks** the durable append
//!   (post-ack discipline): the book and index therefore never expose a
//!   position the durable watermark has not already covered. (The committer's
//!   own watermark is what gates durability; the engine does not re-derive it —
//!   it simply publishes to the book strictly after the ack.) `read_stream`
//!   enumerates a stream's positions through the active index (unsealed tier)
//!   unioned with the sealed tier.
//! - **Cold reads** — sealed segments are served through
//!   [`ReplaySet`](mess_index::sealed::ReplaySet) over a
//!   [`SealedStore`](mess_index::sealed::SealedStore), so an
//!   [`EventStore::load`](crate::EventStore::load) of a sealed corpus runs the
//!   real sealed-replay path.
//! - **Meta** — durable stream heads and the dedupe window live in the
//!   `mess-index` fjall [`MetaStore`].
//! - **Recovery** — on open the engine enumerates segment files and runs
//!   `mess-log` `recover_whole_log` (fast path + advisory manifest) and
//!   `mess-index` `rebuild` (F6) to rehydrate the active index.
//!
//! # Payload materialisation and its durable rehydration (bn-20b)
//!
//! The [`Backend`] seam must return [`StoredRecord`]s carrying
//! `message_type: String` and `data: Vec<u8>`, but the index tier is
//! pointer-only. So the engine keeps an in-process **record book**: the
//! authoritative `(stream name, message type, payload)` for each committed
//! global position, plus the `&str → u64` / `String → u32` interners the
//! numeric log/index tier needs. Reads resolve *positions* through the real
//! index / sealed tiers and then fetch *bytes* from the book.
//!
//! The book is `Arc`-shared across [`LogEngine`] clones (matching
//! [`MockBackend`]'s reuse-the-handle semantics), but a `clone()` is **not** a
//! crash: the durable authority is the log on disk. On a genuine fresh
//! [`LogEngine::open`] over a populated directory,
//! [`recover`](LogEngine::recover) **rehydrates the book from the durable
//! log**:
//!
//! - **Payload bytes** come back through `mess-log`'s read-side materialization
//!   seam — [`scanner::recover_segment_with_image`] + [`AcceptedBatch::frames`]
//!   (the additive payload-decode API this bone added to `mess-log`): each
//!   recovered batch yields its events' `(event_type_id, payload)` straight out
//!   of the durable segment image.
//! - **Names** cannot be re-derived from the log — it stores only interned
//!   numeric ids (`stream_id u64`, `event_type_id u32`), never their strings.
//!   The interner's `id → name` bijection is therefore persisted durably in the
//!   `mess-index` [`MetaStore`]'s `stream_names` / `type_names` tables (written
//!   the first time a name is interned, in
//!   [`append_batch`](LogEngine::append_batch), via
//!   [`persist_new_names`](LogEngine::persist_new_names)) and reloaded here to
//!   reconstruct the interner before the payloads are materialised. This is the
//!   smallest durable surface for the engine's lightweight interner — the role
//!   `$registry` plays in the full design (not implemented here: frames carry
//!   no name payload to derive it from). A new name is `fsync`ed **before** the
//!   covering append can become durable (bn-150), so recovery can never observe
//!   a durable event whose name is missing — see
//!   [`persist_new_names`](LogEngine::persist_new_names)'s doc for why that
//!   co-durability barrier is needed even though every other meta table is a
//!   lag-tolerant derived cache.
//!
//! After rehydration the book is dense from global position 0 again, so
//! post-reopen appends preserve the dense-position invariant and reads return
//! the exact pre-crash data. The single active segment is *resumed in place*
//! ([`SegmentWriter::resume`]) at the recovered `safe_offset`, so the durable
//! log continues to grow one contiguous prefix across any number of reopens.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::mpsc;
use std::sync::{Condvar, Mutex, OnceLock};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use mess_index::meta::{CommitGroup, Head, MetaStore, StreamId};
use mess_index::sealed::{
    BlockCache, ReplaySet, SealBatch, SealDriver, SealInput, SealMetrics,
    SealStream, SealedSegmentIndex, SealedStore,
};
use mess_index::{ActiveIndex, BatchEntry, EventPtr, IndexSnapshot};
use mess_log::committer::{
    AppendOutcome, AppendRequest, Appender, ChainInit, Committer, Durability,
    EventInput, LatencySnapshot, Roller,
};
use mess_log::fold_chain::ChainHead;
use mess_log::lock::StoreLock;
use mess_log::runtime::{RealRuntime, Runtime};
use mess_log::scanner::{self, AcceptedBatch};
use mess_log::sealer::{TrailerFields, encode_trailer};
use mess_log::watermark::Watermark;
use mess_log::writer::{
    ResumeParams, SegmentParams, SegmentSummary, SegmentWriter,
};

use crate::backend::{
    AppendError, Appended, Backend, RecordToAppend, StoredRecord,
    SubscribeBackend,
};
use crate::version::Version;

/// Category id stamped on every batch. The facade does not model categories,
/// so a single constant is correct here.
const CATEGORY_ID: u64 = 0;
/// Segment file name for the (single) active segment.
const ACTIVE_SEGMENT_ID: u64 = 1;

/// Anything that can go wrong inside the composed engine.
///
/// Payloads are stringified (rather than wrapping the source error types) so
/// the engine error is `Clone + PartialEq + Eq` — the interim `MockBackend`'s
/// `Infallible` error is, and the Phase 1/2 suites compare
/// `RegistryError<Self::Error>` values for equality.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum EngineError {
    /// Opening the store directory or acquiring the single-writer lock failed.
    #[error("open: {0}")]
    Open(String),
    /// A durable append could not be committed by `mess-log`.
    #[error("append: {0}")]
    Append(String),
    /// The `mess-index` fjall metadata tables failed.
    #[error("meta: {0}")]
    Meta(String),
    /// A sealed-corpus read failed to decode.
    #[error("sealed read: {0}")]
    SealedRead(String),
}

/// One committed event's authoritative payload, keyed by global position in
/// the [`Book`].
#[derive(Clone)]
struct Payload {
    stream_name:     Arc<str>,
    message_type:    Arc<str>,
    data:            Arc<[u8]>,
    stream_position: u64,
}

/// The in-process record book + interners (see the module docs). All engine
/// clones share one `Arc<Mutex<Book>>`.
#[derive(Default)]
struct Book {
    /// `stream name → interned id` (ids start at 1; 0 is unused).
    stream_ids:    HashMap<String, u64>,
    /// `interned id → stream name`, index `id - 1`.
    stream_names:  Vec<Arc<str>>,
    /// `message type → interned event-type id` (ids start at 1).
    type_ids:      HashMap<String, u32>,
    /// `interned event-type id → message type`, index `id - 1` (the reverse of
    /// `type_ids`, kept in lockstep so recovery can resolve a frame's
    /// `event_type_id` back to its name).
    type_names:    Vec<Arc<str>>,
    /// Payloads by dense global position.
    payloads:      Vec<Payload>,
    /// `stream id → global positions in stream order` (the hot-tier read
    /// index, kept in lockstep with `ActiveIndex::apply_committed`; index
    /// into it is the stream position). Serves O(limit) paged reads for
    /// unsealed streams.
    stream_events: HashMap<u64, Vec<u64>>,
    /// `stream id → last stream position` (the head).
    heads:         HashMap<u64, u64>,
}

impl Book {
    /// Intern `name`, returning `(id, is_new)`. `is_new` is `true` only the
    /// first time this name is seen — the signal to persist the `id → name`
    /// mapping durably (so a reopen can reconstruct it).
    fn intern_stream(&mut self, name: &str) -> (u64, bool) {
        if let Some(&id) = self.stream_ids.get(name) {
            return (id, false);
        }
        let id = self.stream_names.len() as u64 + 1;
        let arc: Arc<str> = Arc::from(name);
        self.stream_names.push(arc);
        self.stream_ids.insert(name.to_string(), id);
        (id, true)
    }

    fn stream_name(&self, id: u64) -> Arc<str> {
        self.stream_names[(id - 1) as usize].clone()
    }

    /// Intern `name`, returning `(id, is_new)` (see [`intern_stream`]).
    fn intern_type(&mut self, name: &str) -> (u32, bool) {
        if let Some(&id) = self.type_ids.get(name) {
            return (id, false);
        }
        let id = self.type_names.len() as u32 + 1;
        let arc: Arc<str> = Arc::from(name);
        self.type_names.push(arc);
        self.type_ids.insert(name.to_string(), id);
        (id, true)
    }

    /// Rebuild the interner (both directions) from the persisted `id → name`
    /// tables. `pairs` need not be sorted; ids MUST be dense `1..=N` (the
    /// interner only ever assigns dense ids and never removes), which keeps the
    /// next assignment monotonic after reload.
    fn load_stream_names(&mut self, mut pairs: Vec<(u64, String)>) {
        pairs.sort_by_key(|(id, _)| *id);
        for (id, name) in pairs {
            debug_assert_eq!(
                id,
                self.stream_names.len() as u64 + 1,
                "dense stream ids"
            );
            self.stream_names.push(Arc::from(name.as_str()));
            self.stream_ids.insert(name, id);
        }
    }

    fn load_type_names(&mut self, mut pairs: Vec<(u32, String)>) {
        pairs.sort_by_key(|(id, _)| *id);
        for (id, name) in pairs {
            debug_assert_eq!(
                id,
                self.type_names.len() as u32 + 1,
                "dense type ids"
            );
            self.type_names.push(Arc::from(name.as_str()));
            self.type_ids.insert(name, id);
        }
    }

    fn head(&self, stream_id: u64) -> Version {
        match self.heads.get(&stream_id) {
            Some(&pos) => Version::At(pos),
            None => Version::NoStream,
        }
    }

    /// Resolve a stream id to its name, or `None` if the interner has no such
    /// id (a durable-name gap — surfaced as a typed error, never a panic).
    fn stream_name_opt(&self, id: u64) -> Option<Arc<str>> {
        self.stream_names.get((id - 1) as usize).cloned()
    }

    /// Resolve an event-type id to its name, or `None` (see
    /// [`stream_name_opt`]).
    fn type_name_opt(&self, id: u32) -> Option<Arc<str>> {
        self.type_names.get((id - 1) as usize).cloned()
    }

    fn record(&self, global_position: u64) -> StoredRecord {
        let p = &self.payloads[global_position as usize];
        StoredRecord {
            stream_id: p.stream_name.to_string(),
            message_type: p.message_type.to_string(),
            data: p.data.to_vec(),
            stream_position: p.stream_position,
            global_position,
        }
    }
}

/// Number of shards in the per-stream append gate (bn-1s0). A fixed-size
/// array — it never grows, so a store is never on the hook for one lock per
/// stream it has ever seen; only for whether two streams alias onto the same
/// shard, which costs extra serialisation, never correctness.
const APPEND_GATE_SHARDS: usize = 256;

/// Per-stream exact-version gate (bn-1s0 — replaces the store-wide mutex).
///
/// Serialises the check-head → reserve critical section **per stream**, so
/// appends to different streams no longer queue behind one lock while an
/// `Exact(v)` race on the *same* stream still resolves to exactly one
/// winner.
///
/// Design: a fixed array of `APPEND_GATE_SHARDS` async mutexes, indexed by
/// `stream_id % APPEND_GATE_SHARDS`, chosen over a keyed map (e.g. a dashmap
/// of `Arc<Mutex<()>>` per stream id) for two reasons:
/// - **No unbounded growth.** A store that has ever seen a million distinct
///   streams still costs exactly `APPEND_GATE_SHARDS` mutexes — a keyed map
///   would need its own eviction/GC policy (or leak one entry per stream
///   forever) to avoid the same hazard.
/// - **No hashing needed.** Stream ids are dense `u64`s minted by the book's
///   interner (1, 2, 3, …), so `% N` already spreads consecutive ids
///   round-robin across shards.
///
/// Two distinct streams that alias onto the same shard serialise against
/// each other unnecessarily — a bounded throughput cost, never a
/// correctness hazard: the version check inside the shard still reads the
/// true per-stream head from the book.
///
/// Each shard is an `Arc<Mutex>` (not a bare `Mutex`) so the guard can be
/// handed out as an [`OwnedMutexGuard`](tokio::sync::OwnedMutexGuard) — a
/// `'static` guard that [`append_batch`](LogEngine::append_batch) moves into
/// its non-cancellable commit+publish blocking task (bn-3nz). Holding the
/// shard across the publish is what keeps the check-head → append → publish
/// section atomic per stream; carrying an OWNED guard (rather than a
/// borrowed `MutexGuard` held on the async future) means a cancelled append
/// future can no longer release the shard early and let a concurrent
/// same-stream append double-write the same stream version while the
/// cancelled append's committed batch is still publishing.
struct AppendGate {
    shards: [Arc<tokio::sync::Mutex<()>>; APPEND_GATE_SHARDS],
}

impl AppendGate {
    fn new() -> Self {
        AppendGate {
            shards: std::array::from_fn(|_| {
                Arc::new(tokio::sync::Mutex::new(()))
            }),
        }
    }

    /// Acquire the shard guarding `stream_id`'s check-and-reserve section,
    /// as an owned guard that can be moved into the commit+publish blocking
    /// task and released only once the publish completes (bn-3nz).
    async fn lock_for(
        &self,
        stream_id: u64,
    ) -> tokio::sync::OwnedMutexGuard<()> {
        let idx = (stream_id as usize) % APPEND_GATE_SHARDS;
        Arc::clone(&self.shards[idx]).lock_owned().await
    }
}

/// Orders the post-ack publish step (record book + active index + meta head)
/// across concurrently-committing streams (bn-1s0), and — since bn-3nz — is
/// waited on **from the committer-side blocking task**, not the async
/// caller's future, so a cancelled append can never strand an assigned
/// position (see below).
///
/// The durable committer assigns each accepted batch a dense, globally
/// unique position range, but once distinct streams can have appends in
/// flight at the same time — the whole point of the per-stream
/// [`AppendGate`] — their `spawn_blocking` acks can resolve in ANY order,
/// not necessarily the order the committer assigned positions in.
/// `Book::payloads` requires strictly increasing-by-position pushes (dense
/// rehydration), and `ActiveIndex::apply_committed` / `MetaStore::apply_group`
/// carry their own out-of-order asserts — so every publish must wait its turn
/// here before touching any of them.
///
/// # Why this is a *blocking* primitive (bn-3nz)
///
/// The append's position is assigned by the durable committer *inside* the
/// `spawn_blocking` task in [`append_batch`](LogEngine::append_batch). A
/// `spawn_blocking` task is **never cancelled** — it always runs to
/// completion even if its `JoinHandle` (the caller's `.await`) is dropped.
/// [`append_batch`](LogEngine::append_batch) therefore also takes its
/// [`turn`](PublishSequencer::turn) and performs the whole book/index/meta
/// publish *within that same non-cancellable task*, so the assign→publish
/// sequence is atomic against API-future cancellation: a dropped append future
/// can no longer commit a position durably and then skip publishing it, which
/// would have left [`turn`](PublishSequencer::turn)'s strict `== next` wait
/// stalling every higher-positioned publish forever. Because the wait now
/// happens on a blocking thread (not a tokio task), it is a plain
/// [`Condvar`], not a `tokio::sync::watch`.
///
/// This only ever guards the in-memory publish step (a handful of
/// `Vec`/`HashMap` writes) — never the slow durable write itself, which the
/// committer already serialises regardless. So it does not reintroduce the
/// store-wide throughput ceiling bn-1s0 removes; it just re-serialises a
/// microseconds-long tail, in position order instead of ack-arrival order.
struct PublishSequencer {
    /// The global position a publish must match to go next, behind a
    /// [`Condvar`]. Waiters block until it equals their `first_global`; the
    /// publisher advances it via [`PublishTurn`]'s `Drop`.
    next:     Mutex<u64>,
    advanced: Condvar,
}

impl PublishSequencer {
    /// `start` is the first position a publish is allowed to claim — the
    /// book's recovered dense length on open (0 for a fresh store).
    fn new_at(start: u64) -> Self {
        PublishSequencer {
            next:     Mutex::new(start),
            advanced: Condvar::new(),
        }
    }

    /// Block until `first_global` is next in line, then hold the turn: the
    /// returned guard advances the sequence to `watermark` when dropped —
    /// on ANY exit path (success, error, or unwind), since the durable
    /// committer has already permanently assigned this position range
    /// regardless of whether the local book/index/meta publish fully
    /// succeeds. Failing to advance on an error path would deadlock every
    /// higher-positioned publish behind this one forever.
    ///
    /// Called on the [`append_batch`](LogEngine::append_batch) blocking task
    /// (a `spawn_blocking` thread), never on an async executor thread — the
    /// wait is a real blocking [`Condvar`] wait.
    fn turn(&self, first_global: u64, watermark: u64) -> PublishTurn<'_> {
        let mut next = self.next.lock().expect("publish sequencer poisoned");
        while *next != first_global {
            next =
                self.advanced.wait(next).expect("publish sequencer poisoned");
        }
        PublishTurn { seq: self, watermark }
    }
}

/// RAII hold on [`PublishSequencer`]'s turn; see [`PublishSequencer::turn`].
struct PublishTurn<'a> {
    seq:       &'a PublishSequencer,
    watermark: u64,
}

impl Drop for PublishTurn<'_> {
    fn drop(&mut self) {
        {
            let mut next =
                self.seq.next.lock().expect("publish sequencer poisoned");
            *next = self.watermark;
        }
        // Wake every waiter: exactly one has the matching `first_global`, the
        // rest re-check and block again. The waiter set is at most the number
        // of appends in flight, so this is cheap.
        self.seq.advanced.notify_all();
    }
}

/// How [`LogEngine::recover`] resolved the active segment: continue an existing
/// one in place, or start fresh.
enum ResumePlan {
    /// No committed segment on disk (or an unheaderable one): create a fresh
    /// active segment at global position 0, epoch 1.
    Fresh,
    /// An existing active segment with a committed prefix: resume appending at
    /// its recovered `safe_offset`, continuing the A1/`batch_id` chain.
    Resume(ResumeInfo),
}

/// The recovered resume state threaded from [`LogEngine::recover`] into
/// [`SegmentWriter::resume`].
struct ResumeInfo {
    segment_id:    u64,
    base_pos:      u64,
    epoch:         u64,
    write_off:     u64,
    next_batch_id: u64,
    next_pos:      u64,
    batch_count:   u64,
    event_count:   u64,
}

/// Shared engine state behind one `Arc`.
struct Inner {
    rt:                   RealRuntime,
    appender:             Appender,
    /// Kept alive so the commit thread lives as long as the engine; also the
    /// owner we `shutdown` on a clean close. `Option` so [`Inner::drop`] can
    /// drop it *first* — joining the committer task, which drops its
    /// [`Roller`] and so closes the roll channel — before joining the seal
    /// thread (`bn-1vu`).
    committer:            Option<Committer<RealRuntime>>,
    /// The background auto-roll sealer thread (`bn-1vu`): receives each rolled
    /// segment's [`SegmentSummary`] over the committer's roll channel and
    /// builds its sidecars + finalizes its footer off the append path.
    /// `Option` so [`Inner::drop`] can join it after the roll channel
    /// closes, draining every queued seal so it is durable before the
    /// engine handle goes away.
    seal_thread:          Option<JoinHandle<()>>,
    /// Held for the engine's lifetime: D9 single-writer-process enforcement.
    _lock:                StoreLock,
    active:               Arc<ActiveIndex>,
    sealed:               Arc<SealedStore>,
    block_cache:          BlockCache,
    /// Shared seal-path metrics (`bn-e2y`): seal-barrier `fsync` latency +
    /// degradation alarm and seal durations, aggregated across the background
    /// roll-sealer and any on-demand [`LogEngine::seal_active`].
    seal_metrics:         Arc<SealMetrics>,
    meta:                 MetaStore,
    book:                 Arc<Mutex<Book>>,
    /// The **published** global watermark — the exclusive end of the readable
    /// global-position sequence (the dense record-book length). Advanced at
    /// the end of each append's publish step (after the book/index/meta
    /// are updated, in publish-turn order), so it tracks what
    /// [`read_global`](Backend::read_global) can serve, NOT merely what the
    /// durable committer has acked. The app-facing subscription / live-tail
    /// API ([`SubscribeBackend`](crate::backend::SubscribeBackend)) awaits
    /// this value; a woken subscriber is therefore guaranteed the position
    /// it waited for is already materialised in the book. Distinct from
    /// the committer's own durable watermark ([`Appender::watermark`]),
    /// which advances a step earlier (at ack, before the in-process
    /// publish).
    read_watermark:       Watermark,
    /// Serialises the exact-version critical section, per stream (bn-1s0).
    append_gate:          AppendGate,
    /// Orders the post-ack book/index/meta publish step by global position
    /// across concurrently-committing streams (bn-1s0).
    publish_seq:          PublishSequencer,
    /// When this engine opened — the in-process baseline for the active
    /// segment's age (bn-e2y). Recovery resumes the active segment in place,
    /// so there is no durable per-segment start timestamp to read here;
    /// this is the age of the live head *since this process opened it*.
    opened_at:            Instant,
    /// The store root (sealed sidecars live under `dir/sealed`).
    dir:                  PathBuf,
    /// Shared with the seal thread's [`LogEngine::run_roll_sealer`] loop
    /// (`bn-u6o`): unset during live operation, so a queued roll's per-segment
    /// wait uses its normal ~10s bound. [`Inner::drop`] sets this ONCE — to
    /// `now + shutdown_seal_budget` — before joining the seal thread, so it
    /// becomes a single deadline shared by every seal still queued at
    /// shutdown, bounding the TOTAL drain wait instead of ~10s per
    /// abandoned roll.
    shutdown_deadline:    Arc<OnceLock<Instant>>,
    /// The shutdown drain budget `Inner::drop` writes into
    /// [`shutdown_deadline`](Self::shutdown_deadline) (`bn-u6o`,
    /// [`EngineOptions::shutdown_seal_budget`]).
    shutdown_seal_budget: Duration,
}

impl Drop for Inner {
    fn drop(&mut self) {
        // Order is load-bearing (`bn-1vu`). 1) Drop the committer: its `Drop`
        // shuts down and joins the commit task, which drops the `Roller` and so
        // closes the roll channel. 2) Join the seal thread: with the channel
        // closed it drains every queued roll seal (making each sidecar + footer
        // durable) and exits — so a drop-then-reopen sees the finished seals on
        // disk, never a half-written tier.
        drop(self.committer.take());
        // `bn-u6o`: publish the shared shutdown deadline BEFORE joining, so it
        // is visible to a wait already in flight and to every seal still
        // queued behind the now-closed channel. One deadline covers the whole
        // drain — a drop with several abandoned rolls queued still returns
        // within `shutdown_seal_budget` total, not that budget times the queue
        // depth. A skipped seal at shutdown is safe: the segment stays durable
        // and unsealed, served from the log (and re-sealable) on reopen — see
        // `run_roll_sealer`'s doc.
        let _ = self
            .shutdown_deadline
            .set(Instant::now() + self.shutdown_seal_budget);
        if let Some(h) = self.seal_thread.take() {
            let _ = h.join();
        }
    }
}

/// Poll cadence + per-segment wait bound for [`LogEngine::run_roll_sealer`]'s
/// bounded wait (`bn-u6o`). Production always uses [`SpinConfig::default`]
/// (matching bn-1vu's original ~10s/50µs figures exactly); a smaller budget is
/// how tests force the "spin bound exceeded" skip path deterministically
/// instead of waiting out the real ~10s.
#[derive(Debug, Clone, Copy)]
struct SpinConfig {
    /// How long the wait loop gives a single queued segment to catch up
    /// before giving up on it (absent a tighter `shutdown_deadline`).
    per_seal_budget: Duration,
    /// Sleep between polls of the hot index/book.
    poll_interval:   Duration,
}

impl Default for SpinConfig {
    fn default() -> Self {
        // 200_000 × 50µs ≈ 10s — the exact bn-1vu figure, just expressed as a
        // wall-clock budget instead of a spin count.
        SpinConfig {
            per_seal_budget: Duration::from_secs(10),
            poll_interval:   Duration::from_micros(50),
        }
    }
}

/// The composed `mess-log` + `mess-index` production backend.
///
/// Cheap to clone — every clone shares the same durable committer, index, and
/// record book (matching [`MockBackend`](crate::mock)'s shared-handle
/// semantics, so a facade reopen over `engine.clone()` sees the same state).
#[derive(Clone)]
pub struct LogEngine {
    inner: Arc<Inner>,
}

/// Open-time knobs for [`LogEngine::open_with`].
#[derive(Debug, Clone)]
pub struct EngineOptions {
    /// Durability mode for the commit thread.
    pub durability:               Durability,
    /// Active-segment size in bytes (preallocated at open).
    pub segment_size:             u64,
    /// Dedupe-window capacity for the meta store.
    pub dedupe_capacity:          usize,
    /// Sealed pointer-block cache budget in bytes (`bn-e2y` / bn-1hx). `0`
    /// disables the cache (every sealed replay decodes fresh); a non-zero
    /// budget caches decoded blocks so a repeated sealed-stream replay (a
    /// projection rebuild, a subscription re-read) skips the decode — and
    /// makes the `cache_hits`/`cache_misses` runtime metrics meaningful
    /// under load.
    pub block_cache_budget_bytes: u64,
    /// Emit the per-batch on-disk fold chain (`crypto_chain`, spec 05 §6,
    /// `bn-3l0`). **Off by default**: when `false`, segments are
    /// byte-identical to a store that never knew about the chain. When
    /// `true`, every batch the committer writes carries its real
    /// `crypto_chain` (offset 72, flag bit 0), per-stream heads are
    /// rehydrated on recovery, and `mess verify --full` recomputes the
    /// chain to catch a CRC-repaired payload tamper.
    pub chain:                    bool,
    /// Seal-time Reed-Solomon parity sidecar policy (bn-2za). **Disabled by
    /// default** (evidence-gated): when enabled, the background sealer writes
    /// a `.par` sidecar next to each sealed segment so `mess verify
    /// --repair` can reconstruct latent-sector / bit-rot damage offline.
    pub parity:                   mess_index::sealed::parity::ParityConfig,
    /// The TOTAL bound on how long [`LogEngine`]'s `Drop` will wait for the
    /// background seal thread to drain queued rolls (`bn-u6o`). Only matters
    /// when a roll was reported but its publish never caught up (an abandoned
    /// append future) — the ordinary case drains near-instantly. A skipped
    /// seal at shutdown is safe (the segment stays durable + unsealed,
    /// served from the log on reopen), so this bounds worst-case shutdown
    /// latency rather than protecting correctness. Default 2s.
    pub shutdown_seal_budget:     Duration,
}

/// Re-export of the committer's runtime metrics snapshot (`bn-e2y`), the
/// barrier-latency + throughput core of [`EngineMetrics`].
pub use mess_log::committer::CommitterMetrics;

/// An in-process snapshot of a [`LogEngine`]'s runtime health (`bn-e2y`).
///
/// The mandatory §2.6 surface is [`commit`](Self::commit)'s barrier-latency
/// percentiles and [`commit`](Self::commit)`.fsync_degraded`. The rest is the
/// doc-03 operational list: cache hit rate, append throughput, sealed-tier
/// size, and the active-segment age. Read via [`LogEngine::metrics`]; every
/// field is a live-process fact (see that method's note on the process split).
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct EngineMetrics {
    /// Durable committer metrics: `fdatasync` p50/p95/p99, the degradation
    /// flag (§2.6), and append-throughput counters.
    pub commit:                    CommitterMetrics,
    /// The durable watermark position (highest durable global position + 1).
    /// A subscription's lag (SUB9, `docs/spec/06-subscriptions.md`) is
    /// `durable_watermark - subscriber_cursor`, computed per subscription by
    /// the subscription layer against this value.
    pub durable_watermark:         u64,
    /// Whether a barrier fault has poisoned the store (D8): writes fail fast,
    /// reads clamp to the frozen watermark. Distinct from `fsync_degraded`
    /// (merely slow but still `Ok`, §2.6).
    pub degraded_poisoned:         bool,
    /// Cumulative sealed block-cache hits.
    pub cache_hits:                u64,
    /// Cumulative sealed block-cache misses.
    pub cache_misses:              u64,
    /// Block-cache hit rate over all lookups so far, `[0, 1]`.
    pub cache_hit_rate:            f64,
    /// Live cached blocks.
    pub cache_entries:             usize,
    /// Resident cache weight in bytes.
    pub cache_weight_bytes:        u64,
    /// Sealed segments installed in the cold tier.
    pub sealed_segment_count:      usize,
    /// Total events committed (record-book length).
    pub total_events:              u64,
    /// Age of the active segment since this process opened it, in seconds.
    pub active_segment_age_secs:   f64,
    /// Seal-path durability-barrier (`fsync`) latency (`bn-e2y`): the
    /// sidecar + directory fsyncs the sealer issues off the append path.
    /// A near-full SSD stalls these exactly as it stalls the commit
    /// barrier, so they are timed and alarmed separately from
    /// [`commit`](Self::commit)`.fsync`.
    pub seal_fsync:                LatencySnapshot,
    /// Whether seal-path barrier latency has crossed the degradation threshold
    /// — the sticky store-status flag (§2.6) for the seal durability site.
    pub seal_fsync_degraded:       bool,
    /// Seal-path barriers that crossed the threshold.
    pub seal_fsync_degraded_trips: u64,
    /// Seal duration (`roll → sealed installed`) latency distribution.
    pub seal_duration:             LatencySnapshot,
    /// Segments sealed since this process opened.
    pub seals:                     u64,
    /// Seals skipped rather than completed (`bn-u6o`): the background
    /// roll-sealer gave up waiting for the hot index/book to catch up (its
    /// bounded per-segment spin, live) or a queued seal did not finish inside
    /// the bounded total shutdown wait (`Inner::drop`). Either way the segment
    /// stays durable and unsealed — served from the log until it is resealed
    /// or the store reopens — but an operator MUST be able to see it happened;
    /// see `mess_index::sealed::SealMetrics::record_seal_skipped`.
    pub seals_skipped:             u64,
}

impl Default for EngineOptions {
    fn default() -> Self {
        EngineOptions {
            // `Process`: ack the moment the covering write returns. The record
            // book and index are in-process, so per-batch fsync buys nothing
            // for correctness here; benches override this with `Group`.
            durability:               Durability::Process,
            segment_size:             256 * 1024 * 1024,
            dedupe_capacity:          mess_index::meta::DEFAULT_DEDUPE_CAPACITY,
            // On by default (64 MiB): the sealed block cache is transparent to
            // results and pays for itself on repeat replay (perf_replay's 48%
            // hit rate), and a live cache is what makes the hit/miss runtime
            // metrics operationally meaningful. Set to 0 to disable.
            block_cache_budget_bytes: 64 * 1024 * 1024,
            // Fold chain off by default: opt in per store for tamper-evident
            // segments (spec 05 §6).
            chain:                    false,
            // bn-2za: parity is opt-in / evidence-gated — off by default.
            parity:
                mess_index::sealed::parity::ParityConfig::default(),
            // bn-u6o: bound total shutdown drain latency, not correctness — a
            // skipped seal at shutdown is safe (served from the log on
            // reopen). 2s is comfortably above a healthy drain (near-instant)
            // and comfortably below the old ~10s-per-abandoned-roll worst
            // case.
            shutdown_seal_budget:     Duration::from_secs(2),
        }
    }
}

impl LogEngine {
    /// Open (creating if absent) a composed engine rooted at `dir`, default
    /// options.
    pub fn open(dir: impl AsRef<Path>) -> Result<Self, EngineError> {
        Self::open_with(dir, EngineOptions::default())
    }

    /// Open with explicit [`EngineOptions`].
    pub fn open_with(
        dir: impl AsRef<Path>,
        opts: EngineOptions,
    ) -> Result<Self, EngineError> {
        let dir = dir.as_ref();
        std::fs::create_dir_all(dir)
            .map_err(|e| EngineError::Open(format!("create_dir_all: {e}")))?;
        let lock = StoreLock::acquire(dir)
            .map_err(|e| EngineError::Open(format!("lock: {e}")))?;

        let rt = RealRuntime::new();

        // The durable metadata store, opened before recovery so the interner's
        // id→name bijection is available to materialise recovered payloads.
        let meta = MetaStore::open_with_capacity(
            dir.join("meta"),
            opts.dedupe_capacity,
        )
        .map_err(|e| EngineError::Meta(e.to_string()))?;

        // Reload the sealed tier from the durable sidecars written by prior
        // seals (see `load_sealed`). Without this the `SealedStore` starts
        // empty on every reopen, so a stream that was sealed before a restart
        // would silently fall back to hot replay instead of the sealed tier.
        // The returned `sealed_ids` are the segments already served cold, so
        // recovery does not re-seed the hot index with their batches (bn-1vu).
        let (sealed, sealed_ids) = Self::load_sealed(dir);
        let sealed = Arc::new(sealed);

        // Recovery on open (F6 + bn-20b + bn-1vu): rehydrate the record book
        // from EVERY durable segment (dense across rolls), re-seed the hot
        // index from the segments not already served cold, and learn
        // how to resume the last (active) segment.
        let active = Arc::new(ActiveIndex::new());
        let (book, plan, chain_heads) =
            Self::recover(&rt, dir, &active, &meta, &sealed_ids, opts.chain)?;

        // The active segment is the highest-id `seg-*.log`; on a fresh store it
        // is `ACTIVE_SEGMENT_ID`. A roll numbers the next one `+1` from here.
        let active_seg_id = match &plan {
            ResumePlan::Fresh => ACTIVE_SEGMENT_ID,
            ResumePlan::Resume(info) => info.segment_id,
        };
        let seg_path = segment_path(dir, active_seg_id);
        let writer = match plan {
            ResumePlan::Fresh => {
                let params = SegmentParams {
                    segment_id:         ACTIVE_SEGMENT_ID,
                    base_pos:           0,
                    epoch:              1,
                    prev_segment_epoch: 0,
                    created_unix_nanos: 0,
                    segment_size:       opts.segment_size,
                };
                SegmentWriter::create(&rt.fs(), &seg_path, params).map_err(
                    |e| EngineError::Open(format!("segment create: {e}")),
                )?
            }
            ResumePlan::Resume(info) => {
                // Resume the existing active segment in place (no header
                // rewrite, no epoch bump): new appends extend the one
                // contiguous committed prefix from the recovered safe_offset.
                let params = ResumeParams {
                    segment_id:    info.segment_id,
                    base_pos:      info.base_pos,
                    epoch:         info.epoch,
                    segment_size:  opts.segment_size,
                    write_off:     info.write_off,
                    next_batch_id: info.next_batch_id,
                    next_pos:      info.next_pos,
                    batch_count:   info.batch_count,
                    event_count:   info.event_count,
                };
                SegmentWriter::resume(&rt.fs(), &seg_path, params).map_err(
                    |e| EngineError::Open(format!("segment resume: {e}")),
                )?
            }
        };

        // Wire live auto-roll (bn-1vu): the committer rolls to a fresh segment
        // when the active one fills and reports each rolled segment over this
        // channel; the seal thread turns it into durable sidecars + a footer
        // off the append path.
        let (roll_tx, roll_rx) = mpsc::channel::<SegmentSummary>();
        let dir_for_paths = dir.to_path_buf();
        let roller =
            Roller::new(move |id| segment_path(&dir_for_paths, id), roll_tx);
        // Fold chain (`bn-3l0`, spec 05 §6): opt-in. When on, seed the
        // committer with the per-stream heads rehydrated by recovery so
        // an append after reopen continues each stream's chain from its
        // durable exit head; when off, `ChainInit::off()` keeps the
        // on-disk bytes byte-identical.
        let chain_init = if opts.chain {
            ChainInit::on(chain_heads)
        } else {
            ChainInit::off()
        };
        let committer = Committer::spawn_with_roll_chained(
            &rt,
            writer,
            opts.durability,
            roller,
            chain_init,
        );
        let appender = committer.appender();

        let book = Arc::new(Mutex::new(book));

        // Shared seal-path metrics (bn-e2y): the background roll-sealer and any
        // on-demand `seal_active` both feed this one sink, so seal-barrier
        // fsync latency, its degradation alarm, and seal durations aggregate.
        let seal_metrics = Arc::new(SealMetrics::new());

        // Spawn the background auto-roll sealer thread.
        std::fs::create_dir_all(dir.join("sealed")).map_err(|e| {
            EngineError::SealedRead(format!("mkdir sealed: {e}"))
        })?;
        // `bn-u6o`: unset until `Inner::drop` publishes it once, turning the
        // sealer's normal ~10s-per-segment wait into a single deadline shared
        // by every seal still queued at shutdown (see the `Inner` field doc).
        let shutdown_deadline: Arc<OnceLock<Instant>> =
            Arc::new(OnceLock::new());
        let seal_thread = {
            let driver =
                SealDriver::new(Arc::clone(&sealed), dir.join("sealed"))
                    .with_metrics(Arc::clone(&seal_metrics))
                    .with_parity(opts.parity);
            let active = Arc::clone(&active);
            let book = Arc::clone(&book);
            let dir = dir.to_path_buf();
            let seal_metrics_for_thread = Arc::clone(&seal_metrics);
            let shutdown_deadline = Arc::clone(&shutdown_deadline);
            std::thread::Builder::new()
                .name("mess-engine-roll-sealer".into())
                .spawn(move || {
                    Self::run_roll_sealer(
                        roll_rx,
                        driver,
                        active,
                        book,
                        dir,
                        seal_metrics_for_thread,
                        shutdown_deadline,
                        SpinConfig::default(),
                    )
                })
                .map_err(|e| EngineError::Open(format!("spawn sealer: {e}")))?
        };

        // The publish sequencer's turn-order starts wherever recovery left
        // the book's dense prefix — 0 on a fresh store, or the recovered
        // event count on a reopen — never a hardcoded 0, or the first
        // post-reopen publish would wait forever for a position that was
        // already durably assigned in a previous process lifetime.
        let recovered_len =
            book.lock().expect("book poisoned").payloads.len() as u64;

        Ok(LogEngine {
            inner: Arc::new(Inner {
                rt,
                appender,
                committer: Some(committer),
                seal_thread: Some(seal_thread),
                _lock: lock,
                active,
                sealed,
                block_cache: if opts.block_cache_budget_bytes == 0 {
                    BlockCache::disabled()
                } else {
                    // `est_blocks` is a rough shard-sizing seed (budget ÷ a
                    // conservative average block size), not a hard cap.
                    let est_blocks =
                        (opts.block_cache_budget_bytes / 8192).max(64) as usize;
                    BlockCache::with_budget_bytes(
                        opts.block_cache_budget_bytes,
                        est_blocks,
                    )
                },
                seal_metrics,
                meta,
                book,
                // Seed the published watermark at the recovered dense book
                // length: 0 on a fresh store, or the recovered event count on a
                // reopen — the same baseline the publish sequencer starts from.
                read_watermark: Watermark::new(recovered_len),
                append_gate: AppendGate::new(),
                publish_seq: PublishSequencer::new_at(recovered_len),
                opened_at: Instant::now(),
                dir: dir.to_path_buf(),
                shutdown_deadline,
                shutdown_seal_budget: opts.shutdown_seal_budget,
            }),
        })
    }

    /// Rehydrate the record book and rebuild the hot index from the durable
    /// log (bn-20b + bn-1vu), and decide how to resume the active segment.
    ///
    /// With live auto-roll the store holds a chain of segments `seg-*.log`
    /// (`seg-1` … `seg-N`, `N` the live head). Recovery scans them in ascending
    /// id order: `mess-log`'s
    /// [`recover_segment_with_image`](scanner::recover_segment_with_image)
    /// gives each segment's accepted committed prefix + durable image, then
    /// [`AcceptedBatch::frames`] materialises every event's `(event_type_id,
    /// payload)` into the book **densely across the whole chain** — resolved to
    /// names through the interner reloaded from the durable
    /// `stream_names`/`type_names` meta tables. A segment already served from
    /// the cold tier (its id in `sealed_ids`, reloaded from its `.pidx`) is
    /// **not** re-seeded into the hot index — the book still gets its
    /// payloads, but the sealed sidecar owns its position enumeration. A
    /// rolled-but-not-yet-sealed segment (crash mid-seal: unsealed `.log`,
    /// no `.pidx`) is not in `sealed_ids`, so its batches DO seed the hot
    /// index and it is served from the log — losing nothing. The last
    /// (highest-id) segment is the resumable live head.
    fn recover(
        rt: &RealRuntime,
        dir: &Path,
        active: &ActiveIndex,
        meta: &MetaStore,
        sealed_ids: &HashSet<u64>,
        chain: bool,
    ) -> Result<(Book, ResumePlan, HashMap<u64, ChainHead>), EngineError> {
        // Reconstruct the interner (both directions) from the durable id→name
        // tables first, so materialised payloads can resolve their names.
        let mut book = Book::default();
        book.load_stream_names(
            meta.stream_names()
                .map_err(|e| EngineError::Meta(e.to_string()))?,
        );
        book.load_type_names(
            meta.type_names().map_err(|e| EngineError::Meta(e.to_string()))?,
        );

        // Per-stream fold-chain heads rehydrated from the recovered frames
        // (spec 05 §5/§6, `bn-3l0`): folding every durable event of a stream in
        // ascending version order leaves each head at the exit value of the
        // committed prefix, so an append after reopen continues the chain
        // exactly. Built only when the store opted into the chain; empty
        // otherwise. Independent of whether a segment is served hot or cold —
        // the fold walks the durable payloads either way.
        let mut chain_heads: HashMap<u64, ChainHead> = HashMap::new();

        // Enumerate the segment chain in ascending id order.
        let segment_ids = enumerate_segment_ids(dir);
        if segment_ids.is_empty() {
            return Ok((book, ResumePlan::Fresh, chain_heads));
        }

        let mut hot_entries: Vec<BatchEntry> = Vec::new();
        let mut last_headed: Option<ResumeInfo> = None;
        let mut watermark = 0u64;

        for seg_id in segment_ids {
            let seg_path = segment_path(dir, seg_id);
            let (rec, image) =
                scanner::recover_segment_with_image(&rt.fs(), &seg_path)
                    .map_err(|e| {
                        EngineError::Open(format!("recover seg {seg_id}: {e}"))
                    })?;
            let Some(header) = rec.header else {
                // A file with no valid header carries no committed batches of
                // this generation — skip it (never resumed, never seeds).
                continue;
            };
            let is_cold = sealed_ids.contains(&header.segment_id);

            // Materialise every committed event into the book, dense across the
            // whole chain (on-disk commit order == ascending global position).
            let mut order: Vec<&AcceptedBatch> = rec.accepted.iter().collect();
            order.sort_by_key(|b| b.first_global_pos);
            for b in &order {
                let sid = b.stream_id;
                let stream_name =
                    book.stream_name_opt(sid).ok_or_else(|| {
                        EngineError::Meta(format!(
                            "recover: no interned name for stream_id {sid}"
                        ))
                    })?;
                // bn-221: `frames` is fallible (misuse-resistant against a
                // wrong image) but this caller always passes
                // the exact image `b` was recovered from, so
                // the error path is unreachable in practice —
                // still propagated rather than unwrapped so a future refactor
                // that breaks that invariant fails loudly instead of panicking.
                let frames = b
                    .frames(&image)
                    .map_err(|e| EngineError::Open(format!("recover: {e}")))?;
                for (k, frame) in frames.enumerate() {
                    let gp = b.first_global_pos + k as u64;
                    debug_assert_eq!(
                        book.payloads.len() as u64,
                        gp,
                        "dense rehydration"
                    );
                    let message_type = book
                        .type_name_opt(frame.event_type_id)
                        .ok_or_else(|| {
                            EngineError::Meta(format!(
                                "recover: no interned name for event_type_id \
                                 {}",
                                frame.event_type_id
                            ))
                        })?;
                    let stream_position = b.first_stream_version + k as u64;
                    if chain {
                        // Fold the on-disk payload into the stream's head, in
                        // ascending version order (§6.2). A stream first seen
                        // here starts at its genesis; `absorb` advances it to
                        // `h[stream_position]`.
                        chain_heads
                            .entry(sid)
                            .or_insert_with(|| ChainHead::genesis(sid))
                            .absorb(frame.payload);
                    }
                    book.payloads.push(Payload {
                        stream_name: stream_name.clone(),
                        message_type,
                        data: Arc::from(frame.payload),
                        stream_position,
                    });
                    book.stream_events.entry(sid).or_default().push(gp);
                    book.heads.insert(sid, stream_position);
                }
                // Seed the hot index only for segments not already served cold.
                if !is_cold {
                    hot_entries.push(BatchEntry {
                        stream_id:            sid,
                        first_stream_version: b.first_stream_version,
                        frame_count:          b.frame_count,
                        first_global_pos:     b.first_global_pos,
                        ptr:                  EventPtr {
                            segment_id: header.segment_id,
                            offset:     b.offset,
                        },
                    });
                }
            }
            watermark = watermark.max(rec.next_pos);

            // The highest-id headed segment is the resumable live head.
            last_headed = Some(ResumeInfo {
                segment_id:    header.segment_id,
                base_pos:      header.base_pos,
                epoch:         header.epoch,
                write_off:     rec.safe_offset,
                next_batch_id: rec.next_batch_id,
                next_pos:      rec.next_pos,
                batch_count:   rec.accepted.len() as u64,
                event_count:   rec.next_pos - header.base_pos,
            });
        }

        active.apply_committed(watermark, &hot_entries);

        match last_headed {
            Some(info) => Ok((book, ResumePlan::Resume(info), chain_heads)),
            None => Ok((book, ResumePlan::Fresh, chain_heads)),
        }
    }

    /// The background auto-roll sealer loop (`bn-1vu`), run on its own thread.
    /// For each rolled (durable, unsealed) segment reported over `rx`, it
    /// builds the pointer + payload sidecars from the hot index snapshot
    /// and the book, finalizes the segment footer (writes + fsyncs the
    /// trailer), and installs the segment into the cold [`SealedStore`] —
    /// all off the append path (D5). A seal failure is best-effort: the
    /// rolled segment stays durable + unsealed and is served from the log
    /// (and re-sealable) on reopen, so a failed seal never loses data. The
    /// loop exits when the roll channel closes (the committer task dropped
    /// its [`Roller`]), draining every queued seal first.
    ///
    /// `bn-u6o`: `seal_metrics` counts + loudly (rate-limited) logs every
    /// segment this loop gives up waiting on (see the two `record_seal_skipped`
    /// call sites below) — before this bone that skip was silent, so an
    /// operator had no way to learn a segment stayed unsealed until reopen.
    /// `shutdown_deadline` is unset during live operation (each segment gets
    /// its own `spin.per_seal_budget`, ~10s by default — the original bn-1vu
    /// bound); once `Inner::drop` publishes it, it becomes a single deadline
    /// shared by every segment still queued, bounding the TOTAL shutdown drain
    /// instead of `per_seal_budget` per abandoned roll.
    #[allow(clippy::too_many_arguments)] // internal seam; each arg is a distinct shared handle
    fn run_roll_sealer(
        rx: mpsc::Receiver<SegmentSummary>,
        driver: SealDriver,
        active: Arc<ActiveIndex>,
        book: Arc<Mutex<Book>>,
        dir: PathBuf,
        seal_metrics: Arc<SealMetrics>,
        shutdown_deadline: Arc<OnceLock<Instant>>,
        spin: SpinConfig,
    ) {
        for summary in rx {
            let base = summary.base_pos;
            let end = summary.end_pos;

            // Wait until the hot index + book have published every event of
            // this segment (post-ack discipline). Under the current
            // serialised append gate this already holds by the time
            // the roll notification lands; the bounded wait keeps
            // it robust if a future append gate (bn-1s0)
            // relaxes that ordering. A gone writer can never lower the applied
            // end, so this cannot deadlock.
            //
            // The per-segment deadline (`spin.per_seal_budget` out) is clamped
            // to `shutdown_deadline` when the latter is set (bn-u6o) — see the
            // fn doc.
            let per_seal_deadline = Instant::now() + spin.per_seal_budget;
            loop {
                let applied = active.snapshot().applied_end;
                let booked =
                    book.lock().expect("book lock").payloads.len() as u64;
                if applied >= end && booked >= end {
                    break;
                }
                let deadline = match shutdown_deadline.get() {
                    Some(&sd) => sd.min(per_seal_deadline),
                    None => per_seal_deadline,
                };
                if Instant::now() >= deadline {
                    // Give up on this seal rather than hang. The segment stays
                    // durable + unsealed (served from the log).
                    break;
                }
                std::thread::sleep(spin.poll_interval);
            }

            let snapshot = active.snapshot();
            if snapshot.applied_end < end {
                // bn-u6o: this is the silent-skip site the bone exists to fix —
                // count it and log loudly (rate-limited) so an operator can see
                // a segment stayed unsealed rather than discovering it only at
                // reopen.
                seal_metrics.record_seal_skipped(&format!(
                    "segment {} [{base}, {end}) never caught up in the hot \
                     index/book (applied_end={}, shutdown_deadline={})",
                    summary.segment_id,
                    snapshot.applied_end,
                    shutdown_deadline.get().is_some(),
                ));
                continue; // incomplete (see the bounded wait above) — leave it unsealed
            }
            let mut input =
                seal_input_for_range(&snapshot, summary.segment_id, base, end);
            if input.streams.is_empty() {
                continue;
            }
            // Attach the segment's payloads in stored (global-position) order
            // so the seal emits the columnar `.pcol` sidecar too
            // (bn-zge / D6).
            {
                let book = book.lock().expect("book lock");
                if book.payloads.len() as u64 >= end {
                    let payloads: Vec<Vec<u8>> = book.payloads
                        [base as usize..end as usize]
                        .iter()
                        .map(|p| p.data.to_vec())
                        .collect();
                    input = input.with_payloads(payloads);
                }
            }

            // Finalize: write + fsync the fixed footer trailer (§3.3.1), making
            // the segment R2-trusted, only after the sidecars are durable.
            let seg_id = summary.segment_id;
            let seg_path = segment_path(&dir, seg_id);
            let seg_path_for_parity = seg_path.clone();
            let sum = summary;
            let finalize = move || finalize_footer(&seg_path, &sum);
            // Best-effort: on failure the segment stays unsealed + recoverable.
            if driver.seal(input, finalize).is_ok() {
                // bn-2za: once the footer is finalized the `.log` bytes are
                // complete — emit the RS parity sidecar over them (no-op unless
                // parity is enabled). Best-effort, like the `.filter`: a parity
                // failure never fails the seal.
                let _ =
                    driver.write_parity_sidecar(&seg_path_for_parity, seg_id);
            }
        }
    }

    /// Rebuild a [`SealedStore`] from the sealed sidecars already durable under
    /// `dir/sealed` — the reopen counterpart to [`seal_active`]/the background
    /// sealer. Each complete `.pidx` (with its opportunistic sibling `.filter`
    /// and `.pcol`, re-attached by [`SealedSegmentIndex::open`]) is installed
    /// so a stream sealed before a restart is served from the cold tier
    /// again rather than silently falling back to hot replay.
    ///
    /// **Crash-mid-seal safety.** The sidecar writer is crash-atomic
    /// (temp-file → fsync → rename, see `SealDriver`'s `write_durable`): a
    /// crash during a seal leaves either the previous state or a complete
    /// `.pidx`, never a torn one under its real name. A partial
    /// `*.pidx.tmp` husk (a seal interrupted before its rename) is ignored
    /// here — it does not match the `.pidx` extension — and a `.pidx` that
    /// fails to parse (CRC / truncation) is skipped, not fatal: the durable
    /// log remains the authority, so that stream is served from the hot
    /// tier until it is re-sealed. Either way the engine reopens into a
    /// readable, recoverable state.
    ///
    /// Returns the installed segment ids too, so recovery knows which segments
    /// are already served cold and must NOT be re-seeded into the hot index
    /// (bn-1vu).
    fn load_sealed(dir: &Path) -> (SealedStore, HashSet<u64>) {
        let store = SealedStore::new();
        let mut ids = HashSet::new();
        let sealed_dir = dir.join("sealed");
        let Ok(entries) = std::fs::read_dir(&sealed_dir) else {
            // No sealed directory yet: nothing has been sealed.
            return (store, ids);
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) != Some("pidx") {
                // Skip `.pidx.tmp` husks, `.pcol`/`.filter` siblings
                // (re-attached by `open`), and anything else.
                continue;
            }
            // A complete, CRC-valid sidecar installs; a torn/corrupt one is
            // skipped (the log stays the truth) so a mid-seal crash never
            // prevents reopen.
            if let Ok(index) = SealedSegmentIndex::open(&path) {
                ids.insert(index.segment_id());
                store.install(Arc::new(index));
            }
        }
        (store, ids)
    }

    /// Test/diagnostic: total events committed to the record book.
    #[must_use]
    pub fn total_events(&self) -> usize {
        self.inner.book.lock().expect("book lock").payloads.len()
    }

    /// An in-process snapshot of the engine's runtime metrics (`bn-e2y`).
    ///
    /// This is the **authoritative** metrics surface: `fdatasync` barrier
    /// latency percentiles and the mandatory degradation flag (§2.6), block
    /// cache hit/miss, append throughput, sealed-tier size, and the active
    /// segment's in-process age. These are live-process facts — a separate
    /// read-only `mess inspect` process cannot observe another process's
    /// counters, so it surfaces only what is observable offline (segment
    /// ages/sizes); the in-process surface here is the API operators poll.
    #[must_use]
    pub fn metrics(&self) -> EngineMetrics {
        let commit = self
            .inner
            .committer
            .as_ref()
            .map(mess_log::committer::Committer::metrics)
            .unwrap_or_default();
        let cache = &self.inner.block_cache;
        let seal = self.inner.seal_metrics.snapshot();
        EngineMetrics {
            commit,
            durable_watermark: self.inner.appender.watermark().get(),
            degraded_poisoned: self.inner.appender.is_degraded(),
            cache_hits: cache.hits(),
            cache_misses: cache.misses(),
            cache_hit_rate: cache.hit_rate(),
            cache_entries: cache.len(),
            cache_weight_bytes: cache.weight_bytes(),
            sealed_segment_count: self.inner.sealed.len(),
            total_events: self
                .inner
                .book
                .lock()
                .expect("book lock")
                .payloads
                .len() as u64,
            active_segment_age_secs: self
                .inner
                .opened_at
                .elapsed()
                .as_secs_f64(),
            seal_fsync: seal.fsync,
            seal_fsync_degraded: seal.fsync_degraded,
            seal_fsync_degraded_trips: seal.fsync_degraded_trips,
            seal_duration: seal.seal_duration,
            seals: seal.seals,
            seals_skipped: seal.seals_skipped,
        }
    }

    /// Test/diagnostic: number of sealed segments currently installed in the
    /// cold tier (populated at open by [`load_sealed`], and by
    /// [`seal_active`](Self::seal_active) at runtime).
    #[must_use]
    pub fn sealed_segment_count(&self) -> usize { self.inner.sealed.len() }

    /// Test/diagnostic: how many times the durable meta store's
    /// [`MetaStore::persist`] has been called (bn-150) — i.e. how many
    /// `fsync`-backed flushes [`persist_new_names`](Self::persist_new_names)
    /// has performed. Lets a test assert the hot append path (no
    /// newly-interned name) adds zero durable flushes, and that a new-name
    /// append adds exactly the expected number.
    #[must_use]
    pub fn meta_persist_call_count(&self) -> u64 {
        self.inner.meta.persist_call_count()
    }

    /// Seal the current active segment into the cold [`SealedStore`], driving
    /// the real `mess-index` [`SealDriver`] (sidecar encode → durable write →
    /// install → evict-from-active). After this, `read_stream` for the sealed
    /// streams routes through the [`ReplaySet`] cold path.
    ///
    /// This seals the CURRENT (live head) active segment on demand, distinct
    /// from the automatic seal a live roll triggers (bn-1vu: when the head
    /// fills the committer rolls and the background sealer seals the rolled
    /// segment off the append path). It is what the sealed-replay bench and
    /// the sealed read path exercise directly.
    pub fn seal_active(&self) -> Result<(), EngineError> {
        let snapshot = self.inner.active.snapshot();
        let input = SealInput::from_snapshot(&snapshot, ACTIVE_SEGMENT_ID, 0);
        if input.streams.is_empty() {
            return Ok(());
        }
        // bn-zge / D6: hand the sealer the segment's payloads in stored
        // (global-position) order so the seal emits the columnar `.pcol`
        // payload sidecar alongside the pointer sidecar and attaches it to the
        // installed segment (the sealed read path can then reassemble payloads
        // without touching the raw log). base_pos is 0 for the single interim
        // active segment, so a global position is exactly the dense book index.
        let event_count = input.event_count() as usize;
        let input = {
            let book = self.inner.book.lock().expect("book lock");
            if book.payloads.len() >= event_count {
                let payloads: Vec<Vec<u8>> = book.payloads[..event_count]
                    .iter()
                    .map(|p| p.data.to_vec())
                    .collect();
                input.with_payloads(payloads)
            } else {
                input
            }
        };
        let driver = SealDriver::new(
            Arc::clone(&self.inner.sealed),
            self.inner.dir.join("sealed"),
        )
        .with_metrics(Arc::clone(&self.inner.seal_metrics));
        std::fs::create_dir_all(self.inner.dir.join("sealed")).map_err(
            |e| EngineError::SealedRead(format!("mkdir sealed: {e}")),
        )?;
        // The finalize step would seal the mess-log segment footer; the engine
        // keeps the segment live for continued appends, so this is a no-op here
        // (the sealed *index* sidecar is what the cold read path consumes).
        driver
            .seal(input, || Ok(()))
            .map_err(|e| EngineError::SealedRead(format!("seal: {e}")))?;
        Ok(())
    }

    /// The sealed-tier read path: enumerate a stream's committed
    /// `(stream_position, global_position)` pairs from the sealed corpus via
    /// the real [`ReplaySet`], for streams whose batches have been sealed and
    /// evicted from the hot [`ActiveIndex`] (the sealed-replay bench). A
    /// `BTreeMap` keeps them in stream order.
    fn sealed_stream_positions(
        &self,
        stream_id: u64,
    ) -> Result<BTreeMap<u64, u64>, EngineError> {
        let mut out: BTreeMap<u64, u64> = BTreeMap::new();
        let sealed_segs = self.inner.sealed.segments_for_stream(stream_id);
        let replay = ReplaySet::from_segments(sealed_segs);
        let entries = replay
            .stream_replay(stream_id, &self.inner.block_cache)
            .map_err(|e| EngineError::SealedRead(format!("{e:?}")))?;
        for e in entries {
            for k in 0..u64::from(e.frame_count) {
                out.insert(e.first_version + k, e.first_global_pos + k);
            }
        }
        // The hot tail (if the stream also has unsealed batches during
        // handoff).
        for e in self.inner.active.stream_entries(stream_id) {
            for k in 0..u64::from(e.frame_count) {
                out.insert(e.first_version + k, e.first_global_pos + k);
            }
        }
        Ok(out)
    }
}

/// The outcome of the exact-version pre-check in [`LogEngine::append_batch`],
/// computed under the book lock and acted on after the lock (and any name
/// persistence) is released.
enum Pre {
    /// The stream was not at `expected`; return a conflict with this actual.
    Conflict(Version),
    /// An empty batch that validated `expected`: a no-op.
    Empty,
    /// A real batch to durably append.
    Proceed {
        sid:              u64,
        events:           Vec<EventInput>,
        first_stream_pos: u64,
    },
}

impl LogEngine {
    /// Write newly-interned id→name mappings to the durable meta store so a
    /// reopen can reconstruct the interner (bn-20b). Returns whether it wrote
    /// anything, so the caller can fold that into a single co-durable flush
    /// across both the stream-name and type-name call sites (bn-150; see
    /// [`append_batch`](LogEngine::append_batch) and the "Why this needs an
    /// explicit flush" note below) — this method itself never flushes.
    ///
    /// # Why a flush is needed at all, and why it is the CALLER's job (bn-150)
    ///
    /// Every other meta table (`stream_heads`, `snapshot_heads`, dedupe) is a
    /// derived cache the log can always rebuild (I5, see the `mess-index`
    /// meta module doc) — fjall's default `PersistMode::Buffer` writes
    /// (a real `write(2)` to the OS page cache, but never `fsync`ed — see
    /// `fjall::keyspace::Keyspace::insert`/`Database::batch`) are fine for
    /// them: a lost *page cache* (i.e. actual power-loss/OS-crash, not a
    /// mere process crash — a process death alone cannot lose a completed
    /// `write(2)`) just means a slightly longer recovery replay, never lost
    /// information. `stream_names`/`type_names` are the one exception: they
    /// are the durable *source of truth* for the name↔id bijection (the
    /// spec's `$registry`, `04-registry.md`, would carry names in the log
    /// itself via a system stream, but that is not implemented here — this
    /// engine's log frames only ever carry the numeric
    /// `stream_id`/`event_type_id`, never the string, so a name lost off the
    /// meta table cannot be re-derived from the log at all; see the module
    /// doc's "Payload materialisation" section).
    ///
    /// Before this fix, a newly-interned name's `put_*_name` call used that
    /// same page-cache-only `Buffer` durability, with **no ordering barrier**
    /// against the covering append's own durable write — which, under a
    /// `Durability::Os`/`Group` engine (`03-durability.md`), IS a real
    /// `fsync`/`fdatasync` barrier. A genuine power-loss event between the
    /// two could keep the append durable while losing its name entirely, so
    /// a subsequent reopen's `recover` hit the "no interned name for
    /// stream_id" gap and returned `EngineError::Meta` — a store that could
    /// no longer open.
    ///
    /// The fix: [`append_batch`](LogEngine::append_batch) folds this
    /// method's two call sites (stream name, then type names) into a single
    /// `MetaStore::persist` (a real `fsync`) whenever *either* wrote
    /// something — one flush even when an append introduces both a new
    /// stream and a new type — performed strictly before the durable
    /// committer append is submitted. So by construction, a covering append
    /// can only become durable once its new name(s) already are: the two can
    /// no longer race. On the hot path — no new stream, no new types, the
    /// overwhelmingly common case once a store's names have stabilised —
    /// neither call site writes anything, so the fold adds no flush at all:
    /// zero fjall calls, zero added latency.
    fn persist_new_names(
        &self,
        stream: Option<(u64, String)>,
        types: &[(u32, String)],
    ) -> Result<bool, mess_index::meta::MetaError> {
        let mut wrote = false;
        if let Some((id, name)) = stream {
            self.inner.meta.put_stream_name(id, &name)?;
            wrote = true;
        }
        for (id, name) in types {
            self.inner.meta.put_type_name(*id, name)?;
            wrote = true;
        }
        Ok(wrote)
    }
}

/// A segment's on-disk path for a given id — the store's naming scheme, shared
/// by open/recovery and the committer's [`Roller`] (bn-1vu) so the two never
/// disagree.
fn segment_path(dir: &Path, segment_id: u64) -> PathBuf {
    dir.join(format!("seg-{segment_id:08}.log"))
}

/// Every existing `seg-<id>.log` id under `dir`, ascending — the segment chain
/// recovery walks (bn-1vu). Files that do not match the naming scheme are
/// ignored.
fn enumerate_segment_ids(dir: &Path) -> Vec<u64> {
    let mut ids = Vec::new();
    let Ok(entries) = std::fs::read_dir(dir) else {
        return ids;
    };
    for entry in entries.flatten() {
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        if let Some(rest) = name.strip_prefix("seg-")
            && let Some(num) = rest.strip_suffix(".log")
            && let Ok(id) = num.parse::<u64>()
        {
            ids.push(id);
        }
    }
    ids.sort_unstable();
    ids
}

/// Build a [`SealInput`] for the segment spanning global positions
/// `[base_pos, end_pos)` from a hot-index `snapshot` (bn-1vu). Unlike
/// [`SealInput::from_snapshot`] (which filters by `EventPtr.segment_id`), this
/// filters by global-position range: the engine serves payloads from the record
/// book, so its `EventPtr` fields are pseudo (the pointer offset is set to the
/// global position, never dereferenced), and the batch → segment mapping is by
/// the durable, correct global position instead. A batch never spans segments
/// (A8), so `first_global_pos ∈ [base, end)` selects exactly this segment's
/// batches.
fn seal_input_for_range(
    snapshot: &IndexSnapshot,
    segment_id: u64,
    base_pos: u64,
    end_pos: u64,
) -> SealInput {
    let mut streams = Vec::new();
    for (&stream_id, entries) in &snapshot.streams {
        let batches: Vec<SealBatch> = entries
            .iter()
            .filter(|e| {
                e.first_global_pos >= base_pos && e.first_global_pos < end_pos
            })
            .map(|e| SealBatch {
                first_version:    e.first_version,
                frame_count:      e.frame_count,
                first_global_pos: e.first_global_pos,
                offset:           e.first_global_pos, /* pseudo: reads come
                                                       * from the book */
            })
            .collect();
        if !batches.is_empty() {
            streams.push(SealStream { stream_id, batches });
        }
    }
    SealInput { segment_id, base_pos, streams, payloads: None }
}

/// Finalize a rolled segment's footer (bn-1vu): write the fixed 100-byte
/// trailer (§3.3.1) at `content_len` and `fsync`, so recovery's R2 fast path
/// can trust the segment. Called from the background sealer's finalize step,
/// only after the sidecars are durable — a crash before this leaves the segment
/// unsealed (fully scanned by recovery), losing nothing.
fn finalize_footer(
    seg_path: &Path,
    summary: &SegmentSummary,
) -> std::io::Result<()> {
    use std::io::{Seek, SeekFrom, Write};
    let fields = TrailerFields::phase3(
        summary.segment_id,
        summary.epoch,
        summary.base_pos,
        summary.batch_count,
        summary.event_count,
        summary.content_len,
    );
    let trailer = encode_trailer(&fields);
    let mut f = std::fs::OpenOptions::new().write(true).open(seg_path)?;
    f.seek(SeekFrom::Start(summary.content_len))?;
    f.write_all(&trailer)?;
    f.sync_all()?;
    Ok(())
}

impl Backend for LogEngine {
    type Error = EngineError;

    async fn head(&self, stream_id: &str) -> Result<Version, Self::Error> {
        let book = self.inner.book.lock().expect("book lock");
        let Some(&sid) = book.stream_ids.get(stream_id) else {
            return Ok(Version::NoStream);
        };
        Ok(book.head(sid))
    }

    async fn read_stream(
        &self,
        stream_id: &str,
        after: Version,
        limit: usize,
    ) -> Result<Vec<StoredRecord>, Self::Error> {
        let sid = {
            let book = self.inner.book.lock().expect("book lock");
            book.stream_ids.get(stream_id).copied()
        };
        let Some(sid) = sid else {
            // Match MockBackend: widen the load→append race window so
            // concurrent writers on one stream genuinely contend.
            tokio::task::yield_now().await;
            return Ok(Vec::new());
        };
        let start = after.next_position();

        // Cold path: a stream whose batches have been sealed + evicted from the
        // hot index is served through the real sealed-replay path
        // (`ReplaySet`).
        if !self.inner.sealed.segments_for_stream(sid).is_empty() {
            let positions = self.sealed_stream_positions(sid)?;
            let book = self.inner.book.lock().expect("book lock");
            return Ok(positions
                .range(start..)
                .take(limit)
                .map(|(_, &gp)| book.record(gp))
                .collect());
        }

        // Hot path: O(limit) slice of the per-stream index (kept in lockstep
        // with `ActiveIndex::apply_committed`).
        let page: Vec<StoredRecord> = {
            let book = self.inner.book.lock().expect("book lock");
            match book.stream_events.get(&sid) {
                Some(events) => events
                    .get(start as usize..)
                    .unwrap_or(&[])
                    .iter()
                    .take(limit)
                    .map(|&gp| book.record(gp))
                    .collect(),
                None => Vec::new(),
            }
        };
        tokio::task::yield_now().await;
        Ok(page)
    }

    async fn read_global(
        &self,
        after: Option<u64>,
        limit: usize,
    ) -> Result<Vec<StoredRecord>, Self::Error> {
        let book = self.inner.book.lock().expect("book lock");
        // Global positions are dense from 0, so `after` maps straight to an
        // index; the record book is the authoritative global order.
        let start = after.map_or(0, |p| p as usize + 1);
        Ok(book
            .payloads
            .get(start..)
            .unwrap_or(&[])
            .iter()
            .take(limit)
            .enumerate()
            .map(|(i, _)| book.record((start + i) as u64))
            .collect())
    }

    async fn append_batch(
        &self,
        stream_id: &str,
        expected: Version,
        records: &[RecordToAppend],
    ) -> Result<Appended, AppendError<Self::Error>> {
        // Intern the stream name to a stable numeric id FIRST, outside any
        // append gate. Interning only touches the book's own interner maps
        // (never the head/version state), and the book's std mutex already
        // serialises concurrent inserts of the same new name — so this is
        // safe ahead of the per-stream gate below, and it is *required*
        // ahead of it: the gate is keyed by `sid`, which doesn't exist until
        // the name is interned.
        let (sid, new_stream) = {
            let mut book = self.inner.book.lock().expect("book lock");
            let (sid, sid_new) = book.intern_stream(stream_id);
            let new_stream = sid_new.then(|| (sid, stream_id.to_string()));
            (sid, new_stream)
        };
        // Write a newly-interned stream name so a reopen can resolve it,
        // even on a path that goes on to conflict — the id was assigned
        // in-process regardless, and the interner is dense, so a later
        // successful append to this stream must find its name persisted.
        // NOT flushed here: folded into one co-durable flush below with any
        // newly-interned type names, ahead of the covering append (bn-150).
        let wrote_stream =
            self.persist_new_names(new_stream, &[]).map_err(|e| {
                AppendError::Backend(EngineError::Meta(e.to_string()))
            })?;

        // Serialise the exact-version critical section PER STREAM (bn-1s0):
        // two appenders racing `Exact(v)` on the SAME stream still resolve to
        // exactly one winner; appenders on DIFFERENT streams no longer queue
        // behind one store-wide lock. An OWNED guard (bn-3nz): on the
        // Proceed path it is moved into the commit+publish blocking task and
        // released only after the publish, so the shard stays held across the
        // whole check-head → append → publish section even if this async
        // future is cancelled — otherwise a dropped future could free the
        // shard while its committed batch is still publishing and let a
        // concurrent same-stream append double-write the same stream version.
        let gate = self.inner.append_gate.lock_for(sid).await;

        // Check the expected version under the book lock, capturing any
        // newly-assigned type names, then drop the lock (we must not hold a
        // std mutex across the append await, nor across fjall I/O).
        let (pre, new_types) = {
            let mut book = self.inner.book.lock().expect("book lock");
            let mut new_types: Vec<(u32, String)> = Vec::new();
            let actual = book.head(sid);
            let pre = if actual != expected {
                Pre::Conflict(actual)
            } else if records.is_empty() {
                // Empty batch: a no-op that still validated `expected`.
                Pre::Empty
            } else {
                let events: Vec<EventInput> = records
                    .iter()
                    .map(|r| {
                        let (tid, tid_new) = book.intern_type(&r.message_type);
                        if tid_new {
                            new_types.push((tid, r.message_type.clone()));
                        }
                        EventInput::plain(tid, 0, 0, r.data.clone())
                    })
                    .collect();
                Pre::Proceed {
                    sid,
                    events,
                    first_stream_pos: expected.next_position(),
                }
            };
            (pre, new_types)
        };

        // Write any newly-interned type names the same way (see above).
        let wrote_types =
            self.persist_new_names(None, &new_types).map_err(|e| {
                AppendError::Backend(EngineError::Meta(e.to_string()))
            })?;

        // Co-durable flush (bn-150): one fsync-backed `MetaStore::persist`
        // covering BOTH call sites above, strictly before the durable
        // committer append below is even submitted — so a covering append
        // can only become durable once its new name(s) already are. Skipped
        // entirely when neither call site wrote anything (the hot path):
        // zero fjall calls, zero added latency.
        //
        // Dispatched via `spawn_blocking`, matching the durable committer
        // append below: `MetaStore::persist` performs a real, synchronous
        // `fsync`/`fdatasync` that can take milliseconds on a real device,
        // and running that directly on the async task would block a tokio
        // executor thread for the duration — starving OTHER concurrent
        // appends (including ones on the hot, no-new-name path) rather than
        // just adding latency to this one. `spawn_blocking` lets this
        // append's flush and every other in-flight append's own work
        // proceed on separate threads, so a burst of new-name appends
        // degrades to "N real fsyncs, however long that takes" rather than
        // serialising the whole engine behind them.
        if wrote_stream || wrote_types {
            let inner = self.inner.clone();
            tokio::task::spawn_blocking(move || inner.meta.persist())
                .await
                .expect("meta persist task panicked")
                .map_err(|e| {
                    AppendError::Backend(EngineError::Meta(e.to_string()))
                })?;
        }

        let (sid, events, first_stream_pos) = match pre {
            Pre::Conflict(actual) => {
                return Err(AppendError::Conflict { expected, actual });
            }
            Pre::Empty => {
                let last_global = self
                    .inner
                    .book
                    .lock()
                    .expect("book lock")
                    .payloads
                    .len()
                    .saturating_sub(1) as u64;
                return Ok(Appended {
                    version:              expected,
                    last_global_position: last_global,
                });
            }
            Pre::Proceed { sid, events, first_stream_pos } => {
                (sid, events, first_stream_pos)
            }
        };

        // Durable append + publish, both inside ONE `spawn_blocking` task
        // (bn-3nz). This is the structural fix for the "dropped append future
        // gaps the position sequence" hazard: the durable committer assigns
        // this batch's global position range as a permanent, irreversible
        // fact, and the book/index/meta publish plus its
        // [`PublishSequencer`] turn are what make that position visible and
        // let the NEXT position publish. If those two steps could be split by
        // a cancellation point — as they were when the publish lived back on
        // the async caller's future, awaiting `spawn_blocking(append)` and
        // then `turn()` separately — a caller that dropped its append future
        // (e.g. a `tokio::select!` timeout) between them would leave a
        // committed-but-never-published position, and `turn`'s strict
        // `== next` wait would stall every higher position forever.
        //
        // A `spawn_blocking` task is never cancelled: it runs to completion
        // even if this `.await`'s `JoinHandle` is dropped. Doing the assign
        // AND the publish inside it therefore makes the whole
        // assign→turn→publish sequence atomic against API-future
        // cancellation. A cancelled append still fully publishes (its events
        // are already durable, so full visibility is the only consistent
        // outcome — never a torn or missing slot); the caller simply never
        // observes the returned [`Appended`].
        let req = AppendRequest {
            stream_id: sid,
            category_id: CATEGORY_ID,
            first_stream_version: first_stream_pos,
            events,
        };
        // Owned copy of the records for the publish step, which now runs in a
        // `'static` blocking closure and so can no longer borrow `records`.
        // (No extra payload copy versus before: the pre-bn-3nz publish also
        // cloned each payload into the book — `Arc::from(rec.data)` — while
        // the durable `events` cloned it for the log; this just moves the
        // book's copy into the closure instead of taking it from the borrow.)
        let records_owned: Vec<RecordToAppend> = records.to_vec();
        let inner = self.inner.clone();
        let appended = tokio::task::spawn_blocking(
            move || -> Result<Appended, EngineError> {
                // Hold the per-stream gate (moved in from the async future) for
                // the whole commit+publish, releasing it only
                // when this closure ends — AFTER the publish
                // below. Because a `spawn_blocking` task always
                // runs to completion, the shard cannot be freed early by a
                // cancelled append future (bn-3nz).
                let _gate = gate;

                // 1) Durable append through the real committer — assigns the
                //    global position range. Off the async executor, on this
                //    blocking thread.
                let outcome = inner
                    .rt
                    .block_on(inner.appender.append(req))
                    .map_err(|e| EngineError::Append(e.to_string()))?;
                let (first_global, last_global) = match outcome {
                    AppendOutcome::Acked { first_position, last_position } => {
                        (first_position, last_position)
                    }
                    AppendOutcome::Indeterminate => {
                        return Err(EngineError::Append(
                            "indeterminate durability".to_string(),
                        ));
                    }
                };
                let frame_count = (last_global - first_global + 1) as u32;
                let watermark = last_global + 1;
                let last_stream_pos =
                    first_stream_pos + u64::from(frame_count) - 1;

                // 2) Wait this batch's turn to publish (bn-1s0): concurrent
                //    distinct-stream commits can ack out of position order, but
                //    the book/index/meta below all require strictly
                //    increasing-by-position writes. `_turn`'s `Drop` advances
                //    the sequence past `watermark` on EVERY exit path below
                //    (success, error, or unwind), so the next position never
                //    stalls behind this one — including if this closure returns
                //    the meta error below.
                let _turn = inner.publish_seq.turn(first_global, watermark);

                // 3) Publish: record book, active index (watermark-gated), meta
                //    head. No `.await` and no cancellation point exists past
                //    the position assignment above, so this always completes.
                {
                    let mut book = inner.book.lock().expect("book lock");
                    let stream_name = book.stream_name(sid);
                    for (i, rec) in records_owned.iter().enumerate() {
                        let gp = first_global + i as u64;
                        let payload = Payload {
                            stream_name:     stream_name.clone(),
                            message_type:    Arc::from(
                                rec.message_type.as_str(),
                            ),
                            data:            Arc::from(rec.data.as_slice()),
                            stream_position: first_stream_pos + i as u64,
                        };
                        debug_assert_eq!(book.payloads.len() as u64, gp);
                        book.payloads.push(payload);
                        book.stream_events.entry(sid).or_default().push(gp);
                    }
                    book.heads.insert(sid, last_stream_pos);
                }

                let batch = BatchEntry {
                    stream_id: sid,
                    first_stream_version: first_stream_pos,
                    frame_count,
                    first_global_pos: first_global,
                    ptr: EventPtr {
                        segment_id: ACTIVE_SEGMENT_ID,
                        offset:     first_global,
                    },
                };
                inner.active.apply_committed(watermark, &[batch]);

                let mut group = CommitGroup::new(watermark);
                group.stream_heads.push((
                    StreamId(sid),
                    Head {
                        version:         last_stream_pos,
                        global_position: last_global,
                    },
                ));
                inner
                    .meta
                    .apply_group(&group)
                    .map_err(|e| EngineError::Meta(e.to_string()))?;

                // Publish complete: every position `< watermark` is now
                // resident in the record book (and index/meta).
                // Advance the published watermark LAST, still
                // holding this batch's publish turn (`_turn`), so it
                // moves in strict global-position order and never announces a
                // position `read_global` cannot yet serve. This is the wake
                // that drives every live-tail subscriber parked
                // on `await_watermark_past`.
                inner.read_watermark.advance(watermark);

                Ok(Appended {
                    version:              Version::At(last_stream_pos),
                    last_global_position: last_global,
                })
            },
        )
        .await
        .expect("append task panicked")
        .map_err(AppendError::Backend)?;

        Ok(appended)
    }
}

impl SubscribeBackend for LogEngine {
    async fn watermark(&self) -> Result<u64, Self::Error> {
        Ok(self.inner.read_watermark.get())
    }

    async fn await_watermark_past(&self, pos: u64) -> Result<(), Self::Error> {
        self.inner.read_watermark.await_past(pos).await;
        Ok(())
    }
}

/// White-box unit tests for `bn-u6o` items 1 and 3:
/// [`LogEngine::run_roll_sealer`]'s bounded per-segment wait, and the bounded
/// TOTAL shutdown wait [`Inner::drop`] arranges via the shared
/// `shutdown_deadline`. Both drive `run_roll_sealer` directly with a synthetic
/// [`SegmentSummary`] whose `end_pos` an intentionally never-advanced
/// [`ActiveIndex`]/[`Book`] can never reach — the shape of a publish an
/// abandoned append future left stranded — so the skip path is forced
/// deterministically instead of waiting out the real ~10s default bound. This
/// needs access to private items (`run_roll_sealer`, `Book`, `SpinConfig`), so
/// it lives inside this module rather than as a `tests/` integration test.
#[cfg(test)]
mod seal_skip_tests {
    use mess_index::sealed::SealedStore;
    use mess_log::writer::SegmentSummary;

    use super::*;

    fn summary(segment_id: u64, base_pos: u64, end_pos: u64) -> SegmentSummary {
        SegmentSummary {
            segment_id,
            epoch: 1,
            base_pos,
            end_pos,
            batch_count: 1,
            event_count: end_pos - base_pos,
            content_len: 100,
        }
    }

    /// Item 1: a queued seal whose end the index/book never reaches must,
    /// once the (test-shrunk) per-segment spin bound elapses, be counted in
    /// `seals_skipped` and logged loudly — not silently dropped, which was
    /// the bn-1vu review nit this bone exists to fix.
    #[test]
    fn run_roll_sealer_counts_and_logs_a_spin_bound_skip() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let (tx, rx) = mpsc::channel();
        tx.send(summary(1, 0, 10)).expect("send");
        drop(tx); // close the channel so the loop drains this one item and exits

        let active = Arc::new(ActiveIndex::new()); // never advanced: applied_end stays 0
        let book = Arc::new(Mutex::new(Book::default())); // never advanced: len stays 0
        let sealed = Arc::new(SealedStore::new());
        let driver =
            SealDriver::new(Arc::clone(&sealed), tmp.path().join("sealed"));
        let seal_metrics = Arc::new(SealMetrics::new());
        let shutdown_deadline: Arc<OnceLock<Instant>> =
            Arc::new(OnceLock::new());
        // A tiny bound so the test does not wait out the real ~10s default.
        let spin = SpinConfig {
            per_seal_budget: Duration::from_millis(30),
            poll_interval:   Duration::from_millis(1),
        };

        let start = Instant::now();
        LogEngine::run_roll_sealer(
            rx,
            driver,
            active,
            book,
            tmp.path().to_path_buf(),
            Arc::clone(&seal_metrics),
            shutdown_deadline,
            spin,
        );
        let elapsed = start.elapsed();

        assert!(
            elapsed < Duration::from_secs(1),
            "must give up promptly, took {elapsed:?}"
        );
        assert_eq!(
            seal_metrics.snapshot().seals_skipped,
            1,
            "the abandoned seal must be counted as skipped"
        );
    }

    /// Item 3: the bounded TOTAL shutdown wait `Inner::drop` arranges. Two
    /// queued seals that can never complete must both end up skipped (and
    /// counted) within ONE shared deadline — not `per_seal_budget` each —
    /// proving the fix bounds the drain's TOTAL wall time rather than only
    /// each individual seal's wait.
    #[test]
    fn shutdown_deadline_bounds_total_wait_across_every_queued_seal() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let (tx, rx) = mpsc::channel();
        tx.send(summary(1, 0, 10)).expect("send");
        tx.send(summary(2, 10, 20)).expect("send");
        drop(tx);

        let active = Arc::new(ActiveIndex::new());
        let book = Arc::new(Mutex::new(Book::default()));
        let sealed = Arc::new(SealedStore::new());
        let driver =
            SealDriver::new(Arc::clone(&sealed), tmp.path().join("sealed"));
        let seal_metrics = Arc::new(SealMetrics::new());
        let shutdown_deadline: Arc<OnceLock<Instant>> =
            Arc::new(OnceLock::new());
        // A per-seal budget far bigger than the shared shutdown budget set
        // below — proving the SHUTDOWN deadline (not the per-seal one) is
        // what bounds this run, exactly as `Inner::drop` clamps the two.
        let spin = SpinConfig {
            per_seal_budget: Duration::from_secs(30),
            poll_interval:   Duration::from_millis(1),
        };
        // Mimic `Inner::drop`: publish the shared deadline BEFORE the sealer
        // loop's wait runs (the real drop sets it, then joins the thread).
        let budget = Duration::from_millis(150);
        shutdown_deadline.set(Instant::now() + budget).expect("first set");

        let start = Instant::now();
        LogEngine::run_roll_sealer(
            rx,
            driver,
            active,
            book,
            tmp.path().to_path_buf(),
            Arc::clone(&seal_metrics),
            shutdown_deadline,
            spin,
        );
        let elapsed = start.elapsed();

        assert!(
            elapsed < Duration::from_secs(1),
            "TOTAL shutdown drain must be bounded near `budget` regardless of \
             queue depth, took {elapsed:?}"
        );
        assert_eq!(
            seal_metrics.snapshot().seals_skipped,
            2,
            "both abandoned seals must be counted as skipped"
        );
    }
}
