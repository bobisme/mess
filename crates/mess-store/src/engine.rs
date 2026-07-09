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
//! - **Durability spine** — every non-empty [`append_batch`](Backend::append_batch)
//!   is a single-stream batch handed to a `mess-log` [`Committer`] running on
//!   a `mess-log` [`RealRuntime`]. The committer assigns dense, event-counted
//!   global positions (A1), does group commit under the configured
//!   [`Durability`], and advances the durable watermark. Because the
//!   [`Backend`] trait is async (tokio) and the committer is driven by
//!   `mess-log`'s own `block_on`/OS-thread runtime, the append crosses the
//!   seam via [`spawn_blocking`](tokio::task::spawn_blocking) — the minimal
//!   adapter between the two executors.
//! - **Exact-version gate** — a store-wide async mutex serialises the
//!   check-head → append → apply critical section, so two writers that loaded
//!   the same [`Version`] genuinely race and exactly one wins with a
//!   [`AppendError::Conflict`]. (The committer already serialises writes
//!   globally; this mutex just makes the version check atomic with the append.)
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
//! [`LogEngine::open`] over a populated directory, [`recover`](LogEngine::recover)
//! **rehydrates the book from the durable log**:
//!
//! - **Payload bytes** come back through `mess-log`'s read-side materialization
//!   seam — [`scanner::recover_segment_with_image`] +
//!   [`AcceptedBatch::frames`] (the additive payload-decode API this bone added
//!   to `mess-log`): each recovered batch yields its events' `(event_type_id,
//!   payload)` straight out of the durable segment image.
//! - **Names** cannot be re-derived from the log — it stores only interned
//!   numeric ids (`stream_id u64`, `event_type_id u32`), never their strings.
//!   The interner's `id → name` bijection is therefore persisted durably in the
//!   `mess-index` [`MetaStore`]'s `stream_names` / `type_names` tables (written
//!   the first time a name is interned, in [`append_batch`](LogEngine::append_batch))
//!   and reloaded here to reconstruct the interner before the payloads are
//!   materialised. This is the smallest durable surface for the engine's
//!   lightweight interner — the role `$registry` plays in the full design.
//!
//! After rehydration the book is dense from global position 0 again, so
//! post-reopen appends preserve the dense-position invariant and reads return
//! the exact pre-crash data. The single active segment is *resumed in place*
//! ([`SegmentWriter::resume`]) at the recovered `safe_offset`, so the durable
//! log continues to grow one contiguous prefix across any number of reopens.

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::Mutex;

use mess_index::sealed::{
    BlockCache, ReplaySet, SealDriver, SealInput, SealedStore,
};
use mess_index::{ActiveIndex, BatchEntry, EventPtr};
use mess_index::meta::{CommitGroup, Head, MetaStore, StreamId};
use mess_log::committer::{
    AppendOutcome, AppendRequest, Appender, Committer, Durability, EventInput,
};
use mess_log::lock::StoreLock;
use mess_log::runtime::{RealRuntime, Runtime};
use mess_log::scanner::{self, AcceptedBatch};
use mess_log::writer::{ResumeParams, SegmentParams, SegmentWriter};

use crate::backend::{
    AppendError, Appended, Backend, RecordToAppend, StoredRecord,
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
    stream_name: Arc<str>,
    message_type: Arc<str>,
    data: Arc<[u8]>,
    stream_position: u64,
}

/// The in-process record book + interners (see the module docs). All engine
/// clones share one `Arc<Mutex<Book>>`.
#[derive(Default)]
struct Book {
    /// `stream name → interned id` (ids start at 1; 0 is unused).
    stream_ids: HashMap<String, u64>,
    /// `interned id → stream name`, index `id - 1`.
    stream_names: Vec<Arc<str>>,
    /// `message type → interned event-type id` (ids start at 1).
    type_ids: HashMap<String, u32>,
    /// `interned event-type id → message type`, index `id - 1` (the reverse of
    /// `type_ids`, kept in lockstep so recovery can resolve a frame's
    /// `event_type_id` back to its name).
    type_names: Vec<Arc<str>>,
    /// Payloads by dense global position.
    payloads: Vec<Payload>,
    /// `stream id → global positions in stream order` (the hot-tier read index,
    /// kept in lockstep with `ActiveIndex::apply_committed`; index into it is
    /// the stream position). Serves O(limit) paged reads for unsealed streams.
    stream_events: HashMap<u64, Vec<u64>>,
    /// `stream id → last stream position` (the head).
    heads: HashMap<u64, u64>,
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
            debug_assert_eq!(id, self.stream_names.len() as u64 + 1, "dense stream ids");
            self.stream_names.push(Arc::from(name.as_str()));
            self.stream_ids.insert(name, id);
        }
    }

    fn load_type_names(&mut self, mut pairs: Vec<(u32, String)>) {
        pairs.sort_by_key(|(id, _)| *id);
        for (id, name) in pairs {
            debug_assert_eq!(id, self.type_names.len() as u32 + 1, "dense type ids");
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

    /// Resolve an event-type id to its name, or `None` (see [`stream_name_opt`]).
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
    segment_id: u64,
    base_pos: u64,
    epoch: u64,
    write_off: u64,
    next_batch_id: u64,
    next_pos: u64,
    batch_count: u64,
    event_count: u64,
}

/// Shared engine state behind one `Arc`.
struct Inner {
    rt: RealRuntime,
    appender: Appender,
    /// Kept alive so the commit thread lives as long as the engine; also the
    /// owner we would `shutdown` on a clean close.
    _committer: Committer<RealRuntime>,
    /// Held for the engine's lifetime: D9 single-writer-process enforcement.
    _lock: StoreLock,
    active: Arc<ActiveIndex>,
    sealed: Arc<SealedStore>,
    block_cache: BlockCache,
    meta: MetaStore,
    book: Mutex<Book>,
    /// Serialises the exact-version critical section across appends.
    append_gate: tokio::sync::Mutex<()>,
    /// The store root (sealed sidecars live under `dir/sealed`).
    dir: PathBuf,
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
    pub durability: Durability,
    /// Active-segment size in bytes (preallocated at open).
    pub segment_size: u64,
    /// Dedupe-window capacity for the meta store.
    pub dedupe_capacity: usize,
}

impl Default for EngineOptions {
    fn default() -> Self {
        EngineOptions {
            // `Process`: ack the moment the covering write returns. The record
            // book and index are in-process, so per-batch fsync buys nothing
            // for correctness here; benches override this with `Group`.
            durability: Durability::Process,
            segment_size: 256 * 1024 * 1024,
            dedupe_capacity: mess_index::meta::DEFAULT_DEDUPE_CAPACITY,
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

        // Recovery on open (F6 + bn-20b): rebuild the active index AND rehydrate
        // the record book from the durable log, and learn how to resume the
        // active segment.
        let active = Arc::new(ActiveIndex::new());
        let (book, plan) = Self::recover(&rt, dir, &active, &meta)?;

        let seg_path = active_segment_path(dir);
        let writer = match plan {
            ResumePlan::Fresh => {
                let params = SegmentParams {
                    segment_id: ACTIVE_SEGMENT_ID,
                    base_pos: 0,
                    epoch: 1,
                    prev_segment_epoch: 0,
                    created_unix_nanos: 0,
                    segment_size: opts.segment_size,
                };
                SegmentWriter::create(&rt.fs(), &seg_path, params)
                    .map_err(|e| EngineError::Open(format!("segment create: {e}")))?
            }
            ResumePlan::Resume(info) => {
                // Resume the existing active segment in place (no header
                // rewrite, no epoch bump): new appends extend the one
                // contiguous committed prefix from the recovered safe_offset.
                let params = ResumeParams {
                    segment_id: info.segment_id,
                    base_pos: info.base_pos,
                    epoch: info.epoch,
                    segment_size: opts.segment_size,
                    write_off: info.write_off,
                    next_batch_id: info.next_batch_id,
                    next_pos: info.next_pos,
                    batch_count: info.batch_count,
                    event_count: info.event_count,
                };
                SegmentWriter::resume(&rt.fs(), &seg_path, params)
                    .map_err(|e| EngineError::Open(format!("segment resume: {e}")))?
            }
        };
        let committer = Committer::spawn(&rt, writer, opts.durability);
        let appender = committer.appender();

        Ok(LogEngine {
            inner: Arc::new(Inner {
                rt,
                appender,
                _committer: committer,
                _lock: lock,
                active,
                sealed: Arc::new(SealedStore::new()),
                block_cache: BlockCache::disabled(),
                meta,
                book: Mutex::new(book),
                append_gate: tokio::sync::Mutex::new(()),
                dir: dir.to_path_buf(),
            }),
        })
    }

    /// Rehydrate the record book and rebuild the active index from the durable
    /// log (bn-20b), and decide how to resume the active segment.
    ///
    /// One scan of the single active segment (the engine does not roll — see
    /// [`seal_active`]) does everything: `mess-log`'s
    /// [`recover_segment_with_image`](scanner::recover_segment_with_image)
    /// gives the accepted committed prefix plus the durable image, then
    /// [`AcceptedBatch::frames`] materialises each event's `(event_type_id,
    /// payload)` — resolved to names through the interner reloaded from the
    /// durable `stream_names`/`type_names` meta tables. The same accepted
    /// batches seed the active index (the F6 rebuild), so a fresh open over a
    /// populated dir returns the exact pre-crash data, not silent-empty.
    fn recover(
        rt: &RealRuntime,
        dir: &Path,
        active: &ActiveIndex,
        meta: &MetaStore,
    ) -> Result<(Book, ResumePlan), EngineError> {
        // Reconstruct the interner (both directions) from the durable id→name
        // tables first, so materialised payloads can resolve their names.
        let mut book = Book::default();
        book.load_stream_names(
            meta.stream_names().map_err(|e| EngineError::Meta(e.to_string()))?,
        );
        book.load_type_names(
            meta.type_names().map_err(|e| EngineError::Meta(e.to_string()))?,
        );

        let seg_path = active_segment_path(dir);
        if !seg_path.exists() {
            return Ok((book, ResumePlan::Fresh));
        }

        let (rec, image) = scanner::recover_segment_with_image(&rt.fs(), &seg_path)
            .map_err(|e| EngineError::Open(format!("recover: {e}")))?;
        let Some(header) = rec.header else {
            // Existing file with no valid header: no committed batches of this
            // generation. Create a fresh segment over it.
            return Ok((book, ResumePlan::Fresh));
        };

        // Materialise the committed prefix into the book in ascending global
        // position (== on-disk commit order for the single active segment) and
        // rebuild the active index from the same accepted batches.
        let mut order: Vec<&AcceptedBatch> = rec.accepted.iter().collect();
        order.sort_by_key(|b| b.first_global_pos);
        let mut entries: Vec<BatchEntry> = Vec::with_capacity(order.len());
        for b in &order {
            let sid = b.stream_id;
            let stream_name = book.stream_name_opt(sid).ok_or_else(|| {
                EngineError::Meta(format!("recover: no interned name for stream_id {sid}"))
            })?;
            // bn-221: `frames` is fallible (misuse-resistant against a wrong
            // image) but this caller always passes the exact image `b` was
            // recovered from, so the error path is unreachable in practice —
            // still propagated rather than unwrapped so a future refactor
            // that breaks that invariant fails loudly instead of panicking.
            let frames = b
                .frames(&image)
                .map_err(|e| EngineError::Open(format!("recover: {e}")))?;
            for (k, frame) in frames.enumerate() {
                let gp = b.first_global_pos + k as u64;
                debug_assert_eq!(book.payloads.len() as u64, gp, "dense rehydration");
                let message_type =
                    book.type_name_opt(frame.event_type_id).ok_or_else(|| {
                        EngineError::Meta(format!(
                            "recover: no interned name for event_type_id {}",
                            frame.event_type_id
                        ))
                    })?;
                let stream_position = b.first_stream_version + k as u64;
                book.payloads.push(Payload {
                    stream_name: stream_name.clone(),
                    message_type,
                    data: Arc::from(frame.payload),
                    stream_position,
                });
                book.stream_events.entry(sid).or_default().push(gp);
                book.heads.insert(sid, stream_position);
            }
            entries.push(BatchEntry {
                stream_id: sid,
                first_stream_version: b.first_stream_version,
                frame_count: b.frame_count,
                first_global_pos: b.first_global_pos,
                ptr: EventPtr { segment_id: header.segment_id, offset: b.offset },
            });
        }
        active.apply_committed(rec.next_pos, &entries);

        let info = ResumeInfo {
            segment_id: header.segment_id,
            base_pos: header.base_pos,
            epoch: header.epoch,
            write_off: rec.safe_offset,
            next_batch_id: rec.next_batch_id,
            next_pos: rec.next_pos,
            batch_count: rec.accepted.len() as u64,
            event_count: rec.next_pos - header.base_pos,
        };
        Ok((book, ResumePlan::Resume(info)))
    }

    /// Test/diagnostic: total events committed to the record book.
    #[must_use]
    pub fn total_events(&self) -> usize {
        self.inner.book.lock().expect("book lock").payloads.len()
    }

    /// Seal the current active segment into the cold [`SealedStore`], driving
    /// the real `mess-index` [`SealDriver`] (sidecar encode → durable write →
    /// install → evict-from-active). After this, `read_stream` for the sealed
    /// streams routes through the [`ReplaySet`] cold path.
    ///
    /// The interim `mess-log` committer owns a single segment and does not roll
    /// (segment roll under a live committer is unimplemented — see the bn-20b
    /// openQuestions), so this is exposed as an explicit trigger rather than
    /// fired automatically on roll. It is what the sealed-replay bench and the
    /// sealed read path exercise.
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
                let payloads: Vec<Vec<u8>> =
                    book.payloads[..event_count].iter().map(|p| p.data.to_vec()).collect();
                input.with_payloads(payloads)
            } else {
                input
            }
        };
        let driver =
            SealDriver::new(Arc::clone(&self.inner.sealed), self.inner.dir.join("sealed"));
        std::fs::create_dir_all(self.inner.dir.join("sealed"))
            .map_err(|e| EngineError::SealedRead(format!("mkdir sealed: {e}")))?;
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
        // The hot tail (if the stream also has unsealed batches during handoff).
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
    Proceed { sid: u64, events: Vec<EventInput>, first_stream_pos: u64 },
}

impl LogEngine {
    /// Persist newly-interned id→name mappings to the durable meta store so a
    /// reopen can reconstruct the interner (bn-20b).
    fn persist_new_names(
        &self,
        stream: Option<(u64, String)>,
        types: &[(u32, String)],
    ) -> Result<(), mess_index::meta::MetaError> {
        if let Some((id, name)) = stream {
            self.inner.meta.put_stream_name(id, &name)?;
        }
        for (id, name) in types {
            self.inner.meta.put_type_name(*id, name)?;
        }
        Ok(())
    }
}

/// The active segment's on-disk path.
fn active_segment_path(dir: &Path) -> PathBuf {
    dir.join(format!("seg-{ACTIVE_SEGMENT_ID:08}.log"))
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
        // hot index is served through the real sealed-replay path (`ReplaySet`).
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
        // Serialise the exact-version critical section.
        let _gate = self.inner.append_gate.lock().await;

        // Intern + check the expected version under the book lock, capturing
        // any newly-assigned interner names, then drop the lock (we must not
        // hold a std mutex across the append await, nor across fjall I/O).
        let (pre, new_stream, new_types) = {
            let mut book = self.inner.book.lock().expect("book lock");
            let mut new_stream: Option<(u64, String)> = None;
            let mut new_types: Vec<(u32, String)> = Vec::new();
            let (sid, sid_new) = book.intern_stream(stream_id);
            if sid_new {
                new_stream = Some((sid, stream_id.to_string()));
            }
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
                Pre::Proceed { sid, events, first_stream_pos: expected.next_position() }
            };
            (pre, new_stream, new_types)
        };

        // Persist any newly-interned id→name mappings durably (buffered) so a
        // reopen can resolve them. This happens even on a conflict/empty path:
        // the id was assigned in-process regardless, and the interner is dense,
        // so a later successful append to this stream must find its name
        // persisted (a first-touch that conflicts — e.g. a stale-expected
        // append to a fresh stream — still interns the name).
        self.persist_new_names(new_stream, &new_types)
            .map_err(|e| AppendError::Backend(EngineError::Meta(e.to_string())))?;

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
                    version: expected,
                    last_global_position: last_global,
                });
            }
            Pre::Proceed { sid, events, first_stream_pos } => (sid, events, first_stream_pos),
        };

        // Durable append through the real committer, off the async executor.
        let req = AppendRequest {
            stream_id: sid,
            category_id: CATEGORY_ID,
            first_stream_version: first_stream_pos,
            events,
        };
        let appender = self.inner.appender.clone();
        let rt = self.inner.rt.clone();
        let outcome = tokio::task::spawn_blocking(move || rt.block_on(appender.append(req)))
            .await
            .expect("append task panicked")
            .map_err(|e| AppendError::Backend(EngineError::Append(e.to_string())))?;

        let (first_global, last_global) = match outcome {
            AppendOutcome::Acked { first_position, last_position } => {
                (first_position, last_position)
            }
            AppendOutcome::Indeterminate => {
                return Err(AppendError::Backend(EngineError::Append(
                    "indeterminate durability".to_string(),
                )));
            }
        };
        let frame_count = (last_global - first_global + 1) as u32;
        let watermark = last_global + 1;
        let last_stream_pos = first_stream_pos + u64::from(frame_count) - 1;

        // Publish: record book, active index (watermark-gated), meta head.
        {
            let mut book = self.inner.book.lock().expect("book lock");
            let stream_name = book.stream_name(sid);
            for (i, rec) in records.iter().enumerate() {
                let gp = first_global + i as u64;
                let payload = Payload {
                    stream_name: stream_name.clone(),
                    message_type: Arc::from(rec.message_type.as_str()),
                    data: Arc::from(rec.data.as_slice()),
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
            ptr: EventPtr { segment_id: ACTIVE_SEGMENT_ID, offset: first_global },
        };
        self.inner.active.apply_committed(watermark, &[batch]);

        let mut group = CommitGroup::new(watermark);
        group.stream_heads.push((
            StreamId(sid),
            Head { version: last_stream_pos, global_position: last_global },
        ));
        self.inner
            .meta
            .apply_group(&group)
            .map_err(|e| AppendError::Backend(EngineError::Meta(e.to_string())))?;

        Ok(Appended {
            version: Version::At(last_stream_pos),
            last_global_position: last_global,
        })
    }
}
