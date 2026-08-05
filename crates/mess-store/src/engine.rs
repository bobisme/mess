//! The composed production [`Backend`]: `mess-log` (durability, ordering,
//! recovery) + `mess-index` (hot [`ActiveIndex`], sealed corpus) behind the
//! [`EventStore`](crate::EventStore) facade.
//!
//! This is the Phase 4 "engine swap": the facade's default backend moves off
//! the interim in-memory [`MockBackend`](crate::mock) onto the real log +
//! index stack, with **zero** changes to the [`Backend`] trait or the facade
//! above it.
//!
//! # How the pieces compose
//!
//! - **Flat append owner** — async producers enqueue owned intents into one
//!   count- and byte-bounded ring. One OS thread validates expected versions in
//!   dequeue order, stages registry ids, owns the `mess-log` segment writer,
//!   assigns positions, writes, issues the covering durability barrier, applies
//!   index/head effects, advances the published watermark, and completes
//!   callers. There is no blocking-pool hop, per-stream gate, second committer
//!   thread, or publish sequencer. Dropping a caller after enqueue only drops
//!   its completion receiver; the owner still retires and publishes the intent.
//! - **Hot reads** — committed batches are applied to a `mess-index`
//!   [`ActiveIndex`] via `apply_committed` — with their **real** `(segment_id,
//!   offset)` pointer from the committer's ack — **only after the committer
//!   acks** the durable append (post-ack discipline): the index never exposes a
//!   position the durable watermark has not already covered. `read_stream`
//!   enumerates a stream's positions through the active index (unsealed tier)
//!   unioned with the sealed tier, and materialises bytes through the bounded
//!   [`BlockReader`] (see "Block-native reads" below).
//! - **Cold reads** — sealed segments are served through
//!   [`ReplaySet`](mess_index::sealed::ReplaySet) over a
//!   [`SealedStore`](mess_index::sealed::SealedStore), so an
//!   [`EventStore::load`](crate::EventStore::load) of a sealed corpus runs the
//!   real sealed-replay path, with payload bytes reassembled from the columnar
//!   `.pcol` sidecar where one exists.
//! - **Derived state** — stream heads and active pointers are published in
//!   memory after the covering write/barrier and rebuilt from the log on open.
//!   The append path writes no metadata anywhere: the log is the only durable
//!   authority the engine has (bn-fj34).
//! - **Recovery** — on open the engine enumerates segment files and runs
//!   `mess-log` `recover_whole_log` (fast path + advisory manifest) and
//!   `mess-index` `rebuild` (F6) to rehydrate the active index.
//!
//! # Block-native reads: no all-history payload mirror (bn-2ib)
//!
//! The [`Backend`] seam must return [`StoredRecord`]s carrying
//! `message_type: String` and `data: Vec<u8>`, but the index tier is
//! pointer-only. Before bn-2ib the engine bridged that gap with an all-history
//! in-process payload mirror (the "record book": one owned payload per
//! committed global position, rebuilt by decoding every durable event on every
//! open — memory and startup proportional to total history). That mirror is
//! **gone**. Reads now resolve *positions* through the real index / sealed
//! tiers and fetch *bytes* from the durable blocks themselves:
//!
//! - **Positions** come from the hot [`ActiveIndex`] (unsealed tier) and the
//!   sealed [`ReplaySet`] pointer sidecars (cold tier), whose [`EventPtr`]s are
//!   now the batch's **real** `(segment_id, byte offset)` — the committer
//!   reports the placement in each [`AppendOutcome::Acked`] and recovery
//!   re-derives it from the scan.
//! - **Bytes** come from a bounded, bytes-weighted **decoded-capsule cache**
//!   ([`BlockReader`]): a miss `pread`s the one batch at its pointer,
//!   re-validates it through the recovery scanner's byte layer (the mandatory
//!   A4/A12 CRC — [`scanner::accepted_batch_at`]), decodes its frames once into
//!   an immutable [`DecodedBatch`] arena, and caches it under `(segment_id,
//!   offset)`. Eviction can never change results: the miss path is the same
//!   durable bytes.
//! - **Sealed payloads** are served from the columnar `.pcol` sidecar (D6) when
//!   the covering segment carries one: the batch's frame *identities*
//!   (event-type ids, versions) still come from the raw batch — no sealed
//!   sidecar stores event-type ids today, see the REPORT — but its payload
//!   *bytes* are reassembled through [`SealedPayloadIndex::reassemble_range`],
//!   falling back byte-identically to the raw batch on any `.pcol` decode error
//!   (verify-on-seal proved the two equal when the sidecar was written; the log
//!   stays truth).
//! - **Names** come from the log itself (`bn-2di`). Frames carry only interned
//!   numeric ids (`stream_id u64`, `event_type_id u32`), so the `id → name`
//!   bijection is written as `$registry` records — spec 04's system stream,
//!   `stream_id 0` — by the same flat owner, in the same segment.
//!   [`append_batch`](Backend::append_batch) writes a new name's `*Registered`
//!   record and the batch that first references its id as ONE direct ordered
//!   unit, so the registration lands at a LOWER global position than its first
//!   use, in the same commit group, under the same barrier. That ordering is
//!   what makes the name durable "for free": recovery accepts a contiguous
//!   prefix, so no crash can keep the reference and lose the registration, and
//!   there is no second storage system to `fsync` (the `stream_names` /
//!   `type_names` key-value tables, and the barrier bn-150 needed to keep them
//!   co-durable, are **deleted** — bn-2di, then bn-fj34). These are ordinary v3
//!   event frames, so they consume canonical global positions.
//!   [`read_global`](Backend::read_global) filters stream 0 from application
//!   results in both hot and sealed tiers; later visible records keep their
//!   assigned positions and can therefore have gaps. Public global positions
//!   and subscription cursors are opaque monotone resume tokens, never dense
//!   application-event indexes.
//!
//! What remains of the book is deliberately tiny and history-**independent**
//! per event: the folded [`registry::RegistryState`] plus the per-stream head
//! versions ([`Book`]). On open, [`recover`](LogEngine::recover) reconstructs
//! exactly that — spec 04 §7.1's three layers (scan the log without resolving a
//! single name; fold `$registry`; resolve, and refuse to open if any referenced
//! id has no name) — with heads + hot index from a header/batch-metadata scan
//! of the **unsealed** segments only (no payload frame is decoded), and
//! heads/watermark for fully-sealed segments straight from their durable
//! sidecar directories without reading the segment bytes at all (a sealed
//! segment's `$registry` batches, if it has any, are resolved through its
//! per-stream pointer index and `pread` individually — never a full scan). The
//! single active segment is *resumed in place* ([`SegmentWriter::resume`]) at
//! the recovered `safe_offset`, so the durable log continues to grow one
//! contiguous prefix across any number of reopens.
//!
//! [`SealedPayloadIndex::reassemble_range`]: mess_index::sealed::SealedPayloadIndex::reassemble_range
//! [`AppendOutcome::Acked`]: mess_log::committer::AppendOutcome::Acked

use std::cmp::Ordering as CmpOrdering;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc;
use std::sync::{Mutex, OnceLock, RwLock};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use mess_index::sealed::regdelta::{RegistryDelta, reg_path};
use mess_index::sealed::{
    BlockCache, NoDicts, PACK_FORMAT_VERSION, PackIdentity, ReplaySet,
    SealBatch, SealDriver, SealInput, SealMetrics, SealStream,
    SealedSegmentIndex, SealedSegmentRef, SealedStore,
};
use mess_index::{ActiveIndex, BatchEntry, EventPtr, GlobalEntry, StreamEntry};
use mess_log::committer::{
    AppendOutcome, AppendRequest, ChainInit, CommitterMetricsHandle,
    DirectAppendRequest, DirectBatchEvents, DirectBatchOutcome,
    DirectCommitter, Durability, EventInput, LatencySnapshot, Roller,
};
use mess_log::encode::{BatchInput, PreparedBatch, Subframe};
use mess_log::fold_chain::ChainHead;
use mess_log::footer_ext::{
    SealPackIdentity, SealSummary, decode_extension, encode_sealed_footer,
};
use mess_log::format::{
    CHAIN_LEN, HEADER_LEN, MARKER_LEN, MAX_BATCH_LEN,
    SEAL_PACK_IDENTITY_HDRDIR_BLAKE3, SUBFRAME_HDR_LEN,
};
use mess_log::lock::StoreLock;
// bn-11ba: the shared lock-free metrics primitives. The composed
// observability surface records with the same fixed-bucket histogram and
// relaxed counter the committer already uses on its own barrier — no
// second metrics core, and nothing on the hot path more expensive than a
// `fetch_add` plus a timestamp diff (spec 03 §2.6's budget).
use mess_log::metrics::{Counter, LatencyHistogram};
use mess_log::runtime::{
    FileHandle, Fs as LogFs, OpenOpts, RealRuntime, Runtime,
};
use mess_log::scanner::{self, AcceptedBatch};
use mess_log::sealer::{read_extension, read_trailer};
use mess_log::watermark::Watermark;
use mess_log::writer::{
    ResumeParams, SegmentParams, SegmentSummary, SegmentWriter,
};
use quick_cache::sync::{Cache, DefaultLifecycle};
use quick_cache::{DefaultHashBuilder, OptionsBuilder, Weighter};
use tokio::sync::{
    OwnedSemaphorePermit, Semaphore, mpsc as tokio_mpsc, oneshot,
};

use crate::backend::{
    AppendError, Appended, Backend, GlobalPage, OwnedAppendBatch,
    OwnedTypeLayout, RecordToAppend, StoredRecord, SubscribeBackend,
};
use crate::observability::{
    AUTHORITY_MODEL, AcceleratorReport, BacklogReport, DurabilityMode,
    DurabilityReport, EngineObservability, FallbackReport, OwnerReport,
    SealedRepresentation, SealedSegmentReport, StateReport,
};
use crate::registry::{self, RegistryRecord};
use crate::sealed_candidate::{self, RefutationReason, SealedCandidateHealth};
use crate::version::Version;

mod backend_impl;
mod open;
mod read;
mod seal;

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
    /// Registry (name↔id) integrity failed: the `$registry` fold rejected the
    /// committed records, ids are not dense, or a committed batch references
    /// a `stream_id`/`event_type_id` no registration in the log ever named.
    ///
    /// `bn-fj34` renamed this from `Meta`. It never meant "a metadata database
    /// failed" — the engine has none, and since `bn-2di` the name↔id bijection
    /// is the log's own `$registry` stream — but the old name and its `meta:`
    /// display prefix said otherwise.
    #[error("registry: {0}")]
    Registry(String),
    /// A sealed-corpus read failed to decode.
    #[error("sealed read: {0}")]
    SealedRead(String),
    /// A block-native read of a committed batch failed (bn-2ib): the pointer
    /// could not be resolved to a CRC-valid batch of the expected identity,
    /// even via the locate-by-scan fallback.
    #[error("read: {0}")]
    Read(String),
}

/// The engine's live registry + per-stream heads (see the module docs).
/// All engine clones share one `Arc<RwLock<Book>>`.
///
/// **Never acquire this lock while already holding it** (`bn-18ab`). It was an
/// `Arc<Mutex<Book>>` until the reader-contention workload showed 8 concurrent
/// readers costing the writer 16-20%; readers hold it only to look things up,
/// so a shared lock removed almost all of that. But `std::sync::RwLock` does
/// not guarantee read recursion — a thread that takes a second read guard
/// while holding one can deadlock against a writer queued between them, and
/// unlike a `Mutex` self-deadlock this one is load-dependent and will not show
/// up in a quiet test run.
///
/// Every acquisition today is a single lookup in its own scope, and the two
/// owner paths that call further engine code (`commit_plans`, `plan_domain`)
/// take their guard in a block or an `if` condition that drops before the
/// call. Keep it that way: if a call site ever needs the book across a nested
/// call, copy out what it needs and drop the guard first.
///
/// Since bn-2ib this holds **no payload bytes and no per-event state**: its
/// size is O(streams + event types), independent of history length. Payloads
/// live in the durable blocks and are read through the bounded
/// [`BlockReader`]; per-stream position enumeration lives in the
/// [`ActiveIndex`] / sealed sidecars.
///
/// # One registry representation (`bn-2di`, review F2)
///
/// The `id ↔ name` bijection is NOT a second, engine-private interner: it *is*
/// [`registry::RegistryState`] — the same fold `$registry` replay produces, the
/// same one [`Registry`](crate::registry::Registry) validates against, the same
/// one that enforces every REG-rule. `stream_arcs`/`type_arcs` below are a pure
/// **projection** of it (an `Arc<str>` per id so the read path clones a
/// refcount instead of a `String`), rebuilt from it and never written
/// independently. Every mutation of the registry — an id the engine mints for a
/// new stream/message type, and a record a `Registry<LogEngine>` writer appends
/// to `$registry` — goes through
/// [`apply_registration`](Book::apply_registration) and hence through
/// `RegistryState::apply`. There is exactly one writer and exactly one fold.
#[derive(Default)]
struct Book {
    /// THE registry (§4-§7 of spec 04). Reserved ids resolve from spec text;
    /// everything else is folded from `$registry` records.
    registry:              registry::RegistryState,
    /// Projection of `registry`: `stream_id → current name`, index `id - 1`.
    /// `None` where an id in `1..=hwm` has no record (only reachable from a
    /// hand-written log — the engine's own ids are dense).
    stream_arcs:           Vec<Option<Arc<str>>>,
    /// Projection of `registry`: `event_type_id → current name`, index `id -
    /// 1`.
    type_arcs:             Vec<Option<Arc<str>>>,
    /// `stream id → last stream position` (the head). Includes stream `0`.
    heads:                 HashMap<u64, u64>,
    /// The next version to stamp on a `$registry` batch (`bn-2di`).
    ///
    /// Stream 0 is the one stream whose batches are minted as a side effect of
    /// appends to *other* streams, several of which can be in flight at once.
    /// So its version cannot be derived from `heads[0]` (which only advances
    /// at publish time, after the ack) — two concurrent new-stream appends
    /// would both read the same head and write two batches claiming the
    /// same version. It is allocated here instead, at **submit** time,
    /// by the single owner after the registration earns a positive ack. Owner
    /// dequeue order == file order == global-position order == version order.
    ///
    /// Seeded on open from the folded registry's record count (stream 0's
    /// versions are dense from 0, so the count *is* the next version).
    registry_next_version: u64,
    /// Sticky: an already-landed `$registry` record could not be folded into
    /// the live registry (an internal invariant failure).
    ///
    /// The owner stages ids locally and folds them only after a positive ack,
    /// so ordinary encode/write/barrier failures cannot set this. If an
    /// impossible fold mismatch does, reopening from the log is the only exit.
    registry_lost:         bool,
}

impl Book {
    /// An empty book. The reserved ids (REG1) need no seeding: `RegistryState`
    /// answers `$registry`/`RegistryEventV1` from spec text, not from any log
    /// event (REG2) — which is exactly what makes the bootstrap non-circular
    /// (§2): a book that has read no bytes at all can already name stream 0 and
    /// event type 0, which is all `RegistryRecord::decode` needs to interpret
    /// the first `$registry` frame ever written.
    fn new() -> Self { Book::default() }

    /// Build the book from a folded [`RegistryState`] — the log-derived path
    /// (`bn-2di`), replacing the fjall `stream_names`/`type_names` load.
    ///
    /// Both namespaces MUST be dense `1..=hwm`. The engine mints dense ids
    /// (`hwm + 1`) and so does [`Registry`](crate::registry::Registry) (§4.1),
    /// so a hole here means log corruption the fold's own REG-rules did not
    /// catch — and density is load-bearing for the open-time integrity check
    /// (see [`finish_recovery`](LogEngine::finish_recovery)), so it must be
    /// loud.
    fn from_registry(
        state: registry::RegistryState,
        registry_events: u64,
    ) -> Result<Self, EngineError> {
        for id in 1..=state.stream_high_water_mark() {
            if state.stream_name(id).is_none() {
                return Err(EngineError::Registry(format!(
                    "stream ids are not dense — no name for stream_id {id} \
                     (high-water {})",
                    state.stream_high_water_mark()
                )));
            }
        }
        for id in 1..=state.event_type_high_water_mark() {
            if state.event_type_name(id).is_none() {
                return Err(EngineError::Registry(format!(
                    "event-type ids are not dense — no name for event_type_id \
                     {id} (high-water {})",
                    state.event_type_high_water_mark()
                )));
            }
        }
        let mut book = Book::new();
        book.registry = state;
        book.registry_next_version = registry_events;
        book.rebuild_arcs();
        Ok(book)
    }

    /// Re-project `registry` into the `Arc<str>` name caches. O(ids), run only
    /// on open and after a `$registry` write that is not a plain engine mint
    /// (an alias can change any id's current name) — never on the hot path.
    fn rebuild_arcs(&mut self) {
        self.stream_arcs = (1..=self.registry.stream_high_water_mark())
            .map(|id| self.registry.stream_name(id).map(Arc::from))
            .collect();
        self.type_arcs = (1..=self.registry.event_type_high_water_mark())
            .map(|id| self.registry.event_type_name(id).map(Arc::from))
            .collect();
    }

    /// Fold ONE registry record into the live state — the single mutation seam
    /// for names, used by the engine's own mints and by a `$registry` write
    /// arriving through the [`Backend`] seam alike (`bn-2di`, review F2).
    ///
    /// The `Arc` projection is extended in place for the dense-append case (the
    /// engine's mints, and `Registry`'s own `hwm + 1` allocation) and rebuilt
    /// otherwise (aliases, or a hand-written sparse id).
    fn apply_registration(
        &mut self,
        record: RegistryRecord,
    ) -> Result<(), EngineError> {
        let dense_stream = matches!(
            &record,
            RegistryRecord::StreamRegistered { stream_id, .. }
                if *stream_id == self.registry.stream_high_water_mark() + 1
        );
        let dense_type = matches!(
            &record,
            RegistryRecord::EventTypeRegistered { event_type_id, .. }
                if *event_type_id == self.registry.event_type_high_water_mark() + 1
        );
        self.registry
            .apply::<std::convert::Infallible>(record)
            .map_err(|e| EngineError::Append(format!("$registry: {e}")))?;
        if dense_stream {
            let id = self.registry.stream_high_water_mark();
            self.stream_arcs.push(self.registry.stream_name(id).map(Arc::from));
        } else if dense_type {
            let id = self.registry.event_type_high_water_mark();
            self.type_arcs
                .push(self.registry.event_type_name(id).map(Arc::from));
        } else {
            self.rebuild_arcs();
        }
        Ok(())
    }

    /// Allocate `n` consecutive `$registry` stream versions, returning the
    /// first. See [`registry_next_version`](Book::registry_next_version).
    fn alloc_registry_versions(&mut self, n: u64) -> u64 {
        let first = self.registry_next_version;
        self.registry_next_version += n;
        first
    }

    /// `$registry`'s own head as the version allocator sees it — the `expected`
    /// value a [`Backend`]-seam write to stream 0 must present. Distinct from
    /// `head(0)` (which lags until the publish), so two writes to `$registry`
    /// can never both be admitted against the same version.
    fn registry_head(&self) -> Version {
        match self.registry_next_version {
            0 => Version::NoStream,
            n => Version::At(n - 1),
        }
    }

    fn head(&self, stream_id: u64) -> Version {
        match self.heads.get(&stream_id) {
            Some(&pos) => Version::At(pos),
            None => Version::NoStream,
        }
    }

    /// Resolve a stream id to its name, or `None` if the registry has no such
    /// id (a durable-name gap — surfaced as a typed error, never a panic).
    ///
    /// The reserved id `0` (`$registry`, REG1) is answered from spec text, not
    /// from `stream_arcs` — which is indexed `id - 1` and so has no slot for it
    /// at all. Note the underflow this guard prevents: `(0 - 1) as usize` on a
    /// `u64` panics in debug and wraps to `usize::MAX` in release.
    fn stream_name_opt(&self, id: u64) -> Option<Arc<str>> {
        if id == registry::REGISTRY_STREAM_ID {
            return Some(Arc::from(registry::RESERVED_STREAM_NAME));
        }
        self.stream_arcs.get((id - 1) as usize).cloned().flatten()
    }

    /// Resolve an event-type id to its name, or `None` (see
    /// [`stream_name_opt`] — including the reserved-id underflow guard).
    fn type_name_opt(&self, id: u32) -> Option<Arc<str>> {
        if id == registry::REGISTRY_EVENT_TYPE_ID {
            return Some(Arc::from(registry::RESERVED_EVENT_TYPE_NAME));
        }
        self.type_arcs.get((id - 1) as usize).cloned().flatten()
    }
}

// ---------------------------------------------------------------------------
// Block-native reads (bn-2ib): the decoded-capsule cache + block reader
// ---------------------------------------------------------------------------

/// The engine's concrete filesystem/file types (the same [`Fs`] seam the
/// writer and recovery scanner use — see [`mess_log::runtime`]).
type EngineFs = <RealRuntime as Runtime>::Fs;
type EngineFile = <EngineFs as LogFs>::File;

/// One decoded, CRC-validated committed batch — the immutable block-backed
/// record view every read path materialises [`StoredRecord`]s from. Shared as
/// an `Arc` through the bounded capsule cache, so a page of reads over a hot
/// batch is an `Arc` bump plus per-record slice copies, never a re-decode.
///
/// Frame `k` (0-based within the batch) is the event at stream position
/// `first_stream_version + k`, global position `first_global_pos + k`, with
/// event-type id `type_ids[k]` and the corresponding payload.
struct DecodedBatch {
    stream_id:            u64,
    first_stream_version: u64,
    first_global_pos:     u64,
    frame_count:          u32,
    /// Per-frame interned event-type id.
    type_ids:             Vec<u32>,
    /// One owned byte arena. Disk-decoded capsules pack only payload bytes;
    /// producer-prepared append capsules adopt the final framed bytes.
    data:                 Vec<u8>,
    /// How each payload is located in `data`.
    payloads:             PayloadLayout,
}

enum PayloadLayout {
    /// `frame_count + 1` compact-arena boundaries.
    Arena(Vec<u32>),
    /// One `(start, end)` range per payload in producer-prepared framed bytes.
    Framed(Vec<(u32, u32)>),
}

impl DecodedBatch {
    /// Frame `k`'s payload bytes.
    #[inline]
    fn payload(&self, k: usize) -> &[u8] {
        let (start, end) = match &self.payloads {
            PayloadLayout::Arena(offs) => (offs[k], offs[k + 1]),
            PayloadLayout::Framed(ranges) => ranges[k],
        };
        &self.data[start as usize..end as usize]
    }

    /// Every frame's payload, in subframe order — the shape the `$registry`
    /// fold consumes (`bn-2di`).
    fn payloads(&self) -> Vec<Vec<u8>> {
        (0..self.frame_count as usize)
            .map(|k| self.payload(k).to_vec())
            .collect()
    }

    /// Resident bytes for the cache weighter.
    fn weight_bytes(&self) -> u64 {
        let layout_bytes = match &self.payloads {
            PayloadLayout::Arena(offs) => offs.len() * 4,
            PayloadLayout::Framed(ranges) => ranges.len() * 8,
        };
        (self.data.len()
            + layout_bytes
            + self.type_ids.len() * 4
            + std::mem::size_of::<DecodedBatch>()) as u64
    }
}

/// Cache key: the batch's physical placement `(segment_id, byte offset)`.
type CapsuleKey = (u64, u64);

/// Weighs a cached capsule by its decoded resident bytes plus a fixed
/// overhead, so the operator-facing budget tracks real memory (the
/// [`BlockCache`] pattern; a zero weight would never be evicted).
#[derive(Clone, Copy, Default)]
struct CapsuleWeighter;

/// Fixed per-entry overhead (key + `Arc` header + map slot).
const CAPSULE_OVERHEAD_BYTES: u64 = 64;

impl Weighter<CapsuleKey, Arc<DecodedBatch>> for CapsuleWeighter {
    #[inline]
    fn weight(&self, _key: &CapsuleKey, val: &Arc<DecodedBatch>) -> u64 {
        val.weight_bytes() + CAPSULE_OVERHEAD_BYTES
    }
}

/// Weighs a cached sealed-segment global batch directory.
#[derive(Clone, Copy, Default)]
struct GlobalDirWeighter;

impl Weighter<(u64, u64), Arc<Vec<GlobalEntry>>> for GlobalDirWeighter {
    #[inline]
    fn weight(&self, _key: &(u64, u64), val: &Arc<Vec<GlobalEntry>>) -> u64 {
        (val.len() * std::mem::size_of::<GlobalEntry>()) as u64
            + CAPSULE_OVERHEAD_BYTES
    }
}

/// One decoded `.pcol` payload block (bn-2ib): the unit the sealed payload
/// tier is cached at. A 128-event block would otherwise be decoded once per
/// overlapping batch — a small-batch corpus pays that decode ~(block/batch)×
/// per replay; caching the decoded block bounds it to ~once.
struct PcolBlock {
    /// Stored-order index of the block's first event within its segment.
    first_event: u64,
    /// Reassembled payload arena for the whole block.
    data:        Vec<u8>,
    /// `n_events + 1` arena boundaries.
    offs:        Vec<u32>,
}

/// Weighs a cached decoded `.pcol` block.
#[derive(Clone, Copy, Default)]
struct PcolWeighter;

impl Weighter<(u64, u64, u64), Arc<PcolBlock>> for PcolWeighter {
    #[inline]
    fn weight(&self, _key: &(u64, u64, u64), val: &Arc<PcolBlock>) -> u64 {
        (val.data.len() + val.offs.len() * 4) as u64 + CAPSULE_OVERHEAD_BYTES
    }
}

/// What a read path *expects* the batch at a pointer to be — cross-checked
/// against the decoded batch's CRC-covered identity so a stale or wrong
/// offset that happens to land on byte-valid bytes is caught, never served.
#[derive(Debug, Clone, Copy)]
struct BatchExpect {
    stream_id:        u64,
    first_global_pos: u64,
    frame_count:      u32,
    /// `None` on the global-read path ([`GlobalEntry`] carries no version;
    /// the CRC-covered header's `first_stream_version` is authoritative).
    first_version:    Option<u64>,
}

impl BatchExpect {
    fn matches(&self, b: &DecodedBatch) -> bool {
        b.stream_id == self.stream_id
            && b.first_global_pos == self.first_global_pos
            && b.frame_count == self.frame_count
            && self.first_version.is_none_or(|v| b.first_stream_version == v)
    }
}

/// The block-native byte fetcher (bn-2ib): resolves a committed
/// [`EventPtr`] to a CRC-validated [`DecodedBatch`] through a bounded,
/// bytes-weighted decoded-capsule cache. Misses `pread` exactly one batch
/// from the segment file; correctness never depends on cache residence
/// (a disabled cache runs the identical miss path for every read).
struct BlockReader {
    fs:          EngineFs,
    dir:         PathBuf,
    /// Read-only segment file handles, opened lazily and kept for the
    /// engine's lifetime (bounded by the number of segments; a handle is a
    /// cheap `Arc`'d fd).
    files:       Mutex<HashMap<u64, EngineFile>>,
    /// The decoded-capsule cache. `None` = disabled (every read decodes
    /// fresh).
    capsules:    Option<Cache<CapsuleKey, Arc<DecodedBatch>, CapsuleWeighter>>,
    /// Decoded per-segment global batch directories for **sealed** segments,
    /// keyed `(segment_id, install generation)` — a segment can be RE-sealed
    /// with grown coverage (an on-demand `seal_active`, or a roll-seal after
    /// one), and derived state cached under the old sidecar must never serve
    /// the new one (bn-2ib review F1/F6). Stale-generation entries become
    /// unreachable and age out under the byte budget.
    global_dirs:
        Option<Cache<(u64, u64), Arc<Vec<GlobalEntry>>, GlobalDirWeighter>>,
    /// Decoded `.pcol` payload blocks, keyed
    /// `(segment_id, install generation, block index)` — see [`PcolBlock`]
    /// for the block granularity and `global_dirs` for the generation
    /// component (a re-seal re-blocks the payload sidecar; a stale partial
    /// tail block under the new index's block map was review finding F1).
    pcol_blocks: Option<Cache<(u64, u64, u64), Arc<PcolBlock>, PcolWeighter>>,
}

impl BlockReader {
    fn new(fs: EngineFs, dir: PathBuf, capsule_budget_bytes: u64) -> Self {
        // One operator-facing budget, split across the three decoded-object
        // tiers (design.md §14.5: bounded by bytes, separated by type):
        // half to record capsules (the unit every read consumes), a quarter
        // each to decoded `.pcol` payload blocks and sealed global batch
        // directories.
        let (capsules, global_dirs, pcol_blocks) = if capsule_budget_bytes == 0
        {
            (None, None, None)
        } else {
            let cap_budget = (capsule_budget_bytes / 2).max(4096);
            let quarter = (capsule_budget_bytes / 4).max(4096);
            (
                Some(Cache::with_weighter(
                    (cap_budget / 4096).max(16) as usize,
                    cap_budget,
                    CapsuleWeighter,
                )),
                // A global directory is one LARGE item per sealed segment
                // (hundreds of KiB for a fine-batched segment). quick_cache
                // admits an item only if it fits its SHARD's hot budget, so
                // this cache runs UNSHARDED — otherwise a directory bigger
                // than budget/shards would be silently rejected and every
                // cold global page would re-decode the segment's pointer
                // blocks.
                Some(Cache::with_options(
                    OptionsBuilder::new()
                        .estimated_items_capacity(64)
                        .weight_capacity(quarter)
                        .shards(1)
                        .build()
                        .expect("static global-dir cache options"),
                    GlobalDirWeighter,
                    DefaultHashBuilder::default(),
                    DefaultLifecycle::default(),
                )),
                Some(Cache::with_weighter(
                    (quarter / 8192).max(16) as usize,
                    quarter,
                    PcolWeighter,
                )),
            )
        };
        BlockReader {
            fs,
            dir,
            files: Mutex::new(HashMap::new()),
            capsules,
            global_dirs,
            pcol_blocks,
        }
    }

    /// The lazily-opened read-only handle for `segment_id`.
    fn file(&self, segment_id: u64) -> Result<EngineFile, EngineError> {
        let mut files = self.files.lock().expect("segment files lock");
        if let Some(f) = files.get(&segment_id) {
            return Ok(f.clone());
        }
        let path = segment_path(&self.dir, segment_id);
        let f = self.fs.open(&path, OpenOpts::read_only()).map_err(|e| {
            EngineError::Read(format!("open seg {segment_id}: {e}"))
        })?;
        files.insert(segment_id, f.clone());
        Ok(f)
    }

    /// Insert a just-published batch (the append path's write-through warm).
    fn insert(&self, segment_id: u64, offset: u64, batch: Arc<DecodedBatch>) {
        if let Some(c) = &self.capsules {
            c.insert((segment_id, offset), batch);
        }
    }

    /// Resolve `ptr` to its decoded batch, expecting `expect`'s CRC-covered
    /// identity. Cache hit → `Arc` bump; miss → one header `pread` + one
    /// batch `pread` + scanner byte-layer validation (A4/A12 CRC always).
    /// A pointer that does not decode to the expected batch (torn file,
    /// stale offset, or a legacy sidecar whose offsets predate real
    /// pointers) falls back to locating the batch by its global position
    /// via a full segment scan — slow, loud in spirit, but never wrong.
    fn batch(
        &self,
        sealed: &SealedStore,
        ptr: EventPtr,
        expect: BatchExpect,
    ) -> Result<Arc<DecodedBatch>, EngineError> {
        let key = (ptr.segment_id, ptr.offset);
        if let Some(c) = &self.capsules
            && let Some(hit) = c.get(&key)
            && expect.matches(&hit)
        {
            return Ok(hit);
        }
        let decoded = match self.read_at(ptr, expect, sealed) {
            Ok(b) => Arc::new(b),
            // Wrong/undecodable pointer: locate by position instead.
            Err(_) => self.locate_by_scan(ptr.segment_id, expect, sealed)?,
        };
        if let Some(c) = &self.capsules {
            c.insert(key, decoded.clone());
        }
        Ok(decoded)
    }

    /// The point-read miss path: `pread` + validate + decode one batch.
    fn read_at(
        &self,
        ptr: EventPtr,
        expect: BatchExpect,
        sealed: &SealedStore,
    ) -> Result<DecodedBatch, EngineError> {
        let file = self.file(ptr.segment_id)?;
        let mut hdr = [0u8; scanner::BATCH_HEADER_LEN];
        pread_exact(&file, ptr.offset, &mut hdr).map_err(|e| {
            EngineError::Read(format!(
                "seg {} off {}: header pread: {e}",
                ptr.segment_id, ptr.offset
            ))
        })?;
        let total_len =
            scanner::peek_batch_total_len(&hdr, 0).map_err(|s| {
                EngineError::Read(format!(
                    "seg {} off {}: bad batch header: {s:?}",
                    ptr.segment_id, ptr.offset
                ))
            })?;
        let mut buf = vec![0u8; total_len as usize];
        pread_exact(&file, ptr.offset, &mut buf).map_err(|e| {
            EngineError::Read(format!(
                "seg {} off {}: batch pread: {e}",
                ptr.segment_id, ptr.offset
            ))
        })?;
        let accepted = scanner::accepted_batch_at(&buf, 0).map_err(|s| {
            EngineError::Read(format!(
                "seg {} off {}: batch decode: {s:?}",
                ptr.segment_id, ptr.offset
            ))
        })?;
        let decoded =
            self.decode_capsule(&accepted, &buf, ptr.segment_id, sealed)?;
        if !expect.matches(&decoded) {
            return Err(EngineError::Read(format!(
                "seg {} off {}: batch identity mismatch (expected {expect:?})",
                ptr.segment_id, ptr.offset
            )));
        }
        Ok(decoded)
    }

    /// The defensive fallback: scan the whole segment (the recovery scanner —
    /// full byte-layer + kernel acceptance) and pick the batch whose
    /// CRC-covered identity matches `expect`. Handles a sidecar whose
    /// pointer offsets are not real byte offsets (stores sealed before
    /// bn-2ib used pseudo offsets).
    ///
    /// The scan's cost is amortized, not repeated (bn-2ib review F3): every
    /// accepted batch of the scanned segment is decoded once and bulk-warmed
    /// into the capsule cache under BOTH its legacy pseudo key
    /// (`offset == first_global_pos` — the key a legacy sidecar's pointers
    /// will ask for) and its real byte-offset key, so a sequential legacy
    /// replay pays ~one scan per cache window instead of one scan per batch.
    /// With the cache disabled only the matching batch is decoded (nothing
    /// could retain the warm), which keeps the old worst case as the floor —
    /// `mess rebuild-index` is the real fix for legacy stores.
    fn locate_by_scan(
        &self,
        segment_id: u64,
        expect: BatchExpect,
        sealed: &SealedStore,
    ) -> Result<Arc<DecodedBatch>, EngineError> {
        let path = segment_path(&self.dir, segment_id);
        let (rec, image) = scanner::recover_segment_with_image(&self.fs, &path)
            .map_err(|e| {
                EngineError::Read(format!("scan seg {segment_id}: {e}"))
            })?;
        let mut found: Option<Arc<DecodedBatch>> = None;
        for b in &rec.accepted {
            let is_match = b.first_global_pos == expect.first_global_pos;
            if self.capsules.is_none() && !is_match {
                continue; // nothing to warm; decode only the match
            }
            let decoded =
                Arc::new(self.decode_capsule(b, &image, segment_id, sealed)?);
            if let Some(c) = &self.capsules {
                c.insert((segment_id, b.first_global_pos), decoded.clone());
                if b.offset != b.first_global_pos {
                    c.insert((segment_id, b.offset), decoded.clone());
                }
            }
            if is_match && expect.matches(&decoded) {
                found = Some(decoded);
            }
        }
        found.ok_or_else(|| {
            EngineError::Read(format!(
                "seg {segment_id}: no committed batch matches {expect:?}"
            ))
        })
    }

    /// The decoded global batch directory of a **sealed** segment, cached by
    /// `(segment_id, install generation)` — see the field doc for why the
    /// generation is load-bearing across re-seals.
    fn global_dir(
        &self,
        seg: &SealedSegmentIndex,
        generation: u64,
    ) -> Result<Arc<Vec<GlobalEntry>>, EngineError> {
        let key = (seg.segment_id(), generation);
        if let Some(c) = &self.global_dirs
            && let Some(hit) = c.get(&key)
        {
            return Ok(hit);
        }
        let dir = Arc::new(seg.global_entries().map_err(|e| {
            EngineError::SealedRead(format!("global entries: {e:?}"))
        })?);
        if let Some(c) = &self.global_dirs {
            c.insert(key, dir.clone());
        }
        Ok(dir)
    }

    /// The decoded `.pcol` block `(segment, install generation, block
    /// index)`, cached — the generation keeps a re-sealed segment's blocks
    /// from ever being served through the old sidecar's block map (review
    /// F1).
    fn pcol_block(
        &self,
        segment_id: u64,
        generation: u64,
        pidx: &mess_index::sealed::SealedPayloadIndex,
        bi: usize,
    ) -> Result<Arc<PcolBlock>, mess_index::sealed::PayloadError> {
        let key = (segment_id, generation, bi as u64);
        if let Some(c) = &self.pcol_blocks
            && let Some(hit) = c.get(&key)
        {
            return Ok(hit);
        }
        let mut data = Vec::new();
        let mut offs = Vec::new();
        pidx.reassemble_block(bi, &NoDicts, &mut data, &mut offs)?;
        let block = Arc::new(PcolBlock {
            first_event: pidx.blocks()[bi].first_event,
            data,
            offs,
        });
        if let Some(c) = &self.pcol_blocks {
            c.insert(key, block.clone());
        }
        Ok(block)
    }

    /// Assemble the stored-order payload range `[lo, hi)` of a sealed
    /// segment's `.pcol` from its (cached) decoded blocks. Returns the
    /// arena + `hi - lo + 1` boundaries, exactly the [`DecodedBatch`] shape.
    fn pcol_range(
        &self,
        segment_id: u64,
        generation: u64,
        pidx: &mess_index::sealed::SealedPayloadIndex,
        lo: u64,
        hi: u64,
    ) -> Result<(Vec<u8>, Vec<u32>), mess_index::sealed::PayloadError> {
        use mess_index::sealed::PayloadError;
        if hi > pidx.event_count() {
            return Err(PayloadError::IndexOutOfRange);
        }
        let mut data = Vec::new();
        let mut offs = Vec::with_capacity((hi - lo + 1) as usize);
        let mut next = lo;
        let mut bi = pidx.block_for(lo).ok_or(PayloadError::IndexOutOfRange)?;
        while next < hi {
            let entry =
                *pidx.blocks().get(bi).ok_or(PayloadError::IndexOutOfRange)?;
            let blk = self.pcol_block(segment_id, generation, pidx, bi)?;
            // A cached block that disagrees with THIS sidecar's block map is
            // a typed error, never a clamp (review F1): the generation key
            // makes this unreachable, but a silent short slice here was the
            // original stale-cache corruption, so it stays guarded. The
            // caller falls back to the raw frames.
            let n_events = blk.offs.len() - 1;
            if blk.first_event != entry.first_event
                || n_events != entry.n_events as usize
                || next < blk.first_event
                || next >= blk.first_event + n_events as u64
            {
                return Err(PayloadError::Corrupt(
                    "cached .pcol block disagrees with the sidecar block map",
                ));
            }
            let b_lo = (next - blk.first_event) as usize;
            let b_hi = ((hi - blk.first_event) as usize).min(n_events);
            for r in b_lo..b_hi {
                offs.push(data.len() as u32);
                data.extend_from_slice(
                    &blk.data[blk.offs[r] as usize..blk.offs[r + 1] as usize],
                );
            }
            next = blk.first_event + n_events as u64;
            bi += 1;
        }
        offs.push(data.len() as u32);
        Ok((data, offs))
    }

    /// Build the [`DecodedBatch`] for an accepted batch: frame identities
    /// (event-type ids) from the raw batch bytes, payload bytes from the
    /// covering sealed segment's `.pcol` sidecar when one is attached (D6 —
    /// the columnar reassembly is verify-on-seal byte-identical to the raw
    /// frames), assembled from bounded-cached decoded blocks; falls back to
    /// the raw frames on any `.pcol` decode error or coverage gap. `image`
    /// must be the buffer `b` was validated against.
    fn decode_capsule(
        &self,
        b: &AcceptedBatch,
        image: &[u8],
        segment_id: u64,
        sealed: &SealedStore,
    ) -> Result<DecodedBatch, EngineError> {
        let n = b.frame_count as usize;

        // bn-3of: fully-from-pack fast path. When the covering SealPack carries
        // BOTH the `EVENT_TYPE_IDS` section (per-event `event_type_id`) and the
        // `.pcol` payload columns for this batch's range, materialize the
        // capsule straight from the pack — the raw frames are never decoded, so
        // a cold `message_type`/payload read never parses the raw batch bytes
        // for type ids (the bn-3fn carry-forward #1 win). The batch was already
        // CRC-validated (`accepted_batch_at`) by the caller, and the section +
        // columns are verify-on-seal byte-identical to those frames, so this is
        // exact. Any coverage gap or decode error falls through to the raw
        // frame decode below, byte-identically.
        if let Some((seg, generation)) = sealed.get_with_gen(segment_id)
            && seg.has_event_types()
            && let Some(pcol) = seg.payload_index()
            && b.first_global_pos >= seg.base_pos()
        {
            let lo = b.first_global_pos - seg.base_pos();
            let hi = lo + u64::from(b.frame_count);
            // bn-dbz: one range call, not one call per frame — a lazily opened
            // pack serves the whole batch from the one index block the run
            // almost always lies inside, instead of re-reading that block per
            // event.
            let type_ids: Option<Vec<u32>> = seg.event_type_ids_range(lo, hi);
            if let Some(type_ids) = type_ids
                && let Ok((data, offs)) =
                    self.pcol_range(segment_id, generation, pcol, lo, hi)
            {
                return Ok(DecodedBatch {
                    stream_id: b.stream_id,
                    first_stream_version: b.first_stream_version,
                    first_global_pos: b.first_global_pos,
                    frame_count: b.frame_count,
                    type_ids,
                    data,
                    payloads: PayloadLayout::Arena(offs),
                });
            }
        }

        // Raw-frame path (legacy sidecars, uncovered range, or any decode
        // error above): type ids + payload from the decoded frames.
        let frames = b.frames(image).map_err(|e| {
            EngineError::Read(format!("seg {segment_id}: frames: {e}"))
        })?;
        let mut type_ids = Vec::with_capacity(n);
        let mut data = Vec::new();
        let mut offs = Vec::with_capacity(n + 1);
        for f in frames {
            type_ids.push(f.event_type_id);
            offs.push(data.len() as u32);
            data.extend_from_slice(f.payload);
        }
        offs.push(data.len() as u32);

        // D6: prefer the columnar payload sidecar's bytes for a sealed batch
        // whose pack lacked the type-id section (e.g. a legacy `.pcol`-only
        // seal) but still covers the payload. A `.pcol` decode error / coverage
        // gap keeps the raw frames — byte-identical by verify-on-seal.
        if let Some((seg, generation)) = sealed.get_with_gen(segment_id)
            && let Some(pcol) = seg.payload_index()
            && b.first_global_pos >= seg.base_pos()
        {
            let lo = b.first_global_pos - seg.base_pos();
            let hi = lo + u64::from(b.frame_count);
            if let Ok((pdata, poffs)) =
                self.pcol_range(segment_id, generation, pcol, lo, hi)
            {
                data = pdata;
                offs = poffs;
            }
        }

        Ok(DecodedBatch {
            stream_id: b.stream_id,
            first_stream_version: b.first_stream_version,
            first_global_pos: b.first_global_pos,
            frame_count: b.frame_count,
            type_ids,
            data,
            payloads: PayloadLayout::Arena(offs),
        })
    }
}

/// Positioned exact read through the [`FileHandle`] seam.
fn pread_exact(
    file: &EngineFile,
    mut off: u64,
    buf: &mut [u8],
) -> std::io::Result<()> {
    let mut filled = 0;
    while filled < buf.len() {
        let n = file.pread(off, &mut buf[filled..])?;
        if n == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "short pread",
            ));
        }
        filled += n;
        off += n as u64;
    }
    Ok(())
}

/// Where the committer durably placed one batch — the `Acked` half of an
/// [`AppendOutcome`], destructured once so the publish path does not re-match
/// it (`bn-2di`: there are now up to two batches per append to publish).
#[derive(Debug, Clone, Copy)]
struct Placed {
    first_global: u64,
    last_global:  u64,
    segment_id:   u64,
    offset:       u64,
}

/// An [`AppendOutcome`] that must be a positive ack. `Indeterminate` means the
/// covering barrier did not complete, so the batch MAY be durable and MAY be
/// accepted by a later recovery scan — there is no safe way to publish it.
fn expect_acked(outcome: AppendOutcome) -> Result<Placed, EngineError> {
    match outcome {
        AppendOutcome::Acked {
            first_position,
            last_position,
            segment_id,
            offset,
        } => Ok(Placed {
            first_global: first_position,
            last_global: last_position,
            segment_id,
            offset,
        }),
        AppendOutcome::Indeterminate => {
            Err(EngineError::Append("indeterminate durability".to_string()))
        }
    }
}

/// Publish ONE durably-placed batch: take its turn in global-position order,
/// seed the hot index, advance the stream head (book + meta), and advance the
/// published read watermark (`bn-2di` — extracted so the `$registry` batch and
/// the domain batch it precedes publish through the exact same path).
///
/// Order note (review F4, preserved): the interner head is updated strictly
/// AFTER `apply_committed`, so a concurrent `head()` can never name a version
/// the index cannot yet serve.
fn publish_batch(
    inner: &PublishState,
    sid: u64,
    first_stream_version: u64,
    frame_count: u32,
    placed: Placed,
) {
    let Placed { first_global, last_global, segment_id, offset } = placed;
    let watermark = last_global + 1;
    let last_stream_pos = first_stream_version + u64::from(frame_count) - 1;

    inner.active.apply_committed(
        watermark,
        &[BatchEntry {
            stream_id: sid,
            first_stream_version,
            frame_count,
            first_global_pos: first_global,
            // The REAL durable placement from the ack (bn-2ib) — the
            // block-native read paths dereference this pointer.
            ptr: EventPtr { segment_id, offset },
        }],
    );

    {
        let mut book = inner.book.write().expect("book lock");
        book.heads.insert(sid, last_stream_pos);
    }

    // Publish complete: every position `< watermark` is now servable through
    // the index tiers. Advance the published watermark LAST, still holding this
    // batch's publish turn (`_turn`), so it moves in strict global-position
    // order and never announces a position `read_global_page` cannot yet
    // account for (as a delivered record or a filtered-position frontier).
    // This is the wake that drives every live-tail subscriber parked on
    // `await_watermark_past`.
    inner.read_watermark.advance(watermark);
}

const OWNER_RING_CAPACITY: usize = 1024;
const OWNER_RING_BYTES: usize = 64 * 1024 * 1024;
/// Above this encoded size, prepare the immutable frame bodies on the producer
/// task. The large-batch append spike measured a decisive crossover here;
/// smaller batches retain the owner's reusable encoder and avoid a fresh
/// producer allocation per append.
const PREPARE_MIN_ENCODED_BYTES: usize = 16 * 1024;

/// Conservative per-frame build-peak overhead for producer preparation
/// (`bn-1gn1`).
///
/// `OWNER_RING_BYTES` bounds what is *queued*; before this bone it did not
/// bound what is *under construction*. `PreparedBatch::encode` materializes the
/// whole framed copy on the producer task, and the permit was only acquired
/// afterwards, so N concurrent producers could each hold a fully built batch
/// (up to `MAX_BATCH_LEN`) entirely off the books.
///
/// The reservation below is taken *before* preparation and must therefore be an
/// over-estimate, never an under-estimate. Per frame it covers the scratch that
/// the final `cost` does not:
///
///   - `PreparedBatch::type_id_offsets`: one `u32`
///   - `PreparedBatch::payload_ranges`:  one `(u32, u32)`
///   - the caller's `subframes` vector:  one `Subframe`
///   - `type_slots`:                     one `u32`
///
/// plus, on the borrowed path, the `&str -> u32` interning map, whose worst
/// case is one entry per frame.
const PREPARE_FRAME_SCRATCH_BYTES: usize = 4
    + 8
    + std::mem::size_of::<Subframe<'static>>()
    + 4
    + std::mem::size_of::<(&'static str, u32)>()
    + std::mem::size_of::<usize>();

/// Conservative build-peak reservation for preparing `frame_count` frames whose
/// final framed size is `encoded_estimate`, submitted for `stream_id` with
/// `type_name_bytes` of distinct event-type names.
///
/// MUST NOT under-estimate the admission `cost` computed after preparation.
/// `reconcile_owner_bytes` can correct a shortfall, but only by releasing and
/// re-acquiring; keeping this an over-estimate is what makes the common path a
/// pure release. `type_name_bytes` is therefore counted in full even though the
/// prepared path dedupes names — the caller passes an upper bound.
///
/// Deliberately ignores allocator slack and `Vec` growth doubling: both are
/// bounded multiples of what is counted here, and the reservation is reconciled
/// down to the exact `cost` the moment preparation completes, so a modest
/// over-estimate costs only a brief hold on the shared ring.
fn prepare_build_peak(
    stream_id_len: usize,
    frame_count: usize,
    encoded_estimate: usize,
    type_name_bytes: usize,
) -> usize {
    stream_id_len
        .saturating_add(encoded_estimate)
        .saturating_add(type_name_bytes)
        .saturating_add(frame_count.saturating_mul(PREPARE_FRAME_SCRATCH_BYTES))
}

type OwnerResult = Result<Appended, AppendError<EngineError>>;

enum OwnerIntentKind {
    Domain { stream: String, expected: Version, input: DomainInput },
    Registry { expected: Version, records: Vec<RegistryRecord> },
}

enum DomainInput {
    Records(Vec<RecordToAppend>),
    Owned(OwnedAppendBatch),
    Prepared {
        /// Distinct type names; `type_slots` indexes this table per frame.
        type_names: Vec<String>,
        type_slots: Vec<u32>,
        batch:      PreparedBatch,
    },
}

impl DomainInput {
    fn is_empty(&self) -> bool {
        match self {
            DomainInput::Records(records) => records.is_empty(),
            DomainInput::Owned(batch) => batch.is_empty(),
            DomainInput::Prepared { batch, .. } => batch.frame_count() == 0,
        }
    }
}

struct OwnerIntent {
    kind:       OwnerIntentKind,
    cost:       usize,
    completion: OwnerCompletion,
}

/// An intent's bounded byte reservation and terminal notification, kept in
/// release-before-notify field order even when a queued intent is cancelled,
/// the owner exits, or unwinding bypasses the ordinary completion path.
struct OwnerCompletion {
    _permit: OwnedSemaphorePermit,
    done:    oneshot::Sender<OwnerResult>,
    /// bn-11ba: where this append's outcome is counted. One `Arc` clone per
    /// *append* (a relaxed refcount bump, no allocation) buys the conflict,
    /// cancellation and end-to-end ack-latency numbers with no extra
    /// plumbing through `plan_domain`/`commit_plans`.
    status:  Arc<OwnerStatus>,
    /// When this intent was admitted to the owner ring. One `Instant::now`
    /// per append batch — never per event.
    queued:  Instant,
}

impl OwnerCompletion {
    /// Append completion is an ownership boundary: once the receiver wakes,
    /// both the channel slot and byte permits must be reusable.
    ///
    /// bn-11ba: this is also the one funnel every append outcome passes
    /// through, so it is where the outcome counters are recorded. A `send`
    /// that fails means the caller dropped its future before the outcome
    /// landed — the events are still committed and published (`bn-3nz`), so
    /// this is a *cancellation*, not a loss.
    fn finish(self, result: OwnerResult) {
        let Self { _permit, done, status, queued } = self;
        drop(_permit);
        if matches!(result, Err(AppendError::Conflict { .. })) {
            status.outcomes.conflicts.incr();
        }
        status.outcomes.ack.record(queued.elapsed());
        if done.send(result).is_err() {
            status.outcomes.cancellations.incr();
        }
    }
}

struct InFlightGuard(Arc<AtomicUsize>);

impl Drop for InFlightGuard {
    fn drop(&mut self) { self.0.fetch_sub(1, Ordering::AcqRel); }
}

/// One-shot, test-only rendezvous at the append owner's admission boundary.
///
/// The owner removes the first intent from the 1,024-slot channel and parks
/// before gathering. Producers then admit the remaining intents (the test
/// cohort is deliberately much smaller than `OWNER_RING_CAPACITY + 1`) and
/// wake the owner only when the complete cohort is visible to `gather`.
/// Every part of this seam is compiled out of non-test builds.
#[cfg(test)]
#[derive(Default)]
struct TestOwnerCohortGate {
    state: TestOwnerCohortMutex,
    ready: std::sync::Condvar,
}

#[cfg(test)]
type TestOwnerCohortMutex = Mutex<TestOwnerCohortState>;

#[cfg(test)]
#[derive(Default)]
struct TestOwnerCohortState {
    generation: u64,
    armed:      Option<TestOwnerCohort>,
    /// Admissions recorded for `generation`, retained after the cohort is
    /// disarmed.  `wait_until_cohort_admitted` clears `armed` as its one-shot
    /// success step, so a waiter woken by that very notification would
    /// otherwise lose the evidence that its own admission had landed.
    admitted:   usize,
}

#[cfg(test)]
struct TestOwnerCohort {
    expected: usize,
    admitted: usize,
}

#[cfg(test)]
impl TestOwnerCohortGate {
    fn arm(self: &Arc<Self>, expected: usize) -> TestOwnerCohortGuard {
        assert!(expected > 0, "owner cohort must contain an intent");
        assert!(
            expected <= OWNER_RING_CAPACITY + 1,
            "owner can park one received intent plus at most the channel's \
             {OWNER_RING_CAPACITY} queued intents"
        );
        let mut state = self.state.lock().expect("owner cohort gate lock");
        assert!(state.armed.is_none(), "owner cohort gate already armed");
        state.generation = state.generation.wrapping_add(1);
        let generation = state.generation;
        state.admitted = 0;
        state.armed = Some(TestOwnerCohort { expected, admitted: 0 });
        TestOwnerCohortGuard { gate: Arc::clone(self), generation }
    }

    /// Record an intent only after the bounded owner channel owns it, while
    /// its producer-side in-flight guard is still held.
    fn record_admitted(&self) {
        let mut state = self.state.lock().expect("owner cohort gate lock");
        let Some(cohort) = state.armed.as_mut() else { return };
        cohort.admitted += 1;
        assert!(
            cohort.admitted <= cohort.expected,
            "more intents admitted than the armed test cohort"
        );
        state.admitted = state.admitted.saturating_add(1);
        self.ready.notify_all();
    }

    fn wait_until_admitted(&self, at_least: usize) {
        let mut state = self.state.lock().expect("owner cohort gate lock");
        assert!(
            state
                .armed
                .as_ref()
                .is_some_and(|cohort| at_least <= cohort.expected),
            "admission wait must target the armed cohort"
        );
        // Wait on the generation's retained admission count rather than the
        // armed cohort: `wait_until_cohort_admitted` disarms the cohort as its
        // one-shot success step, and the waiter it wakes must still observe the
        // admission that completed the cohort.  Abandonment (a disarm before
        // `at_least` landed, or a re-arm) still fails the assertion below.
        let generation = state.generation;
        while state.generation == generation
            && state.admitted < at_least
            && state.armed.is_some()
        {
            state = self.ready.wait(state).expect("owner cohort gate wait");
        }
        assert!(
            state.generation == generation && state.admitted >= at_least,
            "owner cohort was disarmed before the requested admission"
        );
    }

    /// Park the owner after its first receive and before `gather` until the
    /// complete armed cohort is resident in the owner/channel boundary.
    fn wait_until_cohort_admitted(&self) {
        let mut state = self.state.lock().expect("owner cohort gate lock");
        let Some(cohort) = state.armed.as_ref() else { return };
        let generation = state.generation;
        let expected = cohort.expected;
        while state.generation == generation
            && state
                .armed
                .as_ref()
                .is_some_and(|cohort| cohort.admitted < expected)
        {
            state = self.ready.wait(state).expect("owner cohort gate wait");
        }
        if state.generation == generation
            && state
                .armed
                .as_ref()
                .is_some_and(|cohort| cohort.admitted == expected)
        {
            // One shot: later owner receives and producer admissions proceed
            // normally even while the test retains its safety guard.
            state.armed = None;
            self.ready.notify_all();
        }
    }

    fn disarm(&self, generation: u64) {
        // Runs from `TestOwnerCohortGuard::drop`, including while unwinding a
        // panic that poisoned this mutex.  Unwrapping a poisoned lock here
        // would panic inside `Drop` and abort the process, destroying the
        // original diagnostic, so recover the guard instead.
        let mut state = match self.state.lock() {
            Ok(state) => state,
            Err(poisoned) => poisoned.into_inner(),
        };
        if state.generation == generation && state.armed.is_some() {
            state.armed = None;
            self.ready.notify_all();
        }
    }
}

/// Panic/timeout safety: abandoning an armed cohort can never strand the
/// owner's thread or wedge `Inner::drop` while it joins that thread.
#[cfg(test)]
struct TestOwnerCohortGuard {
    gate:       Arc<TestOwnerCohortGate>,
    generation: u64,
}

#[cfg(test)]
impl Drop for TestOwnerCohortGuard {
    fn drop(&mut self) { self.gate.disarm(self.generation); }
}

struct OwnerStatus {
    metrics:                        CommitterMetricsHandle,
    degraded:                       AtomicBool,
    outcome_scratch_retained_slots: AtomicUsize,
    outcome_scratch_retained_bytes: AtomicUsize,
    outcome_scratch_trims:          AtomicUsize,
    /// bn-11ba: the append-outcome and group-shape counters the composed
    /// observability surface reports. Every one of these is recorded exactly
    /// once per *append* or once per *group* — never per event — and each
    /// record is one relaxed `fetch_add` (plus, for the histograms, a
    /// timestamp diff the site already had or one extra `Instant::now`).
    ///
    /// They live on `OwnerStatus` because that is the one handle both the
    /// owner thread and the producer side already share, so nothing new is
    /// threaded through the append path to reach them.
    outcomes:                       OutcomeCounters,
}

/// Append-outcome, queue-delay and group-shape counters (`bn-11ba`).
///
/// `group_width` reuses [`LatencyHistogram`] as a general bounded-error
/// distribution over `u64`; its unit is **intents per group**, not
/// nanoseconds. That is deliberate — a second histogram implementation for
/// the sake of a unit name would be strictly worse.
#[derive(Default)]
struct OutcomeCounters {
    /// Appends refused for an expected-version mismatch.
    conflicts:      Counter,
    /// Appends whose caller dropped its future before the outcome landed.
    /// The events still committed and published (`bn-3nz`); this counts a
    /// caller that stopped listening.
    cancellations:  Counter,
    /// Admission-to-outcome latency, nanoseconds.
    ack:            LatencyHistogram,
    /// Gather-window duration per committed group, nanoseconds — the
    /// owner-side queue delay.
    group_wait:     LatencyHistogram,
    /// Intents per gathered group.
    group_width:    LatencyHistogram,
    /// The owner's durable-commit span per group (write + barrier),
    /// nanoseconds.
    commit_latency: LatencyHistogram,
}

struct AppendOwner {
    tx:                Option<tokio_mpsc::Sender<OwnerIntent>>,
    bytes:             Arc<Semaphore>,
    inflight:          Arc<AtomicUsize>,
    durable_watermark: Watermark,
    status:            Arc<OwnerStatus>,
    durability:        Durability,
    chain_enabled:     bool,
    join:              Option<JoinHandle<()>>,
    #[cfg(test)]
    cohort_gate:       Arc<TestOwnerCohortGate>,
}

struct PublishState {
    active:         Arc<ActiveIndex>,
    book:           Arc<RwLock<Book>>,
    reader:         Arc<BlockReader>,
    read_watermark: Watermark,
}

struct DomainPlan {
    pre:        Pre,
    reqs:       Vec<DirectAppendRequest>,
    staged:     Vec<RegistryRecord>,
    reg_span:   Option<(u64, usize)>,
    completion: OwnerCompletion,
}

impl DomainPlan {
    fn complete(self, result: OwnerResult) { self.completion.finish(result); }
}

#[derive(Default)]
struct PlanOutcomes {
    registry: Option<DirectBatchOutcome>,
    domain:   Option<DirectBatchOutcome>,
}

const OWNER_OUTCOME_RETAINED_SLOTS: usize = 256;
const OWNER_OUTCOME_RETAINED_BYTE_CAP: usize = 1024 * 1024;
const _: () = assert!(
    OWNER_OUTCOME_RETAINED_SLOTS * std::mem::size_of::<PlanOutcomes>()
        <= OWNER_OUTCOME_RETAINED_BYTE_CAP
);

fn cap_plan_outcomes(outcomes: &mut Vec<PlanOutcomes>) -> bool {
    if outcomes.capacity() > OWNER_OUTCOME_RETAINED_SLOTS {
        *outcomes = Vec::with_capacity(OWNER_OUTCOME_RETAINED_SLOTS);
        true
    } else {
        false
    }
}

#[cfg(test)]
mod owner_outcome_scratch_tests {
    use super::*;

    fn direct_outcome(
        outcome: Result<AppendOutcome, mess_log::committer::AppendError>,
    ) -> DirectBatchOutcome {
        DirectBatchOutcome {
            outcome,
            events: DirectBatchEvents::Inputs(Vec::new()),
        }
    }

    #[test]
    fn oversize_result_scratch_is_shed_to_named_count_and_byte_caps() {
        let mut outcomes = Vec::with_capacity(OWNER_OUTCOME_RETAINED_SLOTS + 1);
        outcomes.resize_with(
            OWNER_OUTCOME_RETAINED_SLOTS + 1,
            PlanOutcomes::default,
        );
        outcomes.clear();
        assert!(cap_plan_outcomes(&mut outcomes));
        assert!(outcomes.capacity() <= OWNER_OUTCOME_RETAINED_SLOTS);
        assert!(
            outcomes.capacity() * std::mem::size_of::<PlanOutcomes>()
                <= OWNER_OUTCOME_RETAINED_BYTE_CAP
        );
    }

    #[test]
    fn new_name_unit_keeps_registry_and_owned_domain_outcomes_distinct() {
        // An owned append that mints a stream or event-type name is a
        // two-batch ordered unit: registry first, then domain. Keep callback
        // results in their semantic slots even when the first failure aborts
        // the dependent domain batch, so retirement reports the root cause
        // and never treats the domain payload as committed.
        let mut outcomes = PlanOutcomes::default();
        outcomes.record(
            true,
            0,
            direct_outcome(Err(mess_log::committer::AppendError::StoreFull)),
        );
        outcomes.record(
            true,
            1,
            direct_outcome(Err(mess_log::committer::AppendError::UnitAborted)),
        );

        assert!(matches!(
            &outcomes.registry.as_ref().expect("registry outcome").outcome,
            Err(mess_log::committer::AppendError::StoreFull)
        ));
        assert!(matches!(
            &outcomes.domain.as_ref().expect("domain outcome").outcome,
            Err(mess_log::committer::AppendError::UnitAborted)
        ));
    }
}

impl PlanOutcomes {
    fn record(
        &mut self,
        has_registry: bool,
        batch: usize,
        outcome: DirectBatchOutcome,
    ) {
        let slot = if has_registry {
            match batch {
                0 => &mut self.registry,
                1 => &mut self.domain,
                _ => panic!("domain unit has at most two batches"),
            }
        } else {
            assert_eq!(batch, 0, "domain-only unit has one batch");
            &mut self.domain
        };
        assert!(slot.replace(outcome).is_none(), "duplicate direct outcome");
    }
}

struct FlatOwner {
    direct:                          DirectCommitter<RealRuntime, EngineFs>,
    publish:                         Arc<PublishState>,
    durability:                      Durability,
    inflight:                        Arc<AtomicUsize>,
    status:                          Arc<OwnerStatus>,
    target:                          usize,
    outcomes:                        Vec<PlanOutcomes>,
    outcome_reported_retained_slots: usize,
    outcome_reported_retained_bytes: usize,
    #[cfg(test)]
    cohort_gate:                     Arc<TestOwnerCohortGate>,
}

impl FlatOwner {
    fn gather(
        &mut self,
        rx: &mut tokio_mpsc::Receiver<OwnerIntent>,
        first: OwnerIntent,
    ) -> Vec<OwnerIntent> {
        // `Os` is deliberately sync-per-batch. It is a durability contract,
        // not merely a performance setting, so it never coalesces here.
        if matches!(self.durability, Durability::Os) {
            // bn-11ba: still a group of one, with no gather wait. Recording
            // it keeps `group_width` an honest distribution across modes
            // rather than an empty histogram under `Os`.
            self.status.outcomes.group_width.record_nanos(1);
            self.status.outcomes.group_wait.record_nanos(0);
            return vec![first];
        }
        let mut bytes = first.cost;
        let mut group = vec![first];
        let start = Instant::now();
        let deadline = match self.durability {
            Durability::Group { max_delay, .. } => start + max_delay,
            Durability::Process => start + Duration::from_micros(3),
            Durability::Os => unreachable!(),
        };
        let max_bytes = match self.durability {
            Durability::Group { max_bytes, .. } => max_bytes as usize,
            Durability::Process => OWNER_RING_BYTES,
            Durability::Os => unreachable!(),
        };
        let grace = start + Duration::from_micros(200);
        while group.len() < OWNER_RING_CAPACITY && bytes < max_bytes {
            match rx.try_recv() {
                Ok(intent) => {
                    bytes = bytes.saturating_add(intent.cost);
                    group.push(intent);
                }
                Err(tokio_mpsc::error::TryRecvError::Disconnected) => break,
                Err(tokio_mpsc::error::TryRecvError::Empty) => {
                    let now = Instant::now();
                    if now >= deadline {
                        break;
                    }
                    if matches!(self.durability, Durability::Group { .. })
                        && self.inflight.load(Ordering::Acquire) == 0
                        && group.len() >= self.target
                        && now >= grace
                    {
                        break;
                    }
                    std::hint::spin_loop();
                }
            }
        }
        self.target = group.len().max(1);
        // bn-11ba: once per gathered group (never per append, never per
        // event) — the width the coalescer achieved and the wall time it
        // spent achieving it. `start` was already taken above for the
        // deadline, so the wait costs one extra `Instant::now`.
        self.status.outcomes.group_width.record_nanos(group.len() as u64);
        self.status.outcomes.group_wait.record(start.elapsed());
        group
    }

    fn plan_domain(
        &self,
        stream: String,
        expected: Version,
        input: DomainInput,
        completion: OwnerCompletion,
    ) -> DomainPlan {
        let book = self.publish.book.read().expect("book lock");
        let mut staged = Vec::new();
        let sid = book.registry.stream_id(&stream).unwrap_or_else(|| {
            let id = book.registry.stream_high_water_mark() + 1;
            staged.push(registry::stream_registered(id, &stream));
            id
        });
        let actual = book.head(sid);
        let mut reqs = Vec::new();
        let mut reg_span = None;
        let pre = if actual != expected {
            staged.clear();
            Pre::Conflict(actual)
        } else if input.is_empty() {
            staged.clear();
            Pre::Empty
        } else {
            let mut next_tid = book.registry.event_type_high_water_mark();
            let (tids, domain_req) = match input {
                DomainInput::Records(records) => {
                    // Resolve each distinct message type once per batch. The
                    // last-name check makes homogeneous batches a comparison
                    // plus integer copy per frame, matching the frozen spike;
                    // the map handles non-consecutive repeats exactly.
                    let mut resolved: HashMap<&str, u32> = HashMap::new();
                    let mut last: Option<(&str, u32)> = None;
                    let mut tids = Vec::with_capacity(records.len());
                    for r in &records {
                        let name = r.message_type.as_str();
                        let tid = if let Some((last_name, id)) = last
                            && last_name == name
                        {
                            id
                        } else if let Some(&id) = resolved.get(name) {
                            last = Some((name, id));
                            id
                        } else {
                            let id = if let Some(id) =
                                book.registry.event_type_id(name)
                            {
                                id
                            } else {
                                next_tid += 1;
                                staged.push(registry::event_type_registered(
                                    next_tid, name,
                                ));
                                next_tid
                            };
                            resolved.insert(name, id);
                            last = Some((name, id));
                            id
                        };
                        tids.push(tid);
                    }
                    drop(resolved);
                    let events = records
                        .into_iter()
                        .zip(&tids)
                        .map(|(r, &tid)| EventInput::plain(tid, 0, 0, r.data))
                        .collect();
                    let first_stream_pos = expected.next_position();
                    (
                        tids,
                        DirectAppendRequest::Inputs(AppendRequest {
                            stream_id: sid,
                            category_id: CATEGORY_ID,
                            first_stream_version: first_stream_pos,
                            events,
                        }),
                    )
                }
                DomainInput::Owned(batch) => {
                    // Batch-local slots carry no authority. Resolve every
                    // distinct name against the current RegistryState under
                    // the owner lock, then expand ids in frame order.
                    let mut unique_tids =
                        Vec::with_capacity(batch.type_count());
                    for slot in 0..batch.type_count() {
                        let name = batch.type_name(slot);
                        let id = if let Some(id) =
                            book.registry.event_type_id(name)
                        {
                            id
                        } else {
                            next_tid += 1;
                            staged.push(registry::event_type_registered(
                                next_tid, name,
                            ));
                            next_tid
                        };
                        unique_tids.push(id);
                    }
                    let tids: Vec<u32> = (0..batch.len())
                        .map(|frame| unique_tids[batch.type_slot(frame)])
                        .collect();
                    let events = batch
                        .payloads
                        .into_iter()
                        .zip(&tids)
                        .map(|(data, &tid)| EventInput::plain(tid, 0, 0, data))
                        .collect();
                    let first_stream_pos = expected.next_position();
                    (
                        tids,
                        DirectAppendRequest::Inputs(AppendRequest {
                            stream_id: sid,
                            category_id: CATEGORY_ID,
                            first_stream_version: first_stream_pos,
                            events,
                        }),
                    )
                }
                DomainInput::Prepared { type_names, type_slots, mut batch } => {
                    let mut unique_tids = Vec::with_capacity(type_names.len());
                    for name in &type_names {
                        let id = if let Some(id) =
                            book.registry.event_type_id(name)
                        {
                            id
                        } else {
                            next_tid += 1;
                            staged.push(registry::event_type_registered(
                                next_tid, name,
                            ));
                            next_tid
                        };
                        unique_tids.push(id);
                    }
                    let tids: Vec<u32> = type_slots
                        .into_iter()
                        .map(|slot| unique_tids[slot as usize])
                        .collect();
                    batch
                        .set_event_type_ids(&tids)
                        .expect("prepared type-id shape matches frame count");
                    let first_stream_pos = expected.next_position();
                    (
                        tids,
                        DirectAppendRequest::Prepared {
                            stream_id: sid,
                            category_id: CATEGORY_ID,
                            first_stream_version: first_stream_pos,
                            batch,
                        },
                    )
                }
            };
            let first_stream_pos = expected.next_position();
            if !staged.is_empty() {
                let first_version = book.registry_next_version;
                reg_span = Some((first_version, staged.len()));
                reqs.push(
                    registry_append_request(first_version, &staged).into(),
                );
            }
            reqs.push(domain_req);
            Pre::Proceed { sid, first_stream_pos, tids }
        };
        DomainPlan { pre, reqs, staged, reg_span, completion }
    }

    fn commit_plans(&mut self, mut plans: Vec<DomainPlan>) {
        if plans.is_empty() {
            return;
        }
        let units =
            plans.iter_mut().map(|p| std::mem::take(&mut p.reqs)).collect();
        let mut completed = std::mem::take(&mut self.outcomes);
        completed.clear();
        completed.resize_with(plans.len(), PlanOutcomes::default);
        // bn-11ba: the owner-side durable-commit span — write + barrier as
        // this thread sees it. One timestamp pair per *group*. The barrier
        // half alone is already timed by the committer
        // (`CommitterMetrics::fsync`), so the difference is the write half,
        // which nothing else measures.
        let commit_started = Instant::now();
        let outcomes =
            self.direct.commit_ordered_group(units, |unit, batch, outcome| {
                completed[unit].record(
                    plans[unit].reg_span.is_some(),
                    batch,
                    outcome,
                );
            });
        self.status.outcomes.commit_latency.record(commit_started.elapsed());
        self.refresh_status();
        match outcomes {
            Err(e) => {
                for plan in plans {
                    plan.complete(Err(AppendError::Backend(
                        EngineError::Append(e.to_string()),
                    )));
                }
            }
            Ok(()) => {
                for (plan, outcomes) in
                    plans.into_iter().zip(completed.drain(..))
                {
                    self.retire_domain(plan, outcomes);
                }
            }
        }
        completed.clear();
        let trimmed = cap_plan_outcomes(&mut completed);
        if trimmed {
            self.status.outcome_scratch_trims.fetch_add(1, Ordering::Relaxed);
        }
        let retained_slots = completed.capacity();
        let retained_bytes =
            retained_slots * std::mem::size_of::<PlanOutcomes>();
        debug_assert!(retained_bytes <= OWNER_OUTCOME_RETAINED_BYTE_CAP);
        if trimmed || retained_slots != self.outcome_reported_retained_slots {
            self.status
                .outcome_scratch_retained_slots
                .store(retained_slots, Ordering::Relaxed);
            self.outcome_reported_retained_slots = retained_slots;
        }
        if trimmed || retained_bytes != self.outcome_reported_retained_bytes {
            self.status
                .outcome_scratch_retained_bytes
                .store(retained_bytes, Ordering::Relaxed);
            self.outcome_reported_retained_bytes = retained_bytes;
        }
        self.outcomes = completed;
    }

    fn retire_domain(&mut self, plan: DomainPlan, outcomes: PlanOutcomes) {
        let DomainPlan { pre, staged, reg_span, completion, .. } = plan;
        let PlanOutcomes { registry, domain } = outcomes;
        let Pre::Proceed { sid, first_stream_pos, tids } = pre else {
            unreachable!()
        };
        let mut first_error: Option<EngineError> = None;
        if let Some((first_version, count)) = reg_span {
            match registry.expect("registry outcome").outcome {
                Ok(outcome) => match expect_acked(outcome) {
                    Ok(placed) => {
                        {
                            let mut book =
                                self.publish.book.write().expect("book lock");
                            book.alloc_registry_versions(count as u64);
                            for record in staged {
                                if let Err(e) = book.apply_registration(record)
                                {
                                    book.registry_lost = true;
                                    first_error.get_or_insert(e);
                                }
                            }
                        }
                        publish_batch(
                            &self.publish,
                            registry::REGISTRY_STREAM_ID,
                            first_version,
                            count as u32,
                            placed,
                        );
                    }
                    Err(e) => {
                        first_error.get_or_insert(e);
                    }
                },
                Err(e) => {
                    first_error
                        .get_or_insert(EngineError::Append(e.to_string()));
                }
            }
        }
        let domain = domain.expect("domain outcome");
        let placed = match domain.outcome {
            Ok(outcome) => match expect_acked(outcome) {
                Ok(p) => Some(p),
                Err(e) => {
                    first_error.get_or_insert(e);
                    None
                }
            },
            Err(e) => {
                first_error.get_or_insert(EngineError::Append(e.to_string()));
                None
            }
        };
        let mut appended = None;
        if let Some(placed) = placed {
            let frame_count = tids.len() as u32;
            let last_stream = first_stream_pos + u64::from(frame_count) - 1;

            // Preserve bn-2ib's write-through capsule warm: the first
            // read-after-write is an in-memory hit rather than a header+batch
            // pread, CRC validation, and decode. The cache is bounded and
            // transparent; disabled/rejected/evicted entries fall back to the
            // same durable bytes.
            let (data, payloads) = match domain.events {
                DirectBatchEvents::Inputs(events) => {
                    let payload_bytes =
                        events.iter().map(|event| event.payload.len()).sum();
                    let mut data = Vec::with_capacity(payload_bytes);
                    let mut offs = Vec::with_capacity(events.len() + 1);
                    for event in events {
                        offs.push(data.len() as u32);
                        data.extend_from_slice(&event.payload);
                    }
                    offs.push(data.len() as u32);
                    (data, PayloadLayout::Arena(offs))
                }
                DirectBatchEvents::Prepared(batch) => {
                    let (bytes, ranges) = batch.into_bytes_and_payload_ranges();
                    (bytes, PayloadLayout::Framed(ranges))
                }
            };
            self.publish.reader.insert(
                placed.segment_id,
                placed.offset,
                Arc::new(DecodedBatch {
                    stream_id: sid,
                    first_stream_version: first_stream_pos,
                    first_global_pos: placed.first_global,
                    frame_count,
                    type_ids: tids,
                    data,
                    payloads,
                }),
            );
            publish_batch(
                &self.publish,
                sid,
                first_stream_pos,
                frame_count,
                placed,
            );
            appended = Some(Appended {
                version:              Version::At(last_stream),
                last_global_position: placed.last_global,
            });
        }
        let result = match first_error {
            Some(e) => Err(AppendError::Backend(e)),
            None => Ok(appended.expect("successful domain outcome")),
        };
        completion.finish(result);
    }

    fn process_registry(
        &mut self,
        expected: Version,
        records: Vec<RegistryRecord>,
        completion: OwnerCompletion,
    ) {
        let (first_version, trial) = {
            let book = self.publish.book.read().expect("book lock");
            if book.registry_lost {
                completion
                    .finish(Err(AppendError::Backend(registry_lost_error())));
                return;
            }
            let actual = book.registry_head();
            if actual != expected {
                completion
                    .finish(Err(AppendError::Conflict { expected, actual }));
                return;
            }
            if records.is_empty() {
                completion.finish(Ok(Appended {
                    version:              expected,
                    last_global_position: self
                        .publish
                        .read_watermark
                        .get()
                        .saturating_sub(1),
                }));
                return;
            }
            let mut trial = book.registry.clone();
            for record in &records {
                if let Err(e) =
                    trial.apply::<std::convert::Infallible>(record.clone())
                {
                    completion.finish(Err(AppendError::Backend(
                        EngineError::Append(format!(
                            "{}: {e}",
                            registry::RESERVED_STREAM_NAME
                        )),
                    )));
                    return;
                }
            }
            (book.registry_next_version, trial)
        };
        let mut outcome = None;
        let result = self.direct.commit_ordered_group(
            vec![vec![registry_append_request(first_version, &records).into()]],
            |unit, batch, completed| {
                assert_eq!((unit, batch), (0, 0));
                assert!(outcome.replace(completed).is_none());
            },
        );
        self.refresh_status();
        let outcome = match result {
            Ok(()) => outcome.expect("registry outcome"),
            Err(e) => {
                completion.finish(Err(AppendError::Backend(
                    EngineError::Append(e.to_string()),
                )));
                return;
            }
        };
        let placed = match outcome.outcome.and_then(expect_acked_log) {
            Ok(p) => p,
            Err(e) => {
                completion.finish(Err(AppendError::Backend(
                    EngineError::Append(e.to_string()),
                )));
                return;
            }
        };
        {
            let mut book = self.publish.book.write().expect("book lock");
            book.registry = trial;
            book.rebuild_arcs();
            book.alloc_registry_versions(records.len() as u64);
        }
        publish_batch(
            &self.publish,
            registry::REGISTRY_STREAM_ID,
            first_version,
            records.len() as u32,
            placed,
        );
        completion.finish(Ok(Appended {
            version:              Version::At(
                first_version + records.len() as u64 - 1,
            ),
            last_global_position: placed.last_global,
        }));
    }

    fn refresh_status(&self) {
        self.status
            .degraded
            .store(self.direct.is_degraded(), Ordering::Release);
    }

    fn run(mut self, mut rx: tokio_mpsc::Receiver<OwnerIntent>) {
        while let Some(first) = rx.blocking_recv() {
            #[cfg(test)]
            self.cohort_gate.wait_until_cohort_admitted();
            let gathered = self.gather(&mut rx, first);
            let mut pending = Vec::new();
            let mut streams = HashSet::new();
            for intent in gathered {
                let OwnerIntent { kind, completion, .. } = intent;
                match kind {
                    OwnerIntentKind::Registry { expected, records } => {
                        self.commit_plans(std::mem::take(&mut pending));
                        streams.clear();
                        self.process_registry(expected, records, completion);
                    }
                    OwnerIntentKind::Domain { stream, expected, input } => {
                        // Re-plan after flushing a prior append to this stream;
                        // that makes dequeue order the exact-version arbiter.
                        let repeated = {
                            let book =
                                self.publish.book.read().expect("book lock");
                            book.registry
                                .stream_id(&stream)
                                .is_some_and(|sid| streams.contains(&sid))
                        };
                        if repeated {
                            self.commit_plans(std::mem::take(&mut pending));
                            streams.clear();
                        }
                        if self
                            .publish
                            .book
                            .read()
                            .expect("book lock")
                            .registry_lost
                        {
                            completion.finish(Err(AppendError::Backend(
                                registry_lost_error(),
                            )));
                            continue;
                        }
                        let plan = self
                            .plan_domain(stream, expected, input, completion);
                        match &plan.pre {
                            Pre::Conflict(actual) => {
                                let actual = *actual;
                                plan.complete(Err(AppendError::Conflict {
                                    expected,
                                    actual,
                                }));
                            }
                            Pre::Empty => {
                                plan.complete(Ok(Appended {
                                    version:              expected,
                                    last_global_position: self
                                        .publish
                                        .read_watermark
                                        .get()
                                        .saturating_sub(1),
                                }));
                            }
                            Pre::Proceed { sid, .. } => {
                                let sid = *sid;
                                if !plan.staged.is_empty()
                                    || matches!(self.durability, Durability::Os)
                                {
                                    self.commit_plans(std::mem::take(
                                        &mut pending,
                                    ));
                                    streams.clear();
                                    self.commit_plans(vec![plan]);
                                } else {
                                    streams.insert(sid);
                                    pending.push(plan);
                                }
                            }
                        }
                    }
                }
            }
            self.commit_plans(pending);
        }
        self.refresh_status();
    }
}

fn expect_acked_log(
    outcome: AppendOutcome,
) -> Result<Placed, mess_log::committer::AppendError> {
    match outcome {
        AppendOutcome::Acked {
            first_position,
            last_position,
            segment_id,
            offset,
        } => Ok(Placed {
            first_global: first_position,
            last_global: last_position,
            segment_id,
            offset,
        }),
        AppendOutcome::Indeterminate => {
            Err(mess_log::committer::AppendError::StorePoisoned)
        }
    }
}

/// Shared engine state behind one `Arc`.
struct Inner {
    rt:                   RealRuntime,
    /// The one flat-combined owner. Its thread owns the segment writer and
    /// performs validation, write, barrier, apply, publish, and completion.
    owner:                AppendOwner,
    /// Exact ownership-transfer/copy counters for the public append seam.
    append_input:         AppendInputCounters,
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
    book:                 Arc<RwLock<Book>>,
    /// Block-native byte fetcher: the bounded decoded-capsule cache over the
    /// durable segment blocks (bn-2ib). Every read path resolves positions
    /// through the index tiers and bytes through this.
    reader:               Arc<BlockReader>,
    /// Payload frames decoded during [`recover`](LogEngine::recover) — the
    /// bn-2ib "zero old payload decodes on open" gate's observable. `0` for
    /// every chain-off open; chain-on stores still fold every durable
    /// payload (spec 05 §6 requires it).
    recover_decodes:      u64,
    /// The **published** global watermark — the exclusive end of the canonical
    /// global-position sequence, including filtered `$registry` events.
    /// Advanced at
    /// the end of each append's publish step (after the head/index/meta
    /// are updated, in publish-turn order), so it tracks what
    /// [`read_global_page`](Backend::read_global_page) can account for as a
    /// record or scan frontier, NOT merely what the direct committer has
    /// durably covered. The app-facing subscription /
    /// live-tail
    /// API ([`SubscribeBackend`](crate::backend::SubscribeBackend)) awaits
    /// this value; a woken subscriber is therefore guaranteed the position
    /// it waited for is already scannable through the index tiers, though an
    /// engine-internal position itself is not delivered. Distinct
    /// from
    /// the direct owner's durable watermark,
    /// which advances a step earlier (at ack, before the in-process
    /// publish).
    read_watermark:       Watermark,
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
    /// bn-3of: whether on-demand [`seal_active`](LogEngine::seal_active)
    /// writes a consolidated SealPack (`.seal`) instead of the sidecar
    /// trio ([`EngineOptions::seal_pack`]). The background roll-sealer
    /// captures the same flag directly in its `SealDriver`.
    seal_pack:            bool,
    /// bn-30u: what this open learned about its sealed-index candidates —
    /// which were refuted and why, whether each was quarantined, and which
    /// segments were re-queued for sealing as a result. Fixed at open (the
    /// classification runs once, before any reader exists) and read back via
    /// [`LogEngine::sealed_candidate_health`].
    candidate_health:     SealedCandidateHealth,
    /// bn-11ba: seal jobs queued or in progress right now (rolls reported by
    /// the committer plus owed re-seals enqueued at open, minus jobs the
    /// sealer thread has finished). The live background-seal backlog.
    seal_queue_depth:     Arc<AtomicUsize>,
    /// bn-11ba: cumulative seal jobs the sealer thread has dequeued.
    seal_jobs_dequeued:   Arc<AtomicU64>,
    /// bn-11ba: this open's registry-delta accelerator hit count.
    reg_delta_admitted:   u64,
    /// bn-11ba: this open's registry-delta fallback (point-read) count.
    reg_delta_fallback:   u64,
}

impl Drop for Inner {
    fn drop(&mut self) {
        // Order is load-bearing (`bn-1vu`). 1) Close and join the owner: it
        // drains queued intents, drops its direct committer and `Roller`, and
        // closes the roll channel. 2) Join the seal thread: with the channel
        // closed it drains every queued roll seal (making each sidecar + footer
        // durable) and exits — so a drop-then-reopen sees the finished seals on
        // disk, never a half-written tier.
        drop(self.owner.tx.take());
        if let Some(join) = self.owner.join.take() {
            let _ = join.join();
        }
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
    /// Sleep between polls of the hot index / published watermark.
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
/// Cheap to clone — every clone shares the same flat owner, index,
/// interners, and caches (matching [`MockBackend`](crate::mock)'s
/// shared-handle semantics, so a facade reopen over `engine.clone()` sees
/// the same state).
#[derive(Clone)]
pub struct LogEngine {
    inner: Arc<Inner>,
}

/// Open-time knobs for [`LogEngine::open_with`].
#[derive(Debug, Clone)]
pub struct EngineOptions {
    /// Durability mode for the append owner.
    pub durability:                 Durability,
    /// Active-segment size in bytes (preallocated at open).
    pub segment_size:               u64,
    /// Sealed pointer-block cache budget in bytes (`bn-e2y` / bn-1hx). `0`
    /// disables the cache (every sealed replay decodes fresh); a non-zero
    /// budget caches decoded blocks so a repeated sealed-stream replay (a
    /// projection rebuild, a subscription re-read) skips the decode — and
    /// makes the `cache_hits`/`cache_misses` runtime metrics meaningful
    /// under load.
    pub block_cache_budget_bytes:   u64,
    /// Decoded-capsule cache budget in bytes (bn-2ib): the bounded,
    /// bytes-weighted cache of CRC-validated decoded batches every read path
    /// fetches record bytes through (plus a small slice of it for sealed
    /// per-segment global batch directories). `0` disables it — every read
    /// then decodes fresh from the durable blocks, byte-identically (the
    /// cache is transparent to results by construction).
    pub capsule_cache_budget_bytes: u64,
    /// Emit the per-batch on-disk fold chain (`crypto_chain`, spec 05 §6,
    /// `bn-3l0`). **Off by default**: when `false`, segments are
    /// byte-identical to a store that never knew about the chain. When
    /// `true`, every batch the committer writes carries its real
    /// `crypto_chain` (offset 72, flag bit 0), per-stream heads are
    /// rehydrated on recovery, and `mess verify --full` recomputes the
    /// chain to catch a CRC-repaired payload tamper.
    pub chain:                      bool,
    /// Seal-time Reed-Solomon parity sidecar policy (bn-2za). **Disabled by
    /// default** (evidence-gated): when enabled, the background sealer writes
    /// a `.par` sidecar next to each sealed segment so `mess verify
    /// --repair` can reconstruct latent-sector / bit-rot damage offline.
    pub parity:                     mess_index::sealed::parity::ParityConfig,
    /// bn-3of (Spike I): emit ONE consolidated SealPack (`.seal`) per sealed
    /// segment instead of the `.pidx`/`.filter`/`.pcol` sidecar trio. **On by
    /// default** (bn-ccx1) — the background roll-sealer and
    /// [`seal_active`](LogEngine::seal_active) both write a single `.seal` per
    /// segment (install protocol: build → verify vs raw → fdatasync → rename
    /// → dir fsync → footer → publish), and the footer names
    /// `blake3(header ++ directory)` so a stand-in pack is refutable
    /// (bn-11g).
    ///
    /// Set to `false` for the **compatibility mode**: the sealer writes the
    /// legacy loose sidecar trio and a short unnamed footer, exactly as
    /// pre-bn-ccx1 stores did. Loose sidecars are a permanently supported
    /// shape in both directions — reopen dual-reads (a `.seal` is preferred
    /// when present, else the sidecars), the two families coexist in one
    /// store, and a legacy segment is never spontaneously re-sealed, so
    /// existing stores stay loose until natural re-rolls convert their tail.
    ///
    /// bn-3qh0 closed the one gap the flip left. `mess rebuild-index` still
    /// has no offline pack encoder and deliberately never will: for a
    /// pack-sealed segment it calls
    /// [`withdraw_sealed_index`](crate::withdraw_sealed_index) and the
    /// engine's own re-seal path rebuilds the pack from the log at the
    /// next open. That is what restores a segment whose `.seal` was lost —
    /// the one state bn-30u's candidate lifecycle cannot reach by itself,
    /// because a deleted artifact is no candidate to refute.
    ///
    /// Decision record: bn-1yor (default-on scale and fault matrix) / bn-ccx1
    /// (the flip).
    pub seal_pack:                  bool,
    /// The TOTAL bound on how long [`LogEngine`]'s `Drop` will wait for the
    /// background seal thread to drain queued rolls (`bn-u6o`). Only matters
    /// when a roll was reported but its publish never caught up (an abandoned
    /// append future) — the ordinary case drains near-instantly. A skipped
    /// seal at shutdown is safe (the segment stays durable + unsealed,
    /// served from the log on reopen), so this bounds worst-case shutdown
    /// latency rather than protecting correctness. Default 2s.
    pub shutdown_seal_budget:       Duration,
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
    pub commit: CommitterMetrics,
    /// The direct owner's durable watermark position (highest durable global
    /// position + 1). It can lead the published read watermark while the
    /// in-process index tiers catch up. Consequently, app-facing subscription
    /// lag (SUB9, `docs/spec/06-subscriptions.md`) uses the published read
    /// watermark exposed as [`EngineMetrics::total_events`], not this field.
    /// Both values include filtered `$registry` positions and neither is an
    /// application-event backlog count.
    pub durable_watermark: u64,
    /// Whether a barrier fault has poisoned the store (D8): writes fail fast,
    /// reads clamp to the frozen watermark. Distinct from `fsync_degraded`
    /// (merely slow but still `Ok`, §2.6).
    pub degraded_poisoned: bool,
    /// FlatOwner result slots retained for index-aligned publication.
    pub owner_outcome_scratch_retained_slots: usize,
    /// FlatOwner result scratch retained bytes.
    pub owner_outcome_scratch_retained_bytes: usize,
    /// Oversize owner result groups whose transient capacity was shed.
    pub owner_outcome_scratch_trims: usize,
    /// Intent slots currently occupied in the bounded append-owner channel.
    ///
    /// A quiescent engine reports zero. This is an instantaneous occupancy,
    /// not a cumulative admission counter; callers must sample only after
    /// their append cohort has completed when using it as a leak check.
    pub owner_intent_slots_in_use: usize,
    /// Byte-budget permits currently held by admitted append-owner intents
    /// **and by producer preparation still under construction** (`bn-1gn1`).
    ///
    /// A quiescent engine reports zero. Like
    /// [`owner_intent_slots_in_use`](Self::owner_intent_slots_in_use), this is
    /// an instantaneous occupancy intended for boundedness diagnostics.
    ///
    /// Since `bn-1gn1` this is the engine's whole prepared-memory high-water
    /// mark, not just its queued footprint: a producer reserves its
    /// conservative build peak against the same ring *before* materializing
    /// the prepared copy, then reconciles down to the exact admission
    /// cost. That makes the reading transiently larger than the queued
    /// bytes while preparation is in flight, which is the intended meaning
    /// — it is the number that is actually bounded by `OWNER_RING_BYTES`.
    pub owner_intent_bytes_in_use: usize,
    /// Cumulative sealed block-cache hits.
    pub cache_hits: u64,
    /// Cumulative sealed block-cache misses.
    pub cache_misses: u64,
    /// Block-cache hit rate over all lookups so far, `[0, 1]`.
    pub cache_hit_rate: f64,
    /// Live cached blocks.
    pub cache_entries: usize,
    /// Resident cache weight in bytes.
    pub cache_weight_bytes: u64,
    /// Sealed segments installed in the cold tier.
    pub sealed_segment_count: usize,
    /// Total canonical v3 events committed and published (the published read
    /// watermark), including filtered `$registry` events. Not the number of
    /// application-visible records.
    pub total_events: u64,
    /// Age of the active segment since this process opened it, in seconds.
    pub active_segment_age_secs: f64,
    /// Seal-path durability-barrier (`fsync`) latency (`bn-e2y`): the
    /// sidecar + directory fsyncs the sealer issues off the append path.
    /// A near-full SSD stalls these exactly as it stalls the commit
    /// barrier, so they are timed and alarmed separately from
    /// [`commit`](Self::commit)`.fsync`.
    pub seal_fsync: LatencySnapshot,
    /// Whether seal-path barrier latency has crossed the degradation threshold
    /// — the sticky store-status flag (§2.6) for the seal durability site.
    pub seal_fsync_degraded: bool,
    /// Seal-path barriers that crossed the threshold.
    pub seal_fsync_degraded_trips: u64,
    /// Seal duration (`roll → sealed installed`) latency distribution.
    pub seal_duration: LatencySnapshot,
    /// Segments sealed since this process opened.
    pub seals: u64,
    /// Seals skipped rather than completed (`bn-u6o`): the background
    /// roll-sealer gave up waiting for the hot index/watermark to catch up
    /// (its
    /// bounded per-segment spin, live) or a queued seal did not finish inside
    /// the bounded total shutdown wait (`Inner::drop`). Either way the segment
    /// stays durable and unsealed — served from the log until it is resealed
    /// or the store reopens — but an operator MUST be able to see it happened;
    /// see `mess_index::sealed::SealMetrics::record_seal_skipped`.
    pub seals_skipped: u64,
    /// Sealed-index candidates this open refuted (bn-30u): a sidecar found on
    /// disk that could not be admitted — it did not parse, it named a
    /// different segment, its coverage was not durable, or its segment is
    /// gone. Non-zero is not data loss (the raw log is authority and served
    /// the segment throughout) but it IS a crash window or a sealer bug that
    /// an operator should see. The reason per candidate is in
    /// [`LogEngine::sealed_candidate_health`].
    pub sealed_candidates_refuted: u64,
    /// Refuted candidates successfully renamed out of the candidate namespace
    /// (bn-30u). Equal to
    /// [`sealed_candidates_refuted`](Self::sealed_candidates_refuted) in the
    /// healthy case.
    pub sealed_candidates_quarantined: u64,
    /// Refuted candidates whose quarantine rename FAILED (bn-30u) — a
    /// read-only or full filesystem. The store is still correct, but those
    /// candidates will be re-evaluated at the next reopen: it has not
    /// converged.
    pub sealed_candidate_quarantine_failures: u64,
    /// Rolled segments re-queued for sealing at this open (bn-30u) — the
    /// *pending re-seal* set. Zero for a healthy store; non-zero means the
    /// background sealer is working through segments that lost (or never
    /// had) an admissible sidecar.
    pub sealed_reseals_enqueued: u64,
}

/// Monotonic in-process counters for the append-input path selected by
/// [`LogEngine`]. Unlike allocator telemetry their field meanings are
/// deterministic: copied bytes count only defensive copies at the borrowed
/// compatibility boundary, never framing or the durable write itself. A
/// snapshot loads each field independently; cross-field relationships and
/// interval deltas are coherent only while append submissions are quiescent.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct AppendInputMetrics {
    /// Batches submitted through the Process-only owned path.
    /// Includes conflicts and invalid/empty submissions: this is boundary
    /// traffic, not a successful-commit counter.
    pub owned_batches:       u64,
    /// Records whose payload buffers crossed by move, including submissions
    /// later rejected by the authoritative owner.
    pub owned_records:       u64,
    /// Payload bytes whose buffers crossed by move, including submissions
    /// later rejected by the authoritative owner.
    pub owned_payload_bytes: u64,
    /// Batches entering the borrowed compatibility path, including owned API
    /// submissions deliberately materialized under Group or Os durability and
    /// submissions later rejected by the authoritative owner.
    pub borrowed_batches:    u64,
    /// Records submitted through the borrowed compatibility method.
    pub borrowed_records:    u64,
    /// Records defensively cloned at the borrowed boundary. Large prepared
    /// batches already frame directly and therefore do not increment this.
    pub copied_records:      u64,
    /// Message-type plus payload bytes defensively cloned at that boundary.
    pub copied_bytes:        u64,
}

#[derive(Default)]
struct AppendInputCounters {
    owned_batches:       AtomicU64,
    owned_records:       AtomicU64,
    owned_payload_bytes: AtomicU64,
    borrowed_batches:    AtomicU64,
    borrowed_records:    AtomicU64,
    copied_records:      AtomicU64,
    copied_bytes:        AtomicU64,
}

impl AppendInputCounters {
    fn snapshot(&self) -> AppendInputMetrics {
        AppendInputMetrics {
            owned_batches:       self.owned_batches.load(Ordering::Relaxed),
            owned_records:       self.owned_records.load(Ordering::Relaxed),
            owned_payload_bytes: self
                .owned_payload_bytes
                .load(Ordering::Relaxed),
            borrowed_batches:    self.borrowed_batches.load(Ordering::Relaxed),
            borrowed_records:    self.borrowed_records.load(Ordering::Relaxed),
            copied_records:      self.copied_records.load(Ordering::Relaxed),
            copied_bytes:        self.copied_bytes.load(Ordering::Relaxed),
        }
    }
}

impl Default for EngineOptions {
    fn default() -> Self {
        EngineOptions {
            // `Process`: ack the moment the covering write returns. The
            // index is in-process, so per-batch fsync buys nothing for
            // correctness here; benches override this with `Group`.
            durability:                 Durability::Process,
            segment_size:               256 * 1024 * 1024,
            // On by default (64 MiB): the sealed block cache is transparent to
            // results and pays for itself on repeat replay (perf_replay's 48%
            // hit rate), and a live cache is what makes the hit/miss runtime
            // metrics operationally meaningful. Set to 0 to disable.
            block_cache_budget_bytes:   64 * 1024 * 1024,
            // bn-2ib: 64 MiB of decoded hot/cold record capsules. Bounded and
            // transparent to results; sized so a hot working set (recent
            // appends + a projection's replay window) stays decoded.
            capsule_cache_budget_bytes: 64 * 1024 * 1024,
            // Fold chain off by default: opt in per store for tamper-evident
            // segments (spec 05 §6).
            chain:                      false,
            // bn-2za: parity is opt-in / evidence-gated — off by default.
            parity:
                mess_index::sealed::parity::ParityConfig::default(),
            // bn-3of/bn-ccx1: consolidated SealPack ON by default. The
            // bn-1yor matrix admitted the flip: 3.88 -> 1.00 files per sealed
            // segment, 155x lower reopen residency, ~4x fewer seal fsync
            // barriers, cold-open parity, floors 14/14, for +0.26% store
            // bytes. `false` selects the loose-sidecar compatibility mode.
            seal_pack:                  true,
            // bn-u6o: bound total shutdown drain latency, not correctness — a
            // skipped seal at shutdown is safe (served from the log on
            // reopen). 2s is comfortably above a healthy drain (near-instant)
            // and comfortably below the old ~10s-per-abandoned-roll worst
            // case.
            shutdown_seal_budget:       Duration::from_secs(2),
        }
    }
}

impl LogEngine {
    /// Test/diagnostic: total canonical v3 events committed and published —
    /// the exclusive end of the global-position sequence (the published read
    /// watermark). This includes `$registry` events filtered from
    /// application-facing global reads, so it is not a visible-event count.
    #[must_use]
    pub fn total_events(&self) -> usize {
        self.inner.read_watermark.get() as usize
    }

    /// Test/diagnostic (bn-2ib): payload frames decoded during
    /// [`recover`](Self::recover) on this open. `0` for every chain-off
    /// open — the "open performs zero old payload decodes" gate observable.
    /// Chain-on stores still fold every durable payload (spec 05 §6).
    #[must_use]
    pub fn recover_payload_decodes(&self) -> u64 { self.inner.recover_decodes }

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
        let commit = self.inner.owner.status.metrics.snapshot();
        let cache = &self.inner.block_cache;
        let seal = self.inner.seal_metrics.snapshot();
        let owner_intent_slots_in_use =
            self.inner.owner.tx.as_ref().map_or(0, |tx| {
                OWNER_RING_CAPACITY
                    .checked_sub(tx.capacity())
                    .expect("owner channel capacity exceeds its fixed bound")
            });
        let owner_intent_bytes_in_use = OWNER_RING_BYTES
            .checked_sub(self.inner.owner.bytes.available_permits())
            .expect("owner byte permits exceed their fixed bound");
        EngineMetrics {
            commit,
            durable_watermark: self.inner.owner.durable_watermark.get(),
            degraded_poisoned: self
                .inner
                .owner
                .status
                .degraded
                .load(Ordering::Acquire),
            owner_outcome_scratch_retained_slots: self
                .inner
                .owner
                .status
                .outcome_scratch_retained_slots
                .load(Ordering::Relaxed),
            owner_outcome_scratch_retained_bytes: self
                .inner
                .owner
                .status
                .outcome_scratch_retained_bytes
                .load(Ordering::Relaxed),
            owner_outcome_scratch_trims: self
                .inner
                .owner
                .status
                .outcome_scratch_trims
                .load(Ordering::Relaxed),
            owner_intent_slots_in_use,
            owner_intent_bytes_in_use,
            cache_hits: cache.hits(),
            cache_misses: cache.misses(),
            cache_hit_rate: cache.hit_rate(),
            cache_entries: cache.len(),
            cache_weight_bytes: cache.weight_bytes(),
            sealed_segment_count: self.inner.sealed.len(),
            total_events: self.inner.read_watermark.get(),
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
            sealed_candidates_refuted: self.inner.candidate_health.refuted(),
            sealed_candidates_quarantined: self
                .inner
                .candidate_health
                .quarantined(),
            sealed_candidate_quarantine_failures: self
                .inner
                .candidate_health
                .quarantine_fails,
            sealed_reseals_enqueued: self
                .inner
                .candidate_health
                .reseals_enqueued(),
        }
    }

    /// The composed operational account of this engine (`bn-11ba`) — one
    /// report that says which state is canonical, which accelerators are
    /// installed, and whether background work or fallback paths are
    /// accumulating.
    ///
    /// This is a **composition** of surfaces that already existed
    /// ([`metrics`](Self::metrics),
    /// [`append_input_metrics`](Self::append_input_metrics),
    /// [`sealed_candidate_health`](Self::sealed_candidate_health), the sealed
    /// tier's per-segment accessors, and the canonical registry fold's
    /// high-water marks) plus the append-outcome, group-shape and backlog
    /// counters this bone added. Nothing here is authoritative that was not
    /// authoritative before: see
    /// [`observability::AUTHORITY_MODEL`](crate::observability::AUTHORITY_MODEL).
    ///
    /// Cost: this walks the installed sealed segments and takes a short read
    /// lock on the record book, so it is a **diagnostic** call, not something
    /// to poll in a tight loop. [`metrics`](Self::metrics) remains the cheap
    /// scalar surface.
    #[must_use]
    pub fn observability(&self) -> EngineObservability {
        let m = self.metrics();
        let seal = self.inner.seal_metrics.snapshot();
        let durability = self.inner.owner.durability;
        let outcomes = &self.inner.owner.status.outcomes;

        // --- accelerators: one row per installed sealed segment ------------
        let mut rows: Vec<SealedSegmentReport> = self
            .inner
            .sealed
            .segments_with_gens()
            .into_iter()
            .map(|(sref, generation)| {
                let evicted = self.inner.sealed.is_evicted(sref.segment_id());
                SealedSegmentReport::of(&sref, generation, evicted)
            })
            .collect();
        rows.sort_unstable_by_key(|r| r.segment_id);
        let seal_pack_segments = rows
            .iter()
            .filter(|r| r.representation == SealedRepresentation::SealPack)
            .count();
        let sealed_index_resident_bytes: u64 =
            rows.iter().map(|r| r.resident_bytes).sum();
        let sealed_install_generation =
            rows.iter().map(|r| r.install_generation).max().unwrap_or(0);

        // --- canonical registry high-water marks ---------------------------
        let (stream_hwm, category_hwm, event_type_hwm, dict_hwm) = {
            let book = self.inner.book.read().expect("book lock");
            let st = &book.registry;
            (
                st.stream_high_water_mark(),
                st.category_high_water_mark(),
                st.event_type_high_water_mark(),
                st.dict_high_water_mark(),
            )
        };

        let health = &self.inner.candidate_health;
        EngineObservability {
            authority:    AUTHORITY_MODEL,
            owner:        OwnerReport {
                durability_mode:       DurabilityMode::of(durability),
                queue_slots_in_use:    m.owner_intent_slots_in_use,
                queue_slots_capacity:  OWNER_RING_CAPACITY,
                queue_bytes_in_use:    m.owner_intent_bytes_in_use,
                queue_bytes_capacity:  OWNER_RING_BYTES,
                group_width:           outcomes.group_width.snapshot(),
                group_wait:            outcomes.group_wait.snapshot(),
                ack_latency:           outcomes.ack.snapshot(),
                commit_latency:        outcomes.commit_latency.snapshot(),
                conflicts:             outcomes.conflicts.get(),
                cancellations:         outcomes.cancellations.get(),
                groups:                m.commit.groups,
                batches:               m.commit.batches,
                events:                m.commit.events,
                bytes:                 m.commit.bytes,
                outcome_scratch_slots: m.owner_outcome_scratch_retained_slots,
                outcome_scratch_bytes: m.owner_outcome_scratch_retained_bytes,
                outcome_scratch_trims: m.owner_outcome_scratch_trims,
                append_input:          self.append_input_metrics(),
            },
            durability:   DurabilityReport::compose(
                durability,
                &m.commit,
                &seal,
                m.degraded_poisoned,
                self.inner.owner.chain_enabled,
            ),
            state:        StateReport {
                log_format_version: mess_log::format::FORMAT_VERSION,
                published_watermark: m.total_events,
                durable_watermark: m.durable_watermark,
                active_index_applied_end: self.inner.active.applied_end(),
                active_segment_age_secs: m.active_segment_age_secs,
                sealed_segment_count: m.sealed_segment_count,
                sealed_install_generation,
                sealed_index_resident_bytes,
                block_cache_entries: m.cache_entries,
                block_cache_bytes: m.cache_weight_bytes,
                block_cache_hits: m.cache_hits,
                block_cache_misses: m.cache_misses,
                block_cache_hit_rate: m.cache_hit_rate,
                registry_stream_hwm: stream_hwm,
                registry_category_hwm: category_hwm,
                registry_event_type_hwm: event_type_hwm,
                registry_dict_hwm: dict_hwm,
                recover_payload_decodes: self.inner.recover_decodes,
            },
            accelerators: AcceleratorReport {
                seal_pack_enabled: self.inner.seal_pack,
                seal_pack_segments,
                loose_sidecar_segments: rows.len() - seal_pack_segments,
                segments: rows,
            },
            fallbacks:    FallbackReport {
                sealed_candidates_refuted:     m.sealed_candidates_refuted,
                sealed_candidates_quarantined: m.sealed_candidates_quarantined,
                quarantine_failures:           m
                    .sealed_candidate_quarantine_failures,
                refutations:                   health.refutations.clone(),
                registry_delta_admitted:       self.inner.reg_delta_admitted,
                registry_delta_fallback:       self.inner.reg_delta_fallback,
                seals_skipped:                 m.seals_skipped,
            },
            backlog:      BacklogReport {
                reseals_owed_at_open: m.sealed_reseals_enqueued,
                pending_reseal:       health.pending_reseal.clone(),
                seal_queue_depth:     self
                    .inner
                    .seal_queue_depth
                    .load(Ordering::Relaxed),
                seal_jobs_dequeued:   self
                    .inner
                    .seal_jobs_dequeued
                    .load(Ordering::Relaxed),
                seals_completed:      m.seals,
                seals_skipped:        m.seals_skipped,
                seal_duration:        m.seal_duration,
            },
        }
    }

    /// What this open learned about its sealed-index candidates (bn-30u): the
    /// per-candidate refutation reason, whether each was quarantined, and the
    /// segments re-queued for a fresh seal as a result.
    ///
    /// The counters are also on the `Copy` [`EngineMetrics`]; this is the
    /// surface that carries the *reason strings* and the pending-re-seal
    /// segment ids. Fixed for the lifetime of the engine handle — candidate
    /// classification happens exactly once, during open.
    #[must_use]
    pub fn sealed_candidate_health(&self) -> &SealedCandidateHealth {
        &self.inner.candidate_health
    }

    /// Ownership-transfer and defensive-copy counters for append submissions
    /// since this engine opened.
    ///
    /// Fields are independently sampled atomics. A snapshot is monotonic, but
    /// cross-field relationships and deltas are exact only when submissions
    /// are quiescent across both snapshots.
    #[must_use]
    pub fn append_input_metrics(&self) -> AppendInputMetrics {
        self.inner.append_input.snapshot()
    }

    /// Test/diagnostic: number of sealed segments currently installed in the
    /// cold tier (populated at open by [`load_sealed`], and by
    /// [`seal_active`](Self::seal_active) at runtime).
    #[must_use]
    pub fn sealed_segment_count(&self) -> usize { self.inner.sealed.len() }

    /// Fold `$registry` out of the durable log and return the materialized
    /// state (`bn-2di`) — spec 04 §7.1 step 2, run on demand.
    ///
    /// This is the **verification** seam: it re-derives the registry from the
    /// log bytes exactly as `recover` does, with no reference to the in-memory
    /// interner, so a caller can diff the result against it. `mess doctor` uses
    /// it for the shadow-period equivalence check and `mess migrate
    /// registry` uses it to prove an import re-folds to the
    /// rows it imported.
    ///
    /// Reads through the engine's own tiers (`$registry` has its own stream id,
    /// so the sealed pointer index and the active index resolve its batches
    /// directly), never a full log scan.
    pub async fn fold_registry(
        &self,
    ) -> Result<registry::RegistryState, EngineError> {
        let sid = registry::REGISTRY_STREAM_ID;
        let mut fold = registry::Fold::new();

        // Cold tier: every sealed segment that carries a stream-0 batch.
        let mut segs = self.inner.sealed.segments_for_stream(sid);
        segs.sort_by_key(|s| s.base_pos());
        for seg in &segs {
            for e in seg.stream_entries(sid).map_err(|e| {
                EngineError::SealedRead(format!("$registry entries: {e}"))
            })? {
                let batch = self.inner.reader.batch(
                    &self.inner.sealed,
                    e.ptr,
                    BatchExpect {
                        stream_id:        sid,
                        first_global_pos: e.first_global_pos,
                        frame_count:      e.frame_count,
                        first_version:    Some(e.first_version),
                    },
                )?;
                fold.push_batch(e.first_global_pos, batch.payloads());
            }
        }

        // Hot tier: everything the active index still holds.
        for e in self.inner.active.stream_entries_from(sid, 0, usize::MAX) {
            let batch = self.inner.reader.batch(
                &self.inner.sealed,
                e.ptr,
                BatchExpect {
                    stream_id:        sid,
                    first_global_pos: e.first_global_pos,
                    frame_count:      e.frame_count,
                    first_version:    Some(e.first_version),
                },
            )?;
            fold.push_batch(e.first_global_pos, batch.payloads());
        }

        fold.finish::<std::convert::Infallible>()
            .map_err(|e| EngineError::Registry(format!("fold: {e}")))
    }

    /// Resolve a stream name back to its interned id, through the same interner
    /// the engine names events with (`bn-2di` — the migration's "does this name
    /// round-trip?" probe).
    #[must_use]
    pub fn stream_id_of(&self, name: &str) -> Option<u64> {
        let book = self.inner.book.read().expect("book lock");
        book.registry.stream_id(name)
    }

    /// Resolve an event-type name back to its interned id (see
    /// [`stream_id_of`](Self::stream_id_of)).
    #[must_use]
    pub fn event_type_id_of(&self, name: &str) -> Option<u32> {
        let book = self.inner.book.read().expect("book lock");
        book.registry.event_type_id(name)
    }

    async fn enqueue_owned_domain(
        &self,
        stream_id: &str,
        expected: Version,
        batch: OwnedAppendBatch,
        inflight: InFlightGuard,
    ) -> OwnerResult {
        let encoded_estimate = HEADER_LEN
            .saturating_add(MARKER_LEN)
            .saturating_add(
                usize::from(self.inner.owner.chain_enabled) * CHAIN_LEN,
            )
            .saturating_add(batch.len().saturating_mul(SUBFRAME_HDR_LEN))
            .saturating_add(batch.payload_bytes());
        let can_prepare = !batch.is_empty()
            && encoded_estimate >= PREPARE_MIN_ENCODED_BYTES
            // Invalid-input precedence remains owner-first. Producer
            // preparation is selected only when every typed bound is already
            // known to be representable, so framing below cannot introduce an
            // earlier user-visible error.
            && (encoded_estimate as u64) <= MAX_BATCH_LEN
            && batch.len() <= u32::MAX as usize
            && batch
                .payloads
                .iter()
                .all(|payload| payload.len() <= u32::MAX as usize);

        // bn-1gn1: reserve the conservative build peak BEFORE materializing the
        // prepared copy, so engine-owned construction memory is bounded by the
        // same ring that bounds queued intents. No new queue and no ordering
        // change: this is the ring every append already passes through, taken
        // a few statements earlier on the one path that allocates first.
        let build_permit = if can_prepare {
            let type_name_bytes = match &batch.types {
                OwnedTypeLayout::Empty => 0,
                OwnedTypeLayout::Homogeneous(name) => name.len(),
                OwnedTypeLayout::Heterogeneous(types) => types
                    .names
                    .iter()
                    .map(String::len)
                    .fold(0usize, usize::saturating_add),
            };
            Some(
                self.reserve_owner_bytes(prepare_build_peak(
                    stream_id.len(),
                    batch.len(),
                    encoded_estimate,
                    type_name_bytes,
                ))
                .await?,
            )
        } else {
            None
        };

        let (input, cost) = if can_prepare {
            let OwnedAppendBatch { payloads, types, .. } = batch;
            let frame_count = payloads.len();
            let (type_names, type_slots) = match types {
                OwnedTypeLayout::Empty => {
                    unreachable!("non-empty batch has a type")
                }
                OwnedTypeLayout::Homogeneous(name) => {
                    (vec![name], vec![0; frame_count])
                }
                OwnedTypeLayout::Heterogeneous(types) => (
                    types.names.into_vec(),
                    types
                        .slots
                        .into_iter()
                        .map(|slot| {
                            u32::try_from(slot)
                                .expect("slot count is bounded by frame count")
                        })
                        .collect(),
                ),
            };
            let subframes: Vec<Subframe<'_>> = payloads
                .iter()
                .map(|payload| Subframe::plain(0, 0, 0, payload))
                .collect();
            let zero_chain = [0u8; CHAIN_LEN];
            let prepared = PreparedBatch::encode(&BatchInput {
                segment_epoch:        0,
                batch_id:             0,
                first_global_pos:     0,
                stream_id:            0,
                category_id:          0,
                first_stream_version: 0,
                crypto_chain:         self
                    .inner
                    .owner
                    .chain_enabled
                    .then_some(&zero_chain),
                subframes:            &subframes,
            })
            .map_err(|e| {
                AppendError::Backend(EngineError::Append(e.to_string()))
            })?;
            let cost = stream_id
                .len()
                .saturating_add(prepared.total_len() as usize)
                .saturating_add(
                    type_names
                        .iter()
                        .map(String::len)
                        .fold(0usize, usize::saturating_add),
                )
                .saturating_add(type_slots.len() * 4);
            (
                DomainInput::Prepared {
                    type_names,
                    type_slots,
                    batch: prepared,
                },
                cost,
            )
        } else {
            let cost = stream_id.len().saturating_add(batch.queued_bytes());
            (DomainInput::Owned(batch), cost)
        };

        let kind = OwnerIntentKind::Domain {
            stream: stream_id.to_owned(),
            expected,
            input,
        };
        match build_permit {
            Some(permit) => {
                self.enqueue_owner_with_permit(kind, cost, permit, inflight)
                    .await
            }
            None => self.enqueue_owner(kind, cost, inflight).await,
        }
    }

    /// Append to `$registry` (stream 0) through the [`Backend`] seam — spec
    /// 04's [`Registry`](crate::registry::Registry) writer path, over the real
    /// engine (`bn-2di`, review F2).
    ///
    /// This is the *other* producer of `$registry` records: the record kinds
    /// the engine itself never mints (categories, dictionaries, aliases)
    /// plus explicit stream/event-type registrations. It is NOT a second
    /// writer of the registry — it folds through the exact same
    /// [`RegistryState`](registry::RegistryState) as the engine's own mints
    /// ([`Book::apply_registration`]), under the same lock, with the same
    /// `hwm + 1` allocation rule, and it is subject to the same REG-rules. A
    /// record that does not fold is rejected here, BEFORE it can reach the log
    /// (REG13): `$registry` is append-only and never compacted, so a
    /// rule-violating record that got written would make the store unopenable
    /// forever (REG14 — such a record must never legitimately exist).
    ///
    /// The version check is against the registry's *allocator*, not `heads[0]`:
    /// engine mints advance the allocator under the `Book` lock at submit time
    /// and `heads[0]` only later, at publish time, so checking the head would
    /// admit two writes against the same version.
    async fn append_registry(
        &self,
        expected: Version,
        records: &[RecordToAppend],
        inflight: InFlightGuard,
    ) -> Result<Appended, AppendError<EngineError>> {
        // Decode BEFORE anything else. A caller writing arbitrary domain frames
        // to the literal name `"$registry"` is refused right here — those bytes
        // are the ones recovery decodes as registry records, so accepting them
        // would corrupt the one stream the store's names live in.
        let mut decoded: Vec<RegistryRecord> =
            Vec::with_capacity(records.len());
        for r in records {
            if r.message_type != registry::REGISTRY_EVENT_TYPE_NAME {
                return Err(AppendError::Backend(EngineError::Append(format!(
                    "{}: only {} records may be appended (got {:?})",
                    registry::RESERVED_STREAM_NAME,
                    registry::REGISTRY_EVENT_TYPE_NAME,
                    r.message_type,
                ))));
            }
            let record =
                RegistryRecord::decode::<std::convert::Infallible>(&r.data)
                    .map_err(|e| {
                        AppendError::Backend(EngineError::Append(format!(
                            "{}: {e}",
                            registry::RESERVED_STREAM_NAME
                        )))
                    })?;
            decoded.push(record);
        }

        self.enqueue_owner(
            OwnerIntentKind::Registry { expected, records: decoded },
            records.iter().map(|r| r.data.len() + r.message_type.len()).sum(),
            inflight,
        )
        .await
    }

    /// Acquire `bytes` worth of the owner ring, clamped into range.
    ///
    /// One oversized intent may occupy the whole byte budget; the direct
    /// committer's preflight then returns the real typed encode error. It
    /// never waits forever trying to acquire more permits than exist.
    async fn reserve_owner_bytes(
        &self,
        bytes: usize,
    ) -> Result<OwnedSemaphorePermit, AppendError<EngineError>> {
        Arc::clone(&self.inner.owner.bytes)
            .acquire_many_owned(bytes.clamp(1, OWNER_RING_BYTES) as u32)
            .await
            .map_err(|_| {
                AppendError::Backend(EngineError::Append(
                    "append owner is closed".into(),
                ))
            })
    }

    /// Reconcile a build-peak reservation down (or up) to the exact admission
    /// `cost` of the intent that was actually produced (`bn-1gn1`).
    ///
    /// The reservation taken before preparation is a deliberate over-estimate,
    /// so the common direction is *release*: the excess goes back to the ring
    /// the instant the built size is known, rather than being held for the
    /// lifetime of the queued intent. `merge` covers the theoretical shortfall
    /// so accounting stays exact rather than merely conservative.
    async fn reconcile_owner_bytes(
        &self,
        mut reserved: OwnedSemaphorePermit,
        cost: usize,
    ) -> Result<OwnedSemaphorePermit, AppendError<EngineError>> {
        let want = cost.clamp(1, OWNER_RING_BYTES) as u32;
        let held = reserved.num_permits() as u32;
        match want.cmp(&held) {
            std::cmp::Ordering::Equal => Ok(reserved),
            std::cmp::Ordering::Less => {
                // `split` hands back a permit holding `want`; dropping what
                // remains in `reserved` releases exactly the over-estimate.
                let exact = reserved
                    .split(want as usize)
                    .expect("split of a strictly smaller permit count");
                drop(reserved);
                Ok(exact)
            }
            std::cmp::Ordering::Greater => {
                // Unreachable while `prepare_build_peak` over-estimates, which
                // is the invariant the accounting tests pin. Correct it without
                // holding: acquiring the shortfall while still holding
                // `reserved` is hold-and-wait, and with the ring near capacity
                // two producers doing it concurrently would deadlock the whole
                // append path. Releasing first can only cost a re-queue.
                debug_assert!(
                    false,
                    "build-peak reservation {held} under-estimated cost {want}"
                );
                drop(reserved);
                self.reserve_owner_bytes(cost).await
            }
        }
    }

    async fn enqueue_owner(
        &self,
        kind: OwnerIntentKind,
        cost: usize,
        inflight: InFlightGuard,
    ) -> OwnerResult {
        let permit = self.reserve_owner_bytes(cost).await?;
        self.enqueue_owner_with_permit(kind, cost, permit, inflight).await
    }

    /// `enqueue_owner` for callers that already hold a build-peak reservation.
    async fn enqueue_owner_with_permit(
        &self,
        kind: OwnerIntentKind,
        cost: usize,
        permit: OwnedSemaphorePermit,
        inflight: InFlightGuard,
    ) -> OwnerResult {
        let permit = self.reconcile_owner_bytes(permit, cost).await?;
        let (done, rx) = oneshot::channel();
        let intent = OwnerIntent {
            kind,
            cost,
            completion: OwnerCompletion {
                _permit: permit,
                done,
                // bn-11ba: one `Arc` clone + one `Instant::now` per append
                // batch (not per event) — see `OwnerCompletion`.
                status: Arc::clone(&self.inner.owner.status),
                queued: Instant::now(),
            },
        };
        self.inner
            .owner
            .tx
            .as_ref()
            .expect("live engine has owner sender")
            .send(intent)
            .await
            .map_err(|_| {
                AppendError::Backend(EngineError::Append(
                    "append owner is closed".into(),
                ))
            })?;
        #[cfg(test)]
        self.inner.owner.cohort_gate.record_admitted();
        drop(inflight);
        rx.await.map_err(|_| {
            AppendError::Backend(EngineError::Append(
                "append owner exited before completing intent".into(),
            ))
        })?
    }
}

/// The `$registry` batch carrying `records`, at stream-0 version
/// `first_version` (`bn-2di`). REG5's envelope: the reserved stream, the
/// reserved event type, the frozen bootstrap codec, no schema version.
fn registry_append_request(
    first_version: u64,
    records: &[RegistryRecord],
) -> AppendRequest {
    AppendRequest {
        stream_id:            registry::REGISTRY_STREAM_ID,
        category_id:          registry::ENGINE_CATEGORY_ID,
        first_stream_version: first_version,
        events:               records
            .iter()
            .map(|r| {
                EventInput::plain(
                    registry::REGISTRY_EVENT_TYPE_ID,
                    registry::REGISTRY_SCHEMA_VERSION,
                    registry::REGISTRY_CODEC_ID,
                    r.encode(),
                )
            })
            .collect(),
    }
}

/// The engine pushed a `$registry` record that did not land (`bn-2di`). See
/// [`Book::registry_lost`].
fn registry_lost_error() -> EngineError {
    EngineError::Append(
        "$registry: a registration this process pushed did not commit, so the \
         in-memory registry may name ids the log does not. Refusing every \
         further append; reopen the store (which rebuilds the registry from \
         the log alone) to continue."
            .to_string(),
    )
}

/// The outcome of the exact-version pre-check in [`LogEngine::append_batch`],
/// computed under the book lock and acted on after the lock (and any name
/// persistence) is released.
enum Pre {
    /// The stream was not at `expected`; return a conflict with this actual.
    Conflict(Version),
    /// An empty batch that validated `expected`: a no-op.
    Empty,
    /// A real batch to durably append. The events themselves are NOT held here
    /// — they are moved straight into the `AppendRequest` (`bn-2di`); what the
    /// post-ack publish still needs is only the identity below.
    Proceed {
        sid:              u64,
        first_stream_pos: u64,
        /// Per-record interned event-type ids, in record order (bn-2ib: the
        /// publish step warms the capsule cache with the decoded batch, which
        /// carries type ids).
        tids:             Vec<u32>,
    },
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

impl SubscribeBackend for LogEngine {
    async fn watermark(&self) -> Result<u64, Self::Error> {
        Ok(self.inner.read_watermark.get())
    }

    async fn await_watermark_past(&self, pos: u64) -> Result<(), Self::Error> {
        self.inner.read_watermark.await_past(pos).await;
        Ok(())
    }
}

#[cfg(all(test, not(miri)))]
mod append_gate_tests;
