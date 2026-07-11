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
//! - **Meta** — durable stream heads and the dedupe window live in the
//!   `mess-index` fjall [`MetaStore`].
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
//! - **Names** cannot be re-derived from the log — it stores only interned
//!   numeric ids (`stream_id u64`, `event_type_id u32`), never their strings.
//!   The interner's `id → name` bijection is persisted durably in the
//!   `mess-index` [`MetaStore`]'s `stream_names` / `type_names` tables (written
//!   the first time a name is interned, in
//!   [`append_batch`](LogEngine::append_batch), via
//!   [`persist_new_names`](LogEngine::persist_new_names)) and reloaded on open.
//!   A new name is `fsync`ed **before** the covering append can become durable
//!   (bn-150) — see [`persist_new_names`](LogEngine::persist_new_names).
//!
//! What remains of the book is deliberately tiny and history-**independent**
//! per event: the two name interners plus the per-stream head versions
//! ([`Book`]). On open, [`recover`](LogEngine::recover) reconstructs exactly
//! that — interners from the meta tables, heads + hot index from a
//! header/batch-metadata scan of the **unsealed** segments only (no payload
//! frame is decoded), and heads/watermark for fully-sealed segments straight
//! from their durable sidecar directories without reading the segment bytes
//! at all. The single active segment is *resumed in place*
//! ([`SegmentWriter::resume`]) at the recovered `safe_offset`, so the durable
//! log continues to grow one contiguous prefix across any number of reopens.
//!
//! [`SealedPayloadIndex::reassemble_range`]: mess_index::sealed::SealedPayloadIndex::reassemble_range
//! [`AppendOutcome::Acked`]: mess_log::committer::AppendOutcome::Acked

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc;
use std::sync::{Condvar, Mutex, OnceLock};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use mess_index::meta::{CommitGroup, Head, MetaStore, StreamId};
use mess_index::sealed::{
    BlockCache, NoDicts, ReplaySet, SealBatch, SealDriver, SealInput,
    SealMetrics, SealStream, SealedSegmentIndex, SealedSegmentRef, SealedStore,
};
use mess_index::{ActiveIndex, BatchEntry, EventPtr, GlobalEntry, StreamEntry};
use mess_log::committer::{
    AppendOutcome, AppendRequest, Appender, ChainInit, Committer, Durability,
    EventInput, LatencySnapshot, PreBarrier, Roller,
};
use mess_log::fold_chain::ChainHead;
use mess_log::lock::StoreLock;
use mess_log::runtime::{
    FileHandle, Fs as LogFs, OpenOpts, RealRuntime, Runtime,
};
use mess_log::scanner::{self, AcceptedBatch};
use mess_log::sealer::{TrailerFields, encode_trailer, read_trailer};
use mess_log::watermark::Watermark;
use mess_log::writer::{
    ResumeParams, SegmentParams, SegmentSummary, SegmentWriter,
};
use quick_cache::sync::{Cache, DefaultLifecycle};
use quick_cache::{DefaultHashBuilder, OptionsBuilder, Weighter};

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
    /// A block-native read of a committed batch failed (bn-2ib): the pointer
    /// could not be resolved to a CRC-valid batch of the expected identity,
    /// even via the locate-by-scan fallback.
    #[error("read: {0}")]
    Read(String),
}

/// The in-process name interners + per-stream heads (see the module docs).
/// All engine clones share one `Arc<Mutex<Book>>`.
///
/// Since bn-2ib this holds **no payload bytes and no per-event state**: its
/// size is O(streams + event types), independent of history length. Payloads
/// live in the durable blocks and are read through the bounded
/// [`BlockReader`]; per-stream position enumeration lives in the
/// [`ActiveIndex`] / sealed sidecars.
#[derive(Default)]
struct Book {
    /// `stream name → interned id` (ids start at 1; 0 is unused).
    stream_ids:   HashMap<String, u64>,
    /// `interned id → stream name`, index `id - 1`.
    stream_names: Vec<Arc<str>>,
    /// `message type → interned event-type id` (ids start at 1).
    type_ids:     HashMap<String, u32>,
    /// `interned event-type id → message type`, index `id - 1` (the reverse of
    /// `type_ids`, kept in lockstep so recovery can resolve a frame's
    /// `event_type_id` back to its name).
    type_names:   Vec<Arc<str>>,
    /// `stream id → last stream position` (the head).
    heads:        HashMap<u64, u64>,
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
/// event-type id `type_ids[k]` and payload
/// `data[offs[k] as usize..offs[k + 1] as usize]` (a single arena, not one
/// allocation per event).
struct DecodedBatch {
    stream_id:            u64,
    first_stream_version: u64,
    first_global_pos:     u64,
    frame_count:          u32,
    /// Per-frame interned event-type id.
    type_ids:             Vec<u32>,
    /// Payload arena; see the struct doc for the slicing contract.
    data:                 Vec<u8>,
    /// `frame_count + 1` arena boundaries.
    offs:                 Vec<u32>,
}

impl DecodedBatch {
    /// Frame `k`'s payload bytes.
    #[inline]
    fn payload(&self, k: usize) -> &[u8] {
        &self.data[self.offs[k] as usize..self.offs[k + 1] as usize]
    }

    /// Resident bytes for the cache weighter.
    fn weight_bytes(&self) -> u64 {
        (self.data.len()
            + self.offs.len() * 4
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
        let frames = b.frames(image).map_err(|e| {
            EngineError::Read(format!("seg {segment_id}: frames: {e}"))
        })?;
        let n = b.frame_count as usize;
        let mut type_ids = Vec::with_capacity(n);
        let mut data = Vec::new();
        let mut offs = Vec::with_capacity(n + 1);
        for f in frames {
            type_ids.push(f.event_type_id);
            offs.push(data.len() as u32);
            data.extend_from_slice(f.payload);
        }
        offs.push(data.len() as u32);

        // D6: prefer the columnar payload sidecar's bytes for a sealed batch.
        // A `.pcol` decode error (or a range past its coverage — e.g. an
        // on-demand seal of a still-growing head segment) keeps the raw
        // frames above: typed at this seam, byte-identical by verify-on-seal
        // where the sidecar does cover.
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
            offs,
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

/// Orders the post-ack publish step (stream head + active index + meta head)
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
/// `ActiveIndex::apply_committed` / `MetaStore::apply_group` require
/// in-position-order application (they carry out-of-order asserts), and the
/// published read watermark must advance densely — so every publish must
/// wait its turn here before touching any of them.
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
    /// recovered durable event count on open (0 for a fresh store).
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
    /// regardless of whether the local head/index/meta publish fully
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

/// Everything [`LogEngine::recover`] hands back to
/// [`open_with`](LogEngine::open_with) (bn-2ib).
struct Recovered {
    /// Interners (reloaded from the meta name tables) + per-stream heads.
    book:        Book,
    /// How to resume the live head segment.
    plan:        ResumePlan,
    /// Per-stream fold-chain exit heads (chain-on stores only; spec 05 §6).
    chain_heads: HashMap<u64, ChainHead>,
    /// The recovered durable/published event count — the exclusive end of
    /// the readable global-position sequence, seeding both the publish
    /// sequencer and the published read watermark.
    watermark:   u64,
    /// Payload frames materialised during recovery — 0 on every chain-off
    /// open (the bn-2ib gate observable,
    /// [`LogEngine::recover_payload_decodes`]).
    decodes:     u64,
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

/// Coalesces the durable new-name `SyncAll` into the log committer's
/// group-commit window under a **barriered** [`Durability`] mode (`Os`/`Group`,
/// bn-34o).
///
/// # The problem this closes
///
/// bn-150 made a newly-interned stream/type name durable (`MetaStore::persist`,
/// a real `SyncAll` `fsync`) strictly before the covering append is submitted,
/// so recovery can never resolve a committed event whose name was lost (the
/// store's log frames carry only the numeric `stream_id`/`event_type_id`, never
/// the string, so a lost name is unrecoverable — see
/// [`persist_new_names`](LogEngine::persist_new_names)). bn-2cj kept that
/// barrier only for the durable modes (`Process` uses a barrier-free page-cache
/// `write(2)` instead). But under `Os`/`Group` **every** new stream paid its
/// **own** `SyncAll`, upstream of the committer, and `N` new streams serialized
/// on `N` separate fsyncs through the shared `MetaStore` — the single-writer
/// serialization point spike bn-1jg measured (a k=32 pipeline of new streams
/// won only ~1.3× because the name flush could not parallelize; ~3.4 ms/new
/// stream, 98.8% of the cost). Coalescing it *upstream* (a leader/follower
/// barrier in `append_batch`) does not help a **committer-bound pipeline**: new
/// streams reach that upstream stage staggered at the committer's drain rate,
/// so nothing piles up to share a fsync (measured: ~0.99 barriers/new-name).
///
/// # The mechanism: ride the committer's own group barrier, high-watered by ticket
///
/// The log committer already coalesces the *event* `fdatasync`s of a commit
/// window behind one barrier (`mess-log`'s group commit). So instead of a
/// separate upstream name barrier, the name flush **rides that same window**,
/// via a [`PreBarrier`](mess_log::committer::PreBarrier) the committer invokes
/// on the committer thread strictly before the group's log `fdatasync`
/// ([`LogEngine::name_pre_barrier`]):
///
/// * When [`append_batch`](LogEngine::append_batch) interns a new name under
///   `Os`/`Group`, it writes the name bytes to fjall's page cache
///   (`persist_new_names` → `put_*_name`, a `write(2)`) and bumps a monotone
///   [`buffered`](Self::buffered) ticket — then submits the covering event
///   **with no fsync of its own**. The bump happens before submit, so by the
///   time this append's batch is gathered into a commit group, `buffered`
///   already covers its name.
/// * The committer's pre-barrier hook, once per barriered group, captures
///   `target = buffered` and — only if `target > durable` — issues ONE
///   `SyncAll`, then advances `durable = max(durable, target)`. fjall's
///   `SyncAll` flushes exactly what was buffered when it begins, and every
///   ticket `<= target` had its `put_*_name` sequenced-before the `buffered`
///   bump the hook's load observes, so that single fsync makes **all** those
///   names durable at once — strictly before the group's log `fdatasync`.
///
/// This coalesces perfectly with the committer's grouping, and — crucially —
/// **even under `Os`** (group-of-one): the hook's `durable` high-water advances
/// past *every* already-submitted name in one fsync, so the serial groups that
/// follow find `buffered == durable` and skip the fsync entirely. `N` new
/// streams in flight collapse to a handful of meta `SyncAll`s, not `N`. On the
/// hot path (no new name) `buffered` never moves, so the hook is a pure atomic
/// load and issues no fsync in any group.
///
/// # The ordering invariant is preserved (bn-150/bn-2cj)
///
/// The hook's `SyncAll` runs on the committer thread strictly before that
/// group's log `fdatasync`, which itself precedes the group's acks. So a name
/// is on stable storage before any event referencing it is, and **no event ack
/// — and no durable log barrier covering that event — completes before the
/// names its events reference are durable.** The guarantee is unchanged from
/// bn-150; it is merely issued from inside the window rather than ahead of it,
/// and shared. A hook failure is treated as a barrier failure (Indeterminate +
/// poison, see [`PreBarrier`](mess_log::committer::PreBarrier)): the group's
/// events never ack durable while their names failed to flush.
///
/// # Crash story (walk both orders)
///
/// The window's durable order under `Os`/`Group` is: (1) name bytes → page
/// cache and event bytes → log page cache (both before the barrier, in either
/// order — the events' `pwrite`s and the names' `write(2)`s all precede any
/// fsync); (2) hook `SyncAll` (names on device); (3) log `fdatasync` (events on
/// device); (4) watermark advance + ack. Step (2) is strictly before (3).
///
/// * **Crash between (2) and (3)** — names durable, events not. Recovery trusts
///   the durable watermark, which has not advanced past this group (it advances
///   only after the log `fdatasync`), so these events are not part of the
///   committed prefix; the orphan name rows are a harmless, idempotent superset
///   (re-writing a name later is a no-op). Consistent: names are *more* durable
///   than events, which is always allowed.
/// * **Crash between (3) and (4)** — both durable, ack never returned. Recovery
///   finds the events and resolves their names; consistent (the appends were
///   durable even though the callers never learned so — the [`AppendOutcome`]
///   retry contract covers this).
/// * **The forbidden order — an event durable, its name lost — cannot arise.**
///   An event is in the committed prefix only past the durable watermark, which
///   advances only after the log `fdatasync` (3), which runs only after the
///   hook `SyncAll` (2) made every name buffered before the group durable.
///   Names can be more durable than events, never less.
///
/// `Process` never uses this path (its names ride the barrier-free buffered
/// flush inline, bn-2cj; the committer runs no barrier and so never calls the
/// hook). The hot path (no new name) never bumps `buffered`.
struct NameFlush {
    /// Monotone count of new-name tickets handed out. Bumped by
    /// [`append_batch`](LogEngine::append_batch) (before submit) each time it
    /// buffers newly-interned name bytes under `Os`/`Group`. A hook `SyncAll`
    /// that captures `target >= t` is guaranteed to have flushed ticket `t`'s
    /// bytes (the `put_*_name` write is sequenced-before the bump).
    buffered: AtomicU64,
    /// The highest ticket a completed hook `SyncAll` has made durable. Read
    /// and advanced only by the pre-barrier hook, which runs on the single
    /// committer thread — so it is never concurrently mutated and advances
    /// monotonically. A group whose `buffered == durable` skips the fsync
    /// (nothing new pending).
    durable:  AtomicU64,
}

impl NameFlush {
    fn new() -> Self {
        NameFlush { buffered: AtomicU64::new(0), durable: AtomicU64::new(0) }
    }
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
    /// `Arc` so the committer's bn-34o pre-barrier hook
    /// ([`LogEngine::name_pre_barrier`]) can share the same meta store this
    /// engine writes names into and flush it durable inside the group window.
    meta:                 Arc<MetaStore>,
    /// The engine's durability mode (bn-2cj). Read on the append path to gate
    /// the new-name persist barrier: `Process` pushes new names to the OS
    /// page cache without an `fsync` (the log itself issues no barrier under
    /// `Process`, so a per-name `SyncAll` would be strictly stronger than the
    /// operator asked for); `Os`/`Group` keep the `SyncAll` barrier so a new
    /// name is durable no later than the covering append — coalesced across
    /// the commit window since bn-34o (see [`NameFlush`]). See the
    /// [`persist_new_names`](LogEngine::persist_new_names) flush-site
    /// contract.
    durability:           Durability,
    /// Coalesces the barriered-mode (`Os`/`Group`) new-name `SyncAll` into the
    /// committer's group window so `N` concurrent new streams pay ~1 fsync,
    /// not `N` (bn-34o). `Arc` so the committer's pre-barrier hook shares
    /// the same ticket high-water this engine bumps. Unused under
    /// `Process` (barrier-free buffered flush) and never touched on the
    /// hot path (no new name). See [`NameFlush`].
    name_flush:           Arc<NameFlush>,
    book:                 Arc<Mutex<Book>>,
    /// Block-native byte fetcher: the bounded decoded-capsule cache over the
    /// durable segment blocks (bn-2ib). Every read path resolves positions
    /// through the index tiers and bytes through this.
    reader:               BlockReader,
    /// Payload frames decoded during [`recover`](LogEngine::recover) — the
    /// bn-2ib "zero old payload decodes on open" gate's observable. `0` for
    /// every chain-off open; chain-on stores still fold every durable
    /// payload (spec 05 §6 requires it).
    recover_decodes:      u64,
    /// The **published** global watermark — the exclusive end of the readable
    /// global-position sequence. Advanced at
    /// the end of each append's publish step (after the head/index/meta
    /// are updated, in publish-turn order), so it tracks what
    /// [`read_global`](Backend::read_global) can serve, NOT merely what the
    /// durable committer has acked. The app-facing subscription / live-tail
    /// API ([`SubscribeBackend`](crate::backend::SubscribeBackend)) awaits
    /// this value; a woken subscriber is therefore guaranteed the position
    /// it waited for is already servable through the index tiers. Distinct
    /// from
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
/// Cheap to clone — every clone shares the same durable committer, index,
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
    /// Durability mode for the commit thread.
    pub durability:                 Durability,
    /// Active-segment size in bytes (preallocated at open).
    pub segment_size:               u64,
    /// Dedupe-window capacity for the meta store.
    pub dedupe_capacity:            usize,
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
    /// Total events committed and published (the published read watermark).
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
    /// roll-sealer gave up waiting for the hot index/watermark to catch up
    /// (its
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
            // `Process`: ack the moment the covering write returns. The
            // index is in-process, so per-batch fsync buys nothing for
            // correctness here; benches override this with `Group`.
            durability:                 Durability::Process,
            segment_size:               256 * 1024 * 1024,
            dedupe_capacity:
                mess_index::meta::DEFAULT_DEDUPE_CAPACITY,
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
        // `Arc` so the committer's bn-34o co-durable name pre-barrier hook can
        // share this exact store and flush it durable inside the group window.
        let meta = Arc::new(
            MetaStore::open_with_capacity(
                dir.join("meta"),
                opts.dedupe_capacity,
            )
            .map_err(|e| EngineError::Meta(e.to_string()))?,
        );

        // Reload the sealed tier from the durable sidecars written by prior
        // seals (see `load_sealed`). Without this the `SealedStore` starts
        // empty on every reopen, so a stream that was sealed before a restart
        // would silently fall back to hot replay instead of the sealed tier.
        // The returned `sealed_ids` are the segments already served cold, so
        // recovery does not re-seed the hot index with their batches (bn-1vu).
        let (sealed, sealed_ids, pending_sidecars) =
            Self::load_sealed(dir, &rt.fs());
        let sealed = Arc::new(sealed);

        // Recovery on open (F6 + bn-1vu + bn-2ib): reload the interners from
        // the meta name tables, re-seed the hot index + heads from a
        // batch-metadata scan of the unsealed segments (no payload frame
        // decode), take fully-sealed segments' heads/coverage straight from
        // their durable sidecars (no byte scan at all), and learn how to
        // resume the last (active) segment.
        let active = Arc::new(ActiveIndex::new());
        let recovered = Self::recover(
            &rt,
            dir,
            &active,
            &meta,
            &sealed,
            &sealed_ids,
            &pending_sidecars,
            opts.chain,
        )?;
        let Recovered { book, plan, chain_heads, watermark, decodes } =
            recovered;

        // bn-34o: the coalesced co-durable name flush lives in the committer's
        // group barrier. Build the shared ticket high-water and the pre-barrier
        // hook that rides each group's `fdatasync` (see [`NameFlush`]).
        let name_flush = Arc::new(NameFlush::new());
        let pre_barrier =
            Self::name_pre_barrier(meta.clone(), name_flush.clone());

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
        let committer = Committer::spawn_with_roll_chained_hooked(
            &rt,
            writer,
            opts.durability,
            roller,
            chain_init,
            pre_barrier,
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
        // The published read watermark seeds at the recovered event count —
        // 0 on a fresh store — the same baseline the publish sequencer
        // starts from. Created before the seal thread so the sealer can gate
        // each rolled segment's seal on the canonical published watermark
        // (bn-2ib; previously it gated on the record book's length).
        let read_watermark = Watermark::new(watermark);
        let seal_thread = {
            let driver =
                SealDriver::new(Arc::clone(&sealed), dir.join("sealed"))
                    .with_metrics(Arc::clone(&seal_metrics))
                    .with_parity(opts.parity);
            let active = Arc::clone(&active);
            let published = read_watermark.clone();
            let fs = rt.fs();
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
                        published,
                        fs,
                        dir,
                        seal_metrics_for_thread,
                        shutdown_deadline,
                        SpinConfig::default(),
                    )
                })
                .map_err(|e| EngineError::Open(format!("spawn sealer: {e}")))?
        };

        // The publish sequencer's turn-order starts wherever recovery left
        // the durable prefix — 0 on a fresh store, or the recovered event
        // count on a reopen — never a hardcoded 0, or the first post-reopen
        // publish would wait forever for a position that was already durably
        // assigned in a previous process lifetime.
        let recovered_len = watermark;

        let rt_fs = rt.fs();
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
                durability: opts.durability,
                name_flush,
                book,
                reader: BlockReader::new(
                    rt_fs,
                    dir.to_path_buf(),
                    opts.capsule_cache_budget_bytes,
                ),
                recover_decodes: decodes,
                read_watermark,
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
    /// (`seg-1` … `seg-N`, `N` the live head). Recovery walks them in
    /// ascending id order, and — since bn-2ib — reads as little as each
    /// segment's service tier requires. **No payload frame is ever decoded**
    /// on a chain-off open; reads materialise bytes lazily from the durable
    /// blocks instead (see the module docs).
    ///
    /// - **Fully-sealed, non-head segments** (id in `sealed_ids` — i.e. the
    ///   sidecar cross-checked against a valid segment **footer** at load
    ///   (review F2: the footer fsync is what proves the covered bytes are
    ///   durable) — with coverage contiguous with the running watermark and the
    ///   next segment's header `base_pos`) are not read at all beyond their
    ///   52-byte header + 100-byte trailer: per-stream heads come from the
    ///   sidecar directory ([`SealedSegmentIndex::stream_head`]) and the
    ///   watermark advances by the sidecar's `event_count`. This is what makes
    ///   reopen cost O(unsealed bytes), not O(history).
    /// - **Everything else** — unsealed segments (including a
    ///   rolled-but-not-yet-sealed one after a mid-seal crash), a sealed
    ///   segment whose sidecar coverage does not line up (e.g. an on-demand
    ///   [`seal_active`](LogEngine::seal_active) of a segment that kept
    ///   growing), and always the live head — is scanned with
    ///   [`recover_segment`](scanner::recover_segment): batch **metadata** only
    ///   (the mandatory byte-layer CRC still runs; no frame decode). Scanned
    ///   batches seed the hot index with their real `(segment_id, offset)`
    ///   pointers — except the slice a sealed sidecar already covers, which
    ///   stays cold-served.
    /// - The last (highest-id) headed segment is the resumable live head; if
    ///   the head file carries no valid header, the highest headed scanned or
    ///   sealed segment is scanned for resume instead (positions must never
    ///   restart at 0 while durable history exists).
    ///
    /// With the fold chain **on** (spec 05 §6) every segment is still fully
    /// scanned and every payload folded — the chain head is a function of all
    /// payload bytes; `decodes` reports how many frames that materialised.
    #[allow(clippy::too_many_lines)] // one linear pass; splitting obscures the watermark threading
    #[allow(clippy::too_many_arguments)] // one open-time seam; each arg is a distinct recovered surface
    fn recover(
        rt: &RealRuntime,
        dir: &Path,
        active: &ActiveIndex,
        meta: &MetaStore,
        sealed: &SealedStore,
        sealed_ids: &HashSet<u64>,
        pending_sidecars: &HashMap<u64, SealedSegmentRef>,
        chain: bool,
    ) -> Result<Recovered, EngineError> {
        // Reconstruct the interner (both directions) from the durable id→name
        // tables first, so reads can resolve names.
        let mut book = Book::default();
        book.load_stream_names(
            meta.stream_names()
                .map_err(|e| EngineError::Meta(e.to_string()))?,
        );
        book.load_type_names(
            meta.type_names().map_err(|e| EngineError::Meta(e.to_string()))?,
        );

        // Per-stream fold-chain heads rehydrated from the recovered frames
        // (spec 05 §5/§6, `bn-3l0`); empty (and no frame decoded) otherwise.
        let mut chain_heads: HashMap<u64, ChainHead> = HashMap::new();
        let mut decodes = 0u64;

        // Enumerate the segment chain in ascending id order.
        let segment_ids = enumerate_segment_ids(dir);
        if segment_ids.is_empty() {
            return Ok(Recovered {
                book,
                plan: ResumePlan::Fresh,
                chain_heads,
                watermark: 0,
                decodes,
            });
        }

        // Every segment's (cheap, 52-byte) header up front: the sidecar-trust
        // check needs the NEXT segment's base_pos to prove a sealed sidecar
        // covers its whole segment.
        let fs = rt.fs();
        let headers: Vec<Option<scanner::SegmentHeaderInfo>> = segment_ids
            .iter()
            .map(|&id| {
                scanner::read_segment_header(&fs, &segment_path(dir, id))
                    .map_err(|e| {
                        EngineError::Open(format!("header seg {id}: {e}"))
                    })
            })
            .collect::<Result<_, _>>()?;
        let head_id = *segment_ids.last().expect("non-empty");

        let mut hot_entries: Vec<BatchEntry> = Vec::new();
        let mut last_headed: Option<ResumeInfo> = None;
        let mut watermark = 0u64;

        for (i, &seg_id) in segment_ids.iter().enumerate() {
            let is_head = seg_id == head_id;

            // Sidecar-trusted skip: a fully-sealed, non-head segment whose
            // sidecar coverage is contiguous on both sides needs no byte
            // scan at all (chain-on stores scan everything — the fold needs
            // every payload).
            if !chain
                && !is_head
                && sealed_ids.contains(&seg_id)
                && let Some(sref) = sealed.get(seg_id)
                && let Some(hdr) = headers[i]
                && hdr.segment_id == seg_id
                && hdr.base_pos == watermark
                && sref.base_pos() == watermark
                && headers[i + 1].is_some_and(|h| {
                    h.base_pos == watermark + sref.event_count()
                })
            {
                for &sid in sref.stream_ids() {
                    if let Some(v) = sref.stream_head(sid) {
                        book.heads
                            .entry(sid)
                            .and_modify(|h| *h = (*h).max(v))
                            .or_insert(v);
                    }
                }
                watermark += sref.event_count();
                continue;
            }

            // Scan path: batch metadata (+ image only when the chain fold
            // needs payload bytes).
            let seg_path = segment_path(dir, seg_id);
            let (rec, image) = if chain {
                let (rec, image) =
                    scanner::recover_segment_with_image(&fs, &seg_path)
                        .map_err(|e| {
                            EngineError::Open(format!(
                                "recover seg {seg_id}: {e}"
                            ))
                        })?;
                (rec, Some(image))
            } else {
                let rec =
                    scanner::recover_segment(&fs, &seg_path).map_err(|e| {
                        EngineError::Open(format!("recover seg {seg_id}: {e}"))
                    })?;
                (rec, None)
            };
            let Some(header) = rec.header else {
                // A file with no valid header carries no committed batches of
                // this generation — skip it (never resumed, never seeds).
                continue;
            };
            // The slice of this segment a sealed sidecar already serves cold
            // (an on-demand seal of a still-growing segment covers a prefix;
            // batches past it must stay hot-served).
            let sealed_end = if sealed_ids.contains(&seg_id) {
                // Footer-verified at load (F2): already installed.
                sealed.get(seg_id).map(|s| s.base_pos() + s.event_count())
            } else if let Some(cand) = pending_sidecars.get(&seg_id) {
                // A footerless sidecar (an on-demand `seal_active` of the
                // live head, or a roll-seal whose footer fsync a crash
                // preceded — review F2): install it only now that THIS scan
                // has proven the durable committed prefix reaches its
                // coverage end. A refuted candidate is not installed — the
                // segment is served from the log, losing nothing.
                let end = cand.base_pos() + cand.event_count();
                if header.base_pos == cand.base_pos() && rec.next_pos >= end {
                    sealed.install(cand.clone());
                    Some(end)
                } else {
                    None
                }
            } else {
                None
            };

            let mut order: Vec<&AcceptedBatch> = rec.accepted.iter().collect();
            order.sort_by_key(|b| b.first_global_pos);
            for b in &order {
                let sid = b.stream_id;
                // Fail loudly on a durable-name gap — a committed event whose
                // stream name is missing is unrecoverable (bn-150).
                book.stream_name_opt(sid).ok_or_else(|| {
                    EngineError::Meta(format!(
                        "recover: no interned name for stream_id {sid}"
                    ))
                })?;
                if chain && let Some(image) = &image {
                    // Fold the on-disk payloads into the stream's head, in
                    // ascending version order (§6.2). bn-221: `frames` is
                    // fallible but this caller always passes the exact image
                    // `b` was recovered from — still propagated so a future
                    // refactor fails loudly instead of panicking.
                    let frames = b.frames(image).map_err(|e| {
                        EngineError::Open(format!("recover: {e}"))
                    })?;
                    let head = chain_heads
                        .entry(sid)
                        .or_insert_with(|| ChainHead::genesis(sid));
                    for frame in frames {
                        head.absorb(frame.payload);
                        decodes += 1;
                    }
                }
                book.heads
                    .entry(sid)
                    .and_modify(|h| *h = (*h).max(b.last_stream_version()))
                    .or_insert(b.last_stream_version());
                // Seed the hot index with every batch a sealed sidecar does
                // not already cover.
                if sealed_end.is_none_or(|end| b.first_global_pos >= end) {
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

        // The head segment normally produced the resume info above (it is
        // always scanned). If it could not (no valid header — e.g. a crash
        // between the roll's file creation and its header write), resume
        // from the highest segment that DOES head — scanning it now if it
        // was sidecar-skipped — rather than ever falling back to a fresh
        // segment at position 0 over live durable history.
        if last_headed.is_none() && watermark > 0 {
            for &seg_id in segment_ids.iter().rev() {
                let rec =
                    scanner::recover_segment(&fs, &segment_path(dir, seg_id))
                        .map_err(|e| {
                        EngineError::Open(format!("recover seg {seg_id}: {e}"))
                    })?;
                if let Some(header) = rec.header {
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
                    break;
                }
            }
        }

        active.apply_committed(watermark, &hot_entries);

        let plan = match last_headed {
            Some(info) => ResumePlan::Resume(info),
            None => ResumePlan::Fresh,
        };
        Ok(Recovered { book, plan, chain_heads, watermark, decodes })
    }

    /// The background auto-roll sealer loop (`bn-1vu`), run on its own thread.
    /// For each rolled (durable, unsealed) segment reported over `rx`, it
    /// **reads the rolled segment back** (bn-2ib — the durable bytes are the
    /// seal's sole source; the deleted record book used to be) to build the
    /// pointer + payload sidecars, finalizes the segment footer (writes +
    /// fsyncs the trailer), and installs the segment into the cold
    /// [`SealedStore`] — all off the append path (D5). A seal failure is
    /// best-effort: the rolled segment stays durable + unsealed and is served
    /// from the log (and re-sealable) on reopen, so a failed seal never loses
    /// data. The loop exits when the roll channel closes (the committer task
    /// dropped its [`Roller`]), draining every queued seal first.
    ///
    /// Readiness gates on the hot index's applied end **and the canonical
    /// published watermark** (bn-2ib; previously the record book's length):
    /// sealing only what is published preserves the invariant that the sealed
    /// tier never serves a position `read_global` cannot — the cold read path
    /// is not watermark-clamped, so this gate is what keeps it safe.
    ///
    /// `bn-u6o`: `seal_metrics` counts + loudly (rate-limited) logs every
    /// segment this loop gives up waiting on (see the `record_seal_skipped`
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
        published: Watermark,
        fs: EngineFs,
        dir: PathBuf,
        seal_metrics: Arc<SealMetrics>,
        shutdown_deadline: Arc<OnceLock<Instant>>,
        spin: SpinConfig,
    ) {
        for summary in rx {
            let base = summary.base_pos;
            let end = summary.end_pos;

            // Wait until the hot index + published watermark cover every
            // event of this segment (post-ack discipline). Under the current
            // serialised append gate this already holds by the time the roll
            // notification lands; the bounded wait keeps it robust if a
            // future append gate (bn-1s0) relaxes that ordering. A gone
            // writer can never lower either value, so this cannot deadlock.
            //
            // The per-segment deadline (`spin.per_seal_budget` out) is clamped
            // to `shutdown_deadline` when the latter is set (bn-u6o) — see the
            // fn doc.
            let per_seal_deadline = Instant::now() + spin.per_seal_budget;
            loop {
                if active.applied_end() >= end && published.get() >= end {
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

            if active.applied_end() < end || published.get() < end {
                // bn-u6o: this is the silent-skip site the bone exists to fix —
                // count it and log loudly (rate-limited) so an operator can see
                // a segment stayed unsealed rather than discovering it only at
                // reopen.
                seal_metrics.record_seal_skipped(&format!(
                    "segment {} [{base}, {end}) never caught up \
                     (applied_end={}, published={}, shutdown_deadline={})",
                    summary.segment_id,
                    active.applied_end(),
                    published.get(),
                    shutdown_deadline.get().is_some(),
                ));
                continue; // incomplete (see the bounded wait above) — leave it unsealed
            }

            // Re-read the rolled segment: the durable bytes are the seal's
            // input — both the batch pointers (REAL offsets, so the sealed
            // sidecar's `EventPtr`s dereference) and the payloads for the
            // columnar `.pcol` sidecar (bn-zge / D6), in stored
            // (global-position) order. Verify-on-seal inside the driver
            // byte-compares the `.pcol` reassembly against these frames
            // before anything is written.
            let seg_id = summary.segment_id;
            let seg_path = segment_path(&dir, seg_id);
            let input = match seal_input_from_segment(
                &fs, &seg_path, seg_id, base, end,
            ) {
                Ok(input) => input,
                Err(why) => {
                    seal_metrics.record_seal_skipped(&format!(
                        "segment {seg_id} [{base}, {end}): {why}"
                    ));
                    continue; // unsealed + recoverable — served from the log
                }
            };
            if input.streams.is_empty() {
                continue;
            }

            // Finalize: write + fsync the fixed footer trailer (§3.3.1), making
            // the segment R2-trusted, only after the sidecars are durable.
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
    /// and `.pcol`, re-attached by [`SealedSegmentIndex::open`]) is admitted
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
    /// **Sidecar-before-data crash safety (bn-2ib review F2).** The seal
    /// pipeline makes the sidecar durable strictly BEFORE the segment
    /// footer's whole-file fsync, so a power loss between the two can leave a
    /// CRC-valid sidecar whose covered tail bytes never reached the device
    /// (under `Process` durability nothing else fsynced them). A sidecar is
    /// therefore installed here only when its segment carries a **valid
    /// footer trailer** that cross-checks (`segment_id`, `base_pos`, and
    /// `end_pos == coverage end`) — the footer fsync is what proves the data
    /// bytes are durable. Anything else (notably an on-demand
    /// [`seal_active`](LogEngine::seal_active) sidecar over the still-live
    /// head, which never has a footer) is returned as a **pending candidate**
    /// instead: [`recover`](LogEngine::recover) scans those segments anyway
    /// and installs a candidate only after the scan proves the durable
    /// committed prefix reaches the sidecar's coverage end. A candidate the
    /// scan refutes is simply not installed — the segment is served from the
    /// log, losing nothing.
    ///
    /// Returns `(store, footer_verified_ids, pending)`: recovery trust-skips
    /// only the footer-verified ids and scan-verifies the pending ones.
    fn load_sealed(
        dir: &Path,
        fs: &EngineFs,
    ) -> (SealedStore, HashSet<u64>, HashMap<u64, SealedSegmentRef>) {
        let store = SealedStore::new();
        let mut ids = HashSet::new();
        let mut pending: HashMap<u64, SealedSegmentRef> = HashMap::new();
        let sealed_dir = dir.join("sealed");
        let Ok(entries) = std::fs::read_dir(&sealed_dir) else {
            // No sealed directory yet: nothing has been sealed.
            return (store, ids, pending);
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) != Some("pidx") {
                // Skip `.pidx.tmp` husks, `.pcol`/`.filter` siblings
                // (re-attached by `open`), and anything else.
                continue;
            }
            let Ok(index) = SealedSegmentIndex::open(&path) else {
                continue;
            };
            let seg_id = index.segment_id();
            let coverage_end = index.base_pos() + index.event_count();
            let index: SealedSegmentRef = Arc::new(index);
            // F2: only a valid, cross-checking footer proves the covered
            // bytes are durable; everything else must be scan-verified.
            let footer_ok = read_trailer(fs, &segment_path(dir, seg_id))
                .ok()
                .flatten()
                .is_some_and(|t| {
                    t.segment_id == seg_id
                        && t.base_pos == index.base_pos()
                        && t.end_pos == coverage_end
                });
            if footer_ok {
                ids.insert(seg_id);
                store.install(index);
            } else {
                pending.insert(seg_id, index);
            }
        }
        (store, ids, pending)
    }

    /// Test/diagnostic: total events committed and published — the exclusive
    /// end of the readable global-position sequence (the published read
    /// watermark; before bn-2ib this was the record book's dense length,
    /// which tracked the same value).
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

    /// Test/diagnostic: how many times the durable meta store's
    /// [`MetaStore::persist_buffered`] has been called (bn-2cj) — i.e. how
    /// many barrier-free page-cache name flushes
    /// [`persist_new_names`](Self::persist_new_names) has performed under a
    /// `Durability::Process` engine. Paired with
    /// [`meta_persist_call_count`](Self::meta_persist_call_count) (the
    /// `SyncAll` **barrier** count), a test can prove a `Process` new-stream
    /// append flushed the name to the page cache WITHOUT issuing an `fsync`.
    #[must_use]
    pub fn meta_buffered_persist_call_count(&self) -> u64 {
        self.inner.meta.buffered_persist_call_count()
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
        // The live head is the highest-id segment on disk; its committed
        // prefix is re-read from the durable bytes and clamped to the
        // published watermark (bn-2ib — the deleted record book used to be
        // the payload source, and the segment was assumed to be seg 1 at
        // base 0, which broke after a live roll).
        let ids = enumerate_segment_ids(&self.inner.dir);
        let Some(&head_id) = ids.last() else {
            return Ok(());
        };
        let end = self.inner.read_watermark.get();
        let seg_path = segment_path(&self.inner.dir, head_id);
        let header =
            scanner::read_segment_header(&self.inner.rt.fs(), &seg_path)
                .map_err(|e| EngineError::SealedRead(format!("header: {e}")))?;
        let Some(header) = header else {
            return Ok(()); // an unheadered head holds nothing to seal
        };
        let input = seal_input_from_segment(
            &self.inner.rt.fs(),
            &seg_path,
            head_id,
            header.base_pos,
            end,
        )
        .map_err(EngineError::SealedRead)?;
        if input.streams.is_empty() {
            return Ok(());
        }
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

    /// The sealed-tier position resolve for one stream: its committed batch
    /// entries across the sealed corpus (via the real [`ReplaySet`], pointer
    /// blocks decoded through the bounded [`BlockCache`]) **unioned with the
    /// hot tail** (batches not yet sealed, and — since sealed eviction is
    /// logical — possibly the same batches again during handoff; the union
    /// dedupes by `first_version`, and the two tiers' entries for one batch
    /// carry the same identity). Version-ascending.
    fn sealed_and_hot_entries(
        &self,
        stream_id: u64,
    ) -> Result<Vec<StreamEntry>, EngineError> {
        let sealed_segs = self.inner.sealed.segments_for_stream(stream_id);
        let replay = ReplaySet::from_segments(sealed_segs);
        let mut by_ver: BTreeMap<u64, StreamEntry> = BTreeMap::new();
        for e in replay
            .stream_replay(stream_id, &self.inner.block_cache)
            .map_err(|e| EngineError::SealedRead(format!("{e:?}")))?
        {
            by_ver.insert(e.first_version, e);
        }
        // The hot tail (if the stream also has unsealed batches during
        // handoff). Sealed entries win the dedupe (same batch identity;
        // their pointers are the sidecar's).
        for e in self.inner.active.stream_entries(stream_id) {
            by_ver.entry(e.first_version).or_insert(e);
        }
        Ok(by_ver.into_values().collect())
    }

    /// Materialise owned [`StoredRecord`]s from `(batch, frame)` picks — the
    /// [`StoredRecord`] compatibility adapter over the block-backed views
    /// (bn-2ib): one book lock resolves every name, then each record copies
    /// its payload slice out of the shared batch arena.
    fn materialize(
        &self,
        picks: &[(Arc<DecodedBatch>, usize)],
    ) -> Result<Vec<StoredRecord>, EngineError> {
        let book = self.inner.book.lock().expect("book lock");
        let mut out = Vec::with_capacity(picks.len());
        for (batch, k) in picks {
            let stream_name =
                book.stream_name_opt(batch.stream_id).ok_or_else(|| {
                    EngineError::Meta(format!(
                        "read: no interned name for stream_id {}",
                        batch.stream_id
                    ))
                })?;
            let type_id = batch.type_ids[*k];
            let message_type =
                book.type_name_opt(type_id).ok_or_else(|| {
                    EngineError::Meta(format!(
                        "read: no interned name for event_type_id {type_id}"
                    ))
                })?;
            out.push(StoredRecord {
                stream_id:       stream_name.to_string(),
                message_type:    message_type.to_string(),
                data:            batch.payload(*k).to_vec(),
                stream_position: batch.first_stream_version + *k as u64,
                global_position: batch.first_global_pos + *k as u64,
            });
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
        /// Per-record interned event-type ids, in record order (bn-2ib: the
        /// publish step warms the capsule cache with the decoded batch, which
        /// carries type ids).
        tids:             Vec<u32>,
    },
}

impl LogEngine {
    /// Build the bn-34o co-durable name **pre-barrier hook**: the closure the
    /// committer runs on its own thread inside each barriered commit window,
    /// strictly before that group's log `fdatasync`. It makes every new name
    /// buffered so far durable in ONE coalesced `SyncAll`, high-watered by the
    /// [`NameFlush`] ticket so a group with nothing newly pending
    /// (`buffered == durable`, incl. the entire hot path) skips the fsync and
    /// is a pure atomic load. See [`NameFlush`] for the full mechanism,
    /// ordering invariant, and crash story.
    fn name_pre_barrier(
        meta: Arc<MetaStore>,
        name_flush: Arc<NameFlush>,
    ) -> PreBarrier {
        Arc::new(move || {
            // `buffered` covers every name whose `put_*_name` write is
            // sequenced-before its bump; this SeqCst load observes all such
            // bumps, so the `SyncAll` below flushes all their bytes. `durable`
            // is touched only here (single committer thread), so no CAS loop is
            // needed — a plain load/`fetch_max` is race-free.
            let target = name_flush.buffered.load(Ordering::SeqCst);
            if target > name_flush.durable.load(Ordering::SeqCst) {
                meta.persist()
                    .map_err(|e| std::io::Error::other(e.to_string()))?;
                name_flush.durable.fetch_max(target, Ordering::SeqCst);
            }
            Ok(())
        })
    }

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
    /// The fix (bn-150), as gated by bn-2cj:
    /// [`append_batch`](LogEngine::append_batch) folds this method's two call
    /// sites (stream name, then type names) into a single meta-store flush
    /// whenever *either* wrote something — one flush even when an append
    /// introduces both a new stream and a new type — performed strictly
    /// before the covering committer append is submitted. So by construction,
    /// a covering event can only reach the storage layer once its new name(s)
    /// already have: the two can no longer race in the crash-visible order.
    ///
    /// **Which flush** depends on the engine's [`Durability`] mode (bn-2cj),
    /// and the full rationale lives at that flush site in
    /// [`append_batch`](LogEngine::append_batch):
    /// * `Os`/`Group` (the log issues a real `fdatasync` barrier per ack):
    ///   `MetaStore::persist` — a matching `SyncAll` `fsync` barrier, so the
    ///   name is on stable storage before the event can be. Unchanged from
    ///   bn-150 in strength; bn-34o coalesces the barrier across the commit
    ///   window ([`coalesced_name_barrier`](Self::coalesced_name_barrier),
    ///   [`NameFlush`]) so `N` concurrent new streams share ~1 fsync — durable
    ///   modes are not weakened, only de-serialized.
    /// * `Process` (the log issues NO barrier; §1.1 promises process-crash
    ///   survival only): `MetaStore::persist_buffered` — a
    ///   `PersistMode::Buffer` page-cache `write(2)`, no `fsync`, matching the
    ///   operator's chosen durability for the event bytes and ordered strictly
    ///   before them, so a surviving event can never have a lost name under a
    ///   process crash. This drops the ~3.4 ms/new-stream `fsync` the spike
    ///   bn-1jg measured.
    ///
    /// On the hot path — no new stream, no new types, the overwhelmingly
    /// common case once a store's names have stabilised — neither call site
    /// writes anything, so the fold adds no flush at all in any mode: zero
    /// fjall calls, zero added latency.
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

/// Build a [`SealInput`] for the segment at `seg_path` by reading its durable
/// committed prefix back (bn-2ib — review V2: the seal's sole source is the
/// rolled raw segment, not the deleted record book). Batches are clamped to
/// `[base_pos, end_pos)` (for an on-demand seal of a still-growing head
/// segment, `end_pos` is the published watermark; for a rolled segment it is
/// the roll summary's `end_pos` and the clamp admits everything). The
/// returned input carries:
///
/// - per-stream [`SealBatch`]es with the batch's **real** byte offset, so the
///   sealed pointer sidecar's [`EventPtr`]s dereference straight into the raw
///   segment;
/// - every clamped payload in stored (global-position) order, so the driver
///   emits the columnar `.pcol` sidecar (bn-zge / D6) — and its permanent
///   verify-on-seal byte-compares the reassembly against these exact frames
///   before anything is written.
///
/// `Err(String)` when the scan fails or recovers less than `end_pos` (the
/// caller leaves the segment unsealed — served from the log, losing nothing).
fn seal_input_from_segment(
    fs: &EngineFs,
    seg_path: &Path,
    segment_id: u64,
    base_pos: u64,
    end_pos: u64,
) -> Result<SealInput, String> {
    let (rec, image) = scanner::recover_segment_with_image(fs, seg_path)
        .map_err(|e| format!("re-read for seal: {e}"))?;
    if rec.header.is_none() {
        return Err("re-read for seal: no valid segment header".to_string());
    }
    if rec.next_pos < end_pos {
        return Err(format!(
            "re-read for seal recovered only [{}, {}) of [{base_pos}, \
             {end_pos})",
            base_pos, rec.next_pos
        ));
    }
    let mut order: Vec<&AcceptedBatch> = rec.accepted.iter().collect();
    order.sort_by_key(|b| b.first_global_pos);

    let mut streams: BTreeMap<u64, Vec<SealBatch>> = BTreeMap::new();
    let mut payloads: Vec<Vec<u8>> = Vec::new();
    for b in &order {
        if b.first_global_pos < base_pos || b.first_global_pos >= end_pos {
            continue;
        }
        streams.entry(b.stream_id).or_default().push(SealBatch {
            first_version:    b.first_stream_version,
            frame_count:      b.frame_count,
            first_global_pos: b.first_global_pos,
            offset:           b.offset,
        });
        let frames =
            b.frames(&image).map_err(|e| format!("seal frames: {e}"))?;
        for f in frames {
            payloads.push(f.payload.to_vec());
        }
    }
    let streams: Vec<SealStream> = streams
        .into_iter()
        .map(|(stream_id, batches)| SealStream { stream_id, batches })
        .collect();
    Ok(SealInput { segment_id, base_pos, streams, payloads: None }
        .with_payloads(payloads))
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

        // Resolve positions (batch entries), then materialise bytes through
        // the block reader (bn-2ib).
        //
        // Cold path: a stream with sealed batches is served through the real
        // sealed-replay path (`ReplaySet`) unioned with its hot tail.
        // Hot path: a paged clamped slice of the per-stream `ActiveIndex`.
        let sealed = !self.inner.sealed.segments_for_stream(sid).is_empty();
        let entries: Vec<StreamEntry> = if sealed {
            self.sealed_and_hot_entries(sid)?
        } else {
            self.inner.active.stream_entries_from(sid, start, limit)
        };

        let mut picks: Vec<(Arc<DecodedBatch>, usize)> =
            Vec::with_capacity(limit.min(entries.len() * 2));
        'outer: for e in &entries {
            if e.last_version() < start {
                continue;
            }
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
            let from = start.max(e.first_version);
            for v in from..=e.last_version() {
                if picks.len() >= limit {
                    break 'outer;
                }
                picks.push((batch.clone(), (v - e.first_version) as usize));
            }
        }
        let page = self.materialize(&picks)?;
        if !sealed {
            // Match MockBackend's contention window on the hot path (the
            // pre-bn-2ib shape: the sealed path returned without yielding).
            tokio::task::yield_now().await;
        }
        Ok(page)
    }

    async fn read_global(
        &self,
        after: Option<u64>,
        limit: usize,
    ) -> Result<Vec<StoredRecord>, Self::Error> {
        // Global positions are dense from 0 up to the published watermark —
        // the same bound the record book's length used to impose.
        let wm = self.inner.read_watermark.get();
        let start = after.map_or(0, |p| p + 1);
        let mut picks: Vec<(Arc<DecodedBatch>, usize)> = Vec::new();
        let mut pos = start;
        // Built lazily: only reads that reach positions the hot index no
        // longer covers (sealed history after a reopen) pay for it. Each
        // entry carries its install generation (the derived-cache key
        // component, review F1/F6), sorted into global A1 order.
        let mut sealed_segs: Option<Vec<(SealedSegmentRef, u64)>> = None;

        while picks.len() < limit && pos < wm {
            // Prefer the hot index while it covers `pos` (in a live process
            // it covers everything ever appended; after a reopen its
            // coverage starts at the first unsealed segment).
            let entries =
                self.inner.active.global_range(pos, limit - picks.len());
            if entries.first().is_some_and(|e| e.first_global_pos <= pos) {
                for e in &entries {
                    if picks.len() >= limit || pos >= wm {
                        break;
                    }
                    if e.first_global_pos > pos {
                        // A coverage hole (a sealed segment between two
                        // hot-served ones): fall through to the sealed tier.
                        break;
                    }
                    let batch = self.inner.reader.batch(
                        &self.inner.sealed,
                        e.ptr,
                        BatchExpect {
                            stream_id:        e.stream_id,
                            first_global_pos: e.first_global_pos,
                            frame_count:      e.frame_count,
                            first_version:    None,
                        },
                    )?;
                    while pos < e.end_pos() && picks.len() < limit && pos < wm {
                        picks.push((
                            batch.clone(),
                            (pos - e.first_global_pos) as usize,
                        ));
                        pos += 1;
                    }
                }
                continue;
            }

            // Sealed tier: the segment whose contiguous A1 range covers
            // `pos`, walked through its (cached) global batch directory.
            let segs = sealed_segs.get_or_insert_with(|| {
                let mut v = self.inner.sealed.segments_with_gens();
                v.sort_by_key(|(s, _)| (s.base_pos(), s.segment_id()));
                v
            });
            let Some((seg, generation)) = segs
                .iter()
                .find(|(s, _)| {
                    pos >= s.base_pos() && pos < s.base_pos() + s.event_count()
                })
                .cloned()
            else {
                // Below the watermark every position is hot- or cold-served;
                // a gap here means a raced tier handoff — stop the page
                // rather than serve out of order.
                break;
            };
            let dir = self.inner.reader.global_dir(&seg, generation)?;
            let seg_end = seg.base_pos() + seg.event_count();
            let from = dir.partition_point(|e| e.end_pos() <= pos);
            for e in &dir[from..] {
                if picks.len() >= limit || pos >= wm || pos >= seg_end {
                    break;
                }
                let batch = self.inner.reader.batch(
                    &self.inner.sealed,
                    e.ptr,
                    BatchExpect {
                        stream_id:        e.stream_id,
                        first_global_pos: e.first_global_pos,
                        frame_count:      e.frame_count,
                        first_version:    None,
                    },
                )?;
                while pos < e.end_pos() && picks.len() < limit && pos < wm {
                    picks.push((
                        batch.clone(),
                        (pos - e.first_global_pos) as usize,
                    ));
                    pos += 1;
                }
            }
        }
        self.materialize(&picks)
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
                let mut tids: Vec<u32> = Vec::with_capacity(records.len());
                let events: Vec<EventInput> = records
                    .iter()
                    .map(|r| {
                        let (tid, tid_new) = book.intern_type(&r.message_type);
                        if tid_new {
                            new_types.push((tid, r.message_type.clone()));
                        }
                        tids.push(tid);
                        EventInput::plain(tid, 0, 0, r.data.clone())
                    })
                    .collect();
                Pre::Proceed {
                    sid,
                    events,
                    first_stream_pos: expected.next_position(),
                    tids,
                }
            };
            (pre, new_types)
        };

        // Write any newly-interned type names the same way (see above).
        let wrote_types =
            self.persist_new_names(None, &new_types).map_err(|e| {
                AppendError::Backend(EngineError::Meta(e.to_string()))
            })?;

        // Co-durable name flush, gated on the engine's durability mode
        // (bn-150 established the barrier; bn-2cj gated it; bn-34o coalesces
        // the durable-mode barrier into the committer's group window).
        // Covers BOTH `persist_new_names` call sites above (stream
        // name, then type names). Skipped entirely when neither call
        // site wrote anything (the hot path, once names have
        // stabilised): zero fjall calls, zero added latency, in
        // every mode.
        //
        // # The contract this gate upholds (bn-2cj / bn-34o)
        //
        // `stream_names`/`type_names` are the ONE durable source of truth this
        // store cannot rebuild from the log: the log frames carry only the
        // numeric `stream_id`/`event_type_id`, never the name string, so
        // `recover` resolves each committed event's name PURELY from these
        // meta tables and hard-fails (`EngineError::Meta`, "no interned name
        // for stream_id") if a name is missing (see `recover` and
        // `persist_new_names`' doc). There is no re-derivation path. So the
        // invariant recovery needs is: **no committed event may out-live its
        // stream/type name.** How we uphold it depends on what the LOG's own
        // durability barrier is:
        //
        // * `Os`/`Group` — the log append is acked only after its own real
        //   `fdatasync` barrier (`docs/spec/03-durability.md` §1.2/§1.3).
        //   Rather than a separate `SyncAll` upstream of the committer
        //   (bn-150/bn-2cj, which paid one fsync per new stream and serialized
        //   `N` of them on the shared meta store — spike bn-1jg), bn-34o rides
        //   the SAME group barrier: here we only push the name bytes to fjall's
        //   page cache (`persist_new_names`, done above) and bump the
        //   `NameFlush` ticket high-water, then submit. The committer's
        //   pre-barrier hook (`name_pre_barrier`) issues ONE meta `SyncAll` per
        //   group, strictly before that group's log `fdatasync`, making every
        //   name buffered so far durable at once — coalescing `N` new streams
        //   to a handful of fsyncs (even under `Os`, via the hook's high-water;
        //   see `NameFlush`). The name is still on stable storage before any
        //   event referencing it, and no event acks durable before its name
        //   does — the bn-150 guarantee, unchanged, merely issued from inside
        //   the window and shared. See [`NameFlush`] for the mechanism,
        //   memory-ordering proof, and crash walk.
        //
        // * `Process` — the log append is acked the instant its own covering
        //   `write(2)` reaches the OS page cache; the mode issues NO barrier
        //   and its contract (§1.1) is "survives a process crash only; power
        //   loss has an unbounded, OS-governed loss window." A per-name
        //   `SyncAll` here would be STRICTLY STRONGER than the operator's
        //   chosen durability — and it is exactly that `fsync` that cost ~3.4
        //   ms/new stream (spike bn-1jg, 98.8% of new-stream latency). So we
        //   drop the barrier and instead push the name to the SAME OS page
        //   cache the event bytes go to, via `persist_buffered`
        //   (`PersistMode::Buffer`: a `write(2)`, no `fsync`), which returns
        //   before this append hands its batch to the committer. Because that
        //   name `write(2)` completes strictly BEFORE the covering event's own
        //   `write(2)`, the invariant holds for the crash class `Process`
        //   actually promises to survive:
        //     - process crash / panic / `kill -9` — the ONLY class §1.1
        //       protects. A completed `write(2)` outlives process death, so if
        //       the event `write(2)` happened, the earlier name `write(2)`
        //       necessarily did too. A surviving event can NEVER have a lost
        //       name. Recovery always resolves the name; the store opens.
        //     - OS crash / power loss — outside `Process`'s promise entirely:
        //       both name and event live-or-die by the OS page cache, and a
        //       lost log tail is exactly the "unbounded loss window" the
        //       operator accepted. A lost name for a lost-tail event is
        //       consistent (both gone). The only inconsistent outcome — event
        //       flushed to the device while its earlier-written name was not —
        //       needs the OS to reorder cross-file writeback against program
        //       order within the microsecond gap between the two `write(2)`s;
        //       it is not something `Process` protects against in the first
        //       place (any operator needing a power-loss guarantee runs
        //       `Os`/`Group`, which keep the barrier). It is `persist_buffered`
        //       being a real page-cache `write(2)` ordered before the event —
        //       NOT a mere in-process buffer — that keeps even this residual
        //       window as narrow as the log's own, rather than a whole
        //       writeback interval wide.
        //   Being barrier-free, the `Process` flush is a cheap `BufWriter`
        //   flush, so it runs inline (no `spawn_blocking`).
        if wrote_stream || wrote_types {
            match self.inner.durability {
                Durability::Process => {
                    self.inner.meta.persist_buffered().map_err(|e| {
                        AppendError::Backend(EngineError::Meta(e.to_string()))
                    })?;
                }
                Durability::Os | Durability::Group { .. } => {
                    // bn-34o: DON'T fsync here. The name bytes are already in
                    // fjall's page cache (`persist_new_names` above). Just bump
                    // the ticket high-water — before this append submits below
                    // — and the committer's pre-barrier
                    // hook
                    // ([`name_pre_barrier`](Self::name_pre_barrier)) makes
                    // every name buffered so far durable in
                    // ONE `SyncAll` inside the
                    // covering group's window, strictly before that group's log
                    // `fdatasync`. Because the bump is sequenced-before the
                    // submit, this append's name is covered by its own group's
                    // barrier; because the hook high-waters, the many serial
                    // groups of a burst share a handful of fsyncs, not one
                    // each.
                    self.inner
                        .name_flush
                        .buffered
                        .fetch_add(1, Ordering::SeqCst);
                }
            }
        }

        let (sid, events, first_stream_pos, tids) = match pre {
            Pre::Conflict(actual) => {
                return Err(AppendError::Conflict { expected, actual });
            }
            Pre::Empty => {
                let last_global =
                    self.inner.read_watermark.get().saturating_sub(1);
                return Ok(Appended {
                    version:              expected,
                    last_global_position: last_global,
                });
            }
            Pre::Proceed { sid, events, first_stream_pos, tids } => {
                (sid, events, first_stream_pos, tids)
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
        // Owned copy of the records for the publish step, which runs in a
        // `'static` blocking closure and so cannot borrow `records`. Used
        // only to warm the capsule cache with the just-published batch (the
        // book's per-event payload copy is gone, bn-2ib).
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
                let (first_global, last_global, seg_id, seg_off) = match outcome
                {
                    AppendOutcome::Acked {
                        first_position,
                        last_position,
                        segment_id,
                        offset,
                    } => (first_position, last_position, segment_id, offset),
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

                // 3) Publish: capsule-cache warm, active index
                //    (watermark-gated), stream head, meta head. No `.await` and
                //    no cancellation point exists past the position assignment
                //    above, so this always completes. The per-event book pushes
                //    are gone (bn-2ib): the durable batch itself is the byte
                //    authority, addressed by its real `(segment_id, offset)`
                //    pointer from the ack.
                //
                //    Order note (review F4): the interner head is updated
                //    strictly AFTER `apply_committed`, so a concurrent
                //    `head()` can never name a version the index cannot yet
                //    serve (`read_stream` resolves through the index now;
                //    pre-bn-2ib both lived under one book lock).

                // Warm the capsule cache with the batch just published, so an
                // immediate read-back (the overwhelmingly common hot-stream
                // shape) is a cache hit instead of a `pread` + decode.
                // Bounded + transparent: if the cache rejects or evicts it,
                // the read decodes the same durable bytes.
                {
                    let mut data = Vec::with_capacity(
                        records_owned.iter().map(|r| r.data.len()).sum(),
                    );
                    let mut offs = Vec::with_capacity(records_owned.len() + 1);
                    for rec in &records_owned {
                        offs.push(data.len() as u32);
                        data.extend_from_slice(&rec.data);
                    }
                    offs.push(data.len() as u32);
                    inner.reader.insert(
                        seg_id,
                        seg_off,
                        Arc::new(DecodedBatch {
                            stream_id: sid,
                            first_stream_version: first_stream_pos,
                            first_global_pos: first_global,
                            frame_count,
                            type_ids: tids,
                            data,
                            offs,
                        }),
                    );
                }

                let batch = BatchEntry {
                    stream_id: sid,
                    first_stream_version: first_stream_pos,
                    frame_count,
                    first_global_pos: first_global,
                    // The REAL durable placement from the ack (bn-2ib) — the
                    // block-native read paths dereference this pointer.
                    ptr: EventPtr { segment_id: seg_id, offset: seg_off },
                };
                inner.active.apply_committed(watermark, &[batch]);

                // The head becomes visible only once the index can serve
                // every version up to it (review F4).
                {
                    let mut book = inner.book.lock().expect("book lock");
                    book.heads.insert(sid, last_stream_pos);
                }

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
                // servable through the index tiers.
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
/// [`ActiveIndex`]/published watermark can never reach — the shape of a
/// publish an abandoned append future left stranded — so the skip path is
/// forced deterministically instead of waiting out the real ~10s default
/// bound. This needs access to private items (`run_roll_sealer`,
/// `SpinConfig`), so it lives inside this module rather than as a `tests/`
/// integration test.
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
        let tmp = mess_testkit::sweeping_temp_dir("engine-src-tmp");
        let (tx, rx) = mpsc::channel();
        tx.send(summary(1, 0, 10)).expect("send");
        drop(tx); // close the channel so the loop drains this one item and exits

        let active = Arc::new(ActiveIndex::new()); // never advanced: applied_end stays 0
        let published = Watermark::new(0); // never advanced
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
            published,
            RealRuntime::new().fs(),
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
        let tmp = mess_testkit::sweeping_temp_dir("engine-src-tmp-1");
        let (tx, rx) = mpsc::channel();
        tx.send(summary(1, 0, 10)).expect("send");
        tx.send(summary(2, 10, 20)).expect("send");
        drop(tx);

        let active = Arc::new(ActiveIndex::new());
        let published = Watermark::new(0);
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
            published,
            RealRuntime::new().fs(),
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
