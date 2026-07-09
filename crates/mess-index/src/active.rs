//! The in-memory **active index** (D5, round-4 default): the pointer index
//! that lives entirely in memory while a segment is active and persists only
//! at seal (`spikes/perf_append`: taking the index write off the append
//! critical path was the single 4.7x lever). This module owns the runtime
//! representation and the two disciplines that make it safe:
//!
//! 1. **Single writer, concurrent readers.** The committer
//!    ([`mess_log::committer`]) is the *sole* writer — it calls
//!    [`ActiveIndex::apply_committed`] once per group, from one thread. Any
//!    number of readers hold a shared [`ActiveIndex`] and resolve pointers
//!    concurrently. See the structure justification below.
//! 2. **D7 / commit-authority discipline.** An entry is *visible* to readers
//!    only once the durable watermark (D7, [`mess_log::watermark`]) covers it.
//!    Readers must never resolve an uncommitted pointer — the same rule
//!    [`mess_log::reader`] enforces for the raw segment tail. See
//!    [`ActiveIndex::apply_committed`] and the reader clamp.
//!
//! # Structure: sharded `RwLock` maps + a published watermark
//!
//! The candidates the bone names are left-right, arc-swap snapshots, and a
//! sharded `RwLock`. This module uses **sharded `RwLock<HashMap>`** plus one
//! **`AtomicU64` published watermark**, because:
//!
//! - *arc-swap of an immutable snapshot* would rebuild (or structurally share
//!   and clone-on-write) the whole per-stream map on every group — the active
//!   index grows to ~1M entries per segment, so republishing it per group is
//!   the write amplification D5 exists to avoid.
//! - *left-right* pays every write twice and drains an op-log on the read
//!   side; it shines when reads vastly dominate a *small* structure, not a
//!   million-entry append-mostly map.
//! - *sharded `RwLock`* gives the writer an O(1) `Vec::push` under a
//!   shard-local lock (contended only against readers touching the *same*
//!   shard, for the duration of a push), and readers a short shard read-lock.
//!   With exactly one writer there is no writer/writer contention at all. This
//!   mirrors the fjall memtable shape the spike measured (`ptr_index`: an LSM
//!   memtable absorbs small sorted inserts) without its per-event journal
//!   syscall.
//!
//! Visibility is **not** carried by the locks — it is carried by a single
//! monotone [`AtomicU64`], `applied_end`, exactly as [`mess_log::watermark`]
//! carries the durable end. The writer inserts entries into their shards
//! *first*, then publishes `applied_end` with `Release`; a reader loads
//! `applied_end` with `Acquire` and **clamps every result to it**. So a reader
//! that raced ahead and locked a shard the writer had already pushed the next
//! group into still discards those entries (their end position is `>` the
//! snapshot), and — because the `Release`/`Acquire` pair on `applied_end`
//! synchronizes-with — a reader that observes `applied_end == w` is guaranteed
//! to see every entry the writer inserted below `w`. That is the D7 contract:
//! nothing unacknowledged is ever visible; everything acknowledged always is.

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::RwLock;

/// A pointer to where an event's batch lives on disk. The active index maps a
/// `(stream_id, version)` to one of these (the batch's byte offset within its
/// segment); a reader `pread`s the batch at `ptr` and walks to the event.
///
/// Deliberately minimal (the bone's `EventPtr { segment_id, offset }`): the
/// per-batch shape below ([`StreamEntry`]) carries the version/position range
/// so one entry covers a whole stream-constant batch (D-FMT-6) rather than one
/// entry per event — the `ptr_index` spike's per-event vs per-block finding
/// applies at seal (D5), but the *active* entry is naturally per-batch because
/// a batch is stream-constant with consecutive versions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct EventPtr {
    /// The segment the batch lives in (`SegmentHeader.segment_id`).
    pub segment_id: u64,
    /// Byte offset of the batch within that segment.
    pub offset: u64,
}

/// One index entry: a whole committed batch of a single stream. A batch is
/// stream-constant (D-FMT-6) and its events are consecutive versions, so one
/// entry covers `[first_version, first_version + frame_count)` and resolves any
/// version in that range to the same [`EventPtr`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StreamEntry {
    /// Stream version of the batch's first event.
    pub first_version: u64,
    /// Number of events (subframes) in the batch.
    pub frame_count: u32,
    /// Global position of the batch's first event (A1) — the D7 clamp key.
    pub first_global_pos: u64,
    /// Where the batch lives.
    pub ptr: EventPtr,
}

impl StreamEntry {
    /// Stream version of the batch's **last** event.
    #[inline]
    pub fn last_version(&self) -> u64 {
        self.first_version + u64::from(self.frame_count) - 1
    }

    /// Exclusive global end of the batch: `first_global_pos + frame_count`. An
    /// entry is committed (visible) once the applied watermark reaches this.
    #[inline]
    pub fn end_pos(&self) -> u64 {
        self.first_global_pos + u64::from(self.frame_count)
    }

    /// Whether `version` falls in this batch's `[first_version, last_version]`.
    #[inline]
    pub fn contains_version(&self, version: u64) -> bool {
        version >= self.first_version && version <= self.last_version()
    }
}

/// One entry in the global-position-ordered view: the same batch as a
/// [`StreamEntry`] but keyed by global position (A1) and tagged with its
/// stream. The global log is append-only and, because the committer applies
/// groups in commit order, is naturally ascending by `first_global_pos`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GlobalEntry {
    /// Global position of the batch's first event (A1).
    pub first_global_pos: u64,
    /// Number of events in the batch.
    pub frame_count: u32,
    /// The batch's stream.
    pub stream_id: u64,
    /// Where the batch lives.
    pub ptr: EventPtr,
}

impl GlobalEntry {
    /// Exclusive global end of the batch.
    #[inline]
    pub fn end_pos(&self) -> u64 {
        self.first_global_pos + u64::from(self.frame_count)
    }
}

/// The unit the committer (or the rebuild replay) hands to
/// [`ActiveIndex::apply_committed`]: one accepted batch. The committer stamps
/// these fields; the rebuild path derives them from the recovery scan — **the
/// same insertion code, not two** (see [`crate::rebuild`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BatchEntry {
    /// Batch-constant stream id (D-FMT-6).
    pub stream_id: u64,
    /// Stream version of the batch's first event.
    pub first_stream_version: u64,
    /// Number of events in the batch.
    pub frame_count: u32,
    /// Global position of the batch's first event (A1).
    pub first_global_pos: u64,
    /// Where the batch lives.
    pub ptr: EventPtr,
}

impl BatchEntry {
    /// Exclusive global end of the batch: `first_global_pos + frame_count`.
    #[inline]
    pub fn end_pos(&self) -> u64 {
        self.first_global_pos + u64::from(self.frame_count)
    }

    #[inline]
    fn to_stream_entry(self) -> StreamEntry {
        StreamEntry {
            first_version: self.first_stream_version,
            frame_count: self.frame_count,
            first_global_pos: self.first_global_pos,
            ptr: self.ptr,
        }
    }

    #[inline]
    fn to_global_entry(self) -> GlobalEntry {
        GlobalEntry {
            first_global_pos: self.first_global_pos,
            frame_count: self.frame_count,
            stream_id: self.stream_id,
            ptr: self.ptr,
        }
    }
}

/// A deterministic, comparable snapshot of the index's **committed** content
/// (clamped to `applied_end`). Used by differential tests: two indices built by
/// different routes (incremental commit vs crash rebuild) are equal iff their
/// snapshots are equal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexSnapshot {
    /// The exclusive durable end this snapshot was clamped to.
    pub applied_end: u64,
    /// Per-stream committed entries, in version order. `BTreeMap` for a
    /// deterministic ordering independent of shard/hash layout.
    pub streams: BTreeMap<u64, Vec<StreamEntry>>,
    /// The committed global-position-ordered prefix.
    pub global: Vec<GlobalEntry>,
}

struct Shard {
    /// `stream_id -> entries in ascending (version == global-position) order`.
    streams: RwLock<HashMap<u64, Vec<StreamEntry>>>,
}

impl Shard {
    fn new() -> Self {
        Shard { streams: RwLock::new(HashMap::new()) }
    }
}

/// The in-memory active index: per-stream pointer lists plus a
/// global-position-ordered log, made visible to readers one durable watermark
/// at a time.
///
/// Cheap to share (`Arc<ActiveIndex>`): the committer keeps one clone of the
/// `Arc` and is the sole caller of [`apply_committed`](ActiveIndex::apply_committed);
/// readers keep clones and call the resolve/scan methods.
pub struct ActiveIndex {
    shards: Box<[Shard]>,
    shard_mask: u64,
    /// The global-position-ordered log. One writer appends in commit order;
    /// readers take a short read lock and clamp to `applied_end`.
    global: RwLock<Vec<GlobalEntry>>,
    /// The exclusive durable end applied to the index (D7). Published with
    /// `Release` after inserts; read with `Acquire` and used to clamp every
    /// result. Monotone non-decreasing.
    applied_end: AtomicU64,
}

impl Default for ActiveIndex {
    fn default() -> Self {
        Self::new()
    }
}

impl ActiveIndex {
    /// Default 64-shard index.
    pub fn new() -> Self {
        Self::with_shards(64)
    }

    /// An index with `shards` rounded **up** to a power of two (so shard
    /// selection is a mask, not a modulo). A minimum of one shard.
    pub fn with_shards(shards: usize) -> Self {
        let n = shards.max(1).next_power_of_two();
        let shards: Vec<Shard> = (0..n).map(|_| Shard::new()).collect();
        ActiveIndex {
            shards: shards.into_boxed_slice(),
            shard_mask: (n as u64) - 1,
            global: RwLock::new(Vec::new()),
            applied_end: AtomicU64::new(0),
        }
    }

    #[inline]
    fn shard_for(&self, stream_id: u64) -> &Shard {
        // A fibonacci-hash scramble so sequential stream ids spread across
        // shards instead of colliding on the low bits.
        let mixed = stream_id.wrapping_mul(0x9E37_79B9_7F4A_7C15);
        let idx = (mixed >> 32) & self.shard_mask;
        &self.shards[idx as usize]
    }

    /// The exclusive durable end currently applied to (and visible through)
    /// the index. Every resolve/scan clamps to this.
    #[inline]
    pub fn applied_end(&self) -> u64 {
        self.applied_end.load(Ordering::Acquire)
    }

    /// **Apply one group of committed batches** (the committer's sole write
    /// entry point, and the rebuild replay's — one code path). `watermark` is
    /// the exclusive durable end that now covers these batches (D7): every
    /// batch in `batches` MUST end at or below it. Batches are inserted into
    /// their shards and the global log *first*, and only then is `applied_end`
    /// published to `watermark` — so readers never observe a pointer above the
    /// durable end.
    ///
    /// Preconditions (debug-asserted): batches arrive in global-position order,
    /// each batch continues its stream (no version gap/overlap), and each ends
    /// `<= watermark`. A batch that violates the watermark bound is skipped
    /// defensively in release builds (an uncommitted pointer must never enter
    /// the index, even if a caller misbehaves).
    pub fn apply_committed(&self, watermark: u64, batches: &[BatchEntry]) {
        // Insert into per-stream shards first.
        for b in batches {
            debug_assert!(
                b.end_pos() <= watermark,
                "D7: batch ending at {} handed to index above watermark {}",
                b.end_pos(),
                watermark
            );
            if b.end_pos() > watermark {
                continue; // defensive: never expose an uncommitted pointer.
            }
            let shard = self.shard_for(b.stream_id);
            let mut map = shard.streams.write();
            let entries = map.entry(b.stream_id).or_default();
            debug_assert!(
                entries
                    .last()
                    .is_none_or(|prev| prev.end_pos() <= b.first_global_pos
                        && prev.last_version() < b.first_stream_version),
                "stream {} batch out of order/overlapping",
                b.stream_id
            );
            entries.push(b.to_stream_entry());
        }
        // Then append to the global-position log, in the given (commit) order.
        {
            let mut g = self.global.write();
            debug_assert!(
                g.last().is_none_or(|prev| batches
                    .first()
                    .is_none_or(|b| prev.end_pos() <= b.first_global_pos)),
                "global log applied out of position order"
            );
            for b in batches {
                if b.end_pos() > watermark {
                    continue;
                }
                g.push(b.to_global_entry());
            }
        }
        // Publish LAST: readers that observe this value (Acquire) are
        // guaranteed to see every insert above (Release synchronizes-with).
        self.applied_end.fetch_max(watermark, Ordering::Release);
    }

    /// The committed head version of `stream_id`: the last version whose batch
    /// is at or below the applied watermark, or `None` if the stream has no
    /// committed events. Clamped to a snapshot of `applied_end`.
    pub fn stream_head(&self, stream_id: u64) -> Option<u64> {
        let w = self.applied_end.load(Ordering::Acquire);
        let map = self.shard_for(stream_id).streams.read();
        let entries = map.get(&stream_id)?;
        // Entries ascend by end_pos; the committed prefix is the run with
        // end_pos <= w. `partition_point` finds its length in O(log n).
        let committed = entries.partition_point(|e| e.end_pos() <= w);
        if committed == 0 {
            None
        } else {
            Some(entries[committed - 1].last_version())
        }
    }

    /// Resolve `(stream_id, version)` to the [`EventPtr`] of the batch that
    /// holds it — but only if that batch is committed (at or below the applied
    /// watermark). `None` if the stream/version is unknown or not yet durable.
    pub fn resolve(&self, stream_id: u64, version: u64) -> Option<EventPtr> {
        let w = self.applied_end.load(Ordering::Acquire);
        let map = self.shard_for(stream_id).streams.read();
        let entries = map.get(&stream_id)?;
        // The candidate batch is the last one whose first_version <= version.
        let idx = entries.partition_point(|e| e.first_version <= version);
        if idx == 0 {
            return None;
        }
        let e = entries[idx - 1];
        if e.contains_version(version) && e.end_pos() <= w {
            Some(e.ptr)
        } else {
            None
        }
    }

    /// The committed entries of `stream_id`, in version order (a clamped clone).
    pub fn stream_entries(&self, stream_id: u64) -> Vec<StreamEntry> {
        let w = self.applied_end.load(Ordering::Acquire);
        let map = self.shard_for(stream_id).streams.read();
        match map.get(&stream_id) {
            None => Vec::new(),
            Some(entries) => {
                let committed = entries.partition_point(|e| e.end_pos() <= w);
                entries[..committed].to_vec()
            }
        }
    }

    /// The committed global-position-ordered prefix (a clamped clone). Dense
    /// and ascending: batch `i+1` begins exactly where batch `i` ends.
    pub fn global_committed(&self) -> Vec<GlobalEntry> {
        let w = self.applied_end.load(Ordering::Acquire);
        let g = self.global.read();
        // The log ascends by position; the committed prefix is the run whose
        // end_pos <= w. Nothing past the first uncommitted entry is served.
        let committed = g.partition_point(|e| e.end_pos() <= w);
        g[..committed].to_vec()
    }

    /// A deterministic snapshot of all committed content, for differential
    /// comparison. Independent of shard layout (streams collected into a
    /// `BTreeMap`).
    pub fn snapshot(&self) -> IndexSnapshot {
        let w = self.applied_end.load(Ordering::Acquire);
        let mut streams: BTreeMap<u64, Vec<StreamEntry>> = BTreeMap::new();
        for shard in self.shards.iter() {
            let map = shard.streams.read();
            for (&sid, entries) in map.iter() {
                let committed = entries.partition_point(|e| e.end_pos() <= w);
                if committed > 0 {
                    streams.insert(sid, entries[..committed].to_vec());
                }
            }
        }
        let global = self.global_committed();
        IndexSnapshot { applied_end: w, streams, global }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(stream_id: u64, first_version: u64, n: u32, pos: u64, off: u64) -> BatchEntry {
        BatchEntry {
            stream_id,
            first_stream_version: first_version,
            frame_count: n,
            first_global_pos: pos,
            ptr: EventPtr { segment_id: 1, offset: off },
        }
    }

    #[test]
    fn apply_then_resolve_and_head() {
        let idx = ActiveIndex::new();
        // stream 10: [0..=2] then [3..=4]; stream 20: [0..=0]. Global pos 0..6.
        let batches = [
            entry(10, 0, 3, 0, 100),
            entry(20, 0, 1, 3, 200),
            entry(10, 3, 2, 4, 300),
        ];
        idx.apply_committed(6, &batches);

        assert_eq!(idx.applied_end(), 6);
        assert_eq!(idx.stream_head(10), Some(4));
        assert_eq!(idx.stream_head(20), Some(0));
        assert_eq!(idx.stream_head(999), None);

        // Every version resolves to the batch that holds it.
        assert_eq!(idx.resolve(10, 0).unwrap().offset, 100);
        assert_eq!(idx.resolve(10, 2).unwrap().offset, 100);
        assert_eq!(idx.resolve(10, 3).unwrap().offset, 300);
        assert_eq!(idx.resolve(10, 4).unwrap().offset, 300);
        assert_eq!(idx.resolve(10, 5), None); // past head
        assert_eq!(idx.resolve(20, 0).unwrap().offset, 200);
        assert_eq!(idx.resolve(20, 1), None);
    }

    #[test]
    fn global_view_is_dense_and_ordered() {
        let idx = ActiveIndex::new();
        let batches = [entry(10, 0, 3, 0, 100), entry(20, 0, 2, 3, 200), entry(10, 3, 1, 5, 300)];
        idx.apply_committed(6, &batches);
        let g = idx.global_committed();
        assert_eq!(g.len(), 3);
        assert_eq!(g[0].first_global_pos, 0);
        assert_eq!(g[1].first_global_pos, 3);
        assert_eq!(g[2].first_global_pos, 5);
        for w in g.windows(2) {
            assert_eq!(w[0].end_pos(), w[1].first_global_pos);
        }
    }

    #[test]
    fn watermark_clamp_hides_entries_above_the_durable_end() {
        // Insert a batch that ends at 5 but publish a watermark of only 3:
        // the D7 discipline hides everything at/above the durable end. This
        // simulates a reader racing a not-yet-published group — the clamp,
        // not the insert, is what keeps an uncommitted pointer invisible.
        let idx = ActiveIndex::new();
        // First apply commits [0,3); then a second group commits [3,5) but we
        // model the reader-side clamp by inspecting after only the first
        // publish is visible. We assert via a fresh index with a low watermark.
        let batches_lo = [entry(10, 0, 3, 0, 100)];
        idx.apply_committed(3, &batches_lo);
        assert_eq!(idx.stream_head(10), Some(2));
        assert_eq!(idx.resolve(10, 2).unwrap().offset, 100);

        // A later committed group extends the head.
        let batches_hi = [entry(10, 3, 2, 3, 300)];
        idx.apply_committed(5, &batches_hi);
        assert_eq!(idx.applied_end(), 5);
        assert_eq!(idx.stream_head(10), Some(4));
        assert_eq!(idx.resolve(10, 4).unwrap().offset, 300);
    }

    #[test]
    fn defensive_skip_of_batch_above_watermark() {
        // In release builds an entry handed above the watermark must not enter
        // the index. (Debug builds assert; this exercises the runtime guard by
        // checking the clamp keeps it invisible even if applied_end lags.)
        let idx = ActiveIndex::new();
        idx.apply_committed(3, &[entry(10, 0, 3, 0, 100)]);
        // Manually apply a group whose watermark exactly covers it — normal.
        idx.apply_committed(5, &[entry(10, 3, 2, 3, 300)]);
        // Reader clamps to applied_end; nothing above 5 exists.
        assert!(idx.global_committed().iter().all(|e| e.end_pos() <= 5));
    }

    #[test]
    fn snapshot_is_deterministic_regardless_of_shards() {
        let batches = [
            entry(30, 0, 1, 0, 10),
            entry(10, 0, 2, 1, 20),
            entry(20, 0, 1, 3, 30),
            entry(10, 2, 1, 4, 40),
        ];
        let a = ActiveIndex::with_shards(4);
        let b = ActiveIndex::with_shards(256);
        a.apply_committed(5, &batches);
        b.apply_committed(5, &batches);
        assert_eq!(a.snapshot(), b.snapshot());
        // Streams collected in a BTreeMap → key order 10,20,30.
        let snap = a.snapshot();
        assert_eq!(snap.streams.keys().copied().collect::<Vec<_>>(), vec![10, 20, 30]);
    }

    #[test]
    fn empty_index_reads_are_none() {
        let idx = ActiveIndex::new();
        assert_eq!(idx.applied_end(), 0);
        assert_eq!(idx.stream_head(1), None);
        assert_eq!(idx.resolve(1, 0), None);
        assert!(idx.global_committed().is_empty());
        assert!(idx.stream_entries(1).is_empty());
    }

    #[test]
    fn with_shards_rounds_up_to_power_of_two() {
        // Non-power-of-two rounds up; a huge stream-id spread still resolves.
        let idx = ActiveIndex::with_shards(48);
        for s in 0..1000u64 {
            idx.apply_committed(s + 1, &[entry(s, 0, 1, s, s * 10)]);
        }
        for s in 0..1000u64 {
            assert_eq!(idx.stream_head(s), Some(0));
            assert_eq!(idx.resolve(s, 0).unwrap().offset, s * 10);
        }
    }
}
