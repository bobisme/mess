//! The sealed-segment store and the **sealed-or-active handoff** (D5): the
//! discipline that lets the background sealer evict a segment's entries from the
//! in-memory active index *without ever exposing a gap* to concurrent readers.
//!
//! # The invariant: sealed-or-active at all times
//!
//! Every committed pointer must be resolvable at every instant. While a segment
//! is active, its pointers live in [`crate::ActiveIndex`]. Once sealed, they
//! live in a [`SealedSegmentIndex`](crate::sealed::segment::SealedSegmentIndex).
//! The dangerous window is the swap. The rule
//! (the D5 handoff) is:
//!
//! 1. Build the sidecar and make it **durable** (fsync).
//! 2. Finalize the segment footer (mess-log's seal fsync).
//! 3. **Install** the sealed index into this store — published so readers see
//!    it (`install`).
//! 4. Only *then* may the segment's entries be evicted from the active index
//!    (`mark_active_evicted`).
//!
//! Because install (step 3) strictly precedes eviction (step 4), and the
//! combined resolver ([`resolve`]) consults sealed indexes **first**, a reader
//! can observe exactly three states of any segment `S` — active-only,
//! installed-and-active, or installed-and-evicted — but never
//! evicted-without-installed. The store keeps the "installed" and "evicted"
//! facts in one [`RwLock`]ed record so a reader's snapshot is always one of
//! those three consistent states; the swap race is exercised by
//! `tests/sealed_handoff.rs`.
//!
//! # Physical eviction
//!
//! The active index ([`crate::active`]) is append-only and owns no
//! per-segment eviction API (it is finished; this bone consumes it). This store
//! therefore models eviction *logically*: `mark_active_evicted(S)` records that
//! `S`'s active entries are no longer authoritative, and [`resolve`] stops
//! trusting active entries that point into an evicted segment. Reclaiming the
//! active index's memory for `S` is a follow-up that needs an
//! `ActiveIndex::evict_segment` method; the correctness-relevant handoff — the
//! gapless swap — is complete here.

use std::collections::{HashMap, HashSet};

use parking_lot::RwLock;

use crate::active::{ActiveIndex, EventPtr};
use crate::sealed::segment::SealedSegmentRef;

#[derive(Default)]
struct Inner {
    /// Installed sealed indexes, keyed by segment id.
    segments: HashMap<u64, SealedSegmentRef>,
    /// Segments whose active-index entries have been (logically) evicted; a
    /// subset of `segments`' keys (eviction always follows install).
    evicted: HashSet<u64>,
}

/// A published set of sealed-segment pointer indexes, plus the sealed-or-active
/// handoff bookkeeping. Cheap to share behind an `Arc`.
#[derive(Default)]
pub struct SealedStore {
    inner: RwLock<Inner>,
}

impl SealedStore {
    /// An empty store.
    pub fn new() -> Self {
        Self::default()
    }

    /// **Install** a sealed segment index, publishing it to readers (step 3 of
    /// the handoff). Idempotent replace by `segment_id`. MUST be called only
    /// after the sidecar and footer are durable.
    pub fn install(&self, index: SealedSegmentRef) {
        let id = index.segment_id();
        self.inner.write().segments.insert(id, index);
    }

    /// Mark segment `S`'s active-index entries as evicted (step 4). MUST be
    /// called only after [`install`](Self::install) of `S`. Debug-asserts the
    /// ordering; in release a stray call is still safe (it only makes the
    /// resolver *stricter* about trusting active entries for `S`).
    pub fn mark_active_evicted(&self, segment_id: u64) {
        let mut inner = self.inner.write();
        debug_assert!(
            inner.segments.contains_key(&segment_id),
            "D5 handoff: evicted segment {segment_id} before installing its sealed index"
        );
        inner.evicted.insert(segment_id);
    }

    /// The installed sealed index for `segment_id`, if any.
    pub fn get(&self, segment_id: u64) -> Option<SealedSegmentRef> {
        self.inner.read().segments.get(&segment_id).cloned()
    }

    /// Whether `segment_id`'s active entries have been evicted.
    pub fn is_evicted(&self, segment_id: u64) -> bool {
        self.inner.read().evicted.contains(&segment_id)
    }

    /// Number of installed sealed segments.
    pub fn len(&self) -> usize {
        self.inner.read().segments.len()
    }

    /// Whether no sealed segment is installed.
    pub fn is_empty(&self) -> bool {
        self.inner.read().segments.is_empty()
    }

    /// Resolve `(stream, version)` against the **sealed** indexes only. Scans
    /// installed segments (O(installed); production cross-segment routing is
    /// mess-store's job). Returns the first match's [`EventPtr`]. A corrupt
    /// sidecar (a decode error) is skipped, not fatal — the log stays truth.
    ///
    /// bn-1i7: each segment's `BinaryFuse16` stream-id filter is consulted
    /// **before** its directory (`stream_head`) — a segment the filter says
    /// definitely lacks `stream` is skipped without touching the directory
    /// at all. A missing/corrupt filter (`might_contain_stream` always
    /// `true`) falls back to exactly today's behavior for that segment.
    pub fn resolve_sealed(&self, stream: u64, version: u64) -> Option<EventPtr> {
        let inner = self.inner.read();
        Self::resolve_sealed_locked(&inner, stream, version)
    }

    fn resolve_sealed_locked(inner: &Inner, stream: u64, version: u64) -> Option<EventPtr> {
        for index in inner.segments.values() {
            // bn-1i7: filter says "no" -> definitely absent, skip the
            // directory/pointer-block lookup for this segment entirely.
            if !index.might_contain_stream(stream) {
                continue;
            }
            // Cheap reject on the stream's version range, then point-read.
            if let Some(head) = index.stream_head(stream)
                && version <= head
                && let Ok(Some(ptr)) = index.resolve(stream, version)
            {
                return Some(ptr);
            }
        }
        None
    }

    /// The installed sealed segments that (per directory, not just the
    /// filter) actually contain `stream` — the stream-replay-planning
    /// building block a caller (mess-store) walks further (bn-1i7). Segments
    /// whose filter definitely excludes `stream` are skipped without a
    /// directory lookup; a missing/corrupt filter costs only the skip, never
    /// a wrong answer — the directory check after it remains authoritative.
    pub fn segments_for_stream(&self, stream: u64) -> Vec<SealedSegmentRef> {
        let inner = self.inner.read();
        inner
            .segments
            .values()
            .filter(|index| index.might_contain_stream(stream) && index.stream_head(stream).is_some())
            .cloned()
            .collect()
    }

    /// The **combined resolver** under a single consistent store snapshot —
    /// sealed-or-active, gapless across the handoff. This is why the two store
    /// facts (installed set, evicted set) MUST be read under **one** lock: a
    /// reader that took the sealed lookup from a pre-install snapshot and the
    /// evicted check from a post-evict snapshot would see neither the sealed
    /// index nor a trusted active pointer — a spurious gap. Reading both from
    /// one snapshot makes the invariant `evicted ⊆ installed` hold *for the
    /// reader*: if a segment is evicted in this snapshot it is also installed in
    /// it, so the sealed scan already covered it.
    ///
    /// Sealed indexes are consulted first; on a miss, the active index is
    /// trusted only for segments not evicted in this snapshot. `active`'s own
    /// resolution uses its own locks and may happen at any instant — only the
    /// store snapshot must be consistent.
    pub fn resolve_with(&self, active: &ActiveIndex, stream: u64, version: u64) -> Option<EventPtr> {
        let inner = self.inner.read();
        if let Some(ptr) = Self::resolve_sealed_locked(&inner, stream, version) {
            return Some(ptr);
        }
        let ptr = active.resolve(stream, version)?;
        if inner.evicted.contains(&ptr.segment_id) {
            // Evicted in this snapshot ⇒ also installed in it ⇒ the sealed scan
            // above already had authority for this key and did not resolve it,
            // so the key genuinely is not present. The stale active pointer must
            // not be trusted.
            None
        } else {
            Some(ptr)
        }
    }
}

/// The **combined resolver**: sealed-or-active, gapless across the handoff.
/// Free-function form of [`SealedStore::resolve_with`] (which see for why the
/// resolution happens under a single store snapshot).
pub fn resolve(active: &ActiveIndex, store: &SealedStore, stream: u64, version: u64) -> Option<EventPtr> {
    store.resolve_with(active, stream, version)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::active::BatchEntry;
    use crate::sealed::segment::{
        SealBatch, SealInput, SealStream, SealedSegmentIndex, encode_sidecar,
    };
    use std::sync::Arc;

    fn sealed_seg(segment_id: u64, stream: u64, off: u64) -> SealedSegmentRef {
        let input = SealInput {
            segment_id,
            base_pos: 0,
            streams: vec![SealStream {
                stream_id: stream,
                batches: vec![SealBatch {
                    first_version: 0,
                    frame_count: 3,
                    first_global_pos: 0,
                    offset: off,
                }],
            }],
            payloads: None,
        };
        Arc::new(SealedSegmentIndex::from_bytes(encode_sidecar(&input)).unwrap())
    }

    #[test]
    fn combined_resolver_prefers_sealed_then_active() {
        let active = ActiveIndex::new();
        active.apply_committed(
            3,
            &[BatchEntry {
                stream_id: 10,
                first_stream_version: 0,
                frame_count: 3,
                first_global_pos: 0,
                ptr: EventPtr { segment_id: 1, offset: 100 },
            }],
        );
        let store = SealedStore::new();

        // Active-only: resolves via active.
        assert_eq!(resolve(&active, &store, 10, 1).unwrap().offset, 100);

        // Install the sealed index for seg 1 (different offset to prove which
        // path answered) then mark evicted.
        store.install(sealed_seg(1, 10, 999));
        assert_eq!(resolve(&active, &store, 10, 1).unwrap().offset, 999, "sealed wins");
        store.mark_active_evicted(1);
        assert_eq!(resolve(&active, &store, 10, 1).unwrap().offset, 999);
        // A version not in any sealed or live-active segment: miss.
        assert_eq!(resolve(&active, &store, 10, 9), None);
    }

    /// bn-1i7: `segments_for_stream` returns exactly the installed segments
    /// that actually hold the stream, whether or not each has a filter
    /// attached — the filter only ever narrows which segments get a
    /// directory lookup at all, never the final answer.
    #[test]
    fn segments_for_stream_matches_directory_membership() {
        let store = SealedStore::new();
        store.install(sealed_seg(1, 10, 100)); // has stream 10
        store.install(sealed_seg(2, 20, 200)); // has stream 20 only

        let for_10 = store.segments_for_stream(10);
        assert_eq!(for_10.len(), 1);
        assert_eq!(for_10[0].segment_id(), 1);

        let for_20 = store.segments_for_stream(20);
        assert_eq!(for_20.len(), 1);
        assert_eq!(for_20[0].segment_id(), 2);

        assert!(store.segments_for_stream(999).is_empty());
    }

    /// bn-1i7: a real `BinaryFuse16` filter attached to a segment that lacks
    /// the queried stream must not change `resolve`'s answer — only skip the
    /// directory lookup for that segment. Two installed segments, only one
    /// (with a filter) actually holds the stream being resolved.
    #[test]
    fn resolve_with_real_filter_skips_unrelated_segment_correctly() {
        use crate::sealed::filter::SegmentFilter;

        // Segment 1: streams 100..100+2000 (even spacing), filtered.
        let ids: Vec<u64> = (0..2000u64).map(|i| i * 5).collect();
        let streams1: Vec<SealStream> = ids
            .iter()
            .map(|&id| SealStream {
                stream_id: id,
                batches: vec![SealBatch { first_version: 0, frame_count: 1, first_global_pos: id, offset: 1000 + id }],
            })
            .collect();
        let input1 = SealInput { segment_id: 1, base_pos: 0, streams: streams1, payloads: None };
        let filter1 = SegmentFilter::build(1, &ids).unwrap();
        let mut idx1 = SealedSegmentIndex::from_bytes(encode_sidecar(&input1)).unwrap();
        idx1.attach_filter(filter1);

        // Segment 2: just stream 777 (never present in segment 1's id set).
        let idx2 = sealed_seg(2, 777, 999);

        let store = SealedStore::new();
        store.install(Arc::new(idx1));
        store.install(idx2);

        // 777 must resolve from segment 2 regardless of segment 1's filter.
        assert_eq!(store.resolve_sealed(777, 0).unwrap().offset, 999);
        // A present stream in segment 1 still resolves correctly too.
        assert_eq!(store.resolve_sealed(0, 0).unwrap().offset, 1000);
        // A genuinely absent stream resolves to nothing from either segment.
        assert_eq!(store.resolve_sealed(4_000_003, 0), None);
    }

    #[test]
    fn evicted_active_pointer_is_not_trusted() {
        let active = ActiveIndex::new();
        active.apply_committed(
            3,
            &[BatchEntry {
                stream_id: 10,
                first_stream_version: 0,
                frame_count: 3,
                first_global_pos: 0,
                ptr: EventPtr { segment_id: 1, offset: 100 },
            }],
        );
        let store = SealedStore::new();
        // Install a sealed index for seg 1 that only covers version 0..=2, then
        // evict. A resolve for version 1 must be served by sealed, not active.
        store.install(sealed_seg(1, 10, 777));
        store.mark_active_evicted(1);
        assert_eq!(resolve(&active, &store, 10, 1).unwrap().offset, 777);
    }
}
