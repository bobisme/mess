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
    pub fn resolve_sealed(&self, stream: u64, version: u64) -> Option<EventPtr> {
        let inner = self.inner.read();
        Self::resolve_sealed_locked(&inner, stream, version)
    }

    fn resolve_sealed_locked(inner: &Inner, stream: u64, version: u64) -> Option<EventPtr> {
        for index in inner.segments.values() {
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
