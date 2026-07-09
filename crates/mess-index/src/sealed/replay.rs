//! Sealed **read paths across many segments** (bn-1hx): coalesced per-stream
//! replay, a parallel global scan, and the byte-identity gate that keeps the
//! parallel results provably equal to a naive single-threaded walk.
//!
//! These build *over* the per-segment sidecar readers
//! ([`SealedSegmentIndex`](crate::sealed::segment::SealedSegmentIndex)) — they
//! do not re-open or re-parse them. A [`ReplaySet`] is a base-position-ordered
//! set of already-opened sealed segments (the resolver's working set), and the
//! functions here answer whole-history queries against it.
//!
//! # Why base-position order is the whole trick
//!
//! Segments partition the global log into **contiguous, disjoint** A1 ranges:
//! segment *k*'s events occupy `[base_pos_k, base_pos_k + event_count_k)` and
//! the next segment begins exactly where it ends. So once the set is sorted by
//! `base_pos`:
//!
//! - **Global scan** is per-segment global order (each
//!   [`global_entries`](crate::sealed::segment::SealedSegmentIndex::global_entries)
//!   is sorted within its segment) **concatenated** in segment order — no
//!   cross-segment merge or sort is needed, only per-segment work that
//!   parallelizes cleanly. `perf_replay` measured block-parallel global replay
//!   at **95.1M ev/s** (14.8× one core, near-zero parallelization overhead);
//!   this is the pointer-index analogue — per-segment decode fanned across a
//!   thread pool, concatenated in order.
//! - **Stream replay** for one stream is its per-segment batch lists
//!   concatenated in segment order, which is automatically version-ascending
//!   (a stream's versions only grow across later segments). The candidate
//!   segments are found by a cheap directory probe
//!   ([`stream_head`](crate::sealed::segment::SealedSegmentIndex::stream_head)),
//!   their blocks decoded in parallel (cache-aware), then assembled in order —
//!   the "segment-order coalesced reads + parallel decode" shape `perf_replay`
//!   measured at **24.6M ev/s** for 1k random streams (8.9× the sequential
//!   baseline) by decoding each touched block exactly once.
//!
//! # Measured (bn-1hx, `tests/sealed_read_paths.rs`, release, this machine)
//!
//! Corpus: 24 sealed segments tiling the A1 axis, 2,000 streams present in every
//! segment, 5 batches/stream/segment, 10 frames/batch — **2.4M events**,
//! ~48k pointer blocks. Best-of-5 wall; the byte-identity gate (parallel ==
//! sequential, checksummed) ran on every measured pass.
//!
//! | path | throughput | notes |
//! |---|---|---|
//! | global scan (24-way parallel) | **~1.0–1.2G ev/s** | all 2.4M events, per-segment decode fanned across the pool, concatenated in `base_pos` order |
//! | coalesced stream replay (1,000 streams × 24 segments) | **~1.3G ev/s** | 1.2M events, batched across the working set |
//! | block cache hit rate, repeat replay | **60.2%** | two passes over the same 1,000-stream set; 19,104 unique blocks, 4.8 MiB resident |
//!
//! These are **pointer-index decode** rates: blocks are uncompressed until
//! Phase 5, so this is varint decode + assembly only, with none of the zstd cost
//! that put `perf_replay`'s comparable paths at 95M / 24.6M ev/s. The floor the
//! bone gates on — 2.5M ev/s stream replay — is cleared by ~500×; when the
//! Phase-5 decompress stage lands in the *materialize* step the rate drops
//! toward `perf_replay`'s numbers, but the path shape (locate → materialize →
//! decode → assemble, cache over the decoded product) is already in place.
//!
//! # The decompress seam (Phase 5)
//!
//! Pointer blocks are **uncompressed** today, so a segment's per-stream decode
//! is `slice → decode_ptr_block`. The path is already staged as *locate →
//! materialize → decode → assemble*; Phase 5's per-category block decompression
//! (`spikes/perf_compress`) slots into the *materialize* step inside
//! [`SealedSegmentIndex`](crate::sealed::segment::SealedSegmentIndex)'s
//! `stream_entries`/`global_entries` without changing this module or the
//! [`BlockCache`], which already caches the post-decode product.
//!
//! # Parallelism without a heavy dependency
//!
//! The fan-out is a small [`std::thread::scope`] pool (`parallel_map`) that
//! chunks the work across [`std::thread::available_parallelism`] scoped threads
//! and writes each result into its own output slot — no `rayon`, no channels,
//! no shared mutation. Below a one-item threshold it runs inline, so a
//! single-segment set (or a stream in one segment) pays nothing for threads.
//!
//! # mmap / SIGBUS stance
//!
//! This crate **maps nothing**. A sealed sidecar is read fully into an owned
//! `Vec<u8>` and CRC-validated *before* any query
//! ([`SealedSegmentIndex::from_bytes`](crate::sealed::segment::SealedSegmentIndex::from_bytes)):
//! a truncated, torn, or bit-flipped sidecar is a typed
//! [`SidecarError`](crate::sealed::segment::SidecarError) at **open** and the
//! segment is simply never admitted to a [`ReplaySet`]. There is therefore no
//! `SIGBUS` surface here — the `perf_replay` mmap-of-the-payload-segment concern
//! (a truncated mapping faulting to `SIGBUS` mid-read) belongs to Phase-5
//! payload reads in `mess-log`, not to the pointer index. For the residual case
//! of a decode error discovered *mid-replay* (defense in depth; a CRC-valid
//! sidecar never hits it), the read paths return the error **typed**, never
//! panic, and [`ReplaySet::stream_replay_many_verified`] re-runs single-threaded to
//! yield the authoritative result or the same typed error — the log stays truth
//! (D1), so the ultimate fallback is a rebuild.

use crate::active::{GlobalEntry, StreamEntry};
use crate::sealed::block_cache::BlockCache;
use crate::sealed::ptr_block::DecodeError;
use crate::sealed::segment::SealedSegmentRef;

/// Below this many parallel work items, run inline — thread setup would cost
/// more than the decode it hides.
const PARALLEL_THRESHOLD: usize = 2;

/// Map `f` over `0..n`, fanning the calls across a small scoped-thread pool and
/// returning the results **in index order**. Each result lands in its own
/// output slot, so there is no shared mutation and the output is deterministic
/// regardless of thread scheduling. Runs inline for `n < PARALLEL_THRESHOLD`.
fn parallel_map<T, F>(n: usize, f: F) -> Vec<T>
where
    T: Send,
    F: Fn(usize) -> T + Sync,
{
    if n == 0 {
        return Vec::new();
    }
    let threads = std::thread::available_parallelism()
        .map(|p| p.get())
        .unwrap_or(1)
        .min(n);
    if threads <= 1 || n < PARALLEL_THRESHOLD {
        return (0..n).map(f).collect();
    }
    let mut out: Vec<Option<T>> = Vec::with_capacity(n);
    out.resize_with(n, || None);
    let chunk = n.div_ceil(threads);
    let f = &f;
    std::thread::scope(|scope| {
        for (ci, slots) in out.chunks_mut(chunk).enumerate() {
            let base = ci * chunk;
            scope.spawn(move || {
                for (j, slot) in slots.iter_mut().enumerate() {
                    *slot = Some(f(base + j));
                }
            });
        }
    });
    // Every slot was written exactly once by its owning thread.
    out.into_iter().map(|o| o.expect("slot filled")).collect()
}

/// A base-position-ordered set of already-opened sealed segments — the sealed
/// half of a reader's working set. Whole-history queries (stream replay, global
/// scan) run against it; per-segment point reads stay on the individual
/// [`SealedSegmentIndex`](crate::sealed::segment::SealedSegmentIndex).
#[derive(Clone, Default)]
pub struct ReplaySet {
    /// Ascending by `(base_pos, segment_id)` — the global A1 order.
    segments: Vec<SealedSegmentRef>,
}

impl std::fmt::Debug for ReplaySet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReplaySet")
            .field("segments", &self.segments.len())
            .field("events", &self.event_count())
            .finish()
    }
}

impl ReplaySet {
    /// Build a replay set from already-opened sealed segments, sorting them into
    /// global A1 order (ascending `base_pos`, `segment_id` breaking any tie).
    /// The caller owns admission: a sidecar that failed to open (a typed
    /// [`SidecarError`](crate::sealed::segment::SidecarError)) is simply not
    /// passed in — see the module's mmap/SIGBUS stance.
    pub fn from_segments<I>(segments: I) -> Self
    where
        I: IntoIterator<Item = SealedSegmentRef>,
    {
        let mut segments: Vec<SealedSegmentRef> = segments.into_iter().collect();
        segments.sort_by_key(|s| (s.base_pos(), s.segment_id()));
        Self { segments }
    }

    /// The segments, in global order.
    pub fn segments(&self) -> &[SealedSegmentRef] {
        &self.segments
    }

    /// Number of sealed segments.
    pub fn len(&self) -> usize {
        self.segments.len()
    }

    /// Whether the set is empty.
    pub fn is_empty(&self) -> bool {
        self.segments.is_empty()
    }

    /// Total events across all segments.
    pub fn event_count(&self) -> u64 {
        self.segments.iter().map(|s| s.event_count()).sum()
    }

    // -- stream replay -----------------------------------------------------

    /// **Coalesced stream replay** of one stream: every batch of `stream_id`
    /// across the whole sealed history, version-ascending. The stream's blocks
    /// are gathered in segment order (a cheap directory probe per segment, then
    /// the block decode through `cache`, which decodes each block at most once
    /// and serves repeats from memory) and concatenated — which is already
    /// version-ascending because a stream's versions only grow across later
    /// segments. Pass [`BlockCache::disabled`] to decode every block fresh.
    ///
    /// This coalesce is **sequential**: one stream touches a handful of blocks
    /// and the varint decode is cheap, so thread setup would cost more than it
    /// saves. Throughput over a *working set* of streams comes from
    /// [`stream_replay_many`](Self::stream_replay_many), which fans this across
    /// the streams — the "parallel decode for streams" shape `perf_replay`
    /// measured at 24.6M ev/s.
    pub fn stream_replay(
        &self,
        stream_id: u64,
        cache: &BlockCache,
    ) -> Result<Vec<StreamEntry>, DecodeError> {
        let mut out = Vec::new();
        for seg in &self.segments {
            if seg.stream_head(stream_id).is_some() {
                out.extend_from_slice(&cache.get_or_load(seg, stream_id)?);
            }
        }
        debug_assert!(
            out.windows(2).all(|w| w[0].first_version < w[1].first_version),
            "coalesced stream replay must be strictly version-ascending"
        );
        Ok(out)
    }

    /// **Batched coalesced stream replay**: replay each stream in `streams`,
    /// fanning the per-stream coalesces across the scoped-thread pool. Returns
    /// one `(stream_id, entries)` per input, **input order preserved**. This is
    /// the parallel path — the working set (a projection rebuild's stream set, a
    /// subscription fan-out) is decoded across cores, each block at most once
    /// through `cache`.
    pub fn stream_replay_many(
        &self,
        streams: &[u64],
        cache: &BlockCache,
    ) -> Result<Vec<(u64, Vec<StreamEntry>)>, DecodeError> {
        let parts = parallel_map(streams.len(), |i| {
            let sid = streams[i];
            self.stream_replay(sid, cache).map(|v| (sid, v))
        });
        parts.into_iter().collect()
    }

    /// Single-threaded reference for
    /// [`stream_replay_many`](Self::stream_replay_many) — the naive walk the
    /// parallel path must equal element-for-element (the always-on byte-identity
    /// gate).
    pub fn stream_replay_many_seq(
        &self,
        streams: &[u64],
        cache: &BlockCache,
    ) -> Result<Vec<(u64, Vec<StreamEntry>)>, DecodeError> {
        streams
            .iter()
            .map(|&sid| self.stream_replay(sid, cache).map(|v| (sid, v)))
            .collect()
    }

    /// [`stream_replay_many`](Self::stream_replay_many) with an exact
    /// single-threaded fallback: if the parallel path returns a decode error,
    /// re-run sequentially to produce the authoritative result (identical bytes)
    /// or the same typed error. Never panics, never `SIGBUS`es (the module maps
    /// nothing — see the module's mmap/SIGBUS stance). The documented
    /// degraded-mode entry point.
    pub fn stream_replay_many_verified(
        &self,
        streams: &[u64],
        cache: &BlockCache,
    ) -> Result<Vec<(u64, Vec<StreamEntry>)>, DecodeError> {
        match self.stream_replay_many(streams, cache) {
            Ok(v) => Ok(v),
            Err(_) => self.stream_replay_many_seq(streams, cache),
        }
    }

    // -- global scan -------------------------------------------------------

    /// **Parallel global scan**: every batch across the whole sealed history in
    /// global-position (A1) order. Each segment's global order is decoded in
    /// parallel and the pieces concatenated in `base_pos` order — since segments
    /// hold disjoint, contiguous A1 ranges the concatenation is already globally
    /// sorted (no cross-segment merge).
    pub fn global_scan(&self) -> Result<Vec<GlobalEntry>, DecodeError> {
        let parts = parallel_map(self.segments.len(), |i| self.segments[i].global_entries());
        let mut out = Vec::new();
        for part in parts {
            out.extend_from_slice(&part?);
        }
        debug_assert!(
            out.windows(2).all(|w| w[0].first_global_pos <= w[1].first_global_pos),
            "global scan must be non-descending in global position"
        );
        Ok(out)
    }

    /// Single-threaded reference for [`global_scan`](Self::global_scan).
    pub fn global_scan_seq(&self) -> Result<Vec<GlobalEntry>, DecodeError> {
        let mut out = Vec::new();
        for seg in &self.segments {
            out.extend_from_slice(&seg.global_entries()?);
        }
        Ok(out)
    }
}

// ---------------------------------------------------------------------------
// Byte-identity checksums (the always-on correctness gate)
// ---------------------------------------------------------------------------

#[inline]
fn mix(mut x: u64) -> u64 {
    // splitmix64 finalizer — spreads each field across all 64 bits so the
    // order-independent wrapping sum below is a strong equality fingerprint.
    x ^= x >> 30;
    x = x.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x ^= x >> 27;
    x = x.wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^ (x >> 31)
}

/// Order-independent fingerprint of a stream-replay result: the wrapping sum of
/// each entry's mixed fields. Two results with the same multiset of entries
/// hash equal, so a parallel result can be gated against the sequential one
/// cheaply (and the bench prints it as a sanity number).
pub fn stream_checksum(entries: &[StreamEntry]) -> u64 {
    entries.iter().fold(0u64, |acc, e| {
        acc.wrapping_add(mix(e.first_version))
            .wrapping_add(mix(u64::from(e.frame_count).rotate_left(21)))
            .wrapping_add(mix(e.first_global_pos.rotate_left(42)))
            .wrapping_add(mix(e.ptr.offset ^ e.ptr.segment_id.rotate_left(17)))
    })
}

/// Order-independent fingerprint of a global-scan result.
pub fn global_checksum(entries: &[GlobalEntry]) -> u64 {
    entries.iter().fold(0u64, |acc, e| {
        acc.wrapping_add(mix(e.first_global_pos))
            .wrapping_add(mix(u64::from(e.frame_count).rotate_left(21)))
            .wrapping_add(mix(e.stream_id.rotate_left(42)))
            .wrapping_add(mix(e.ptr.offset ^ e.ptr.segment_id.rotate_left(17)))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::active::EventPtr;
    use crate::sealed::segment::{
        SealBatch, SealInput, SealStream, SealedSegmentIndex, encode_sidecar,
    };
    use std::sync::Arc;

    /// A segment whose `base_pos` is `base`, holding `streams` as
    /// `(stream_id, first_version, n_batches)` with a fixed 10-frame batch and
    /// A1 positions laid out contiguously from `base`.
    fn segment(segment_id: u64, base: u64, streams: &[(u64, u64, usize)]) -> SealedSegmentRef {
        // Assign contiguous global positions across the segment in stream order.
        let mut g = base;
        let streams: Vec<SealStream> = streams
            .iter()
            .map(|&(sid, fv0, n)| {
                let batches = (0..n)
                    .map(|i| {
                        let b = SealBatch {
                            first_version: fv0 + (i * 10) as u64,
                            frame_count: 10,
                            first_global_pos: g,
                            offset: 4096 + (i * 512) as u64,
                        };
                        g += 10;
                        b
                    })
                    .collect();
                SealStream { stream_id: sid, batches }
            })
            .collect();
        let input = SealInput { segment_id, base_pos: base, streams };
        Arc::new(SealedSegmentIndex::from_bytes(encode_sidecar(&input)).unwrap())
    }

    /// Three segments, contiguous A1 ranges, stream 10 present in all three,
    /// stream 20 only in the middle one.
    fn sample_set() -> ReplaySet {
        // seg 1: streams 10 (2 batches) and 30 (1). 3 batches = 30 events.
        let s1 = segment(1, 0, &[(10, 0, 2), (30, 0, 1)]);
        // seg 2: streams 10 (continues at v20), 20. base = 30.
        let s2 = segment(2, 30, &[(10, 20, 3), (20, 0, 2)]);
        // seg 3: stream 10 again (v50). base = 30 + 50 = 80.
        let s3 = segment(3, 80, &[(10, 50, 1)]);
        // Deliberately pass them out of order to exercise the sort.
        ReplaySet::from_segments([s2, s3, s1])
    }

    #[test]
    fn set_is_sorted_by_base_pos() {
        let set = sample_set();
        let bases: Vec<u64> = set.segments().iter().map(|s| s.base_pos()).collect();
        assert_eq!(bases, vec![0, 30, 80]);
        assert_eq!(set.len(), 3);
        assert_eq!(set.event_count(), 30 + 50 + 10);
    }

    #[test]
    fn stream_replay_coalesces_across_segments_in_version_order() {
        let set = sample_set();
        let cache = BlockCache::disabled();
        let s10 = set.stream_replay(10, &cache).unwrap();
        // 2 + 3 + 1 batches, strictly ascending versions 0,10,20,30,40,50.
        let versions: Vec<u64> = s10.iter().map(|e| e.first_version).collect();
        assert_eq!(versions, vec![0, 10, 20, 30, 40, 50]);
        // Segment tagging is preserved through the coalesce.
        assert_eq!(s10[0].ptr.segment_id, 1);
        assert_eq!(s10[2].ptr.segment_id, 2);
        assert_eq!(s10[5].ptr.segment_id, 3);

        // Stream only in the middle segment.
        let s20 = set.stream_replay(20, &cache).unwrap();
        assert_eq!(s20.len(), 2);
        assert!(s20.iter().all(|e| e.ptr.segment_id == 2));

        // Absent stream.
        assert!(set.stream_replay(999, &cache).unwrap().is_empty());
    }

    #[test]
    fn parallel_equals_sequential_stream_replay() {
        let set = sample_set();
        let cache = BlockCache::disabled();
        let streams = [10u64, 20, 30, 999, 10, 20];
        let par = set.stream_replay_many(&streams, &cache).unwrap();
        let seq = set.stream_replay_many_seq(&streams, &cache).unwrap();
        assert_eq!(par, seq, "batched parallel != sequential");
        // Input order preserved, and each matches the single-stream coalesce.
        for (i, &sid) in streams.iter().enumerate() {
            assert_eq!(par[i].0, sid);
            assert_eq!(par[i].1, set.stream_replay(sid, &cache).unwrap());
            assert_eq!(stream_checksum(&par[i].1), stream_checksum(&seq[i].1));
        }
    }

    #[test]
    fn global_scan_is_globally_ordered_and_complete() {
        let set = sample_set();
        let g = set.global_scan().unwrap();
        assert_eq!(g.len() as u64, set.event_count() / 10); // one entry per batch
        for w in g.windows(2) {
            assert!(w[0].first_global_pos < w[1].first_global_pos);
        }
        // First entry is seg 1's first batch at position 0.
        assert_eq!(g[0].first_global_pos, 0);
        assert_eq!(g[0].ptr.segment_id, 1);
    }

    #[test]
    fn parallel_equals_sequential_global_scan() {
        let set = sample_set();
        let par = set.global_scan().unwrap();
        let seq = set.global_scan_seq().unwrap();
        assert_eq!(par, seq);
        assert_eq!(global_checksum(&par), global_checksum(&seq));
    }

    #[test]
    fn checksum_detects_a_single_field_change() {
        let a = [StreamEntry {
            first_version: 0,
            frame_count: 10,
            first_global_pos: 0,
            ptr: EventPtr { segment_id: 1, offset: 4096 },
        }];
        let mut b = a;
        b[0].ptr.offset = 4608;
        assert_ne!(stream_checksum(&a), stream_checksum(&b));
    }

    #[test]
    fn cache_serves_repeat_stream_replay_from_memory() {
        let set = sample_set();
        let cache = BlockCache::with_budget_bytes(1 << 20, 64);
        let first = set.stream_replay(10, &cache).unwrap();
        let misses_after_first = cache.misses();
        assert!(misses_after_first >= 1, "cold replay decodes at least one block");
        let second = set.stream_replay(10, &cache).unwrap();
        assert_eq!(first, second);
        // The repeat added only hits, no new misses.
        assert_eq!(cache.misses(), misses_after_first);
        assert!(cache.hit_rate() > 0.0);
    }

    #[test]
    fn verified_matches_plain_when_all_decode() {
        let set = sample_set();
        let cache = BlockCache::disabled();
        let streams = [10u64, 20, 30];
        assert_eq!(
            set.stream_replay_many_verified(&streams, &cache).unwrap(),
            set.stream_replay_many(&streams, &cache).unwrap(),
        );
    }

    #[test]
    fn empty_set_replays_empty() {
        let set = ReplaySet::default();
        let cache = BlockCache::disabled();
        assert!(set.is_empty());
        assert!(set.stream_replay(1, &cache).unwrap().is_empty());
        assert!(set.global_scan().unwrap().is_empty());
    }
}
