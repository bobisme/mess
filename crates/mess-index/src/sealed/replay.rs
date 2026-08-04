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
//!   concatenated in segment order, which is automatically version-ascending (a
//!   stream's versions only grow across later segments). The candidate segments
//!   are found by a cheap directory probe
//!   ([`stream_head`](crate::sealed::segment::SealedSegmentIndex::stream_head)),
//!   their blocks decoded in parallel (cache-aware), then assembled in order —
//!   the "segment-order coalesced reads + parallel decode" shape `perf_replay`
//!   measured at **24.6M ev/s** for 1k random streams (8.9× the sequential
//!   baseline) by decoding each touched block exactly once.
//!
//! # Measured (bn-1hx, `tests/sealed_read_paths.rs`, release, this machine)
//!
//! Corpus: 24 sealed segments tiling the A1 axis, 2,000 streams present in
//! every segment, 5 batches/stream/segment, 10 frames/batch — **2.4M events**,
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
//! Phase 5, so this is varint decode + assembly only, with none of the zstd
//! cost that put `perf_replay`'s comparable paths at 95M / 24.6M ev/s. The
//! floor the bone gates on — 2.5M ev/s stream replay — is cleared by ~500×;
//! when the Phase-5 decompress stage lands in the *materialize* step the rate
//! drops toward `perf_replay`'s numbers, but the path shape (locate →
//! materialize → decode → assemble, cache over the decoded product) is already
//! in place.
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
//! `SIGBUS` surface here — the `perf_replay` mmap-of-the-payload-segment
//! concern (a truncated mapping faulting to `SIGBUS` mid-read) belongs to
//! Phase-5 payload reads in `mess-log`, not to the pointer index. For the
//! residual case of a decode error discovered *mid-replay* (defense in depth; a
//! CRC-valid sidecar never hits it), the read paths return the error **typed**,
//! never panic, and [`ReplaySet::stream_replay_many_verified`] re-runs
//! single-threaded to yield the authoritative result or the same typed error —
//! the log stays truth (D1), so the ultimate fallback is a rebuild.

use crate::active::{GlobalEntry, StreamEntry};
use crate::sealed::block_cache::BlockCache;
use crate::sealed::payload::{DictResolver, PayloadError};
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
    /// Build a replay set from already-opened sealed segments, sorting them
    /// into global A1 order (ascending `base_pos`, `segment_id` breaking
    /// any tie). The caller owns admission: a sidecar that failed to open
    /// (a typed [`SidecarError`](crate::sealed::segment::SidecarError)) is
    /// simply not passed in — see the module's mmap/SIGBUS stance.
    pub fn from_segments<I>(segments: I) -> Self
    where
        I: IntoIterator<Item = SealedSegmentRef>,
    {
        let mut segments: Vec<SealedSegmentRef> =
            segments.into_iter().collect();
        segments.sort_by_key(|s| (s.base_pos(), s.segment_id()));
        Self { segments }
    }

    /// The segments, in global order.
    pub fn segments(&self) -> &[SealedSegmentRef] { &self.segments }

    /// Number of sealed segments.
    pub fn len(&self) -> usize { self.segments.len() }

    /// Whether the set is empty.
    pub fn is_empty(&self) -> bool { self.segments.is_empty() }

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

    /// **Seeking stream replay** (bn-1u6p): the batches of `stream_id` that
    /// hold `from_version` or any later version, version-ascending, decoding
    /// only enough blocks to cover `max_events` events at or past
    /// `from_version`.
    ///
    /// This is the sealed-tier twin of
    /// [`ActiveIndex::stream_entries_from`](crate::active::ActiveIndex::stream_entries_from),
    /// and it exists for the same reason: a *paged* read of a deep stream must
    /// cost O(page), not O(stream history).
    /// [`stream_replay`](Self::stream_replay) answers "the whole stream"
    /// and is the right shape for a projection rebuild; serving one page
    /// through it makes every page of a 1.37M-event stream pay 140 ms of
    /// entry resolution to deliver 0.2 ms of records (measured, bn-e93g).
    ///
    /// Two facts already in the format make the seek free:
    ///
    /// - each segment's per-stream directory entry records the stream's **head
    ///   version in that segment**
    ///   ([`stream_head`](crate::sealed::segment::SealedSegmentIndex::stream_head)),
    ///   so a segment whose head is below the cursor is skipped **without
    ///   decoding its block at all** — the directory probe is a hash lookup;
    /// - a decoded block is **version-ascending by construction**, so the first
    ///   batch at or past the cursor is a binary search inside the one segment
    ///   that straddles it.
    ///
    /// # Batch granularity
    ///
    /// Entries are batches, so the batch **straddling** the cursor
    /// (`first_version < from_version <= last_version`) IS returned — the
    /// caller needs it to serve the events at and past the cursor and trims
    /// the earlier frames itself. `partition_point` on `last_version <
    /// from_version` is what keeps it in.
    ///
    /// # Boundedness contract
    ///
    /// The walk stops as soon as the returned entries cover `max_events`
    /// events at or past `from_version`. So the result is **truncated only
    /// when it is also over-full**: if it covers fewer than `max_events`
    /// events, it is the complete sealed remainder of the stream. Callers
    /// merging this with another tier rely on that (see `LogEngine`'s
    /// sealed/hot merge) — a short result can be trusted to have no missing
    /// suffix.
    pub fn stream_replay_from(
        &self,
        stream_id: u64,
        from_version: u64,
        max_events: usize,
        cache: &BlockCache,
    ) -> Result<Vec<StreamEntry>, DecodeError> {
        let mut out: Vec<StreamEntry> = Vec::new();
        let mut covered = 0usize;
        for seg in &self.segments {
            if covered >= max_events {
                break;
            }
            // Directory probe only: a segment whose head for this stream is
            // below the cursor is skipped without touching its block.
            match seg.stream_head(stream_id) {
                None => continue,
                Some(head) if head < from_version => continue,
                Some(_) => {}
            }
            let block = cache.get_or_load(seg, stream_id)?;
            // First batch whose LAST event is at/after the cursor — keeps the
            // straddling batch.
            let start =
                block.partition_point(|e| e.last_version() < from_version);
            for e in &block[start..] {
                if covered >= max_events {
                    break;
                }
                out.push(*e);
                // Events this batch contributes at or past the cursor.
                let lo = from_version.max(e.first_version);
                covered += (e.last_version() - lo + 1) as usize;
            }
        }
        debug_assert!(
            out.windows(2).all(|w| w[0].first_version < w[1].first_version),
            "seeking stream replay must be strictly version-ascending"
        );
        debug_assert!(
            out.first().is_none_or(|e| e.last_version() >= from_version),
            "seeking stream replay must not return pre-cursor batches"
        );
        Ok(out)
    }

    /// **Batched coalesced stream replay**: replay each stream in `streams`,
    /// fanning the per-stream coalesces across the scoped-thread pool. Returns
    /// one `(stream_id, entries)` per input, **input order preserved**. This is
    /// the parallel path — the working set (a projection rebuild's stream set,
    /// a subscription fan-out) is decoded across cores, each block at most
    /// once through `cache`.
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
    /// parallel path must equal element-for-element (the always-on
    /// byte-identity gate).
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
    /// re-run sequentially to produce the authoritative result (identical
    /// bytes) or the same typed error. Never panics, never `SIGBUS`es (the
    /// module maps nothing — see the module's mmap/SIGBUS stance). The
    /// documented degraded-mode entry point.
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
    /// parallel and the pieces concatenated in `base_pos` order — since
    /// segments hold disjoint, contiguous A1 ranges the concatenation is
    /// already globally sorted (no cross-segment merge).
    pub fn global_scan(&self) -> Result<Vec<GlobalEntry>, DecodeError> {
        let parts = parallel_map(self.segments.len(), |i| {
            self.segments[i].global_entries()
        });
        let mut out = Vec::new();
        for part in parts {
            out.extend_from_slice(&part?);
        }
        debug_assert!(
            out.windows(2)
                .all(|w| w[0].first_global_pos <= w[1].first_global_pos),
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

    // -- payload reads (D6 `.pcol`, bn-zge) --------------------------------

    /// Whether **every** segment in the set carries an attached D6 payload
    /// sidecar (`.pcol`) — i.e. the whole sealed history can be replayed
    /// payload-side ([`Self::payload_scan`]) without touching the raw log. A
    /// pointer-only seal (no payloads handed to the sealer) leaves a segment
    /// without one, and the caller falls back to the raw segment for it.
    pub fn all_have_payloads(&self) -> bool {
        self.segments.iter().all(|s| s.has_payload())
    }

    /// The index of the segment whose contiguous A1 range
    /// `[base_pos, base_pos + event_count)` contains `global_pos`, or `None`
    /// when `global_pos` is past the sealed history. Segments are `base_pos`
    /// ordered and disjoint, so this is a binary search.
    fn segment_for_global(&self, global_pos: u64) -> Option<usize> {
        let mut lo = 0usize;
        let mut hi = self.segments.len();
        while lo < hi {
            let mid = (lo + hi) / 2;
            let s = &self.segments[mid];
            let base = s.base_pos();
            if global_pos < base {
                hi = mid;
            } else if global_pos >= base + s.event_count() {
                lo = mid + 1;
            } else {
                return Some(mid);
            }
        }
        None
    }

    /// **Payload point read** through the D6 payload sidecar: reassemble the
    /// event at global position `global_pos` byte-exact, dispatching to
    /// whichever segment covers it and through `resolver` for a
    /// row-fallback dictionary block. Works uniformly for columnar and
    /// row-fallback blocks (the block kind is internal to the segment's
    /// `.pcol`). Returns:
    ///
    /// - `Ok(Some(bytes))` — reassembled from the covering segment's `.pcol`;
    /// - `Ok(None)` — `global_pos` is past the sealed history, **or** the
    ///   covering segment has no `.pcol` attached (pointer-only seal — the
    ///   caller reads the payload from the raw log instead);
    /// - `Err(_)` — a block decode / dictionary-resolution failure.
    pub fn payload_at(
        &self,
        global_pos: u64,
        resolver: &impl DictResolver,
    ) -> Result<Option<Vec<u8>>, PayloadError> {
        let Some(si) = self.segment_for_global(global_pos) else {
            return Ok(None);
        };
        let seg = &self.segments[si];
        let local = global_pos - seg.base_pos();
        seg.reassemble_payload(local, resolver)
    }

    /// **Payload global replay**: reassemble every sealed payload in global
    /// (A1) order across the whole set, byte-exact, appending bytes to
    /// `out` and `event_count + 1` boundaries to `offs`. Because segments
    /// hold disjoint, contiguous A1 ranges, concatenating each segment's
    /// `.pcol` reassembly in `base_pos` order is already globally ordered —
    /// this is the payload-side analogue of
    /// [`global_scan`](Self::global_scan), and it handles **mixed
    /// segments** (columnar + row-fallback blocks) through the one path.
    ///
    /// Requires every segment to carry a `.pcol` ([`Self::all_have_payloads`]);
    /// a segment without one returns [`PayloadError::Corrupt`] rather than
    /// silently skipping events (a global replay must be complete).
    pub fn payload_scan(
        &self,
        resolver: &impl DictResolver,
        out: &mut Vec<u8>,
        offs: &mut Vec<u32>,
    ) -> Result<(), PayloadError> {
        out.clear();
        offs.clear();
        for (i, seg) in self.segments.iter().enumerate() {
            let payload = seg.payload_index().ok_or(PayloadError::Corrupt(
                "segment in payload_scan has no .pcol sidecar",
            ))?;
            payload.reassemble_all(resolver, out, offs)?;
            // reassemble_all appends this segment's boundaries plus a trailing
            // `out.len()`. Drop that duplicate between segments so the next
            // segment's first boundary (also `out.len()`) is not repeated; the
            // final segment keeps its trailing boundary.
            if i + 1 < self.segments.len() {
                offs.pop();
            }
        }
        Ok(())
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
    x = x.wrapping_mul(0xBF58_476D_1CE4_E5B9);
    x ^= x >> 27;
    x = x.wrapping_mul(0x94D0_49BB_1331_11EB);
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
    use std::sync::Arc;

    use super::*;
    use crate::active::EventPtr;
    use crate::sealed::segment::{
        SealBatch, SealInput, SealStream, SealedSegmentIndex, encode_sidecar,
    };

    /// A segment whose `base_pos` is `base`, holding `streams` as
    /// `(stream_id, first_version, n_batches)` with a fixed 10-frame batch and
    /// A1 positions laid out contiguously from `base`.
    fn segment(
        segment_id: u64,
        base: u64,
        streams: &[(u64, u64, usize)],
    ) -> SealedSegmentRef {
        // Assign contiguous global positions across the segment in stream
        // order.
        let mut g = base;
        let streams: Vec<SealStream> = streams
            .iter()
            .map(|&(sid, fv0, n)| {
                let batches = (0..n)
                    .map(|i| {
                        let b = SealBatch {
                            first_version:    fv0 + (i * 10) as u64,
                            frame_count:      10,
                            first_global_pos: g,
                            offset:           4096 + (i * 512) as u64,
                        };
                        g += 10;
                        b
                    })
                    .collect();
                SealStream { stream_id: sid, batches }
            })
            .collect();
        let input = SealInput {
            segment_id,
            base_pos: base,
            streams,
            payloads: None,
            event_type_ids: None,
        };
        Arc::new(
            SealedSegmentIndex::from_bytes(encode_sidecar(&input)).unwrap(),
        )
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
        let bases: Vec<u64> =
            set.segments().iter().map(|s| s.base_pos()).collect();
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

    /// bn-1u6p reference: the bounded seek must equal the whole-stream
    /// coalesce filtered to `[from_version, ..)` and cut once `max_events`
    /// events at or past the cursor are covered. Written independently of
    /// [`ReplaySet::stream_replay_from`] so it is a real oracle.
    fn seek_oracle(
        all: &[StreamEntry],
        from_version: u64,
        max_events: usize,
    ) -> Vec<StreamEntry> {
        let mut out = Vec::new();
        let mut covered = 0usize;
        for e in all {
            // A batch is in the answer iff any of its events is at or past
            // the cursor — including one that STRADDLES it.
            if e.last_version() < from_version {
                continue;
            }
            if covered >= max_events {
                break;
            }
            out.push(*e);
            covered += (e.last_version() - from_version.max(e.first_version)
                + 1) as usize;
        }
        out
    }

    /// Exhaustive cursor x limit sweep over a three-segment stream: every
    /// cursor from before the start to past the head, at page sizes that cut
    /// inside a batch, at a batch boundary, and past the end.
    #[test]
    fn stream_replay_from_matches_filtered_whole_stream_replay() {
        let set = sample_set();
        let cache = BlockCache::disabled();
        let all = set.stream_replay(10, &cache).unwrap();
        // Stream 10 spans versions 0..=59 in six 10-frame batches across
        // segments 1, 2, 3.
        assert_eq!(all.len(), 6);
        assert_eq!(all.last().unwrap().last_version(), 59);

        for from in 0..=62u64 {
            for max in [0usize, 1, 5, 9, 10, 11, 20, 60, 1_000] {
                let got =
                    set.stream_replay_from(10, from, max, &cache).unwrap();
                assert_eq!(
                    got,
                    seek_oracle(&all, from, max),
                    "stream 10, from_version {from}, max_events {max}"
                );
            }
        }

        // A stream present in only one segment, and an absent stream.
        let all20 = set.stream_replay(20, &cache).unwrap();
        for from in 0..=25u64 {
            for max in [1usize, 10, 1_000] {
                assert_eq!(
                    set.stream_replay_from(20, from, max, &cache).unwrap(),
                    seek_oracle(&all20, from, max),
                    "stream 20, from_version {from}, max_events {max}"
                );
            }
        }
        assert!(
            set.stream_replay_from(999, 0, 1_000, &cache).unwrap().is_empty()
        );
    }

    /// The straddling batch is the one that silently drops records if the
    /// partition predicate is off by one: with 10-frame batches, a cursor at
    /// v25 sits inside the batch `[20, 29]`, which MUST be returned so the
    /// caller can trim frames 20..24 itself.
    #[test]
    fn stream_replay_from_returns_the_batch_straddling_the_cursor() {
        let set = sample_set();
        let cache = BlockCache::disabled();
        let got = set.stream_replay_from(10, 25, 1, &cache).unwrap();
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].first_version, 20);
        assert_eq!(got[0].last_version(), 29);
        // Exactly at a batch's first version: that batch, not the one before.
        let got = set.stream_replay_from(10, 30, 1, &cache).unwrap();
        assert_eq!(got[0].first_version, 30);
        // Exactly at a batch's last version: still that batch.
        let got = set.stream_replay_from(10, 29, 1, &cache).unwrap();
        assert_eq!(got[0].first_version, 20);
        // Past the head: nothing.
        assert!(
            set.stream_replay_from(10, 60, 1_000, &cache).unwrap().is_empty()
        );
    }

    /// Structural boundedness, not wall-clock: a cursor inside the last
    /// segment must DECODE only that segment's block. The earlier segments are
    /// skipped on their directory head alone, so an enabled cache records one
    /// miss and holds one block.
    #[test]
    fn stream_replay_from_decodes_only_the_segments_it_needs() {
        let set = sample_set();

        // Whole-stream replay: all three of stream 10's blocks.
        let all_cache = BlockCache::with_budget_bytes(1 << 20, 16);
        set.stream_replay(10, &all_cache).unwrap();
        assert_eq!(all_cache.misses(), 3, "baseline touches every segment");
        assert_eq!(all_cache.len(), 3);

        // Seek into segment 3's range (versions 50..=59), page of 5.
        let seek_cache = BlockCache::with_budget_bytes(1 << 20, 16);
        let got = set.stream_replay_from(10, 52, 5, &seek_cache).unwrap();
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].ptr.segment_id, 3);
        assert_eq!(
            seek_cache.misses(),
            1,
            "a seek past segments 1 and 2 must not decode their blocks"
        );
        assert_eq!(seek_cache.len(), 1);

        // A one-batch page from version 0 stops after the first segment's
        // first batch: one block decoded, not three.
        let page_cache = BlockCache::with_budget_bytes(1 << 20, 16);
        let got = set.stream_replay_from(10, 0, 10, &page_cache).unwrap();
        assert_eq!(got.len(), 1);
        assert_eq!(page_cache.misses(), 1);

        // Past the head: no block decoded at all.
        let none_cache = BlockCache::with_budget_bytes(1 << 20, 16);
        assert!(
            set.stream_replay_from(10, 1_000, 1_000, &none_cache)
                .unwrap()
                .is_empty()
        );
        assert_eq!(none_cache.misses(), 0);
        assert_eq!(none_cache.len(), 0);
    }

    /// The contract the engine's sealed/hot merge leans on: a result shorter
    /// than `max_events` is the COMPLETE remainder of the sealed stream, never
    /// a truncated one.
    #[test]
    fn stream_replay_from_short_result_is_complete() {
        let set = sample_set();
        let cache = BlockCache::disabled();
        let all = set.stream_replay(10, &cache).unwrap();
        for from in 0..=59u64 {
            for max in [1usize, 3, 10, 25, 100] {
                let got =
                    set.stream_replay_from(10, from, max, &cache).unwrap();
                let covered: usize = got
                    .iter()
                    .map(|e| {
                        (e.last_version() - from.max(e.first_version) + 1)
                            as usize
                    })
                    .sum();
                if covered < max {
                    let want = seek_oracle(&all, from, usize::MAX);
                    assert_eq!(
                        got, want,
                        "short result must be the whole remainder (from \
                         {from}, max {max})"
                    );
                }
            }
        }
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
            first_version:    0,
            frame_count:      10,
            first_global_pos: 0,
            ptr:              EventPtr { segment_id: 1, offset: 4096 },
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
        assert!(
            misses_after_first >= 1,
            "cold replay decodes at least one block"
        );
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

    // -- payload reads through the ReplaySet (D6 `.pcol`, bn-zge) -----------

    use crate::columnar::{emit_int, emit_str};
    use crate::sealed::payload::{
        BlockKind, NoDicts, PayloadSealOpts, SealedPayloadIndex,
        encode_payload_sidecar,
    };

    /// A shreddable msgpack map (columnar) at sequence `seq`.
    fn msgpack(seq: u64) -> Vec<u8> {
        let mut m = vec![0x82];
        emit_str(&mut m, b"seq");
        emit_int(&mut m, seq as i64);
        emit_str(&mut m, b"kind");
        emit_str(&mut m, b"demo");
        m
    }

    /// A build of a sealed segment carrying an attached `.pcol` whose blocks
    /// are a MIX of columnar and row-fallback (small block size over
    /// alternating msgpack/binary runs). `base` is the segment's A1 base;
    /// the segment holds `payloads` at global positions `base..base +
    /// payloads.len()`.
    fn segment_with_payloads(
        segment_id: u64,
        base: u64,
        payloads: &[Vec<u8>],
    ) -> SealedSegmentRef {
        use crate::sealed::segment::SealedSegmentIndex;
        use crate::sealed::segment::{
            SealBatch, SealInput, SealStream, encode_sidecar,
        };
        // A single stream/batch whose frame_count == event count fixes the
        // segment's event_count and base_pos for the global→local mapping.
        let input = SealInput {
            segment_id,
            base_pos: base,
            streams: vec![SealStream {
                stream_id: 1,
                batches:   vec![SealBatch {
                    first_version:    0,
                    frame_count:      payloads.len() as u32,
                    first_global_pos: base,
                    offset:           4096,
                }],
            }],
            payloads: None,
            event_type_ids: None,
        };
        let mut idx =
            SealedSegmentIndex::from_bytes(encode_sidecar(&input)).unwrap();
        let refs: Vec<&[u8]> = payloads.iter().map(Vec::as_slice).collect();
        let opts = PayloadSealOpts { block_events: 4, ..Default::default() };
        let bytes = encode_payload_sidecar(segment_id, &refs, &opts).unwrap();
        idx.attach_payload(SealedPayloadIndex::from_bytes(bytes).unwrap());
        std::sync::Arc::new(idx)
    }

    /// A mixed corpus for one segment: alternating short runs of msgpack
    /// (columnar) and binary (row fallback) so a 4-event block size yields
    /// both.
    fn mixed(seed: u64) -> Vec<Vec<u8>> {
        let mut v = Vec::new();
        for r in 0..3u64 {
            for i in 0..6 {
                v.push(msgpack(seed * 100 + r * 6 + i));
            }
            for i in 0..5u8 {
                v.push(vec![0xFF, 0x00, (seed as u8).wrapping_add(i), 0x99]);
            }
        }
        v
    }

    #[test]
    fn payload_scan_reassembles_mixed_segments_in_global_order() {
        // Three segments tiling the A1 axis, each with mixed columnar/row
        // blocks.
        let p1 = mixed(1);
        let p2 = mixed(2);
        let p3 = mixed(3);
        let b1 = 0u64;
        let b2 = b1 + p1.len() as u64;
        let b3 = b2 + p2.len() as u64;
        // Deliberately out of order to exercise the base_pos sort.
        let set = ReplaySet::from_segments([
            segment_with_payloads(3, b3, &p3),
            segment_with_payloads(1, b1, &p1),
            segment_with_payloads(2, b2, &p2),
        ]);
        assert!(set.all_have_payloads());

        // Each segment must actually be mixed (both block kinds present).
        for seg in set.segments() {
            let kinds: Vec<BlockKind> = seg
                .payload_index()
                .unwrap()
                .blocks()
                .iter()
                .map(|b| b.kind)
                .collect();
            assert!(
                kinds.contains(&BlockKind::Columnar),
                "expected columnar block"
            );
            assert!(
                kinds.contains(&BlockKind::Row),
                "expected row-fallback block"
            );
        }

        // The expected global-order concatenation.
        let mut expect: Vec<Vec<u8>> = Vec::new();
        expect.extend(p1.iter().cloned());
        expect.extend(p2.iter().cloned());
        expect.extend(p3.iter().cloned());

        // Full global payload replay is byte-exact across the mixed segments.
        let mut out = Vec::new();
        let mut offs = Vec::new();
        set.payload_scan(&NoDicts, &mut out, &mut offs).unwrap();
        assert_eq!(
            offs.len(),
            expect.len() + 1,
            "one boundary per event + tail"
        );
        for (i, w) in offs.windows(2).enumerate() {
            assert_eq!(
                &out[w[0] as usize..w[1] as usize],
                expect[i].as_slice(),
                "scan mismatch at {i}"
            );
        }

        // Point reads by GLOBAL position dispatch to the right segment + block.
        for gp in 0..expect.len() as u64 {
            assert_eq!(
                set.payload_at(gp, &NoDicts).unwrap().as_deref(),
                Some(expect[gp as usize].as_slice()),
                "payload_at mismatch at global {gp}",
            );
        }
        // Past the sealed history: a clean None, not a panic.
        assert_eq!(
            set.payload_at(expect.len() as u64, &NoDicts).unwrap(),
            None
        );
    }

    #[test]
    fn payload_scan_requires_every_segment_to_have_a_pcol() {
        // One payload-backed segment + one pointer-only segment ⇒ scan refuses.
        let p1 = mixed(1);
        let seg_payload = segment_with_payloads(1, 0, &p1);
        let seg_pointer = segment(2, p1.len() as u64, &[(1, 0, 1)]);
        let set = ReplaySet::from_segments([seg_payload, seg_pointer]);
        assert!(!set.all_have_payloads());
        let mut out = Vec::new();
        let mut offs = Vec::new();
        assert!(matches!(
            set.payload_scan(&NoDicts, &mut out, &mut offs),
            Err(crate::sealed::payload::PayloadError::Corrupt(_))
        ));
        // But payload_at still works for the covered range, and returns None
        // (fall back to raw) for the pointer-only segment's range.
        assert_eq!(
            set.payload_at(0, &NoDicts).unwrap().as_deref(),
            Some(p1[0].as_slice())
        );
        assert_eq!(set.payload_at(p1.len() as u64, &NoDicts).unwrap(), None);
    }
}
