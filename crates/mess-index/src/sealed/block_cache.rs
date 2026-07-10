//! A bounded-**bytes** LRU of decoded pointer blocks, wired into sealed stream
//! replay and **bypassed for point reads** (bn-1hx).
//!
//! # What it caches, and why per-(segment, stream)
//!
//! The unit is one stream's pointer block in one sealed segment: the decoded
//! [`StreamEntry`] list that [`SealedSegmentIndex::stream_entries`] produces.
//! `perf_replay` measured a decompressed-block LRU at a **48% hit rate on
//! repeat replay** — a stream replayed twice (a projection rebuild that reruns,
//! a subscription that re-reads from a checkpoint) decodes each touched block
//! only once. The same spike found the cache **useless for random point
//! reads**: a point read seeks one version through the intra-block skip table
//! (O([`SKIP_K`](crate::sealed::ptr_block::SKIP_K)) varints straight off the
//! in-memory sidecar bytes) and never materializes the whole block, so caching
//! the decoded list would only evict genuinely hot replay blocks for no
//! benefit. Point reads therefore **do not touch this cache** — see
//! [`crate::sealed::segment::SealedSegmentIndex::resolve`], which is unchanged.
//!
//! # The Phase-5 decompress seam
//!
//! Pointer blocks are **uncompressed** until Phase 5, so today "load a block"
//! is `slice → decode_ptr_block`. Phase 5 inserts a decompress stage between
//! the slice and the decode (per-category dictionaries,
//! `spikes/perf_compress`). This cache stores the **post-decode** product —
//! exactly what a decompressed- block cache holds — so when the decompress
//! stage lands, the cached value and this API are unchanged; only the miss path
//! inside [`SealedSegmentIndex::stream_entries`] grows a decompress step.
//!
//! # Bounding by bytes, not count
//!
//! Streams vary from one batch to hundreds of thousands, so a fixed *item*
//! count would either waste memory on tiny streams or blow the budget on fat
//! ones. The cache is bounded by the **decoded byte weight** of its entries
//! (`quick_cache`'s weighter) so the operator sets one number — a memory budget
//! — and eviction is LRU-ish within it. The off-switch ([`disabled`]) stores
//! nothing and always misses, so a caller runs the identical cache-miss
//! fallthrough with no forked code path (the `mess-store` `StateCache`
//! pattern).
//!
//! [`disabled`]: BlockCache::disabled
//! [`SealedSegmentIndex::stream_entries`]: crate::sealed::segment::SealedSegmentIndex::stream_entries

use std::sync::Arc;

use quick_cache::Weighter;
use quick_cache::sync::Cache;

use crate::active::StreamEntry;
use crate::sealed::ptr_block::DecodeError;
use crate::sealed::segment::SealedSegmentIndex;

/// Cache key: one stream's block in one sealed segment.
type BlockKey = (u64, u64);

/// Cache value: the decoded batch list, shared so a hit is an `Arc` bump rather
/// than a deep clone of the (possibly large) list.
pub type CachedBlock = Arc<[StreamEntry]>;

/// Weighs a cached block by the bytes its decoded entries occupy, plus a fixed
/// per-entry overhead for the key and the `Arc` header — so the byte budget
/// tracks real resident memory, not just payload.
#[derive(Clone, Copy, Default)]
struct BlockWeighter;

/// Fixed per-cached-block overhead (key tuple + `Arc<[_]>` header + map slot),
/// so that even a one-batch block has a non-trivial, non-zero weight (a
/// zero-weight entry is never an eviction candidate in `quick_cache` and would
/// leak).
const BLOCK_OVERHEAD_BYTES: u64 = 64;

impl Weighter<BlockKey, CachedBlock> for BlockWeighter {
    #[inline]
    fn weight(&self, _key: &BlockKey, val: &CachedBlock) -> u64 {
        (val.len() * std::mem::size_of::<StreamEntry>()) as u64
            + BLOCK_OVERHEAD_BYTES
    }
}

/// A capacity-bounded (by decoded bytes) cache of decoded pointer blocks.
///
/// Cheap to clone: a clone shares the same underlying map (the `Arc` inside
/// `quick_cache`), so every reader that clones this handle sees the same warm
/// blocks. See the [module docs](self) for the design rationale.
#[derive(Clone)]
pub struct BlockCache {
    /// `None` is the off-switch: no map, every `get` misses.
    inner: Option<Arc<Cache<BlockKey, CachedBlock, BlockWeighter>>>,
}

impl std::fmt::Debug for BlockCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BlockCache")
            .field("enabled", &self.is_enabled())
            .field("entries", &self.len())
            .field("weight_bytes", &self.weight_bytes())
            .finish()
    }
}

impl Default for BlockCache {
    /// Disabled — keeps a reader that did not opt in allocation-free.
    fn default() -> Self { Self::disabled() }
}

impl BlockCache {
    /// The off-switch: stores nothing, always misses. A replay run against a
    /// disabled cache takes the identical decode-every-block path.
    #[must_use]
    pub fn disabled() -> Self { Self { inner: None } }

    /// An enabled cache holding at most `budget_bytes` of decoded block weight
    /// (clamped to at least one block's worth). `est_blocks` seeds the shard
    /// sizing — a rough guess at how many blocks fit, i.e.
    /// `budget_bytes / average_block_bytes`; it need not be exact.
    #[must_use]
    pub fn with_budget_bytes(budget_bytes: u64, est_blocks: usize) -> Self {
        let budget = budget_bytes.max(BLOCK_OVERHEAD_BYTES);
        let cache =
            Cache::with_weighter(est_blocks.max(1), budget, BlockWeighter);
        Self { inner: Some(Arc::new(cache)) }
    }

    /// Whether this cache actually stores entries.
    #[must_use]
    pub fn is_enabled(&self) -> bool { self.inner.is_some() }

    /// Number of live cached blocks (0 when disabled).
    #[must_use]
    pub fn len(&self) -> usize { self.inner.as_ref().map_or(0, |c| c.len()) }

    /// Whether the cache holds no blocks.
    #[must_use]
    pub fn is_empty(&self) -> bool { self.len() == 0 }

    /// Current resident weight in bytes (0 when disabled).
    #[must_use]
    pub fn weight_bytes(&self) -> u64 {
        self.inner.as_ref().map_or(0, |c| c.weight())
    }

    /// Cumulative cache hits since creation (0 when disabled).
    #[must_use]
    pub fn hits(&self) -> u64 { self.inner.as_ref().map_or(0, |c| c.hits()) }

    /// Cumulative cache misses since creation (0 when disabled).
    #[must_use]
    pub fn misses(&self) -> u64 {
        self.inner.as_ref().map_or(0, |c| c.misses())
    }

    /// Hit rate over all lookups so far in `[0, 1]`; `0.0` when disabled or
    /// when nothing has been looked up yet.
    #[must_use]
    pub fn hit_rate(&self) -> f64 {
        let (h, m) = (self.hits(), self.misses());
        let total = h + m;
        if total == 0 { 0.0 } else { h as f64 / total as f64 }
    }

    /// Fetch `stream_id`'s decoded block in `index`, decoding it on a miss and
    /// caching the result. On the off-switch this is exactly
    /// [`stream_entries`](SealedSegmentIndex::stream_entries) wrapped in an
    /// `Arc`, with no map
    /// touch — so enabling the cache never changes the decoded result, only
    /// whether the decode is skipped.
    ///
    /// A decode error is returned typed and is **not** cached (the sidecar is
    /// advisory; the log stays truth): the caller can fall back to rebuilding.
    /// An absent stream caches an empty block, so repeated negative lookups
    /// stay cheap.
    pub fn get_or_load(
        &self,
        index: &SealedSegmentIndex,
        stream_id: u64,
    ) -> Result<CachedBlock, DecodeError> {
        let key = (index.segment_id(), stream_id);
        if let Some(cache) = &self.inner
            && let Some(hit) = cache.get(&key)
        {
            return Ok(hit);
        }
        let decoded: CachedBlock = index.stream_entries(stream_id)?.into();
        if let Some(cache) = &self.inner {
            cache.insert(key, decoded.clone());
        }
        Ok(decoded)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::active::EventPtr;
    use crate::sealed::segment::{
        SealBatch, SealInput, SealStream, encode_sidecar,
    };

    fn seg(
        segment_id: u64,
        streams: &[(u64, usize)],
    ) -> Arc<SealedSegmentIndex> {
        let streams = streams
            .iter()
            .map(|&(sid, n)| SealStream {
                stream_id: sid,
                batches:   (0..n)
                    .map(|i| SealBatch {
                        first_version:    (i * 10) as u64,
                        frame_count:      10,
                        first_global_pos: (i * 10) as u64,
                        offset:           4096 + (i * 512) as u64,
                    })
                    .collect(),
            })
            .collect();
        let input =
            SealInput { segment_id, base_pos: 0, streams, payloads: None };
        Arc::new(
            SealedSegmentIndex::from_bytes(encode_sidecar(&input)).unwrap(),
        )
    }

    #[test]
    fn disabled_cache_matches_direct_decode() {
        let idx = seg(1, &[(10, 5), (20, 3)]);
        let cache = BlockCache::disabled();
        assert!(!cache.is_enabled());
        let via_cache = cache.get_or_load(&idx, 10).unwrap();
        let direct = idx.stream_entries(10).unwrap();
        assert_eq!(&via_cache[..], &direct[..]);
        // Off-switch stores nothing and never reports a hit.
        assert_eq!(cache.len(), 0);
        assert_eq!(cache.hits(), 0);
    }

    #[test]
    fn second_lookup_is_a_hit() {
        let idx = seg(1, &[(10, 5)]);
        let cache = BlockCache::with_budget_bytes(1 << 20, 16);
        let a = cache.get_or_load(&idx, 10).unwrap();
        let b = cache.get_or_load(&idx, 10).unwrap();
        assert_eq!(&a[..], &b[..]);
        assert!(Arc::ptr_eq(&a, &b), "hit returns the same Arc");
        assert_eq!(cache.hits(), 1);
        assert_eq!(cache.misses(), 1);
        assert!((cache.hit_rate() - 0.5).abs() < 1e-9);
    }

    #[test]
    fn absent_stream_caches_empty() {
        let idx = seg(1, &[(10, 2)]);
        let cache = BlockCache::with_budget_bytes(1 << 20, 16);
        assert!(cache.get_or_load(&idx, 999).unwrap().is_empty());
        assert!(cache.get_or_load(&idx, 999).unwrap().is_empty());
        assert_eq!(cache.hits(), 1);
    }

    #[test]
    fn byte_budget_evicts() {
        // Budget for ~2 five-batch blocks; loading many distinct streams must
        // keep resident weight under the budget (eviction runs).
        let per_block = 5 * std::mem::size_of::<StreamEntry>() as u64
            + BLOCK_OVERHEAD_BYTES;
        let budget = per_block * 2;
        let cache = BlockCache::with_budget_bytes(budget, 2);
        let idx = seg(1, &(0..64).map(|s| (s as u64, 5)).collect::<Vec<_>>());
        for s in 0..64u64 {
            let _ = cache.get_or_load(&idx, s).unwrap();
        }
        assert!(
            cache.weight_bytes() <= budget,
            "resident {} over budget {budget}",
            cache.weight_bytes()
        );
        assert!(cache.len() < 64, "eviction should have dropped entries");
    }

    #[test]
    fn keyed_by_segment_and_stream() {
        // Same stream id in two segments is two distinct cache entries.
        let a = seg(1, &[(10, 5)]);
        let b = seg(2, &[(10, 7)]);
        let cache = BlockCache::with_budget_bytes(1 << 20, 16);
        let ba = cache.get_or_load(&a, 10).unwrap();
        let bb = cache.get_or_load(&b, 10).unwrap();
        assert_eq!(ba.len(), 5);
        assert_eq!(bb.len(), 7);
        assert_eq!(ba[0].ptr, EventPtr { segment_id: 1, offset: 4096 });
        assert_eq!(bb[0].ptr, EventPtr { segment_id: 2, offset: 4096 });
        assert_eq!(cache.misses(), 2);
    }
}
