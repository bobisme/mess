//! The **sealed pointer index** (bn-20e, D5): the read-optimized, on-disk form
//! of a segment's slice of the [active index](crate::active), produced by a
//! background sealer off the append hot path.
//!
//! While a segment is active its pointers live in memory, tuned for cheap
//! append (`spikes/perf_append`: keeping the index write off the critical path
//! was the 4.7x lever). When the segment rolls, the seal pass rewrites that
//! slice into a compact, immutable sidecar tuned for reads — packed varint-delta
//! **pointer blocks** with an intra-block **skip table** — and hands readers
//! over to it without ever exposing a gap. This turns the write-optimized
//! representation into a read-optimized one *off the hot path* (D5).
//!
//! # Modules
//!
//! - [`ptr_block`] — the packed varint-delta pointer-block encoding and the
//!   intra-block skip table (this crate owns these bytes). Kani round-trip
//!   proofs live beside the codec.
//! - [`segment`] — the sidecar file format ([`SealInput`] →
//!   [`SealedSegmentIndex`]) and why it is a sidecar rather than a footer
//!   extension section (spec 01 §3.3.2 advisory-skip).
//! - [`store`] — the [`SealedStore`] and the sealed-or-active handoff: the
//!   gapless swap that lets the sealer evict a segment's active entries.
//! - [`driver`] — the [`SealDriver`] and [`BackgroundSealer`] thread that runs
//!   the seal off the append path.
//! - [`replay`] — the read paths *across many* sealed segments (bn-1hx):
//!   coalesced per-stream replay, the parallel global scan, and the always-on
//!   byte-identity gate ([`ReplaySet`]).
//! - [`block_cache`] — a bounded-bytes LRU of decoded pointer blocks wired into
//!   stream replay and bypassed for point reads ([`BlockCache`]).
//! - [`filter`] — the seal-time `BinaryFuse16` membership filter over a
//!   segment's `stream_id`s (bn-1i7): consulted before the pointer index so a
//!   segment definitely lacking a stream is skipped without a directory
//!   lookup. Persisted as a sibling `.filter` file, independently
//!   rebuildable (I5).
//! - [`retention`] — the retention-blocking rule (bn-2ug,
//!   `docs/spec/05-fold-certificates.md` §8.2): a segment MUST NOT be deleted
//!   while it holds a live snapshot's certification frame (`v`/`v+1`) unless a
//!   durable `SnapshotAnchor` (Path C) discharges it. Pure decision function
//!   plus the wiring seam for the (currently unbuilt) retention executor.
//!
//! # Staged passes (Phase 5 seam)
//!
//! The seal is structured as ordered passes — consolidate the index slice →
//! encode pointer blocks + skip tables → durably write the sidecar → finalize
//! the footer → swap. Phase-5 payload rewriting (per-category dictionaries,
//! ~128-event compressed blocks; `spikes/seal_pipeline`, `perf_compress`) slots
//! in as an additional pass between "consolidate" and "encode", writing its
//! columnar blocks alongside these pointer blocks; the pointer-block and
//! handoff machinery here is unchanged by it.

pub mod block_cache;
pub mod driver;
pub mod filter;
pub mod payload;
pub mod ptr_block;
pub mod replay;
pub mod retention;
pub mod segment;
pub mod store;

pub use block_cache::{BlockCache, CachedBlock};
pub use driver::{BackgroundSealer, FinalizeFn, SealDriver, SealError};
pub use filter::{FilterError, SegmentFilter};
pub use payload::{
    ARCHIVE_BLOCK_EVENTS, ARCHIVE_ZSTD_LEVEL, ArchivePolicy, BlockEntry, BlockKind, DictResolver,
    NoDicts, PayloadError, PayloadSealOpts, ReblockError, ReblockOutcome, SealedPayloadIndex,
    archive_reblock, encode_payload_sidecar, pcol_path, verify_reassembly,
};
pub use ptr_block::{BatchPtr, DecodeError, SKIP_K};
pub use replay::{ReplaySet, global_checksum, stream_checksum};
pub use retention::{
    BlockingReason, CertFrame, LiveSnapshotRef, RetentionDecision, SegmentStreamSpan,
    decide_segment, segment_retention_decision, spans_for_segment,
};
pub use segment::{
    SealBatch, SealInput, SealStream, SealedSegmentIndex, SealedSegmentRef, SidecarError,
    encode_sidecar, filter_path_for,
};
pub use store::{SealedStore, resolve};
