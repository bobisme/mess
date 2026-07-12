//! Spike D (bn-23x) — algebraic segment effects + Merkle-page kernel
//! checkpoints (design.md §9/§10, research/02, research/05 §8).
//!
//! FAST-RECOVERY KILL POINT. This crate prototypes the algebra, not engine
//! integration:
//!
//! * a logical capsule model — v3-shaped user batches plus SYNTHETIC control
//!   records standing in for v4 (`StreamRegistered`, `SnapshotInstalled`,
//!   `ProjectionCheckpoint`, `DedupeKey`, `AllocatorSet`) so the full state
//!   product `K = H × S × P × R × D × A` is exercised;
//! * two independent state implementations: a deliberately boring
//!   `BTreeMap`/`VecDeque` oracle ([`oracle`]) and a dense-array kernel
//!   ([`kernel`]) — both fold capsules, both produce the same canonical
//!   digest spec ([`codec::DigestBuilder`]);
//! * [`effect::SegmentEffect`] with path-composed head transitions
//!   (first-prior + last-head boundaries), right-biased latest-value maps,
//!   pointwise-max frontiers, conflict-union registry deltas, and
//!   position-window dedupe epochs; ordered composition `E1 ⊗ E2` returns
//!   `⊥` on any continuity/conflict violation;
//! * Merkle-page checkpoints: fixed 4096-cell logical pages, BLAKE3
//!   content-addressed blobs, a manifest with commit cursor + page table +
//!   log-prefix anchor + CRC, alternating `current.a`/`current.b` advisory
//!   pointers, temp+fsync+rename install (design §10.3), anchor validation
//!   (§10.4) — over a crash-modeling in-memory dir for fault injection and
//!   a real dir for perf;
//! * the seven recovery variants of research/05 §8 ([`recover`]), all of
//!   which must produce identical canonical state digests.

pub mod builder;
pub mod checkpoint;
pub mod codec;
pub mod effect;
pub mod hist;
pub mod kernel;
pub mod model;
pub mod oracle;
pub mod recover;
pub mod timing;

/// Cells per logical checkpoint page (design §10.2's example figure).
pub const PAGE_CELLS: u64 = 4096;
