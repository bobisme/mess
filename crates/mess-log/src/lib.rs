//! mess-log: the append-only batch log (D1/D2 in
//! `notes/mess-research/12_convergence.md`).
//!
//! Present so far (bn-13s):
//! - [`acceptance`] — the pure commit-authority decision kernel the
//!   recovery scanner (bn-39n) MUST call for every accept/stop
//!   decision (A1/A5/A9/A10).
//! - [`model`] — the abstract commit/recovery protocol model checked
//!   exhaustively by stateright (`tests/stateright.rs`) and reusable as
//!   the DST in-memory reference (bn-3kn).
//! - [`runtime`] — the sim-capable Clock/Fs/Spawn abstraction (bn-z98)
//!   the segment writer + committer are generic over; real (std) and sim
//!   (virtual time + fault-injecting in-memory fs) impls.
//! - [`lock`] — `StoreLock`, the D9 single-writer-process OS lock file
//!   (bn-gke).
//! - [`watermark`] — the position-ordered durable watermark (D7): a
//!   watch-channel-like value the committer advances and D11 subscriptions
//!   will await (bn-11m).
//! - [`committer`] — the single committer thread: group commit, the
//!   `Durability::{Process, Os, Group}` modes, and watermark-gated acks
//!   (bn-11m, `docs/spec/03-durability.md`).
//! - [`scanner`] — the recovery scanner (bn-39n,
//!   `docs/spec/02-recovery.md`): full A-rule acceptance via the
//!   [`acceptance`] kernel, typed stop reasons, safe-truncation offset.
//! - [`recover_all`] — whole-log recovery (bn-2en, spec 02 §8): the
//!   orchestrator that recovers every segment (serially or on the R1
//!   per-segment parallel fan-out) and stitches them into one committed
//!   prefix, enforcing the §8.1 cross-segment A1/A9 chain; hosts the R2
//!   fast path (footer/manifest-seeded) and the §8.4 full/fast equivalence.
//! - [`manifest`] — the advisory manifest / segment catalog (bn-2en,
//!   spec 02 §8.3, R2): a small, rebuildable, never-authoritative cache of
//!   the sealed-segment footer trailers.
//! - [`fold_chain`] — the BLAKE3 fold-chain constructions (bn-1d0,
//!   `docs/spec/05-fold-certificates.md` §3, §6): genesis / frame_hash /
//!   chain_step, the append-side [`fold_chain::ChainHead`] that materializes the
//!   per-batch `crypto_chain`, and the read-side intra-batch recompute.
//! - [`footer_ext`] — the sealed-footer extension encode/decode (bn-1d0,
//!   spec 01 §3.3.2): the durable Tier-1 `StreamHeadTable` head anchor (G6) and
//!   the Path-C `SnapshotAnchorList` retention certificates.
//! - [`certificates`] — fold certificates and `load_verified` (bn-1d0,
//!   spec 05 §7): the batch-granular verification algorithm with typed outcomes,
//!   Paths A/B/C, tail replay, and the head-anchor truncation check.

pub mod acceptance;
pub mod certificates;
pub mod committer;
pub mod crc;
pub mod degraded;
pub mod encode;
pub mod fold_chain;
pub mod footer_ext;
pub mod format;
pub mod lock;
pub mod manifest;
pub mod metrics;
pub mod model;
pub mod reader;
pub mod recover_all;
pub mod runtime;
pub mod scanner;
pub mod sealer;
pub mod subscription;
pub mod watermark;
pub mod writer;

// bn-1gx: loom memory-ordering interleaving models for the cross-thread
// atomic protocols (watermark publish/wakeup, committer group handoff).
// Compiled only under `--cfg loom` during `cargo test`; see `just loom`.
#[cfg(all(loom, test))]
mod loom_tests;
