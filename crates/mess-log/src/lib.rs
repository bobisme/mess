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

pub mod acceptance;
pub mod committer;
pub mod crc;
pub mod encode;
pub mod format;
pub mod lock;
pub mod model;
pub mod reader;
pub mod runtime;
pub mod scanner;
pub mod sealer;
pub mod watermark;
pub mod writer;
