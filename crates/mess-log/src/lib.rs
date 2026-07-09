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

pub mod acceptance;
pub mod lock;
pub mod model;
pub mod runtime;
