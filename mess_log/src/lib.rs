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

pub mod acceptance;
pub mod model;
