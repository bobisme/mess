//! Spike J (bn-2gu) — the composed decision.
//!
//! Library half of the harness: the carried-forward Spike-B `FlatEngine` (see
//! `flat.rs` for provenance) plus the quiet-guard, so both the benchmark
//! binary and the correctness tests can use them.

pub mod flat;
pub mod timing;

// `flat` already re-exports the engine's own vocabulary (`Version`,
// `RecordToAppend`, `Appended`, `AppendError`) — the flat engine implements
// the same contract, it does not fork it.
pub use flat::{
    AppendError, Appended, FlatConfig, FlatEngine, FlatError, OwnerExit,
    OwnerStats, RecordToAppend, Variant, Version,
};
