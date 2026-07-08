//! mess v1: shared core types, traits, and semantics used across the
//! workspace — the replaceable seams from `notes/mess-research/01_*.md`
//! and `notes/mess-research/12_convergence.md`.
//!
//! Home for cross-crate concepts that other crates implement or consume:
//! - commit authority and recovery semantics (D1 in doc 12)
//! - batch framing (D2) as shared types
//! - expected-version / commit contracts used by the store and index
//!   crates alike
//!
//! Empty scaffold so far (bn-1i1); code lands as Phase 1 work begins.
