//! mess v1: `EventStore` API surface — engine composition over
//! `mess-log` and `mess-index` (`store.command()` is the north star; see
//! `notes/mess-research/12_convergence.md`).
//!
//! Scope:
//! - durability modes and their consequences (D7): `Process` / `Os` /
//!   `Group { max_delay, max_bytes }`
//! - process model — one writer per store directory (D9)
//! - subscription handoff from catch-up to live (D11)
//!
//! Empty scaffold so far (bn-1i1); code lands as Phase 1 work begins.
