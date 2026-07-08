//! mess v1: metadata tables and the pointer index (D3/D5 in
//! `notes/mess-research/12_convergence.md`).
//!
//! Scope:
//! - the active/sealed index lifecycle — cheap per-event entries while
//!   active, consolidated into packed pointer blocks at seal (D5)
//! - registry storage touchpoints for the event-sourced registry (D3):
//!   interned `stream_id` / `category_id` / `event_type_id` lookups
//!
//! Empty scaffold so far (bn-1i1); code lands as Phase 1 work begins.
