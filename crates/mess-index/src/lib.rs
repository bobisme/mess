//! mess v1: the pointer index (D5 in `notes/mess-research/12_convergence.md`).
//!
//! # What this crate is (bn-25d)
//!
//! The **active index**: the in-memory pointer index that lets a reader resolve
//! `(stream_id, version)` — or a global position — to the byte offset of the
//! batch that holds it, without scanning the log. Round-4 default (the 4.7x
//! composed-append win, `spikes/perf_append`): the index lives entirely in
//! memory while a segment is active and persists only at seal (D5), so the
//! append hot path never pays a per-event index write.
//!
//! Two disciplines make that safe:
//!
//! - **Single writer, concurrent readers.** The committer
//!   ([`mess_log::committer`]) is the sole writer; readers are concurrent. The
//!   structure is a sharded `RwLock` map + one published watermark — see
//!   [`active`] for why (vs left-right / arc-swap).
//! - **D7 commit-authority discipline.** Entries become visible only at or
//!   below the durable watermark; a reader never resolves an uncommitted
//!   pointer (the same rule [`mess_log::reader`] enforces on the raw tail).
//!
//! # Recovery rebuild (F6)
//!
//! Because the index is in memory, recovery [`rebuild`](rebuild::rebuild)s it
//! for **every segment lacking a sealed footer** — a rolled-but-unsealed
//! segment is normal under async sealing (F6). Rebuild replays each unsealed
//! segment's recovered accepted prefix through the *same*
//! [`ActiveIndex::apply_committed`] the committer uses (one code path). Sealed
//! segments are skipped: their consolidated index blocks are bn-20e (the
//! documented seam), so this rebuild covers the unsealed segments only.
//!
//! # Deferred
//!
//! Sealed-segment packed index blocks (bn-20e) and the registry storage
//! touchpoints (D3) are separate bones; this crate is the active index and its
//! rebuild.

pub mod active;
pub mod rebuild;

pub use active::{ActiveIndex, BatchEntry, EventPtr, GlobalEntry, IndexSnapshot, StreamEntry};
pub use rebuild::{RebuildReport, rebuild, rebuild_into};

// bn-1ku: exact-KV metadata tables (stream/snapshot heads, projection
// checkpoints, recent-dedupe window) on fjall. See `meta` for the rebuild
// story (I5) and per-table high-water / lag-detection contract.
pub mod meta;
