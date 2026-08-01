//! **v4 commit-capsule format** (Spike E, bn-9mw): atomic control+event commit
//! capsules — the format kill point for the Asterism design.
//!
//! v4 extends v3 to commit engine **control** records (registry assignments,
//! dedupe keys, projection checkpoints, snapshot installs) atomically alongside
//! domain events, and to commit **control-only** capsules that carry no domain
//! events and consume no global position. It preserves every v3 A1–A12
//! guarantee; the crash story is meant to stay as boring and absolute as v3's.
//!
//! The exhaustive + randomized gates passed
//! (`spikes/capsule_v4_prelude/REPORT.md`), but v4 was then **DECLINED** as a
//! product at its admission gate — see `docs/adr/0003-v4-admission.md`. This
//! module is therefore permanently non-normative: **v4 write is OFF and stays
//! off**, the production engine writes v3 ([`crate::writer::SegmentWriter`]),
//! and the v4 [`writer::CapsuleWriter`] is used only by tests and fuzz targets
//! — this crate's own, plus the spike's D4 retry test
//! (`crates/mess-store/tests/v4_d4_retry.rs`), the one cross-crate dependent,
//! which must be removed alongside this module if it is ever deleted. It is
//! retained deliberately, as the frozen artifact that keeps ADR 0003 §9's
//! reopening paths cheap — not as work in progress.
//!
//! Module map:
//! - [`format`] — byte constants/offsets, the §23 open decisions resolved.
//! - [`control`] — the control-record TLV codec (byte layer only; §7–§14).
//! - [`capsule`] — the capsule encoder + physical decoder, split-coverage CRC.
//! - [`recover`] — the recovery scanner: version dispatch, batch_id/position
//!   contiguity, control-only rules, prelude-first registry resolution via the
//!   [`recover::RegistryView`] seam, and the [`recover::CommitCursor`].
//! - [`writer`] — the opt-in v4 segment writer.

pub mod capsule;
pub mod control;
pub mod format;
pub mod recover;
pub mod writer;
