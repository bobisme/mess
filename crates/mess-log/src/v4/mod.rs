//! **v4 commit-capsule format** (Spike E, bn-9mw): atomic control+event commit
//! capsules — the format kill point for the Asterism design.
//!
//! v4 extends v3 to commit engine **control** records (registry assignments,
//! dedupe keys, projection checkpoints, snapshot installs) atomically alongside
//! domain events, and to commit **control-only** capsules that carry no domain
//! events and consume no global position. It preserves every v3 A1–A12
//! guarantee; the crash story is meant to stay as boring and absolute as v3's.
//!
//! Non-normative until the exhaustive + randomized gates pass
//! (`spikes/capsule_v4_prelude/REPORT.md`). **v4 write is OFF by default**: the
//! production engine keeps writing v3 ([`crate::writer::SegmentWriter`]); the
//! v4 [`writer::CapsuleWriter`] is opt-in, used by tests and (eventually) the
//! migration boundary.
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
