//! The v4 recovery scanner (§20): reconstruct the committed prefix of a v4
//! segment from its durable bytes, dispatching by segment version and enforcing
//! the v4 protocol rules the physical [`decode_capsule`] does not.
//!
//! # What this layer adds over physical decode (§6, §20, §21)
//!
//! [`decode_capsule`] proves a capsule is byte-valid (magic/version/flags/caps,
//! marker echoes, the mandatory split-coverage CRC, exact control+event
//! tiling). This scanner then enforces, per capsule in on-disk order:
//!
//! - **`control_count + event_count >= 1`** (§6 — the A5 replacement, review
//!   C1): a capsule that carries neither a control nor an event is a hole.
//! - **`batch_id == expected_batch_id`** (§6, review C1: `batch_id` is
//!   recovery-significant in v4) — MANDATORY contiguity, `+1` per capsule.
//! - **`first_global_pos == expected_global_pos`** — advances by `event_count`
//!   only, so a control-only capsule never moves the global position (§21).
//! - **`segment_epoch == segment.epoch`** (A9).
//! - **control-only invariants** (§6): `event_count == 0` ⇒ `stream_id == 0 &&
//!   category_id == 0 && first_stream_version == 0` and the `CONTROL_ONLY` flag
//!   (the flag agreement itself is already checked physically).
//! - **prelude-first registry resolution** (§20, requirement 6): the capsule's
//!   controls are applied to a *speculative* clone of the committed registry
//!   view **before** the event region is validated, so a capsule may register
//!   an ID and use it in its own events. Only if every control applies AND
//!   every event's type id resolves in the speculative view is the capsule
//!   accepted and the view committed.
//! - **stop at first invalid capsule, no resync** (A10): the first rejection is
//!   terminal.
//!
//! # The [`RegistryView`] seam (review V6/S4)
//!
//! Registry *semantics* live in `mess-store`'s [`RegistryState`], which this
//! crate cannot depend on. The scanner is generic over a [`RegistryView`]: the
//! production impl (in `mess-store`) folds each decoded [`ControlRecord`] into
//! `RegistryState` and answers `event_type_resolves` from it — never weakening
//! `AlreadyRegistered`. For physical-only recovery (the torn matrix, the byte
//! model) [`NullRegistryView`] accepts every control and resolves every id: the
//! *physical* prelude-first ordering (controls applied, then events checked) is
//! still exercised, but no registry rule is enforced.

use std::io;
use std::path::Path;

use super::capsule::{
    CapsuleDecodeError, decode_controls, for_each_event_type_id,
    validate_capsule,
};
use super::control::ControlRecord;
use super::format::{FORMAT_VERSION_V4, LFLAG_CONTROL_ONLY};
use crate::format::{
    SEGMENT_HEADER_CRC_OFF, SEGMENT_HEADER_LEN, SEGMENT_MAGIC, SH_BASE_POS_OFF,
    SH_EPOCH_OFF, SH_FORMAT_VERSION_OFF, SH_MAGIC_OFF,
    SH_PREV_SEGMENT_EPOCH_OFF, SH_SEGMENT_ID_OFF,
};
use crate::runtime::{FileHandle, Fs, OpenOpts};

/// A speculative registry view the recovery scanner threads through a segment.
///
/// It is cloned once per capsule (the §20 `speculative_registry` model): the
/// clone receives the capsule's controls in ordinal order, and only if the
/// whole capsule is accepted does it replace the committed view. Registry
/// *semantics* are the impl's concern; the scanner only sequences the calls
/// (controls first, then event-id resolution — prelude-first).
pub trait RegistryView: Clone {
    /// Why a control could not be applied to this view (a registry-rule
    /// violation). Any error rejects the capsule and stops the scan.
    type Reject: std::fmt::Debug;

    /// Apply one control record to this speculative view, in ordinal order.
    fn apply(&mut self, control: &ControlRecord) -> Result<(), Self::Reject>;

    /// After all of a capsule's controls are applied, whether an event
    /// subframe's `event_type_id` resolves in this speculative view (§10/§11:
    /// an event may reference a type registered by an earlier control in the
    /// same capsule).
    fn event_type_resolves(&self, event_type_id: u32) -> bool;
}

/// A registry view that enforces nothing: every control applies, every id
/// resolves. Used for physical-only recovery (torn matrix, byte model), where
/// the property under test is the *physical* capsule format and prelude-first
/// sequencing, not registry semantics.
#[derive(Debug, Clone, Copy, Default)]
pub struct NullRegistryView;

impl RegistryView for NullRegistryView {
    type Reject = std::convert::Infallible;

    fn apply(&mut self, _control: &ControlRecord) -> Result<(), Self::Reject> {
        Ok(())
    }

    fn event_type_resolves(&self, _event_type_id: u32) -> bool { true }
}

/// The validated v4 `SegmentHeader` fields the scan seeds from (§3.2 family,
/// `format_version = 4`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SegmentHeaderV4 {
    pub segment_id:         u64,
    pub base_pos:           u64,
    pub epoch:              u64,
    pub prev_segment_epoch: u64,
}

/// One accepted capsule, in on-disk (commit) order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AcceptedCapsule {
    pub offset:               u64,
    pub total_len:            u64,
    pub batch_id:             u64,
    pub first_global_pos:     u64,
    pub event_count:          u32,
    pub control_count:        u32,
    pub segment_epoch:        u64,
    pub stream_id:            u64,
    pub category_id:          u64,
    pub first_stream_version: u64,
    /// Whether this is a control-only capsule (`event_count == 0`): it
    /// advances the [`CommitCursor`] but not the global position (§21).
    pub control_only:         bool,
}

/// The internal replication/recovery/checkpoint cursor (§21). Advances on
/// **every** accepted capsule, including control-only ones — unlike the
/// domain-facing global position, which advances only by `event_count`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CommitCursor {
    pub segment_id:      u64,
    pub segment_epoch:   u64,
    pub batch_id:        u64,
    pub byte_offset:     u64,
    pub global_position: u64,
}

/// Why the v4 scan stopped. Mirrors the v3 [`crate::scanner::ScanStop`]
/// taxonomy plus the v4-specific control/batch-id/registry reasons.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScanStopV4 {
    /// Clean tail: content consumed with no fault.
    EndOfSegment,
    /// The v4 `SegmentHeader` is torn/absent/wrong-magic/wrong-version or fails
    /// its CRC.
    BadSegmentHeader,
    /// A physical decode fault (torn header, bad magic/version/flags/length,
    /// bad marker, bad CRC, bad tiling, malformed/unknown control).
    Physical(CapsuleDecodeError),
    /// §6: `control_count + event_count == 0`.
    EmptyCapsule,
    /// A9: `segment_epoch` differs from the segment's current epoch.
    EpochMismatch,
    /// §6: `batch_id` is not the expected next id (MANDATORY contiguity).
    BatchIdGap { expected: u64, found: u64 },
    /// §6/§20: `first_global_pos` is not the expected next position.
    PositionGap { expected: u64, found: u64 },
    /// §6: a control-only capsule (`event_count == 0`) carries a nonzero
    /// `stream_id`/`category_id`/`first_stream_version`.
    ControlOnlyStreamNonzero,
    /// A control record was rejected by the registry view (a REG-rule
    /// violation), reported as a human string (the view's `Reject: Debug`).
    RegistryRejected { detail: String },
    /// §20/§11: an event subframe references an `event_type_id` that does not
    /// resolve in the speculative registry view.
    EventTypeUnresolved { event_type_id: u32 },
}

/// The outcome of recovering one v4 segment: the committed prefix plus resume
/// state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoveryV4 {
    pub header:          Option<SegmentHeaderV4>,
    pub accepted:        Vec<AcceptedCapsule>,
    /// First byte past the committed prefix — the safe truncation point and
    /// where new appends resume.
    pub safe_offset:     u64,
    /// The domain global position following the last accepted capsule
    /// (advances by `event_count` only).
    pub next_global_pos: u64,
    /// The per-segment `batch_id` the next append stamps (`+1` per capsule).
    pub next_batch_id:   u64,
    /// The internal commit cursor after the last accepted capsule (§21).
    pub commit_cursor:   CommitCursor,
    pub stop:            ScanStopV4,
}

impl RecoveryV4 {
    #[must_use]
    pub fn capsule_count(&self) -> usize { self.accepted.len() }
}

/// Physical-only recovery of a v4 segment image ([`NullRegistryView`]) — the
/// entry point the torn matrix and byte model use.
#[must_use]
pub fn scan_v4_image_physical(img: &[u8]) -> RecoveryV4 {
    let (rec, _view) = scan_v4_image(img, NullRegistryView);
    rec
}

/// Recover a v4 segment image, threading a [`RegistryView`] for the
/// prelude-first semantic layer. Returns the recovery plus the committed view
/// after the accepted prefix (so a caller can seed the live engine's registry).
///
/// Pure function of `img` and the seed `view` (property 4: deterministic,
/// idempotent).
pub fn scan_v4_image<V: RegistryView>(
    img: &[u8],
    seed_view: V,
) -> (RecoveryV4, V) {
    let Some(header) = decode_segment_header_v4(img) else {
        let cursor = CommitCursor {
            segment_id:      0,
            segment_epoch:   0,
            batch_id:        0,
            byte_offset:     0,
            global_position: 0,
        };
        return (
            RecoveryV4 {
                header:          None,
                accepted:        Vec::new(),
                safe_offset:     0,
                next_global_pos: 0,
                next_batch_id:   0,
                commit_cursor:   cursor,
                stop:            ScanStopV4::BadSegmentHeader,
            },
            seed_view,
        );
    };

    let mut view = seed_view;
    let mut accepted: Vec<AcceptedCapsule> = Vec::new();
    let mut off = SEGMENT_HEADER_LEN;
    let mut expected_batch_id = 0u64;
    let mut expected_global = header.base_pos;
    let mut cursor = CommitCursor {
        segment_id:      header.segment_id,
        segment_epoch:   header.epoch,
        batch_id:        0,
        byte_offset:     SEGMENT_HEADER_LEN as u64,
        global_position: header.base_pos,
    };

    let stop = loop {
        if off >= img.len() {
            break ScanStopV4::EndOfSegment;
        }

        // --- physical validate (§4), allocation-free fast path ---------
        let h = match validate_capsule(img, off) {
            Ok(h) => h,
            Err(e) => break ScanStopV4::Physical(e),
        };

        // --- protocol rules (§6, §20) ----------------------------------
        if h.control_count == 0 && h.event_count == 0 {
            break ScanStopV4::EmptyCapsule; // §6
        }
        if h.segment_epoch != header.epoch {
            break ScanStopV4::EpochMismatch; // A9
        }
        if h.batch_id != expected_batch_id {
            break ScanStopV4::BatchIdGap {
                expected: expected_batch_id,
                found:    h.batch_id,
            };
        }
        if h.first_global_pos != expected_global {
            break ScanStopV4::PositionGap {
                expected: expected_global,
                found:    h.first_global_pos,
            };
        }
        let control_only = h.event_count == 0;
        debug_assert_eq!(
            control_only,
            h.logical_flags & LFLAG_CONTROL_ONLY != 0,
            "decode_capsule already cross-checked the CONTROL_ONLY flag",
        );
        if control_only
            && (h.stream_id != 0
                || h.category_id != 0
                || h.first_stream_version != 0)
        {
            break ScanStopV4::ControlOnlyStreamNonzero; // §6
        }

        // --- prelude-first registry resolution (§20) -------------------
        // Speculative clone: controls apply to it in ordinal order, then the
        // events are resolved against it. Only a fully-valid capsule commits
        // the clone back — a rejection leaves the committed view untouched.
        //
        // Controls are materialized only when present (the common no-control
        // capsule allocates nothing here); events are resolved by an
        // allocation-free walk over the event region.
        let capsule = &img[off..off + h.total_len as usize];
        let mut speculative = view.clone();
        let mut registry_stop: Option<ScanStopV4> = None;
        if h.control_count > 0 {
            match decode_controls(capsule, &h) {
                Ok(controls) => {
                    for control in &controls {
                        if let Err(e) = speculative.apply(control) {
                            registry_stop =
                                Some(ScanStopV4::RegistryRejected {
                                    detail: format!("{e:?}"),
                                });
                            break;
                        }
                    }
                }
                // An unknown control (kind, version) or malformed payload —
                // the framing tiled, but the record cannot be interpreted.
                Err(e) => registry_stop = Some(ScanStopV4::Physical(e)),
            }
        }
        if registry_stop.is_none() {
            let all_resolve = for_each_event_type_id(capsule, &h, |etid| {
                speculative.event_type_resolves(etid)
            });
            if !all_resolve {
                // Re-find the first unresolved id for the typed stop.
                let mut bad = 0u32;
                for_each_event_type_id(capsule, &h, |etid| {
                    if speculative.event_type_resolves(etid) {
                        true
                    } else {
                        bad = etid;
                        false
                    }
                });
                registry_stop = Some(ScanStopV4::EventTypeUnresolved {
                    event_type_id: bad,
                });
            }
        }
        if let Some(stop) = registry_stop {
            break stop;
        }

        // --- accept ----------------------------------------------------
        view = speculative;
        accepted.push(AcceptedCapsule {
            offset: off as u64,
            total_len: h.total_len,
            batch_id: h.batch_id,
            first_global_pos: h.first_global_pos,
            event_count: h.event_count,
            control_count: h.control_count,
            segment_epoch: h.segment_epoch,
            stream_id: h.stream_id,
            category_id: h.category_id,
            first_stream_version: h.first_stream_version,
            control_only,
        });
        expected_batch_id += 1;
        expected_global += u64::from(h.event_count);
        off += h.total_len as usize;
        cursor = CommitCursor {
            segment_id:      header.segment_id,
            segment_epoch:   header.epoch,
            batch_id:        expected_batch_id, // next id
            byte_offset:     off as u64,
            global_position: expected_global,
        };
    };

    let recovery = RecoveryV4 {
        header: Some(header),
        accepted,
        safe_offset: off as u64,
        next_global_pos: expected_global,
        next_batch_id: expected_batch_id,
        commit_cursor: cursor,
        stop,
    };
    (recovery, view)
}

/// Recover a v4 segment file, reading it entirely through the [`Fs`] seam.
pub fn recover_v4_segment<F: Fs, V: RegistryView>(
    fs: &F,
    path: &Path,
    seed_view: V,
) -> io::Result<(RecoveryV4, V)> {
    let bytes = read_segment_through_fs(fs, path)?;
    Ok(scan_v4_image(&bytes, seed_view))
}

/// Recover a v4 segment file physically (no registry semantics).
pub fn recover_v4_segment_physical<F: Fs>(
    fs: &F,
    path: &Path,
) -> io::Result<RecoveryV4> {
    let bytes = read_segment_through_fs(fs, path)?;
    Ok(scan_v4_image_physical(&bytes))
}

fn read_segment_through_fs<F: Fs>(fs: &F, path: &Path) -> io::Result<Vec<u8>> {
    let file = fs.open(path, OpenOpts::read_only())?;
    let len = usize::try_from(file.len()?).unwrap_or(usize::MAX);
    let mut buf = vec![0u8; len];
    let mut filled = 0;
    while filled < len {
        let n = file.pread(filled as u64, &mut buf[filled..])?;
        if n == 0 {
            break;
        }
        filled += n;
    }
    buf.truncate(filled);
    Ok(buf)
}

/// Decode + validate the fixed 52-byte v4 `SegmentHeader` (the v3 family with
/// `format_version = 4`). `None` if too short / wrong magic / wrong version /
/// bad CRC.
fn decode_segment_header_v4(img: &[u8]) -> Option<SegmentHeaderV4> {
    if img.len() < SEGMENT_HEADER_LEN {
        return None;
    }
    if rd_u32(img, SH_MAGIC_OFF) != SEGMENT_MAGIC {
        return None;
    }
    if rd_u16(img, SH_FORMAT_VERSION_OFF) != FORMAT_VERSION_V4 {
        return None;
    }
    let want = rd_u32(img, SEGMENT_HEADER_CRC_OFF);
    if crc32c::crc32c(&img[..SEGMENT_HEADER_CRC_OFF]) != want {
        return None;
    }
    Some(SegmentHeaderV4 {
        segment_id:         rd_u64(img, SH_SEGMENT_ID_OFF),
        base_pos:           rd_u64(img, SH_BASE_POS_OFF),
        epoch:              rd_u64(img, SH_EPOCH_OFF),
        prev_segment_epoch: rd_u64(img, SH_PREV_SEGMENT_EPOCH_OFF),
    })
}

/// The on-disk `format_version` a segment header declares (§2, §22): `3` (v3),
/// `4` (v4), or the raw value for an unknown one. Used by open-time write-mode
/// version detection ([`segment_dir_write_format`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SegmentFormat {
    V3,
    V4,
    Unknown(u16),
    /// The header is torn/absent/wrong-magic/CRC-bad — carries no committed
    /// batches of any generation; not a version signal.
    NoValidHeader,
}

/// Peek a segment file's declared `format_version` through the [`Fs`] seam
/// (§22): read the 52-byte header, verify magic + CRC, and classify. A torn or
/// CRC-bad header is [`SegmentFormat::NoValidHeader`] (it names no version).
pub fn peek_segment_format<F: Fs>(
    fs: &F,
    path: &Path,
) -> io::Result<SegmentFormat> {
    let file = fs.open(path, OpenOpts::read_only())?;
    let mut buf = [0u8; SEGMENT_HEADER_LEN];
    let mut filled = 0;
    while filled < SEGMENT_HEADER_LEN {
        let n = file.pread(filled as u64, &mut buf[filled..])?;
        if n == 0 {
            break;
        }
        filled += n;
    }
    if filled < SEGMENT_HEADER_LEN {
        return Ok(SegmentFormat::NoValidHeader);
    }
    if rd_u32(&buf, SH_MAGIC_OFF) != SEGMENT_MAGIC {
        return Ok(SegmentFormat::NoValidHeader);
    }
    let want = rd_u32(&buf, SEGMENT_HEADER_CRC_OFF);
    if crc32c::crc32c(&buf[..SEGMENT_HEADER_CRC_OFF]) != want {
        return Ok(SegmentFormat::NoValidHeader);
    }
    Ok(match rd_u16(&buf, SH_FORMAT_VERSION_OFF) {
        3 => SegmentFormat::V3,
        4 => SegmentFormat::V4,
        other => SegmentFormat::Unknown(other),
    })
}

/// Whether any segment among `paths` is a v4 segment (§22). A **v3-only writer
/// MUST refuse write mode** on a directory for which this returns `true`: it
/// cannot interpret v4 capsules and must not append v3 batches into a store the
/// v4 boundary already crossed.
///
/// This function *is* that detection; the "old binary refuses" behaviour is by
/// construction for a future v3-only binary that calls it before opening a
/// writer (documented in the REPORT — a v3-only binary cannot be produced from
/// this workspace, which is v4-aware).
pub fn directory_contains_v4_segment<F: Fs>(
    fs: &F,
    paths: &[&Path],
) -> io::Result<bool> {
    for p in paths {
        if matches!(peek_segment_format(fs, p)?, SegmentFormat::V4) {
            return Ok(true);
        }
    }
    Ok(false)
}

#[inline]
fn rd_u16(d: &[u8], o: usize) -> u16 {
    u16::from_le_bytes(d[o..o + 2].try_into().unwrap())
}
#[inline]
fn rd_u32(d: &[u8], o: usize) -> u32 {
    u32::from_le_bytes(d[o..o + 4].try_into().unwrap())
}
#[inline]
fn rd_u64(d: &[u8], o: usize) -> u64 {
    u64::from_le_bytes(d[o..o + 8].try_into().unwrap())
}
