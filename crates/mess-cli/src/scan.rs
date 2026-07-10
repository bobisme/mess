//! Shared segment analysis: recover a `.log` segment through the acceptance
//! kernel scanner ([`mess_log::scanner`]) and read its sealed trailer, so
//! `doctor`, `inspect`, and `verify` all see the same per-segment facts.

use std::path::Path;

use mess_log::runtime::real::RealFs;
use mess_log::scanner::{Recovery, ScanStop, recover_segment};
use mess_log::sealer::{SegmentCatalogEntry, read_trailer};

/// Everything the CLI learns from one segment on disk.
#[derive(Debug)]
pub struct SegmentScan {
    pub segment_id: u64,
    /// The recovery scan result (typed accept/stop over the acceptance kernel).
    pub recovery: Recovery,
    /// The decoded sealed trailer, if the segment is sealed (§3.3.1); `None`
    /// means the segment is unsealed (the active/rolled head) or its trailer
    /// did not validate.
    pub trailer: Option<SegmentCatalogEntry>,
    /// The raw byte length of the file.
    pub file_len: u64,
}

impl SegmentScan {
    /// The number of accepted (committed) batches.
    #[must_use]
    pub fn batch_count(&self) -> usize {
        self.recovery.accepted.len()
    }

    /// Σ frame_count over the accepted prefix.
    #[must_use]
    pub fn event_count(&self) -> u64 {
        self.recovery.accepted.iter().map(|b| u64::from(b.frame_count)).sum()
    }

    /// Whether the segment carries a valid sealed trailer.
    #[must_use]
    pub fn is_sealed(&self) -> bool {
        self.trailer.is_some()
    }

    /// The segment's own header epoch, if the header validated.
    #[must_use]
    pub fn epoch(&self) -> Option<u64> {
        self.recovery.header.as_ref().map(|h| h.epoch)
    }

    /// The segment's own header base position, if the header validated.
    #[must_use]
    pub fn base_pos(&self) -> Option<u64> {
        self.recovery.header.as_ref().map(|h| h.base_pos)
    }
}

/// Recover and characterise one segment file.
pub fn scan_segment(segment_id: u64, log_path: &Path) -> std::io::Result<SegmentScan> {
    let fs = RealFs;
    let recovery = recover_segment(&fs, log_path)?;
    let trailer = read_trailer(&fs, log_path)?;
    let file_len = std::fs::metadata(log_path).map(|m| m.len()).unwrap_or(0);
    Ok(SegmentScan { segment_id, recovery, trailer, file_len })
}

/// The stable kebab-case finding kind for a non-clean [`ScanStop`]. Used by
/// `verify`/`doctor` so a corruption class maps to a stable machine token.
#[must_use]
pub fn scan_stop_kind(stop: ScanStop) -> &'static str {
    match stop {
        ScanStop::EndOfSegment => "clean",
        ScanStop::BadSegmentHeader => "segment-header-corrupt",
        ScanStop::TornHeader => "batch-header-torn",
        ScanStop::BadMagic => "batch-bad-magic",
        ScanStop::BadVersion => "batch-bad-version",
        ScanStop::UnknownFlags => "batch-unknown-flags",
        ScanStop::BadLength => "batch-bad-length",
        ScanStop::Incomplete => "batch-incomplete",
        ScanStop::BadMarker => "batch-bad-marker",
        ScanStop::BadCrc => "batch-bad-crc",
        ScanStop::BadFrames => "batch-bad-frames",
        ScanStop::EmptyBatch => "batch-empty",
        ScanStop::EpochMismatch => "batch-epoch-mismatch",
        ScanStop::PositionGap => "batch-position-gap",
    }
}
