//! The recovery scanner (bn-39n): reconstruct the committed prefix (D1) of a
//! segment from its durable bytes, read **exclusively through the runtime
//! [`Fs`] seam** so every adversarial case runs on the sim fault fs.
//!
//! Spec: [`docs/spec/02-recovery.md`] is normative; the byte format is
//! [`docs/spec/01-log-format.md`]. Recovery *is* the commit-authority
//! definition (D1): it accepts only marker-terminated, CRC-valid, contiguous
//! (A1), current-epoch (A9) batches, and stops at the first hole, never
//! resynchronizing past it (A10).
//!
//! # Contract with the acceptance kernel
//!
//! This scanner owns the **byte layer** (§2.1 steps 1–6): header magic/version,
//! the A2 `total_len` cap, the A3 marker echoes, the **mandatory** A4/A12
//! full-batch CRC over the R4 split coverage ([`crate::crc::batch_crc`]), and
//! exact subframe tiling. Each candidate reduces to a
//! [`CandidateStatus`](crate::acceptance::CandidateStatus). It then routes
//! **every accept/stop decision** through the acceptance kernel
//! ([`AcceptState::step`]) — the A5/A9/A1/A10 rules are *not* re-implemented
//! here. That is the kernel's contract: the stateright model checks the kernel
//! exhaustively, and that guarantee only transfers to production because this
//! scanner calls it rather than forking its logic.
//!
//! # A12 discipline
//!
//! There is exactly one batch-decode path ([`decode_batch`]) and it **always**
//! computes the A4 CRC. No `cfg`, feature flag, "weak" mode, or fast path skips
//! it — a CRC-off path is forbidden (A12), and none exists in this file.
//!
//! # Deferred to Phase 4
//!
//! Correctness-first, single-threaded: the `recovery_scale` mmap scan and the
//! R1 per-segment parallelism are Phase 4 perf work. This scanner reads the
//! whole segment through [`FileHandle::pread`] into one buffer and walks it.
//!
//! [`docs/spec/02-recovery.md`]: ../../../../docs/spec/02-recovery.md
//! [`docs/spec/01-log-format.md`]: ../../../../docs/spec/01-log-format.md

use std::collections::BTreeMap;
use std::io;
use std::path::Path;

use crate::acceptance::{AcceptState, Candidate, CandidateStatus, Step, StopReason as KernelStop};
use crate::crc::batch_crc;
use crate::format::*;
use crate::runtime::{FileHandle, Fs, OpenOpts};

/// Why the scan stopped. A superset of the kernel's
/// [`StopReason`](crate::acceptance::StopReason): the kernel collapses every
/// byte fault into `ByteFault`, and this scanner keeps the byte-level detail
/// (§2.2 enumerates exactly these).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScanStop {
    /// The segment content was consumed with no fault: a clean tail.
    EndOfSegment,
    /// The `SegmentHeader` itself is torn/absent/wrong-magic or fails its
    /// `header_crc`. The segment holds no committed batches of this generation
    /// (§8.3 "trailer/header did not complete" posture).
    BadSegmentHeader,
    /// Fewer than `HEADER_LEN` bytes remain: a torn batch header (A11 — no
    /// trust from a partial header).
    TornHeader,
    /// `magic != HEADER_MAGIC` (garbage, torn header, or foreign format).
    BadMagic,
    /// `format_version != FORMAT_VERSION`.
    BadVersion,
    /// A `flags` bit outside `FLAGS_KNOWN_MASK` is set (§4.2.1).
    UnknownFlags,
    /// A2: `total_len` outside `[MIN_BATCH_LEN, MAX_BATCH_LEN]`.
    BadLength,
    /// `total_len` exceeds the bytes remaining (the batch is not all here).
    Incomplete,
    /// A3: marker magic / `total_len` echo / `batch_crc` echo mismatch.
    BadMarker,
    /// A4/A12: the full-batch CRC over the R4 split coverage does not verify.
    BadCrc,
    /// Subframes do not tile `[header(+chain), total_len - MARKER_LEN)` exactly.
    BadFrames,
    /// A5 (kernel): `frame_count == 0`.
    EmptyBatch,
    /// A9 (kernel): the batch's `segment_epoch` differs from the segment's
    /// current epoch — a stale prior generation in recycled space.
    EpochMismatch,
    /// A1 (kernel): `first_global_pos` is not the expected next position.
    PositionGap,
}

/// A batch recovery accepted, in on-disk (commit) order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AcceptedBatch {
    /// Byte offset of the batch within the segment.
    pub offset: u64,
    /// On-disk length (§4.6).
    pub total_len: u64,
    /// Per-segment batch id (D-FMT-5).
    pub batch_id: u64,
    /// A1 global position of this batch's first event.
    pub first_global_pos: u64,
    /// Number of events (subframes).
    pub frame_count: u32,
    /// A9 epoch stamped in the header (== the segment's current epoch).
    pub segment_epoch: u64,
    /// Batch-constant stream id (D-FMT-6).
    pub stream_id: u64,
    /// Batch-constant category id (D-FMT-6).
    pub category_id: u64,
    /// Stream version of this batch's first event.
    pub first_stream_version: u64,
    /// Whether a `crypto_chain` is present (flags bit 0).
    pub has_crypto_chain: bool,
}

impl AcceptedBatch {
    /// Stream version of this batch's **last** event: `first_stream_version +
    /// frame_count - 1` (a batch is stream-constant, D-FMT-6, and its events
    /// are consecutive versions).
    pub fn last_stream_version(&self) -> u64 {
        self.first_stream_version + u64::from(self.frame_count) - 1
    }
}

/// The validated `SegmentHeader` fields the scan seeded from (§3.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SegmentHeaderInfo {
    pub segment_id: u64,
    pub base_pos: u64,
    pub epoch: u64,
    pub prev_segment_epoch: u64,
}

/// A durable epoch anchor for the §5 cross-check: the epoch a trusted, not
/// rebuilt-from-the-log source (a sealed predecessor's trailer / the manifest)
/// says the active segment should carry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EpochAnchor {
    /// The epoch the anchor names as current for this segment.
    pub epoch: u64,
}

/// The outcome of recovering one segment: the committed prefix plus the
/// writer's resume state (§1).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Recovery {
    /// The segment header the scan seeded from, or `None` if it did not
    /// validate ([`ScanStop::BadSegmentHeader`]).
    pub header: Option<SegmentHeaderInfo>,
    /// Accepted batches in commit order.
    pub accepted: Vec<AcceptedBatch>,
    /// Per-stream head version: `stream_id -> last_stream_version` over the
    /// accepted prefix (a G6/index seed; advisory, D1).
    pub stream_heads: BTreeMap<u64, u64>,
    /// Byte offset of the first invalid/incomplete batch — simultaneously the
    /// safe truncation point and where new appends resume (§1).
    pub safe_offset: u64,
    /// `next_global_pos`: the A1 position that follows the last accepted batch.
    pub next_pos: u64,
    /// `next_batch_id`: the per-segment id the next append will stamp.
    pub next_batch_id: u64,
    /// The typed reason the scan stopped.
    pub stop: ScanStop,
}

impl Recovery {
    /// The number of accepted batches.
    pub fn batch_count(&self) -> usize {
        self.accepted.len()
    }
}

/// Recover a segment file, reading it entirely through the [`Fs`] seam and
/// seeding the A1/A9 scan from the segment's own (checksummed) `SegmentHeader`
/// (§4: the seed is authoritative because it is checksummed).
pub fn recover_segment<F: Fs>(fs: &F, path: &Path) -> io::Result<Recovery> {
    recover_segment_anchored(fs, path, None)
}

/// Recover a segment, additionally applying the §5 durable-epoch-anchor
/// cross-check. If `anchor` names an epoch **newer** than the header on disk,
/// the header did not survive (A11): the segment is treated as containing no
/// committed batches of the new generation ([`ScanStop::EpochMismatch`]),
/// rather than trusting an older, resurrected header.
pub fn recover_segment_anchored<F: Fs>(
    fs: &F,
    path: &Path,
    anchor: Option<EpochAnchor>,
) -> io::Result<Recovery> {
    let bytes = read_segment_through_fs(fs, path)?;
    Ok(scan_image(&bytes, anchor))
}

/// Read the whole segment through [`FileHandle::pread`] — the only I/O this
/// scanner performs, and the reason every adversarial fixture exercises the
/// sim fault fs.
fn read_segment_through_fs<F: Fs>(fs: &F, path: &Path) -> io::Result<Vec<u8>> {
    let file = fs.open(path, OpenOpts::read_only())?;
    let len = usize::try_from(file.len()?).unwrap_or(usize::MAX);
    let mut buf = vec![0u8; len];
    let mut filled = 0;
    while filled < len {
        let n = file.pread(filled as u64, &mut buf[filled..])?;
        if n == 0 {
            break; // short at EOF: the file is smaller than len() reported.
        }
        filled += n;
    }
    buf.truncate(filled);
    Ok(buf)
}

/// The pure scan over an in-memory segment image. Separated so it is trivially
/// deterministic and idempotent (property 4): a pure function of the durable
/// bytes. All callers reach it only after reading those bytes through the Fs.
fn scan_image(img: &[u8], anchor: Option<EpochAnchor>) -> Recovery {
    let Some(header) = decode_segment_header(img) else {
        return Recovery {
            header: None,
            accepted: Vec::new(),
            stream_heads: BTreeMap::new(),
            safe_offset: 0,
            next_pos: 0,
            next_batch_id: 0,
            stop: ScanStop::BadSegmentHeader,
        };
    };

    // §5: if a trusted anchor names a newer epoch than the durable header, the
    // header itself did not survive — do not trust an older, resurrected one.
    if let Some(a) = anchor
        && a.epoch > header.epoch
    {
        return Recovery {
            header: Some(header),
            accepted: Vec::new(),
            stream_heads: BTreeMap::new(),
            safe_offset: SEGMENT_HEADER_LEN as u64,
            next_pos: header.base_pos,
            next_batch_id: 0,
            stop: ScanStop::EpochMismatch,
        };
    }

    scan_batches(img, header)
}

/// The kernel-driven per-segment batch walk (§2.3). Every accept/stop decision
/// is made by [`AcceptState::step`]; this loop only decodes bytes and records
/// what the kernel accepts.
fn scan_batches(img: &[u8], header: SegmentHeaderInfo) -> Recovery {
    let mut state = AcceptState::new(header.epoch, header.base_pos);
    let mut accepted: Vec<AcceptedBatch> = Vec::new();
    let mut stream_heads: BTreeMap<u64, u64> = BTreeMap::new();
    let mut off = SEGMENT_HEADER_LEN;
    let mut next_batch_id = 0u64;

    let stop = loop {
        if off >= img.len() {
            break ScanStop::EndOfSegment;
        }

        // Byte layer: reduce this slot to a CandidateStatus, keeping the
        // detailed byte reason for our own typed stop.
        let (status, byte_reason, decoded) = match decode_batch(img, off) {
            Ok(d) => (
                CandidateStatus::ByteValid(Candidate {
                    epoch: d.segment_epoch,
                    batch_id: d.batch_id,
                    first_global_pos: d.first_global_pos,
                    frame_count: d.frame_count,
                }),
                None,
                Some(d),
            ),
            Err(reason) => (CandidateStatus::ByteInvalid, Some(reason), None),
        };

        // Protocol layer: the kernel decides. A10 is enforced inside it — once
        // it stops, we break and never look further (no resync).
        match state.step(status) {
            Step::Accept(_) => {
                let d = decoded.expect("a ByteValid candidate always carries its decode");
                let last_v = d.first_stream_version + u64::from(d.frame_count) - 1;
                stream_heads
                    .entry(d.stream_id)
                    .and_modify(|h| *h = (*h).max(last_v))
                    .or_insert(last_v);
                accepted.push(AcceptedBatch {
                    offset: off as u64,
                    total_len: d.total_len,
                    batch_id: d.batch_id,
                    first_global_pos: d.first_global_pos,
                    frame_count: d.frame_count,
                    segment_epoch: d.segment_epoch,
                    stream_id: d.stream_id,
                    category_id: d.category_id,
                    first_stream_version: d.first_stream_version,
                    has_crypto_chain: d.has_crypto_chain,
                });
                next_batch_id = d.batch_id + 1;
                off += d.total_len as usize;
            }
            Step::Stopped(kernel_stop) => break map_stop(kernel_stop, byte_reason),
        }
    };

    Recovery {
        header: Some(header),
        accepted,
        stream_heads,
        safe_offset: off as u64,
        next_pos: state.next_pos(),
        next_batch_id,
        stop,
    }
}

/// Map a kernel stop to the scanner's typed stop, recovering the byte-level
/// detail the kernel collapsed into `ByteFault`.
fn map_stop(kernel: KernelStop, byte_reason: Option<ScanStop>) -> ScanStop {
    match kernel {
        KernelStop::ByteFault => {
            byte_reason.expect("a kernel ByteFault always has a byte-level reason")
        }
        KernelStop::EmptyBatch => ScanStop::EmptyBatch,
        KernelStop::EpochMismatch => ScanStop::EpochMismatch,
        KernelStop::PositionDiscontinuity => ScanStop::PositionGap,
        // The kernel only returns `EndOfScan` from `accepted_prefix`'s fold,
        // never from `step`; our loop reaches end-of-segment structurally.
        KernelStop::EndOfScan => ScanStop::EndOfSegment,
    }
}

/// The protocol-relevant fields decoded from a byte-valid batch.
struct Decoded {
    total_len: u64,
    frame_count: u32,
    batch_id: u64,
    first_global_pos: u64,
    segment_epoch: u64,
    stream_id: u64,
    category_id: u64,
    first_stream_version: u64,
    has_crypto_chain: bool,
}

/// Decode + byte-validate one candidate at `off` (§2.1 steps 1–6). Returns the
/// decoded protocol fields on success, or the specific byte fault. The A4 CRC
/// (step 5) is **always** computed — there is no path through this function
/// that skips it (A12).
fn decode_batch(img: &[u8], off: usize) -> Result<Decoded, ScanStop> {
    let rem = img.len() - off;
    if rem < HEADER_LEN {
        return Err(ScanStop::TornHeader); // A11
    }

    if rd_u32(img, off + BH_MAGIC_OFF) != HEADER_MAGIC {
        return Err(ScanStop::BadMagic);
    }
    if rd_u16(img, off + BH_FORMAT_VERSION_OFF) != FORMAT_VERSION {
        return Err(ScanStop::BadVersion);
    }
    let flags = rd_u16(img, off + BH_FLAGS_OFF);
    if flags & !FLAGS_KNOWN_MASK != 0 {
        return Err(ScanStop::UnknownFlags); // §4.2.1
    }
    let has_crypto_chain = flags & FLAG_CRYPTO_CHAIN != 0;

    let frame_count = rd_u32(img, off + BH_FRAME_COUNT_OFF);
    let batch_id = rd_u64(img, off + BH_BATCH_ID_OFF);
    let total_len = rd_u64(img, off + BH_TOTAL_LEN_OFF);
    let first_global_pos = rd_u64(img, off + BH_FIRST_GLOBAL_POS_OFF);
    let segment_epoch = rd_u64(img, off + BH_SEGMENT_EPOCH_OFF);
    let stream_id = rd_u64(img, off + BH_STREAM_ID_OFF);
    let category_id = rd_u64(img, off + BH_CATEGORY_ID_OFF);
    let first_stream_version = rd_u64(img, off + BH_FIRST_STREAM_VERSION_OFF);
    let header_crc = rd_u32(img, off + HEADER_CRC_OFF);

    // A2: length cap first, so a corrupt `total_len` cannot drive a wild read
    // (A11: never trust the header just because its magic survived).
    if !(MIN_BATCH_LEN..=MAX_BATCH_LEN).contains(&total_len) {
        return Err(ScanStop::BadLength);
    }
    if total_len > rem as u64 {
        return Err(ScanStop::Incomplete);
    }
    let total_len_usize = total_len as usize;
    let batch = &img[off..off + total_len_usize];

    // A3: marker magic + length echo + crc echo.
    let m = total_len_usize - MARKER_LEN;
    if rd_u32(batch, m + CM_MAGIC_OFF) != MARKER_MAGIC
        || rd_u64(batch, m + CM_TOTAL_LEN_ECHO_OFF) != total_len
        || rd_u32(batch, m + CM_BATCH_CRC_ECHO_OFF) != header_crc
    {
        return Err(ScanStop::BadMarker);
    }

    // A4 / A12: the mandatory full-batch CRC over the R4 split coverage. This
    // is the ONLY check that catches a marker that persisted before its frames
    // did — the defining reordering hazard. No path skips it.
    if batch_crc(batch) != header_crc {
        return Err(ScanStop::BadCrc);
    }

    // Structural: the `frame_count` subframes must tile exactly (necessary,
    // not sufficient — the CRC is what actually guards payload bytes, A12).
    if !subframes_tile(batch, frame_count, has_crypto_chain) {
        return Err(ScanStop::BadFrames);
    }

    Ok(Decoded {
        total_len,
        frame_count,
        batch_id,
        first_global_pos,
        segment_epoch,
        stream_id,
        category_id,
        first_stream_version,
        has_crypto_chain,
    })
}

/// Do the `frame_count` subframes exactly tile `[header(+chain), total_len -
/// MARKER_LEN)`? Each consumes `SUBFRAME_HDR_LEN + compressed_len` bytes and
/// the last must end precisely at the marker (§2.1 step 6).
fn subframes_tile(batch: &[u8], frame_count: u32, has_crypto_chain: bool) -> bool {
    let frames_end = batch.len() - MARKER_LEN;
    let mut p = HEADER_LEN + if has_crypto_chain { CHAIN_LEN } else { 0 };
    if p > frames_end {
        return false; // the crypto chain does not even fit before the marker.
    }
    for _ in 0..frame_count {
        if p + SUBFRAME_HDR_LEN > frames_end {
            return false;
        }
        let compressed_len = rd_u32(batch, p + SF_COMPRESSED_LEN_OFF) as usize;
        // `p + SUBFRAME_HDR_LEN <= frames_end` was just checked; adding a
        // (possibly huge, corrupt) compressed_len can only push `p` past the
        // end, which the bound below rejects. No overflow on 64-bit usize.
        let next = p + SUBFRAME_HDR_LEN + compressed_len;
        if next > frames_end {
            return false;
        }
        p = next;
    }
    p == frames_end
}

/// Decode + validate the fixed 52-byte `SegmentHeader` (§3.2). Returns `None`
/// if the image is too short, has the wrong magic/version, or fails its
/// `header_crc` over `[0, 48)`.
fn decode_segment_header(img: &[u8]) -> Option<SegmentHeaderInfo> {
    if img.len() < SEGMENT_HEADER_LEN {
        return None;
    }
    if rd_u32(img, SH_MAGIC_OFF) != SEGMENT_MAGIC {
        return None;
    }
    if rd_u16(img, SH_FORMAT_VERSION_OFF) != FORMAT_VERSION {
        return None;
    }
    let want = rd_u32(img, SEGMENT_HEADER_CRC_OFF);
    if crc32c::crc32c(&img[..SEGMENT_HEADER_CRC_OFF]) != want {
        return None;
    }
    Some(SegmentHeaderInfo {
        segment_id: rd_u64(img, SH_SEGMENT_ID_OFF),
        base_pos: rd_u64(img, SH_BASE_POS_OFF),
        epoch: rd_u64(img, SH_EPOCH_OFF),
        prev_segment_epoch: rd_u64(img, SH_PREV_SEGMENT_EPOCH_OFF),
    })
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
