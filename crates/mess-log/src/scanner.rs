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

use crate::acceptance::{
    AcceptState, Candidate, CandidateStatus, Step, StopReason as KernelStop,
};
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
    /// Subframes do not tile `[header(+chain), total_len - MARKER_LEN)`
    /// exactly.
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
    pub offset:               u64,
    /// On-disk length (§4.6).
    pub total_len:            u64,
    /// Per-segment batch id (D-FMT-5).
    pub batch_id:             u64,
    /// A1 global position of this batch's first event.
    pub first_global_pos:     u64,
    /// Number of events (subframes).
    pub frame_count:          u32,
    /// A9 epoch stamped in the header (== the segment's current epoch).
    pub segment_epoch:        u64,
    /// Batch-constant stream id (D-FMT-6).
    pub stream_id:            u64,
    /// Batch-constant category id (D-FMT-6).
    pub category_id:          u64,
    /// Stream version of this batch's first event.
    pub first_stream_version: u64,
    /// Whether a `crypto_chain` is present (flags bit 0).
    pub has_crypto_chain:     bool,
}

impl AcceptedBatch {
    /// Stream version of this batch's **last** event: `first_stream_version +
    /// frame_count - 1` (a batch is stream-constant, D-FMT-6, and its events
    /// are consecutive versions).
    pub fn last_stream_version(&self) -> u64 {
        self.first_stream_version + u64::from(self.frame_count) - 1
    }

    /// The read-side materialization seam (bn-20b): decode this recovered
    /// batch's `frame_count` events back to `(event_type_id, payload)` from the
    /// segment image the batch was recovered from.
    ///
    /// This is the ONLY public path from a recovered batch to its payload
    /// bytes. The recovery scanner is a byte-*validator* — it proves the batch
    /// is a marker-terminated, CRC-valid, exactly-tiling committed batch and
    /// records its position/identity ([`AcceptedBatch`]) — but it does not
    /// return payloads, because the log tier is pointer-only (§module docs of
    /// `mess-store`'s engine). A materializing reader (the composed engine's
    /// record-book rehydration on reopen) needs the bytes back; it recovers a
    /// segment, then calls `frames(&image)` on each [`AcceptedBatch`] to obtain
    /// per-event `(event_type_id, schema_version, codec_id, payload)`.
    ///
    /// `segment_image` MUST be the same durable image the batch was recovered
    /// from (the exact bytes [`recover_segment_with_image`] returns alongside
    /// the [`Recovery`]); `self.offset .. self.offset + self.total_len` indexes
    /// this batch within it. Because the batch already byte-validated (its
    /// subframes tile exactly, [`decode_batch`] proved it) **against the
    /// correct image**, the walk over that image is allocation-free — each
    /// yielded [`RecoveredFrame`] borrows its payload straight out of
    /// `segment_image`.
    ///
    /// # Misuse resistance (bn-221)
    ///
    /// A caller can pass the wrong image (a different segment's bytes, a
    /// truncated/reallocated buffer, or anything else that is not the exact
    /// image this batch was recovered from). This method never panics on
    /// that input: it re-validates the batch's byte range against
    /// `segment_image` and re-runs the same subframe-tiling check
    /// [`decode_batch`] used, returning [`WrongSegmentImage`] instead of
    /// slicing out of bounds or handing back an iterator that could panic on
    /// `next()`. On the correct image this re-validation always succeeds (it
    /// is the same image the original tiling proof was over) and costs one
    /// extra O(`frame_count`) pass with no allocation.
    pub fn frames<'a>(
        &self,
        segment_image: &'a [u8],
    ) -> Result<Frames<'a>, WrongSegmentImage> {
        let wrong_image = || WrongSegmentImage {
            batch_offset:    self.offset,
            batch_total_len: self.total_len,
            image_len:       segment_image.len(),
        };
        let end =
            self.offset.checked_add(self.total_len).ok_or_else(wrong_image)?;
        if end > segment_image.len() as u64 {
            return Err(wrong_image());
        }
        // Sound: `self.offset <= end <= segment_image.len()`, and
        // `segment_image.len()` is itself a valid `usize`, so neither cast
        // below can wrap or truncate.
        let start = self.offset as usize;
        let end = end as usize;
        let batch = &segment_image[start..end];
        if !subframes_tile(batch, self.frame_count, self.has_crypto_chain) {
            return Err(wrong_image());
        }
        let pos =
            HEADER_LEN + if self.has_crypto_chain { CHAIN_LEN } else { 0 };
        Ok(Frames { batch, pos, remaining: self.frame_count })
    }
}

/// Why [`AcceptedBatch::frames`] could not materialize this batch's events
/// from a given `segment_image` (bn-221): the image is not the one the batch
/// was recovered from — either too short to contain the batch's byte range,
/// or a same-or-different-length image whose bytes at this offset do not
/// byte-validate as this batch's subframe tiling. This is a **misuse**
/// signal, not a corruption finding: on the correct image (the one actually
/// returned alongside this batch's [`Recovery`]) `frames` cannot fail, since
/// the tiling this re-checks is exactly what [`decode_batch`] already proved
/// during recovery.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WrongSegmentImage {
    /// The batch's byte offset within its segment.
    pub batch_offset:    u64,
    /// The batch's on-disk length (§4.6).
    pub batch_total_len: u64,
    /// The length of the `segment_image` actually supplied.
    pub image_len:       usize,
}

impl std::fmt::Display for WrongSegmentImage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "AcceptedBatch::frames: segment_image (len {}) is not the image \
             this batch (offset {}, total_len {}) was recovered from",
            self.image_len, self.batch_offset, self.batch_total_len
        )
    }
}

impl std::error::Error for WrongSegmentImage {}

/// One recovered event's read-side materialization (bn-20b): the interned
/// event-type id (`04-registry.md`) and the on-disk payload bytes, borrowed
/// from the segment image the containing batch was recovered from. Yielded by
/// [`AcceptedBatch::frames`] in on-disk (stream) order.
///
/// The batch-level identity a materializing caller also needs — `stream_id`,
/// `first_stream_version`, `first_global_pos`, `segment_epoch` — lives on the
/// [`AcceptedBatch`] itself; a frame's stream version is
/// `first_stream_version + frame index`, its global position
/// `first_global_pos + frame index`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecoveredFrame<'a> {
    /// Interned event type id (§4.3). Resolves to the message-type name
    /// through the caller's own id→name mapping (the log stores only the
    /// id).
    pub event_type_id:  u32,
    /// Schema version of the event type at write time (§4.3).
    pub schema_version: u16,
    /// Interned payload codec id (`0` = bootstrap, verbatim payload) (§4.3).
    pub codec_id:       u16,
    /// The on-disk payload bytes — exactly `compressed_len` bytes, borrowed
    /// from the segment image. For a `codec_id`/`compression_id == 0` frame
    /// (the common uncompressed shape) these are the verbatim event bytes.
    pub payload:        &'a [u8],
}

/// Iterator over an [`AcceptedBatch`]'s recovered event frames (bn-20b). Walks
/// the `EventSubframe`s (§4.3) of a byte-validated batch, so it never fails and
/// never allocates. See [`AcceptedBatch::frames`].
#[derive(Debug)]
pub struct Frames<'a> {
    /// The whole batch slice `[header(+chain) .. marker]`.
    batch:     &'a [u8],
    /// Offset of the next subframe within `batch`.
    pos:       usize,
    /// Frames not yet yielded.
    remaining: u32,
}

impl<'a> Iterator for Frames<'a> {
    type Item = RecoveredFrame<'a>;

    fn next(&mut self) -> Option<RecoveredFrame<'a>> {
        if self.remaining == 0 {
            return None;
        }
        let p = self.pos;
        let event_type_id = rd_u32(self.batch, p + SF_EVENT_TYPE_ID_OFF);
        let schema_version = rd_u16(self.batch, p + SF_SCHEMA_VERSION_OFF);
        let codec_id = rd_u16(self.batch, p + SF_CODEC_ID_OFF);
        let compressed_len =
            rd_u32(self.batch, p + SF_COMPRESSED_LEN_OFF) as usize;
        let payload = &self.batch
            [p + SUBFRAME_HDR_LEN..p + SUBFRAME_HDR_LEN + compressed_len];
        self.pos = p + SUBFRAME_HDR_LEN + compressed_len;
        self.remaining -= 1;
        Some(RecoveredFrame {
            event_type_id,
            schema_version,
            codec_id,
            payload,
        })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining as usize, Some(self.remaining as usize))
    }
}

impl ExactSizeIterator for Frames<'_> {}

/// The validated `SegmentHeader` fields the scan seeded from (§3.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SegmentHeaderInfo {
    pub segment_id:         u64,
    pub base_pos:           u64,
    pub epoch:              u64,
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
    pub header:        Option<SegmentHeaderInfo>,
    /// Accepted batches in commit order.
    pub accepted:      Vec<AcceptedBatch>,
    /// Per-stream head version: `stream_id -> last_stream_version` over the
    /// accepted prefix (a G6/index seed; advisory, D1).
    pub stream_heads:  BTreeMap<u64, u64>,
    /// Byte offset of the first invalid/incomplete batch — simultaneously the
    /// safe truncation point and where new appends resume (§1).
    pub safe_offset:   u64,
    /// `next_global_pos`: the A1 position that follows the last accepted
    /// batch.
    pub next_pos:      u64,
    /// `next_batch_id`: the per-segment id the next append will stamp.
    pub next_batch_id: u64,
    /// The typed reason the scan stopped.
    pub stop:          ScanStop,
}

impl Recovery {
    /// The number of accepted batches.
    pub fn batch_count(&self) -> usize { self.accepted.len() }
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

/// Recover a segment **and** return the durable image it was recovered from,
/// so a materializing caller can decode per-frame payloads via
/// [`AcceptedBatch::frames`] (bn-20b — the read-side materialization seam).
///
/// Reads the whole segment through the [`Fs`] seam exactly once (the same read
/// [`recover_segment`] does) and hands the buffer back alongside the
/// [`Recovery`]: `recovery.accepted[i].frames(&image)` then yields batch *i*'s
/// events. The engine's record-book rehydration on reopen is the sole caller;
/// the pure byte scan ([`scan_image`]) and its recovered offsets are unchanged.
pub fn recover_segment_with_image<F: Fs>(
    fs: &F,
    path: &Path,
) -> io::Result<(Recovery, Vec<u8>)> {
    let bytes = read_segment_through_fs(fs, path)?;
    let recovery = scan_image(&bytes, None);
    Ok((recovery, bytes))
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
///
/// `pub` (bn-gux, widened from private): this is the cleanest byte-slice scan
/// entry point for `fuzz/fuzz_targets/fuzz_scanner.rs` — arbitrary bytes in,
/// typed `Recovery`/`ScanStop` out, no `Fs`/sim-disk plumbing needed to
/// exercise the byte layer (§2.1 steps 1–6) plus the acceptance kernel. No
/// behavior changed; every existing caller already went through this
/// function via [`recover_segment_anchored`].
pub fn scan_image(img: &[u8], anchor: Option<EpochAnchor>) -> Recovery {
    let Some(header) = decode_segment_header(img) else {
        return Recovery {
            header:        None,
            accepted:      Vec::new(),
            stream_heads:  BTreeMap::new(),
            safe_offset:   0,
            next_pos:      0,
            next_batch_id: 0,
            stop:          ScanStop::BadSegmentHeader,
        };
    };

    // §5: if a trusted anchor names a newer epoch than the durable header, the
    // header itself did not survive — do not trust an older, resurrected one.
    if let Some(a) = anchor
        && a.epoch > header.epoch
    {
        return Recovery {
            header:        Some(header),
            accepted:      Vec::new(),
            stream_heads:  BTreeMap::new(),
            safe_offset:   SEGMENT_HEADER_LEN as u64,
            next_pos:      header.base_pos,
            next_batch_id: 0,
            stop:          ScanStop::EpochMismatch,
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
                    epoch:            d.segment_epoch,
                    batch_id:         d.batch_id,
                    first_global_pos: d.first_global_pos,
                    frame_count:      d.frame_count,
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
                let d = decoded
                    .expect("a ByteValid candidate always carries its decode");
                let last_v =
                    d.first_stream_version + u64::from(d.frame_count) - 1;
                stream_heads
                    .entry(d.stream_id)
                    .and_modify(|h| *h = (*h).max(last_v))
                    .or_insert(last_v);
                accepted.push(AcceptedBatch {
                    offset:               off as u64,
                    total_len:            d.total_len,
                    batch_id:             d.batch_id,
                    first_global_pos:     d.first_global_pos,
                    frame_count:          d.frame_count,
                    segment_epoch:        d.segment_epoch,
                    stream_id:            d.stream_id,
                    category_id:          d.category_id,
                    first_stream_version: d.first_stream_version,
                    has_crypto_chain:     d.has_crypto_chain,
                });
                next_batch_id = d.batch_id + 1;
                off += d.total_len as usize;
            }
            Step::Stopped(kernel_stop) => {
                break map_stop(kernel_stop, byte_reason)
            }
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
        KernelStop::ByteFault => byte_reason
            .expect("a kernel ByteFault always has a byte-level reason"),
        KernelStop::EmptyBatch => ScanStop::EmptyBatch,
        KernelStop::EpochMismatch => ScanStop::EpochMismatch,
        KernelStop::PositionDiscontinuity => ScanStop::PositionGap,
        // The kernel only returns `EndOfScan` from `accepted_prefix`'s fold,
        // never from `step`; our loop reaches end-of-segment structurally.
        KernelStop::EndOfScan => ScanStop::EndOfSegment,
    }
}

/// The protocol-relevant fields decoded from a byte-valid batch.
///
/// `pub` (bn-gux, widened from private): only so [`decode_batch`]'s `Result`
/// can name it across the crate boundary from `fuzz/fuzz_targets/
/// fuzz_batch_decode.rs` (Rust forbids a public fn returning a private type,
/// E0446). Fields stay private/unnamed — the fuzz target never inspects them,
/// only checks `decode_batch` never panics.
pub struct Decoded {
    total_len:            u64,
    frame_count:          u32,
    batch_id:             u64,
    first_global_pos:     u64,
    segment_epoch:        u64,
    stream_id:            u64,
    category_id:          u64,
    first_stream_version: u64,
    has_crypto_chain:     bool,
}

/// Decode + byte-validate one candidate at `off` (§2.1 steps 1–6). Returns the
/// decoded protocol fields on success, or the specific byte fault. The A4 CRC
/// (step 5) is **always** computed — there is no path through this function
/// that skips it (A12).
///
/// `pub` (bn-gux, widened from private): the batch-header/subframe decode
/// entry point for `fuzz/fuzz_targets/fuzz_batch_decode.rs`. Precondition
/// carried over from the sole production call site ([`scan_batches`]): `off <
/// img.len()` (the scan loop never calls this once `off >= img.len()`). The
/// fuzz harness respects that precondition (it always fuzzes `off == 0`)
/// rather than this function gaining a new, never-exercised-in-production
/// bounds check purely to humor an out-of-contract fuzz input.
pub fn decode_batch(img: &[u8], off: usize) -> Result<Decoded, ScanStop> {
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
fn subframes_tile(
    batch: &[u8],
    frame_count: u32,
    has_crypto_chain: bool,
) -> bool {
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
        segment_id:         rd_u64(img, SH_SEGMENT_ID_OFF),
        base_pos:           rd_u64(img, SH_BASE_POS_OFF),
        epoch:              rd_u64(img, SH_EPOCH_OFF),
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

#[cfg(test)]
mod frame_tests {
    //! bn-20b: the read-side materialization round trip. Encode real batches
    //! through the canonical write path ([`SegmentWriter`], which encodes via
    //! [`crate::encode::BatchEncoder`]), recover the segment, and prove
    //! [`AcceptedBatch::frames`] yields back the exact `(event_type_id,
    //! payload)` of every event — the property the engine's book rehydration
    //! rests on.
    use super::*;
    use crate::encode::Subframe;
    use crate::runtime::{Runtime, SimRuntime};
    use crate::writer::{BatchSpec, SegmentParams, SegmentWriter};

    /// One input batch: stream id, its first stream version, and each event's
    /// `(event_type_id, payload)`.
    struct InBatch {
        stream_id:            u64,
        first_stream_version: u64,
        events:               Vec<(u32, Vec<u8>)>,
    }

    #[test]
    fn frames_round_trip_exact_payloads_and_type_ids() {
        let rt = SimRuntime::new(7);
        let fs = rt.fs();
        let path = std::path::Path::new("/seg-frames");

        // Three single-stream batches with distinct event-type ids, frame
        // counts, and per-event payloads — the exact shapes the engine writes.
        let batches = vec![
            InBatch {
                stream_id:            10,
                first_stream_version: 0,
                events:               vec![
                    (1, b"open-alice".to_vec()),
                    (2, vec![0xAB; 8]),
                ],
            },
            InBatch {
                stream_id:            10,
                first_stream_version: 2,
                events:               vec![(2, 100i64.to_le_bytes().to_vec())],
            },
            InBatch {
                stream_id:            77,
                first_stream_version: 0,
                events:               vec![
                    (3, b"".to_vec()),
                    (3, b"other-stream".to_vec()),
                    (1, vec![9; 3]),
                ],
            },
        ];

        let mut writer =
            SegmentWriter::create(&fs, path, SegmentParams::new(1, 0, 1, 0))
                .unwrap();
        for b in &batches {
            let subs: Vec<Subframe> = b
                .events
                .iter()
                .map(|(etid, payload)| Subframe::plain(*etid, 0, 0, payload))
                .collect();
            writer
                .append(&BatchSpec {
                    stream_id:            b.stream_id,
                    category_id:          0,
                    first_stream_version: b.first_stream_version,
                    crypto_chain:         None,
                    subframes:            &subs,
                })
                .unwrap();
        }
        writer.close().unwrap();

        let (recovery, image) = recover_segment_with_image(&fs, path).unwrap();
        assert_eq!(recovery.stop, ScanStop::EndOfSegment);
        assert_eq!(recovery.accepted.len(), batches.len());

        // Every accepted batch's frames decode back to the exact input.
        let mut expected_global = 0u64;
        for (batch, input) in recovery.accepted.iter().zip(&batches) {
            assert_eq!(batch.stream_id, input.stream_id);
            assert_eq!(batch.first_stream_version, input.first_stream_version);
            assert_eq!(batch.first_global_pos, expected_global);
            assert_eq!(batch.frame_count as usize, input.events.len());

            let frames: Vec<RecoveredFrame> =
                batch.frames(&image).unwrap().collect();
            assert_eq!(frames.len(), input.events.len());
            for (frame, (etid, payload)) in frames.iter().zip(&input.events) {
                assert_eq!(
                    frame.event_type_id, *etid,
                    "event_type_id must round-trip"
                );
                assert_eq!(
                    frame.payload,
                    &payload[..],
                    "payload bytes must round-trip"
                );
                assert_eq!(frame.codec_id, 0);
            }
            expected_global += input.events.len() as u64;
        }
        assert_eq!(recovery.next_pos, expected_global);
    }

    /// bn-221: `frames` must never panic on a wrong/short `segment_image` — it
    /// used to slice `segment_image[start..end]` (and the iterator then sliced
    /// per-subframe payloads) with no bounds check at all, so a caller error
    /// (wrong image, or an image truncated/reallocated after recovery) was an
    /// out-of-bounds-slice panic. It must now return [`WrongSegmentImage`] in
    /// every misuse shape: too-short image, empty image, a same-length image
    /// whose bytes at this offset are simply wrong (corrupted subframe tiling),
    /// and an entirely different (but long-enough) segment's image.
    #[test]
    fn frames_rejects_wrong_or_short_image() {
        let rt = SimRuntime::new(11);
        let fs = rt.fs();

        let path_a = std::path::Path::new("/seg-a");
        let mut writer_a =
            SegmentWriter::create(&fs, path_a, SegmentParams::new(1, 0, 1, 0))
                .unwrap();
        writer_a
            .append(&BatchSpec {
                stream_id:            1,
                category_id:          0,
                first_stream_version: 0,
                crypto_chain:         None,
                subframes:            &[Subframe::plain(1, 0, 0, b"hello")],
            })
            .unwrap();
        writer_a.close().unwrap();
        let (recovery_a, image_a) =
            recover_segment_with_image(&fs, path_a).unwrap();
        assert_eq!(recovery_a.accepted.len(), 1);
        let batch_a = recovery_a.accepted[0];

        // Sanity: the correct image always decodes without error.
        assert!(batch_a.frames(&image_a).is_ok());

        // 1. Too-short image: even one byte truncated off the correct image
        // makes the batch's own byte range fall outside it.
        let short = &image_a[..image_a.len() - 1];
        let err = batch_a.frames(short).unwrap_err();
        assert_eq!(err.image_len, short.len());
        assert_eq!(err.batch_offset, batch_a.offset);
        assert_eq!(err.batch_total_len, batch_a.total_len);

        // 2. Empty image.
        assert!(batch_a.frames(&[]).is_err());

        // 3. A same-length image whose bytes are simply wrong: corrupt the
        // single subframe's `compressed_len` field to a huge value. The batch
        // byte range still fits inside the image (length unchanged), so only
        // the re-run subframe-tiling check catches it — this is exactly the
        // shape that used to reach the iterator and panic on an
        // out-of-bounds subframe-payload slice.
        let mut corrupted = image_a.clone();
        let cl_off =
            batch_a.offset as usize + HEADER_LEN + SF_COMPRESSED_LEN_OFF;
        corrupted[cl_off..cl_off + 4].copy_from_slice(&u32::MAX.to_le_bytes());
        let err = batch_a.frames(&corrupted).unwrap_err();
        assert_eq!(err.image_len, corrupted.len());
        assert_eq!(err.batch_offset, batch_a.offset);
        assert_eq!(err.batch_total_len, batch_a.total_len);

        // 4. An entirely different segment's (long-enough) image: the batch's
        // byte range fits, but the bytes there belong to a differently-shaped
        // batch (different frame_count/payloads) and do not tile as batch_a.
        let path_b = std::path::Path::new("/seg-b");
        let mut writer_b =
            SegmentWriter::create(&fs, path_b, SegmentParams::new(2, 0, 1, 0))
                .unwrap();
        writer_b
            .append(&BatchSpec {
                stream_id:            2,
                category_id:          0,
                first_stream_version: 0,
                crypto_chain:         None,
                subframes:            &[
                    Subframe::plain(
                        9,
                        0,
                        0,
                        b"a differently shaped batch payload here",
                    ),
                    Subframe::plain(9, 0, 0, b"second frame payload"),
                ],
            })
            .unwrap();
        writer_b.close().unwrap();
        let (_recovery_b, image_b) =
            recover_segment_with_image(&fs, path_b).unwrap();
        assert!(
            image_b.len() >= image_a.len(),
            "fixture must give a same-or-longer wrong image so the failure is \
             tiling, not the earlier length check"
        );
        assert!(batch_a.frames(&image_b).is_err());
    }
}
