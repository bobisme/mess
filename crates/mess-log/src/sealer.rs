//! Segment sealing and the R2 fast path (bn-sbt): the lifecycle layer that
//! turns a full, unsealed segment into a **sealed** one and lets recovery trust
//! a sealed segment via its footer trailer instead of scanning its body.
//!
//! Spec: [`docs/spec/01-log-format.md`] §3.3 (the `SegmentFooter`: a variable
//! extension region followed by a fixed trailer at EOF) and §6 (seal
//! semantics); [`docs/spec/02-recovery.md`] §8.3 (footer validation, the R2
//! last-segment-only fast path, and the corrupt-footer fallback).
//!
//! # What sealing does (§3.3, §6)
//!
//! When a segment rolls off the active head it is **sealed**: the writer
//! appends the `SegmentFooter` — first the extension region (§3.3.2), then the
//! fixed trailer (§3.3.1) whose `ext_offset`/`ext_len`/`ext_crc` locate and
//! cover it — and makes the whole footer durable in one seal `fdatasync`. The
//! trailer's presence-and-validity is what marks a segment complete; a segment
//! with no valid trailer is either the active segment or was interrupted
//! mid-seal and MUST be fully scanned (§8.3).
//!
//! **Phase 3 scope.** The extension region carries the per-stream fold anchors
//! (`StreamHeadTable`, `SnapshotAnchor` list) that are a Phase 5 concern. In
//! Phase 3 the extension is always **empty**: `ext_len == 0` is legal (§3.3.1),
//! and then `ext_crc == 0` and `ext_offset == sealed_len`. The trailer's
//! segment-catalog fields (counts, epoch, positions) are written and validated
//! in full — those are what the R2 fast path needs.
//!
//! # The R2 fast path, and its A12 discipline (§8.3)
//!
//! [`recover_fast`] prefers the trailer: it `pread`s the final
//! [`SEGMENT_TRAILER_LEN`] bytes from EOF, validates `footer_crc`, cross-checks
//! the trailer against the segment's own `SegmentHeader` (§3.3.1 requires
//! `segment_id`/`epoch`/`base_pos` to match — this is what rejects a **stale**
//! trailer left behind a freshly recycled header, see the recycling discipline
//! below), and on success returns the segment's catalog **without scanning the
//! body**. On *any* footer problem — wrong magic/version, `footer_crc`
//! mismatch, a short tail, or a header/trailer disagreement — it treats the
//! segment as **unsealed** and falls back to the authoritative full scan
//! ([`crate::scanner::recover_segment`]). This never accepts an unverified
//! batch (A12): the fast path only *seeds and skips* sealed, immutable
//! segments; a torn seal degrades to a scan, never to acceptance of bad data.
//!
//! # Recycling discipline (delete-only in v1)
//!
//! Per this task's bone (A9) the v1 policy is **segments are deleted, never
//! recycled in place**: a live segment file is never reused for a new
//! generation. The format nonetheless defends against a resurrected prior
//! generation (the torn-matrix "stale-prior-generation background", §01 §1.3),
//! because freed disk regions are not zeroed:
//!
//! - A fresh segment's `SegmentHeader` — carrying a strictly larger `epoch`
//!   (A9) — is written and `fdatasync`ed **before any batch**
//!   ([`crate::writer::SegmentWriter::create`]). Every batch stamps that epoch
//!   (A9), so a stale prior-generation batch left behind a new header carries
//!   the *old* epoch and is rejected by the scanner (`EpochMismatch`).
//! - A stale prior-generation **trailer** left at (or past) EOF behind a fresh
//!   header is rejected by [`recover_fast`]'s header/trailer cross-check (its
//!   `epoch`/`base_pos`/`segment_id` will not match the new header), so it is
//!   never trusted as a seal — the segment falls back to a full scan, which in
//!   turn rejects the stale batches by epoch. The bytes decide (D1); the
//!   trailer only seeds and skips.
//!
//! [`docs/spec/01-log-format.md`]: ../../../../docs/spec/01-log-format.md
//! [`docs/spec/02-recovery.md`]: ../../../../docs/spec/02-recovery.md

use std::io;
use std::path::Path;

use crate::format::*;
use crate::runtime::{FileHandle, Fs, OpenOpts};
use crate::scanner::{self, Recovery};

/// The fixed-trailer fields for a sealed segment (§3.3.1). Phase 3 always seals
/// with an empty extension region, so the extension-locator fields are derived
/// (`ext_offset == content_len`, `ext_len == 0`, `ext_crc == 0`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TrailerFields {
    /// MUST equal the `SegmentHeader.segment_id` (§3.3.1).
    pub segment_id:  u64,
    /// R3: the trailer carries the A9 epoch; MUST equal the header's `epoch`.
    pub epoch:       u64,
    /// MUST equal the header's `base_pos` (§3.3.1).
    pub base_pos:    u64,
    /// Number of accepted batches in the segment.
    pub batch_count: u64,
    /// Total events (Σ `frame_count`) in the segment.
    pub event_count: u64,
    /// Byte offset where the extension region begins — the first byte after
    /// the last batch's `CommitMarker` (§3.3.1). In Phase 3 this is the
    /// segment's whole content length.
    pub ext_offset:  u64,
    /// Byte length of the extension region. **`0` in Phase 3.**
    pub ext_len:     u64,
    /// CRC32C over the extension region. **MUST be `0` when `ext_len == 0`.**
    pub ext_crc:     u32,
}

impl TrailerFields {
    /// The trailer for a Phase-3 seal (empty extension) given the segment's
    /// catalog summary. `content_len` is the first byte after the last
    /// `CommitMarker` (the writer's `write_off`).
    pub fn phase3(
        segment_id: u64,
        epoch: u64,
        base_pos: u64,
        batch_count: u64,
        event_count: u64,
        content_len: u64,
    ) -> Self {
        TrailerFields {
            segment_id,
            epoch,
            base_pos,
            batch_count,
            event_count,
            ext_offset: content_len,
            ext_len: 0,
            ext_crc: 0,
        }
    }

    /// `end_pos = base_pos + event_count`: the A1 seed handed across the
    /// segment boundary (the next segment's `base_pos`), and the trailer's
    /// `end_pos` (§3.3.1).
    pub fn end_pos(&self) -> u64 { self.base_pos + self.event_count }

    /// `sealed_len = ext_offset + ext_len`: the byte offset at which the
    /// trailer begins (§3.3.1).
    pub fn sealed_len(&self) -> u64 { self.ext_offset + self.ext_len }
}

/// Encode the fixed 100-byte `SegmentFooter` trailer (§3.3.1), including its
/// trailing `footer_crc` over `[0, 96)`. The extension region (if any) is a
/// separate write that precedes the trailer; in Phase 3 it is empty and this is
/// the only footer write.
pub fn encode_trailer(t: &TrailerFields) -> [u8; SEGMENT_TRAILER_LEN] {
    debug_assert!(
        t.ext_len != 0 || t.ext_crc == 0,
        "§3.3.1: ext_crc MUST be 0 when ext_len == 0"
    );
    let mut b = [0u8; SEGMENT_TRAILER_LEN];
    put_u32(&mut b, FT_MAGIC_OFF, FOOTER_MAGIC);
    put_u16(&mut b, FT_FORMAT_VERSION_OFF, FORMAT_VERSION);
    put_u16(&mut b, FT_FLAGS_OFF, 0); // reserved, MUST be 0
    put_u64(&mut b, FT_SEGMENT_ID_OFF, t.segment_id);
    put_u64(&mut b, FT_EPOCH_OFF, t.epoch);
    put_u64(&mut b, FT_BASE_POS_OFF, t.base_pos);
    put_u64(&mut b, FT_BATCH_COUNT_OFF, t.batch_count);
    put_u64(&mut b, FT_EVENT_COUNT_OFF, t.event_count);
    put_u64(&mut b, FT_END_POS_OFF, t.end_pos());
    put_u64(&mut b, FT_SEALED_LEN_OFF, t.sealed_len());
    put_u64(&mut b, FT_EXT_OFFSET_OFF, t.ext_offset);
    put_u64(&mut b, FT_EXT_LEN_OFF, t.ext_len);
    put_u32(&mut b, FT_EXT_CRC_OFF, t.ext_crc);
    put_u16(&mut b, FT_REPAIR_SIDECAR_KIND_OFF, 0); // D-FMT-3: none in v3
    put_u16(&mut b, FT_RESERVED_OFF, 0);
    put_u64(&mut b, FT_REPAIR_SIDECAR_REF_OFF, 0);
    let crc = crc32c::crc32c(&b[..SEGMENT_FOOTER_CRC_OFF]);
    put_u32(&mut b, SEGMENT_FOOTER_CRC_OFF, crc);
    b
}

/// The segment-catalog entry a sealed segment's trailer supplies to the R2 fast
/// path (§8.3): the fields recovery can trust without scanning the body. This
/// is the advisory, rebuildable seed (R2/D1) — never a commit authority.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SegmentCatalogEntry {
    pub segment_id:  u64,
    /// R3/A9 epoch, covered by `footer_crc`.
    pub epoch:       u64,
    pub base_pos:    u64,
    /// `base_pos + event_count`: the next segment's `base_pos` seed (§8.1).
    pub end_pos:     u64,
    pub batch_count: u64,
    pub event_count: u64,
    /// Extension-region locator (§3.3.2); `ext_len == 0` in Phase 3.
    pub ext_offset:  u64,
    pub ext_len:     u64,
    pub ext_crc:     u32,
}

/// Decode + validate the fixed trailer from the final [`SEGMENT_TRAILER_LEN`]
/// bytes of a sealed segment (§3.3.1). Returns `None` if `tail` is too short,
/// has the wrong `magic`/`format_version`, or fails its `footer_crc` over
/// `[0, 96)` — in every such case §8.3 says treat the segment as **not
/// sealed**.
pub fn decode_trailer(tail: &[u8]) -> Option<SegmentCatalogEntry> {
    if tail.len() < SEGMENT_TRAILER_LEN {
        return None;
    }
    // The trailer is the LAST SEGMENT_TRAILER_LEN bytes of whatever was read.
    let t = &tail[tail.len() - SEGMENT_TRAILER_LEN..];
    if rd_u32(t, FT_MAGIC_OFF) != FOOTER_MAGIC {
        return None;
    }
    if rd_u16(t, FT_FORMAT_VERSION_OFF) != FORMAT_VERSION {
        return None;
    }
    let want = rd_u32(t, SEGMENT_FOOTER_CRC_OFF);
    if crc32c::crc32c(&t[..SEGMENT_FOOTER_CRC_OFF]) != want {
        return None;
    }
    Some(SegmentCatalogEntry {
        segment_id:  rd_u64(t, FT_SEGMENT_ID_OFF),
        epoch:       rd_u64(t, FT_EPOCH_OFF),
        base_pos:    rd_u64(t, FT_BASE_POS_OFF),
        end_pos:     rd_u64(t, FT_END_POS_OFF),
        batch_count: rd_u64(t, FT_BATCH_COUNT_OFF),
        event_count: rd_u64(t, FT_EVENT_COUNT_OFF),
        ext_offset:  rd_u64(t, FT_EXT_OFFSET_OFF),
        ext_len:     rd_u64(t, FT_EXT_LEN_OFF),
        ext_crc:     rd_u32(t, FT_EXT_CRC_OFF),
    })
}

/// `pread` exactly the final [`SEGMENT_TRAILER_LEN`] bytes from EOF and
/// validate them as a sealed segment's trailer (R2, §8.3). Returns `Ok(None)`
/// when the file is shorter than a trailer or the tail does not validate (⇒ the
/// caller treats it as unsealed). This performs the `pread`-exact-from-EOF fast
/// path: one positioned read of 100 bytes, no full-file read.
pub fn read_trailer<F: Fs>(
    fs: &F,
    path: &Path,
) -> io::Result<Option<SegmentCatalogEntry>> {
    let file = fs.open(path, OpenOpts::read_only())?;
    let len = file.len()?;
    if len < SEGMENT_TRAILER_LEN as u64 {
        return Ok(None);
    }
    let off = len - SEGMENT_TRAILER_LEN as u64;
    let mut buf = [0u8; SEGMENT_TRAILER_LEN];
    let mut filled = 0usize;
    while filled < SEGMENT_TRAILER_LEN {
        let n = file.pread(off + filled as u64, &mut buf[filled..])?;
        if n == 0 {
            return Ok(None); // short tail: not a complete trailer ⇒ unsealed.
        }
        filled += n;
    }
    Ok(decode_trailer(&buf))
}

/// The result of [`recover_fast`]: either the segment was trusted via its
/// footer trailer (R2, no body scan) or it was fully scanned (unsealed, or a
/// footer that did not validate / cross-check — §8.3).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FastRecovery {
    /// The trailer validated and cross-checked against the header: the sealed
    /// segment is trusted via R2 without scanning its body. Carries the catalog
    /// entry (counts, epoch, positions) and the validated header seeds.
    Sealed { catalog: SegmentCatalogEntry, header: scanner::SegmentHeaderInfo },
    /// No trusted trailer (unsealed active segment, torn seal, corrupt
    /// `footer_crc`, or a stale trailer that fails the header cross-check): the
    /// authoritative full scan (§8.2) was run. The bytes decide (D1).
    Scanned(Recovery),
}

impl FastRecovery {
    /// `end_pos` / next-segment `base_pos` seed regardless of path (§8.1).
    pub fn end_pos(&self) -> u64 {
        match self {
            FastRecovery::Sealed { catalog, .. } => catalog.end_pos,
            FastRecovery::Scanned(r) => r.next_pos,
        }
    }

    /// The segment's `epoch` regardless of path (R3/A9 anchor for the next
    /// segment, §8.1). `None` if a scanned segment had no valid header.
    pub fn epoch(&self) -> Option<u64> {
        match self {
            FastRecovery::Sealed { catalog, .. } => Some(catalog.epoch),
            FastRecovery::Scanned(r) => r.header.as_ref().map(|h| h.epoch),
        }
    }

    /// Number of committed batches on either path.
    pub fn batch_count(&self) -> u64 {
        match self {
            FastRecovery::Sealed { catalog, .. } => catalog.batch_count,
            FastRecovery::Scanned(r) => r.accepted.len() as u64,
        }
    }
}

/// Recover a segment, **preferring the R2 trailer fast path** (§8.3). Reads the
/// `SegmentHeader` and the fixed trailer; if the trailer validates *and*
/// cross-checks against the header (§3.3.1: `segment_id`/`epoch`/`base_pos`
/// match — the guard that rejects a stale recycled trailer), the segment is
/// trusted via its catalog with **no body scan**. Otherwise it falls back to
/// the authoritative full scan ([`scanner::recover_segment`]).
///
/// This is additive over the scanner: full recovery remains the only commit
/// authority (A12/D1); this only *seeds and skips* immutable sealed segments.
pub fn recover_fast<F: Fs>(fs: &F, path: &Path) -> io::Result<FastRecovery> {
    // Read the (checksummed) header seed first: the fast path cross-checks the
    // trailer against it, and the fallback scan needs the file open anyway.
    let header = read_segment_header(fs, path)?;
    if let (Some(hdr), Some(cat)) = (header, read_trailer(fs, path)?) {
        // §3.3.1: the trailer's segment_id/epoch/base_pos MUST equal the
        // header's. A mismatch means the trailer does not belong to THIS header
        // — the recycling-discipline case: a stale prior-generation trailer
        // resurrected behind a freshly synced header. Do not trust it; scan.
        let coherent = cat.segment_id == hdr.segment_id
            && cat.epoch == hdr.epoch
            && cat.base_pos == hdr.base_pos
            && cat.end_pos == cat.base_pos + cat.event_count
            && cat.ext_offset >= SEGMENT_HEADER_LEN as u64
            && (cat.ext_len != 0 || cat.ext_crc == 0);
        if coherent {
            return Ok(FastRecovery::Sealed { catalog: cat, header: hdr });
        }
    }
    Ok(FastRecovery::Scanned(scanner::recover_segment(fs, path)?))
}

/// Read + validate just the `SegmentHeader` (§3.2) through the Fs seam, for the
/// fast path's header/trailer cross-check. `None` if it is torn / wrong-magic /
/// fails `header_crc`.
///
/// `pub` (bn-2en): whole-log recovery ([`crate::recover_all`]) reads the header
/// once per segment to cross-check the manifest/footer catalog seed against it,
/// reusing this exact validated decode rather than duplicating the 52-byte
/// header parse.
pub fn read_segment_header<F: Fs>(
    fs: &F,
    path: &Path,
) -> io::Result<Option<scanner::SegmentHeaderInfo>> {
    let file = fs.open(path, OpenOpts::read_only())?;
    let mut buf = [0u8; SEGMENT_HEADER_LEN];
    let mut filled = 0usize;
    while filled < SEGMENT_HEADER_LEN {
        let n = file.pread(filled as u64, &mut buf[filled..])?;
        if n == 0 {
            return Ok(None);
        }
        filled += n;
    }
    Ok(decode_segment_header(&buf))
}

/// Decode + validate the fixed 52-byte `SegmentHeader` (§3.2). Mirrors the
/// scanner's private decoder but returns the public
/// [`scanner::SegmentHeaderInfo`] so the fast path can cross-check without
/// depending on scanner internals.
fn decode_segment_header(img: &[u8]) -> Option<scanner::SegmentHeaderInfo> {
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
    Some(scanner::SegmentHeaderInfo {
        segment_id:         rd_u64(img, SH_SEGMENT_ID_OFF),
        base_pos:           rd_u64(img, SH_BASE_POS_OFF),
        epoch:              rd_u64(img, SH_EPOCH_OFF),
        prev_segment_epoch: rd_u64(img, SH_PREV_SEGMENT_EPOCH_OFF),
    })
}

#[inline]
fn put_u16(buf: &mut [u8], off: usize, v: u16) {
    buf[off..off + 2].copy_from_slice(&v.to_le_bytes());
}
#[inline]
fn put_u32(buf: &mut [u8], off: usize, v: u32) {
    buf[off..off + 4].copy_from_slice(&v.to_le_bytes());
}
#[inline]
fn put_u64(buf: &mut [u8], off: usize, v: u64) {
    buf[off..off + 8].copy_from_slice(&v.to_le_bytes());
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
mod tests {
    use super::*;

    fn sample() -> TrailerFields {
        TrailerFields::phase3(
            7,
            42,
            1000,
            3,
            5,
            SEGMENT_HEADER_LEN as u64 + 384,
        )
    }

    #[test]
    fn trailer_roundtrips_through_decode() {
        let t = sample();
        let bytes = encode_trailer(&t);
        assert_eq!(bytes.len(), SEGMENT_TRAILER_LEN);
        let cat = decode_trailer(&bytes).expect("valid trailer decodes");
        assert_eq!(cat.segment_id, 7);
        assert_eq!(cat.epoch, 42);
        assert_eq!(cat.base_pos, 1000);
        assert_eq!(cat.batch_count, 3);
        assert_eq!(cat.event_count, 5);
        assert_eq!(cat.end_pos, 1005); // base_pos + event_count
        assert_eq!(cat.ext_offset, SEGMENT_HEADER_LEN as u64 + 384);
        assert_eq!(cat.ext_len, 0);
        assert_eq!(cat.ext_crc, 0);
    }

    #[test]
    fn empty_extension_derives_locator_fields() {
        let t = sample();
        // §3.3.1: ext_len == 0 ⇒ ext_crc == 0 and ext_offset == sealed_len.
        assert_eq!(t.ext_len, 0);
        assert_eq!(t.ext_crc, 0);
        assert_eq!(t.sealed_len(), t.ext_offset);
    }

    #[test]
    fn footer_crc_mismatch_rejected() {
        let mut bytes = encode_trailer(&sample());
        bytes[FT_EPOCH_OFF] ^= 0xFF; // flip an epoch byte inside CRC coverage
        assert!(
            decode_trailer(&bytes).is_none(),
            "corrupt footer_crc ⇒ unsealed"
        );
    }

    #[test]
    fn wrong_magic_rejected() {
        let mut bytes = encode_trailer(&sample());
        bytes[FT_MAGIC_OFF] ^= 0x01;
        assert!(decode_trailer(&bytes).is_none());
    }

    #[test]
    fn short_tail_rejected() {
        let bytes = encode_trailer(&sample());
        assert!(decode_trailer(&bytes[..SEGMENT_TRAILER_LEN - 1]).is_none());
    }

    #[test]
    fn decode_reads_trailer_from_the_tail_of_a_longer_buffer() {
        // decode_trailer must locate the trailer at the END of the slice, so a
        // whole-file read (header + batches + trailer) decodes correctly.
        let t = sample();
        let mut file = vec![0xABu8; 200];
        file.extend_from_slice(&encode_trailer(&t));
        let cat = decode_trailer(&file).expect("trailer at tail decodes");
        assert_eq!(cat.segment_id, 7);
        assert_eq!(cat.epoch, 42);
    }
}
