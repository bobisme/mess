//! The advisory **manifest / segment catalog** (bn-2en, R2): a small,
//! implementation-defined file that caches the sealed segments' footer trailers
//! so whole-log recovery's fast path is not `O(#sealed segments)` trailer
//! `pread`s (one manifest read seeds them all).
//!
//! Spec: [`docs/spec/02-recovery.md`] §8.3 (R2). The manifest is owned by the
//! recovery document and is, verbatim from the spec, **advisory (D1),
//! rebuildable from and verifiable against the footers, and never
//! authoritative**. Every property this module must satisfy follows from that
//! one sentence:
//!
//! - **Missing** — [`read_manifest`] returns `None`; recovery reads trailers.
//! - **Corrupt** — a wrong magic/version or a failed `manifest_crc` makes
//!   [`decode_manifest`] return `None`; the whole file is discarded (a single
//!   corrupt entry cannot be trusted, so the CRC guards the lot). Recovery
//!   reads trailers.
//! - **Stale** — an entry that no longer matches the segment it names (the
//!   segment was deleted, or the entry disagrees with the segment's own
//!   `SegmentHeader`) is rejected *per entry* at recovery time
//!   ([`crate::recover_all`]); recovery falls back to reading that segment's
//!   trailer (and, if that too fails, to a full scan). A stale manifest can
//!   therefore only ever *cost* work, never change the committed prefix.
//!
//! The manifest is a pure byte cache: it stores exactly the
//! [`SegmentCatalogEntry`] fields a sealed footer supplies, keyed by
//! `segment_id`. It is **never** consulted as a commit authority — a manifest
//! entry only *seeds and skips* an immutable sealed segment; the bytes decide
//! (D1). Building one is [`build_manifest`] over the catalog entries recovery
//! already produced; it is rebuildable at any time from the footers alone.
//!
//! [`docs/spec/02-recovery.md`]: ../../../../docs/spec/02-recovery.md

use std::io;
use std::path::Path;

use crate::runtime::{FileHandle, Fs, OpenOpts};
use crate::sealer::SegmentCatalogEntry;

/// `Manifest.magic`: a distinct 32-bit tag so a truncated/foreign file is
/// rejected before its bytes are trusted. (`0x5EA1_0CA7` — "seal catalog".)
pub const MANIFEST_MAGIC: u32 = 0x5EA1_0CA7;

/// The manifest format version. Bumped only on an incompatible layout change;
/// a reader that sees a version it does not know treats the manifest as absent
/// (advisory-skip, never a hard error).
pub const MANIFEST_VERSION: u16 = 1;

/// Fixed manifest header length: `magic(4) + version(2) + flags(2) +
/// entry_count(4) + reserved(4)`.
pub const MANIFEST_HEADER_LEN: usize = 16;

/// Per-entry length: the nine `SegmentCatalogEntry` fields (`segment_id`,
/// `epoch`, `base_pos`, `end_pos`, `batch_count`, `event_count`, `ext_offset`,
/// `ext_len` = 8 bytes each; `ext_crc` = 4) plus 4 bytes of padding = 72.
pub const MANIFEST_ENTRY_LEN: usize = 72;

// Header field offsets.
const MH_MAGIC_OFF: usize = 0;
const MH_VERSION_OFF: usize = 4;
const MH_FLAGS_OFF: usize = 6;
const MH_ENTRY_COUNT_OFF: usize = 8;
// bytes [12, 16) reserved (MUST be 0).

// Entry field offsets, relative to the entry's first byte.
const ME_SEGMENT_ID_OFF: usize = 0;
const ME_EPOCH_OFF: usize = 8;
const ME_BASE_POS_OFF: usize = 16;
const ME_END_POS_OFF: usize = 24;
const ME_BATCH_COUNT_OFF: usize = 32;
const ME_EVENT_COUNT_OFF: usize = 40;
const ME_EXT_OFFSET_OFF: usize = 48;
const ME_EXT_LEN_OFF: usize = 56;
const ME_EXT_CRC_OFF: usize = 64;
// bytes [68, 72) reserved (MUST be 0).

/// A decoded advisory manifest: the cached sealed-segment catalog entries in
/// the order they were written (the writer emits them in `segment_id` order).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Manifest {
    entries: Vec<SegmentCatalogEntry>,
}

impl Manifest {
    /// A manifest over `entries` (assumed already in `segment_id` order; the
    /// lookup does not depend on the order).
    pub fn new(entries: Vec<SegmentCatalogEntry>) -> Self {
        Manifest { entries }
    }

    /// The cached entries.
    pub fn entries(&self) -> &[SegmentCatalogEntry] { &self.entries }

    /// The number of cached sealed segments.
    pub fn len(&self) -> usize { self.entries.len() }

    /// Whether the manifest caches no segments.
    pub fn is_empty(&self) -> bool { self.entries.is_empty() }

    /// The cached catalog entry for `segment_id`, if any. `O(n)` — recovery
    /// resolves each segment once, so a linear probe over the (small) sealed
    /// set is fine and avoids allocating an index for a cache that is thrown
    /// away after recovery.
    pub fn get(&self, segment_id: u64) -> Option<&SegmentCatalogEntry> {
        self.entries.iter().find(|e| e.segment_id == segment_id)
    }
}

/// Encode `entries` into the on-disk manifest bytes, terminated by a
/// `manifest_crc` over everything before it (so any corruption anywhere in the
/// file fails the CRC and the whole manifest is discarded — advisory-safe).
pub fn build_manifest(entries: &[SegmentCatalogEntry]) -> Vec<u8> {
    let mut b =
        vec![0u8; MANIFEST_HEADER_LEN + entries.len() * MANIFEST_ENTRY_LEN + 4];
    put_u32(&mut b, MH_MAGIC_OFF, MANIFEST_MAGIC);
    put_u16(&mut b, MH_VERSION_OFF, MANIFEST_VERSION);
    put_u16(&mut b, MH_FLAGS_OFF, 0);
    put_u32(&mut b, MH_ENTRY_COUNT_OFF, entries.len() as u32);
    // bytes [12,16) already zero (reserved).
    for (i, e) in entries.iter().enumerate() {
        let o = MANIFEST_HEADER_LEN + i * MANIFEST_ENTRY_LEN;
        put_u64(&mut b, o + ME_SEGMENT_ID_OFF, e.segment_id);
        put_u64(&mut b, o + ME_EPOCH_OFF, e.epoch);
        put_u64(&mut b, o + ME_BASE_POS_OFF, e.base_pos);
        put_u64(&mut b, o + ME_END_POS_OFF, e.end_pos);
        put_u64(&mut b, o + ME_BATCH_COUNT_OFF, e.batch_count);
        put_u64(&mut b, o + ME_EVENT_COUNT_OFF, e.event_count);
        put_u64(&mut b, o + ME_EXT_OFFSET_OFF, e.ext_offset);
        put_u64(&mut b, o + ME_EXT_LEN_OFF, e.ext_len);
        put_u32(&mut b, o + ME_EXT_CRC_OFF, e.ext_crc);
        // bytes [o+68, o+72) reserved (already zero).
    }
    let crc_off = b.len() - 4;
    let crc = crc32c::crc32c(&b[..crc_off]);
    put_u32(&mut b, crc_off, crc);
    b
}

/// Decode + validate the manifest bytes. Returns `None` — meaning *treat as
/// absent, read trailers* — for any of: too short, wrong `magic`/`version`, an
/// `entry_count` that does not match the byte length, or a failed
/// `manifest_crc`. Never panics on arbitrary input (it is fed whatever bytes a
/// possibly-corrupt file holds).
pub fn decode_manifest(bytes: &[u8]) -> Option<Manifest> {
    if bytes.len() < MANIFEST_HEADER_LEN + 4 {
        return None;
    }
    if rd_u32(bytes, MH_MAGIC_OFF) != MANIFEST_MAGIC {
        return None;
    }
    if rd_u16(bytes, MH_VERSION_OFF) != MANIFEST_VERSION {
        return None;
    }
    let entry_count = rd_u32(bytes, MH_ENTRY_COUNT_OFF) as usize;
    // The declared entry count must exactly account for the file length. This
    // rejects a truncated/oversized file before we index into it.
    let expected = MANIFEST_HEADER_LEN
        .checked_add(entry_count.checked_mul(MANIFEST_ENTRY_LEN)?)?
        .checked_add(4)?;
    if bytes.len() != expected {
        return None;
    }
    let crc_off = bytes.len() - 4;
    if crc32c::crc32c(&bytes[..crc_off]) != rd_u32(bytes, crc_off) {
        return None;
    }
    let mut entries = Vec::with_capacity(entry_count);
    for i in 0..entry_count {
        let o = MANIFEST_HEADER_LEN + i * MANIFEST_ENTRY_LEN;
        entries.push(SegmentCatalogEntry {
            segment_id:  rd_u64(bytes, o + ME_SEGMENT_ID_OFF),
            epoch:       rd_u64(bytes, o + ME_EPOCH_OFF),
            base_pos:    rd_u64(bytes, o + ME_BASE_POS_OFF),
            end_pos:     rd_u64(bytes, o + ME_END_POS_OFF),
            batch_count: rd_u64(bytes, o + ME_BATCH_COUNT_OFF),
            event_count: rd_u64(bytes, o + ME_EVENT_COUNT_OFF),
            ext_offset:  rd_u64(bytes, o + ME_EXT_OFFSET_OFF),
            ext_len:     rd_u64(bytes, o + ME_EXT_LEN_OFF),
            ext_crc:     rd_u32(bytes, o + ME_EXT_CRC_OFF),
        });
    }
    Some(Manifest { entries })
}

/// Write `entries` to the manifest file at `path` through the [`Fs`] seam and
/// `fdatasync` it durable. The manifest is advisory, so a caller MAY write it
/// to a temp name and [`rename`](Fs::rename) for atomicity; this helper writes
/// in place (a torn manifest simply fails its CRC on the next read and is
/// ignored).
pub fn write_manifest<F: Fs>(
    fs: &F,
    path: &Path,
    entries: &[SegmentCatalogEntry],
) -> io::Result<()> {
    let bytes = build_manifest(entries);
    let file = fs.open(
        path,
        OpenOpts {
            read:     true,
            write:    true,
            create:   true,
            truncate: true,
        },
    )?;
    let mut off = 0usize;
    while off < bytes.len() {
        let n = file.pwrite(off as u64, &bytes[off..])?;
        if n == 0 {
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "short manifest write",
            ));
        }
        off += n;
    }
    file.fdatasync()
}

/// Read + decode the manifest at `path` through the [`Fs`] seam. Returns
/// `Ok(None)` when the file is missing (`NotFound`) or does not validate — in
/// every such case recovery MUST fall back to reading trailers (§8.3, R2), so a
/// missing/corrupt manifest is indistinguishable and equally harmless.
pub fn read_manifest<F: Fs>(
    fs: &F,
    path: &Path,
) -> io::Result<Option<Manifest>> {
    let file = match fs.open(path, OpenOpts::read_only()) {
        Ok(f) => f,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e),
    };
    let len = usize::try_from(file.len()?).unwrap_or(usize::MAX);
    let mut buf = vec![0u8; len];
    let mut filled = 0usize;
    while filled < len {
        let n = file.pread(filled as u64, &mut buf[filled..])?;
        if n == 0 {
            break;
        }
        filled += n;
    }
    buf.truncate(filled);
    Ok(decode_manifest(&buf))
}

#[inline]
fn put_u16(b: &mut [u8], o: usize, v: u16) {
    b[o..o + 2].copy_from_slice(&v.to_le_bytes());
}
#[inline]
fn put_u32(b: &mut [u8], o: usize, v: u32) {
    b[o..o + 4].copy_from_slice(&v.to_le_bytes());
}
#[inline]
fn put_u64(b: &mut [u8], o: usize, v: u64) {
    b[o..o + 8].copy_from_slice(&v.to_le_bytes());
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

    fn cat(
        segment_id: u64,
        epoch: u64,
        base_pos: u64,
        events: u64,
        batches: u64,
    ) -> SegmentCatalogEntry {
        SegmentCatalogEntry {
            segment_id,
            epoch,
            base_pos,
            end_pos: base_pos + events,
            batch_count: batches,
            event_count: events,
            ext_offset: 52 + 200,
            ext_len: 0,
            ext_crc: 0,
        }
    }

    #[test]
    fn roundtrips_through_decode() {
        let entries =
            vec![cat(1, 10, 0, 5, 3), cat(2, 11, 5, 4, 2), cat(3, 12, 9, 6, 4)];
        let bytes = build_manifest(&entries);
        let m = decode_manifest(&bytes).expect("valid manifest decodes");
        assert_eq!(m.entries(), entries.as_slice());
        assert_eq!(m.get(2).unwrap().base_pos, 5);
        assert_eq!(m.get(3).unwrap().end_pos, 15);
        assert!(m.get(9).is_none());
    }

    #[test]
    fn empty_manifest_roundtrips() {
        let bytes = build_manifest(&[]);
        let m = decode_manifest(&bytes).expect("empty manifest is valid");
        assert!(m.is_empty());
    }

    #[test]
    fn corrupt_crc_is_treated_as_absent() {
        let mut bytes = build_manifest(&[cat(1, 10, 0, 5, 3)]);
        let i = MANIFEST_HEADER_LEN + ME_EPOCH_OFF; // flip a byte inside CRC coverage
        bytes[i] ^= 0xFF;
        assert!(
            decode_manifest(&bytes).is_none(),
            "any corruption ⇒ discard the whole manifest"
        );
    }

    #[test]
    fn wrong_magic_and_version_rejected() {
        let mut a = build_manifest(&[cat(1, 10, 0, 5, 3)]);
        a[MH_MAGIC_OFF] ^= 0x01;
        assert!(decode_manifest(&a).is_none());
        let mut b = build_manifest(&[cat(1, 10, 0, 5, 3)]);
        put_u16(&mut b, MH_VERSION_OFF, MANIFEST_VERSION + 1);
        // recompute a *valid* CRC so only the version differs — must still
        // reject.
        let crc_off = b.len() - 4;
        let crc = crc32c::crc32c(&b[..crc_off]);
        put_u32(&mut b, crc_off, crc);
        assert!(
            decode_manifest(&b).is_none(),
            "unknown version ⇒ treat as absent"
        );
    }

    #[test]
    fn truncated_and_lying_count_rejected() {
        let bytes = build_manifest(&[cat(1, 10, 0, 5, 3), cat(2, 11, 5, 4, 2)]);
        assert!(
            decode_manifest(&bytes[..bytes.len() - 1]).is_none(),
            "truncated ⇒ reject"
        );
        // A count that does not match the byte length must be rejected before
        // it can drive an out-of-bounds entry read.
        let mut lie = bytes.clone();
        put_u32(&mut lie, MH_ENTRY_COUNT_OFF, 9999);
        assert!(decode_manifest(&lie).is_none());
    }

    #[test]
    fn short_buffer_never_panics() {
        for n in 0..MANIFEST_HEADER_LEN + 4 {
            assert!(decode_manifest(&vec![0u8; n]).is_none());
        }
    }
}
