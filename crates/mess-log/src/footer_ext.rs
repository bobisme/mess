//! The sealed-segment footer **extension region** encode/decode (`bn-1d0`):
//! the durable Tier-1 head anchor (`StreamHeadTable`) and the Path-C retention
//! certificates (`SnapshotAnchorList`).
//!
//! Byte layout is owned by **[`docs/spec/01-log-format.md`] §3.3.2**; the
//! *semantics* of the hashes it carries are owned by
//! **[`docs/spec/05-fold-certificates.md`] §5 (G6) / §8.2**. This module is the
//! seal-path encoder the sealer drives: in Phase 3 the extension is always
//! empty (`ext_len == 0`); Phase 5 (this bone) fills it with the fold anchors.
//!
//! # Region shape (§3.3.2)
//!
//! The region is a back-to-back sequence of **typed sections**, each a fixed
//! 16-byte header ([`crate::format::EXT_SECTION_HDR_LEN`]) followed by
//! `payload_len` bytes of fixed-size entries:
//!
//! ```text
//! section header (16 B): kind:u16 | section_flags:u16 | entry_count:u32 | payload_len:u64
//! StreamHeadEntry (48 B): stream_id:u64 | last_version:u64 | head_hash:[u8;32]
//! SnapshotAnchor  (48 B): stream_id:u64 | version:u64      | chain_hash:[u8;32]
//! ```
//!
//! A reader walks sections by hopping `16 + payload_len` and MUST advisory-skip
//! an unknown `kind` (§3.3.2, D-FMT-3) — [`decode_extension`] does. The whole
//! region is covered by the trailer's `ext_crc` (a separate CRC from
//! `footer_crc`, §3.3.1), so a partial rewrite is caught by recovery.
//!
//! [`docs/spec/01-log-format.md`]: ../../../../docs/spec/01-log-format.md
//! [`docs/spec/05-fold-certificates.md`]: ../../../../docs/spec/05-fold-certificates.md

use crate::fold_chain::Hash;
use crate::format::*;
use crate::sealer::{encode_trailer, TrailerFields};

/// One `StreamHeadEntry` (§3.3.2): the durable Tier-1 head anchor `A(S)` for a
/// stream with ≥1 committed event in the sealed segment (§5).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StreamHeadEntry {
    /// Interned stream id (`04-registry.md`).
    pub stream_id: u64,
    /// Stream version of the stream's **last** event in this segment.
    pub last_version: u64,
    /// `h[last_version]` — the fold-chain value after that event.
    pub head_hash: Hash,
}

/// One `SnapshotAnchor` (§3.3.2): the Path-C retention certificate recorded at
/// seal for a live snapshot at version `v` (§8.2, gap 9).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SnapshotAnchor {
    /// Interned stream id (`04-registry.md`).
    pub stream_id: u64,
    /// The snapshot's `stream_version` `v` (0-based last-index).
    pub version: u64,
    /// `h[v]` — the fold-chain value certifying the prefix `0..=v`.
    pub chain_hash: Hash,
}

/// The parsed contents of a footer extension region (known sections only;
/// unknown kinds are advisory-skipped, §3.3.2).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ParsedExtension {
    /// `StreamHeadTable` entries (kind `1`), in stored order.
    pub heads: Vec<StreamHeadEntry>,
    /// `SnapshotAnchorList` entries (kind `2`), in stored order.
    pub anchors: Vec<SnapshotAnchor>,
}

fn put_u16(buf: &mut Vec<u8>, v: u16) {
    buf.extend_from_slice(&v.to_le_bytes());
}
fn put_u32(buf: &mut Vec<u8>, v: u32) {
    buf.extend_from_slice(&v.to_le_bytes());
}
fn put_u64(buf: &mut Vec<u8>, v: u64) {
    buf.extend_from_slice(&v.to_le_bytes());
}
fn rd_u16(d: &[u8], o: usize) -> u16 {
    u16::from_le_bytes(d[o..o + 2].try_into().unwrap())
}
fn rd_u32(d: &[u8], o: usize) -> u32 {
    u32::from_le_bytes(d[o..o + 4].try_into().unwrap())
}
fn rd_u64(d: &[u8], o: usize) -> u64 {
    u64::from_le_bytes(d[o..o + 8].try_into().unwrap())
}

fn push_section_header(buf: &mut Vec<u8>, kind: u16, entry_count: u32, payload_len: u64) {
    put_u16(buf, kind); // EXT_KIND_OFF
    put_u16(buf, 0); // EXT_SECTION_FLAGS_OFF — MUST be 0 in v3
    put_u32(buf, entry_count); // EXT_ENTRY_COUNT_OFF
    put_u64(buf, payload_len); // EXT_PAYLOAD_LEN_OFF
}

/// Encode the extension region bytes for the given anchors (§3.3.2). Emits a
/// `StreamHeadTable` section (kind 1) iff `heads` is non-empty, then a
/// `SnapshotAnchorList` section (kind 2) iff `anchors` is non-empty. When both
/// are empty the region is empty (`Vec::new()`), the Phase-3 `ext_len == 0`
/// shape. `payload_len == entry_count * 48` for each known kind.
#[must_use]
pub fn encode_extension(heads: &[StreamHeadEntry], anchors: &[SnapshotAnchor]) -> Vec<u8> {
    let mut buf = Vec::new();
    if !heads.is_empty() {
        let payload_len = (heads.len() * EXT_ENTRY_LEN) as u64;
        push_section_header(&mut buf, EXT_KIND_STREAM_HEAD_TABLE, heads.len() as u32, payload_len);
        for e in heads {
            put_u64(&mut buf, e.stream_id);
            put_u64(&mut buf, e.last_version);
            buf.extend_from_slice(&e.head_hash);
        }
    }
    if !anchors.is_empty() {
        let payload_len = (anchors.len() * EXT_ENTRY_LEN) as u64;
        push_section_header(&mut buf, EXT_KIND_SNAPSHOT_ANCHOR_LIST, anchors.len() as u32, payload_len);
        for a in anchors {
            put_u64(&mut buf, a.stream_id);
            put_u64(&mut buf, a.version);
            buf.extend_from_slice(&a.chain_hash);
        }
    }
    buf
}

/// CRC32C over the extension region — the trailer's `ext_crc` (§3.3.1). MUST be
/// `0` when the region is empty; this returns `0` for an empty slice, matching
/// the Phase-3 invariant.
#[must_use]
pub fn extension_crc(ext: &[u8]) -> u32 {
    if ext.is_empty() {
        0
    } else {
        crc32c::crc32c(ext)
    }
}

/// Walk the extension region, collecting the known sections and advisory-
/// skipping any unknown `kind` (§3.3.2). Malformed input (a section header that
/// runs past the region, or a known section whose `payload_len != entry_count *
/// 48`) stops the walk and returns what was parsed so far — never panics, never
/// fails acceptance (batch acceptance is decided by the per-batch checksums,
/// not the extension).
#[must_use]
pub fn decode_extension(ext: &[u8]) -> ParsedExtension {
    let mut out = ParsedExtension::default();
    let mut off = 0usize;
    while off + EXT_SECTION_HDR_LEN <= ext.len() {
        let hdr = &ext[off..off + EXT_SECTION_HDR_LEN];
        let kind = rd_u16(hdr, EXT_KIND_OFF);
        let entry_count = rd_u32(hdr, EXT_ENTRY_COUNT_OFF) as usize;
        let payload_len = rd_u64(hdr, EXT_PAYLOAD_LEN_OFF) as usize;
        let payload_start = off + EXT_SECTION_HDR_LEN;
        let Some(payload_end) = payload_start.checked_add(payload_len) else {
            break;
        };
        if payload_end > ext.len() {
            break; // truncated / malformed: stop the walk.
        }
        let payload = &ext[payload_start..payload_end];
        match kind {
            EXT_KIND_STREAM_HEAD_TABLE => {
                if payload_len != entry_count * EXT_ENTRY_LEN {
                    break;
                }
                for c in payload.chunks_exact(EXT_ENTRY_LEN) {
                    out.heads.push(StreamHeadEntry {
                        stream_id: rd_u64(c, 0),
                        last_version: rd_u64(c, 8),
                        head_hash: c[16..48].try_into().unwrap(),
                    });
                }
            }
            EXT_KIND_SNAPSHOT_ANCHOR_LIST => {
                if payload_len != entry_count * EXT_ENTRY_LEN {
                    break;
                }
                for c in payload.chunks_exact(EXT_ENTRY_LEN) {
                    out.anchors.push(SnapshotAnchor {
                        stream_id: rd_u64(c, 0),
                        version: rd_u64(c, 8),
                        chain_hash: c[16..48].try_into().unwrap(),
                    });
                }
            }
            _ => { /* unknown kind: advisory-skip by payload_len (§3.3.2) */ }
        }
        off = payload_end;
    }
    out
}

/// The segment-catalog summary the seal path already has when it composes the
/// footer (§3.3.1): the fixed-trailer scalar fields, minus the extension
/// locator (which [`encode_sealed_footer`] derives from the encoded extension).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SealSummary {
    /// MUST equal the `SegmentHeader.segment_id`.
    pub segment_id: u64,
    /// The A9 epoch (R3), MUST equal the header's `epoch`.
    pub epoch: u64,
    /// MUST equal the header's `base_pos`.
    pub base_pos: u64,
    /// Number of accepted batches in the segment.
    pub batch_count: u64,
    /// Total events (Σ `frame_count`) in the segment.
    pub event_count: u64,
    /// First byte after the last `CommitMarker` (the writer's `write_off`),
    /// i.e. `ext_offset` — where the extension region begins.
    pub content_len: u64,
}

/// Compose a complete sealed-segment footer carrying the fold anchors: the
/// extension region (§3.3.2) immediately followed by the fixed 100-byte trailer
/// (§3.3.1) whose `ext_offset`/`ext_len`/`ext_crc` locate and cover it. Returns
/// `(footer_bytes, trailer_fields)`; `footer_bytes == extension ++ trailer` is
/// the single durable write the seal `fdatasync` covers (§6).
#[must_use]
pub fn encode_sealed_footer(
    summary: &SealSummary,
    heads: &[StreamHeadEntry],
    anchors: &[SnapshotAnchor],
) -> (Vec<u8>, TrailerFields) {
    let ext = encode_extension(heads, anchors);
    let ext_crc = extension_crc(&ext);
    let t = TrailerFields {
        segment_id: summary.segment_id,
        epoch: summary.epoch,
        base_pos: summary.base_pos,
        batch_count: summary.batch_count,
        event_count: summary.event_count,
        ext_offset: summary.content_len,
        ext_len: ext.len() as u64,
        ext_crc,
    };
    let trailer = encode_trailer(&t);
    let mut footer = ext;
    footer.extend_from_slice(&trailer);
    (footer, t)
}

/// Look up the durable Tier-1 head anchor for `stream_id` in a parsed
/// extension (§5): the `(last_version, head_hash)` pair, or `None` if this
/// segment lists no head for the stream. The **authoritative** anchor is the
/// entry from the latest sealed segment that lists the stream (§5.2); a caller
/// scanning newest-first takes the first hit.
#[must_use]
pub fn head_anchor_for(ext: &ParsedExtension, stream_id: u64) -> Option<(u64, Hash)> {
    ext.heads
        .iter()
        .find(|e| e.stream_id == stream_id)
        .map(|e| (e.last_version, e.head_hash))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fold_chain::genesis;
    use crate::sealer::decode_trailer;

    #[test]
    fn empty_extension_is_empty_and_crc_zero() {
        let ext = encode_extension(&[], &[]);
        assert!(ext.is_empty());
        assert_eq!(extension_crc(&ext), 0);
        assert_eq!(decode_extension(&ext), ParsedExtension::default());
    }

    #[test]
    fn heads_and_anchors_roundtrip() {
        let heads = vec![
            StreamHeadEntry { stream_id: 7, last_version: 49, head_hash: genesis(7) },
            StreamHeadEntry { stream_id: 9, last_version: 3, head_hash: genesis(9) },
        ];
        let anchors = vec![SnapshotAnchor { stream_id: 7, version: 19, chain_hash: genesis(7) }];
        let ext = encode_extension(&heads, &anchors);
        // payload_len == entry_count * 48 for each section, plus two headers.
        assert_eq!(ext.len(), EXT_SECTION_HDR_LEN * 2 + (2 + 1) * EXT_ENTRY_LEN);
        let parsed = decode_extension(&ext);
        assert_eq!(parsed.heads, heads);
        assert_eq!(parsed.anchors, anchors);
        assert_eq!(head_anchor_for(&parsed, 9), Some((3, genesis(9))));
        assert_eq!(head_anchor_for(&parsed, 100), None);
    }

    #[test]
    fn unknown_section_kind_is_skipped() {
        // Build: [unknown kind 999 with 16-byte payload][StreamHeadTable].
        let heads = vec![StreamHeadEntry { stream_id: 1, last_version: 0, head_hash: [0xAB; 32] }];
        let known = encode_extension(&heads, &[]);
        let mut ext = Vec::new();
        push_section_header(&mut ext, 999, 0, 16);
        ext.extend_from_slice(&[0xEE; 16]);
        ext.extend_from_slice(&known);
        let parsed = decode_extension(&ext);
        assert_eq!(parsed.heads, heads, "known section after an unknown one is still read");
    }

    #[test]
    fn truncated_section_stops_walk_without_panic() {
        let mut ext = Vec::new();
        // Claim a 48-byte payload but supply only 10 bytes.
        push_section_header(&mut ext, EXT_KIND_STREAM_HEAD_TABLE, 1, 48);
        ext.extend_from_slice(&[0u8; 10]);
        let parsed = decode_extension(&ext);
        assert!(parsed.heads.is_empty());
    }

    #[test]
    fn sealed_footer_locates_and_covers_the_extension() {
        let heads = vec![StreamHeadEntry { stream_id: 7, last_version: 49, head_hash: [0x11; 32] }];
        let content_len = SEGMENT_HEADER_LEN as u64 + 500;
        let summary = SealSummary {
            segment_id: 7,
            epoch: 42,
            base_pos: 1000,
            batch_count: 5,
            event_count: 50,
            content_len,
        };
        let (footer, t) = encode_sealed_footer(&summary, &heads, &[]);
        // Footer = extension ++ 100-byte trailer.
        let ext_bytes = &footer[..footer.len() - SEGMENT_TRAILER_LEN];
        assert_eq!(t.ext_offset, content_len);
        assert_eq!(t.ext_len as usize, ext_bytes.len());
        assert_eq!(t.ext_crc, crc32c::crc32c(ext_bytes));
        // The trailer decodes and its locator fields point at the extension.
        let cat = decode_trailer(&footer[footer.len() - SEGMENT_TRAILER_LEN..]).unwrap();
        assert_eq!(cat.ext_offset, content_len);
        assert_eq!(cat.ext_len as usize, ext_bytes.len());
        assert_eq!(cat.ext_crc, t.ext_crc);
        assert_eq!(cat.event_count, 50);
        // Re-parse the extension the trailer points at.
        assert_eq!(decode_extension(ext_bytes).heads, heads);
    }
}
