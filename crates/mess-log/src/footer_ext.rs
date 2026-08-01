//! The sealed-segment footer **extension region** encode/decode (`bn-1d0`,
//! `bn-11g`): the durable Tier-1 head anchor (`StreamHeadTable`), the Path-C
//! retention certificates (`SnapshotAnchorList`), and the exact identity of the
//! SealPack this seal installed (`SealPackIdentity`).
//!
//! Byte layout is owned by **[`docs/spec/01-log-format.md`] §3.3.2/§3.3.3**;
//! the *semantics* of the fold hashes it carries are owned by
//! **[`docs/spec/05-fold-certificates.md`] §5 (G6) / §8.2**. This module is the
//! seal-path encoder the sealer drives: in Phase 3 the extension is always
//! empty (`ext_len == 0`); Phase 5 (bn-1d0) fills it with the fold anchors, and
//! a pack-mode seal (bn-11g) always adds the pack identity.
//!
//! # Region shape (§3.3.2)
//!
//! The region is a back-to-back sequence of **typed sections**, each a fixed
//! 16-byte header ([`crate::format::EXT_SECTION_HDR_LEN`]) followed by
//! `payload_len` bytes of fixed-size entries:
//!
//! ```text
//! section header (16 B): kind:u16 | section_flags:u16 | entry_count:u32 | payload_len:u64
//! StreamHeadEntry    (48 B): stream_id:u64     | last_version:u64        | head_hash:[u8;32]
//! SnapshotAnchor     (48 B): stream_id:u64     | version:u64             | chain_hash:[u8;32]
//! SealPackIdentity   (48 B): identity_kind:u16 | pack_format_version:u16 |
//!                            reserved:u32      | segment_id:u64          | identity:[u8;32]
//! ```
//!
//! A reader walks sections by hopping `16 + payload_len` and MUST advisory-skip
//! an unknown `kind` (§3.3.2, D-FMT-3) — [`decode_extension`] does. The whole
//! region is covered by the trailer's `ext_crc` (a separate CRC from
//! `footer_crc`, §3.3.1), so a partial rewrite is caught by recovery.
//!
//! # The pack identity is NOT advisory-skippable (§3.3.3, bn-11g)
//!
//! Every other section here degrades safely when it is unreadable: the anchors
//! are rebuilt by scanning. The `SealPackIdentity` does not, because "no
//! identity" is itself a meaningful state — it is the legacy footer, which
//! grants a same-coverage pack coverage-only trust. If a corrupt extension
//! read as "no identity", one flipped bit would *downgrade* trust instead of
//! degrading it, and a stale or copied pack would sail through.
//!
//! So the presence of the section is announced by
//! [`FOOTER_FLAG_SEAL_PACK_IDENTITY`](crate::format::FOOTER_FLAG_SEAL_PACK_IDENTITY)
//! in the **fixed trailer**, covered by `footer_crc`, not by `ext_crc`. A
//! reader that sees the flag set and cannot resolve the identity — bad
//! `ext_crc`, no kind-`3` section, an `identity_kind` it does not know —
//! installs no pack at all and reads the raw segment (§3.3.3 reader rule 2).
//! [`ParsedExtension::pack_identity`] is `None` in exactly those cases, which
//! is why callers must consult the flag and not just this field.
//!
//! [`docs/spec/01-log-format.md`]: ../../../../docs/spec/01-log-format.md
//! [`docs/spec/05-fold-certificates.md`]: ../../../../docs/spec/05-fold-certificates.md

use crate::fold_chain::Hash;
use crate::format::*;
use crate::sealer::{TrailerFields, encode_trailer};

/// One `StreamHeadEntry` (§3.3.2): the durable Tier-1 head anchor `A(S)` for a
/// stream with ≥1 committed event in the sealed segment (§5).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StreamHeadEntry {
    /// Interned stream id (`04-registry.md`).
    pub stream_id:    u64,
    /// Stream version of the stream's **last** event in this segment.
    pub last_version: u64,
    /// `h[last_version]` — the fold-chain value after that event.
    pub head_hash:    Hash,
}

/// One `SnapshotAnchor` (§3.3.2): the Path-C retention certificate recorded at
/// seal for a live snapshot at version `v` (§8.2, gap 9).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SnapshotAnchor {
    /// Interned stream id (`04-registry.md`).
    pub stream_id:  u64,
    /// The snapshot's `stream_version` `v` (0-based last-index).
    pub version:    u64,
    /// `h[v]` — the fold-chain value certifying the prefix `0..=v`.
    pub chain_hash: Hash,
}

/// The `SealPackIdentity` entry (§3.3.3, bn-11g): the exact SealPack a seal
/// installed, named by a hash whose **domain and version** are carried in
/// `identity_kind`.
///
/// This is the segment footer's half of the pack trust chain. The pack's half
/// is that it recomputes and verifies this same hash at every open, so a
/// reader compares a value it derived from the bytes in front of it against a
/// value the footer's `fsync` committed to — never a self-attestation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SealPackIdentity {
    /// Hash domain + version.
    /// [`SEAL_PACK_IDENTITY_HDRDIR_BLAKE3`] is the only value defined in v3;
    /// an unknown value MUST make the reader install no pack (§3.3.3 rule 2c).
    pub identity_kind:       u16,
    /// The named pack's own `format_version`. A pack whose header disagrees is
    /// not the named pack.
    pub pack_format_version: u16,
    /// MUST equal the trailer's `segment_id`.
    pub segment_id:          u64,
    /// The identity bytes under `identity_kind`.
    pub identity:            Hash,
}

impl SealPackIdentity {
    /// Whether this entry's `identity_kind` is one this build can check. An
    /// unknown kind is not an error to *parse* — it is a hard stop on
    /// *trusting a pack*, which is the caller's decision to make.
    #[must_use]
    pub fn kind_is_known(&self) -> bool {
        self.identity_kind == SEAL_PACK_IDENTITY_HDRDIR_BLAKE3
    }

    /// Lowercase hex of the identity — the operator-facing rendering used by
    /// `mess verify` / `mess inspect` and the refutation log lines.
    #[must_use]
    pub fn hex(&self) -> String { hex32(&self.identity) }
}

/// Lowercase hex of a 32-byte identity/anchor value.
#[must_use]
pub fn hex32(h: &Hash) -> String {
    let mut s = String::with_capacity(64);
    for b in h {
        use std::fmt::Write as _;
        let _ = write!(s, "{b:02x}");
    }
    s
}

/// The parsed contents of a footer extension region (known sections only;
/// unknown kinds are advisory-skipped, §3.3.2).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ParsedExtension {
    /// `StreamHeadTable` entries (kind `1`), in stored order.
    pub heads:         Vec<StreamHeadEntry>,
    /// `SnapshotAnchorList` entries (kind `2`), in stored order.
    pub anchors:       Vec<SnapshotAnchor>,
    /// The `SealPackIdentity` entry (kind `3`), if the region carried exactly
    /// one well-formed section of that kind (bn-11g).
    ///
    /// `None` means "this parse found no identity", which is **not** the same
    /// as "the footer named no pack" — see the module docs. The trailer's
    /// [`FOOTER_FLAG_SEAL_PACK_IDENTITY`](crate::format::FOOTER_FLAG_SEAL_PACK_IDENTITY)
    /// is the authority on whether one was written.
    pub pack_identity: Option<SealPackIdentity>,
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

fn push_section_header(
    buf: &mut Vec<u8>,
    kind: u16,
    entry_count: u32,
    payload_len: u64,
) {
    put_u16(buf, kind); // EXT_KIND_OFF
    put_u16(buf, 0); // EXT_SECTION_FLAGS_OFF — MUST be 0 in v3
    put_u32(buf, entry_count); // EXT_ENTRY_COUNT_OFF
    put_u64(buf, payload_len); // EXT_PAYLOAD_LEN_OFF
}

/// Encode the extension region bytes (§3.3.2/§3.3.3). Emits, in order: the
/// `SealPackIdentity` section (kind 3) iff `pack` is `Some`, a
/// `StreamHeadTable` section (kind 1) iff `heads` is non-empty, then a
/// `SnapshotAnchorList` section (kind 2) iff `anchors` is non-empty. When all
/// three are empty the region is empty (`Vec::new()`), the Phase-3
/// `ext_len == 0` shape. `payload_len == entry_count * 48` for each known kind.
///
/// The identity goes first so a reader that only wants it touches the fewest
/// bytes; section *order* is not normative and [`decode_extension`] does not
/// depend on it.
#[must_use]
pub fn encode_extension(
    heads: &[StreamHeadEntry],
    anchors: &[SnapshotAnchor],
    pack: Option<&SealPackIdentity>,
) -> Vec<u8> {
    let mut buf = Vec::new();
    if let Some(p) = pack {
        push_section_header(
            &mut buf,
            EXT_KIND_SEAL_PACK_IDENTITY,
            1,
            EXT_ENTRY_LEN as u64,
        );
        put_u16(&mut buf, p.identity_kind);
        put_u16(&mut buf, p.pack_format_version);
        put_u32(&mut buf, 0); // reserved — MUST be 0 in v3
        put_u64(&mut buf, p.segment_id);
        buf.extend_from_slice(&p.identity);
    }
    if !heads.is_empty() {
        let payload_len = (heads.len() * EXT_ENTRY_LEN) as u64;
        push_section_header(
            &mut buf,
            EXT_KIND_STREAM_HEAD_TABLE,
            heads.len() as u32,
            payload_len,
        );
        for e in heads {
            put_u64(&mut buf, e.stream_id);
            put_u64(&mut buf, e.last_version);
            buf.extend_from_slice(&e.head_hash);
        }
    }
    if !anchors.is_empty() {
        let payload_len = (anchors.len() * EXT_ENTRY_LEN) as u64;
        push_section_header(
            &mut buf,
            EXT_KIND_SNAPSHOT_ANCHOR_LIST,
            anchors.len() as u32,
            payload_len,
        );
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
    if ext.is_empty() { 0 } else { crc32c::crc32c(ext) }
}

/// Walk the extension region, collecting the known sections and advisory-
/// skipping any unknown `kind` (§3.3.2). Malformed input (a section header that
/// runs past the region, or a known section whose `payload_len != entry_count *
/// 48`) stops the walk and returns what was parsed so far — never panics, never
/// fails acceptance (batch acceptance is decided by the per-batch checksums,
/// not the extension).
///
/// The one section that does **not** follow "return what was parsed so far" is
/// `SealPackIdentity` (§3.3.3): a region carrying anything other than exactly
/// one well-formed kind-`3` section yields `pack_identity: None`, because a
/// partially-resolved identity is indistinguishable to the caller from a
/// resolved one and would let a wrong pack through.
#[must_use]
pub fn decode_extension(ext: &[u8]) -> ParsedExtension {
    let mut out = ParsedExtension::default();
    let mut identity_sections = 0usize;
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
                        stream_id:    rd_u64(c, 0),
                        last_version: rd_u64(c, 8),
                        head_hash:    c[16..48].try_into().unwrap(),
                    });
                }
            }
            EXT_KIND_SNAPSHOT_ANCHOR_LIST => {
                if payload_len != entry_count * EXT_ENTRY_LEN {
                    break;
                }
                for c in payload.chunks_exact(EXT_ENTRY_LEN) {
                    out.anchors.push(SnapshotAnchor {
                        stream_id:  rd_u64(c, 0),
                        version:    rd_u64(c, 8),
                        chain_hash: c[16..48].try_into().unwrap(),
                    });
                }
            }
            // §3.3.3: exactly one entry, and exactly one such section in the
            // region. A malformed count, or a second section, is not "a
            // slightly wrong identity" — it is an identity that cannot be
            // resolved, and the caller must fail closed rather than pick one.
            // So a duplicate poisons the result (see after the loop) instead
            // of silently letting the first or last win.
            EXT_KIND_SEAL_PACK_IDENTITY => {
                identity_sections += 1;
                if entry_count != 1 || payload_len != EXT_ENTRY_LEN {
                    break;
                }
                let c = payload;
                out.pack_identity = Some(SealPackIdentity {
                    identity_kind:       rd_u16(c, 0),
                    pack_format_version: rd_u16(c, 2),
                    segment_id:          rd_u64(c, 8),
                    identity:            c[16..48].try_into().unwrap(),
                });
            }
            _ => { /* unknown kind: advisory-skip by payload_len (§3.3.2) */ }
        }
        off = payload_end;
    }
    if identity_sections != 1 {
        out.pack_identity = None;
    }
    out
}

/// The segment-catalog summary the seal path already has when it composes the
/// footer (§3.3.1): the fixed-trailer scalar fields, minus the extension
/// locator (which [`encode_sealed_footer`] derives from the encoded extension).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SealSummary {
    /// MUST equal the `SegmentHeader.segment_id`.
    pub segment_id:  u64,
    /// The A9 epoch (R3), MUST equal the header's `epoch`.
    pub epoch:       u64,
    /// MUST equal the header's `base_pos`.
    pub base_pos:    u64,
    /// Number of accepted batches in the segment.
    pub batch_count: u64,
    /// Total events (Σ `frame_count`) in the segment.
    pub event_count: u64,
    /// First byte after the last `CommitMarker` (the writer's `write_off`),
    /// i.e. `ext_offset` — where the extension region begins.
    pub content_len: u64,
}

/// Compose a complete sealed-segment footer: the extension region
/// (§3.3.2/§3.3.3) immediately followed by the fixed 100-byte trailer (§3.3.1)
/// whose `ext_offset`/`ext_len`/`ext_crc` locate and cover it. Returns
/// `(footer_bytes, trailer_fields)`; `footer_bytes == extension ++ trailer` is
/// the single durable write the seal `fdatasync` covers (§6).
///
/// When `pack` is `Some`, the identity section is emitted **and** the trailer's
/// [`FOOTER_FLAG_SEAL_PACK_IDENTITY`] is set. The two are set together, here,
/// on purpose: they are one fact recorded in two coverage domains
/// (`ext_crc` and `footer_crc`), and a writer that could set one without the
/// other would be able to produce the exact ambiguity §3.3.3 exists to
/// forbid.
#[must_use]
pub fn encode_sealed_footer(
    summary: &SealSummary,
    heads: &[StreamHeadEntry],
    anchors: &[SnapshotAnchor],
    pack: Option<&SealPackIdentity>,
) -> (Vec<u8>, TrailerFields) {
    let ext = encode_extension(heads, anchors, pack);
    let ext_crc = extension_crc(&ext);
    let t = TrailerFields {
        flags: if pack.is_some() { FOOTER_FLAG_SEAL_PACK_IDENTITY } else { 0 },
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
pub fn head_anchor_for(
    ext: &ParsedExtension,
    stream_id: u64,
) -> Option<(u64, Hash)> {
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
        let ext = encode_extension(&[], &[], None);
        assert!(ext.is_empty());
        assert_eq!(extension_crc(&ext), 0);
        assert_eq!(decode_extension(&ext), ParsedExtension::default());
    }

    #[test]
    fn heads_and_anchors_roundtrip() {
        let heads = vec![
            StreamHeadEntry {
                stream_id:    7,
                last_version: 49,
                head_hash:    genesis(7),
            },
            StreamHeadEntry {
                stream_id:    9,
                last_version: 3,
                head_hash:    genesis(9),
            },
        ];
        let anchors = vec![SnapshotAnchor {
            stream_id:  7,
            version:    19,
            chain_hash: genesis(7),
        }];
        let ext = encode_extension(&heads, &anchors, None);
        // payload_len == entry_count * 48 for each section, plus two headers.
        assert_eq!(
            ext.len(),
            EXT_SECTION_HDR_LEN * 2 + (2 + 1) * EXT_ENTRY_LEN
        );
        let parsed = decode_extension(&ext);
        assert_eq!(parsed.heads, heads);
        assert_eq!(parsed.anchors, anchors);
        assert_eq!(head_anchor_for(&parsed, 9), Some((3, genesis(9))));
        assert_eq!(head_anchor_for(&parsed, 100), None);
    }

    #[test]
    fn unknown_section_kind_is_skipped() {
        // Build: [unknown kind 999 with 16-byte payload][StreamHeadTable].
        let heads = vec![StreamHeadEntry {
            stream_id:    1,
            last_version: 0,
            head_hash:    [0xAB; 32],
        }];
        let known = encode_extension(&heads, &[], None);
        let mut ext = Vec::new();
        push_section_header(&mut ext, 999, 0, 16);
        ext.extend_from_slice(&[0xEE; 16]);
        ext.extend_from_slice(&known);
        let parsed = decode_extension(&ext);
        assert_eq!(
            parsed.heads, heads,
            "known section after an unknown one is still read"
        );
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
        let heads = vec![StreamHeadEntry {
            stream_id:    7,
            last_version: 49,
            head_hash:    [0x11; 32],
        }];
        let content_len = SEGMENT_HEADER_LEN as u64 + 500;
        let summary = SealSummary {
            segment_id: 7,
            epoch: 42,
            base_pos: 1000,
            batch_count: 5,
            event_count: 50,
            content_len,
        };
        let (footer, t) = encode_sealed_footer(&summary, &heads, &[], None);
        // Footer = extension ++ 100-byte trailer.
        let ext_bytes = &footer[..footer.len() - SEGMENT_TRAILER_LEN];
        assert_eq!(t.ext_offset, content_len);
        assert_eq!(t.ext_len as usize, ext_bytes.len());
        assert_eq!(t.ext_crc, crc32c::crc32c(ext_bytes));
        // The trailer decodes and its locator fields point at the extension.
        let cat = decode_trailer(&footer[footer.len() - SEGMENT_TRAILER_LEN..])
            .unwrap();
        assert_eq!(cat.ext_offset, content_len);
        assert_eq!(cat.ext_len as usize, ext_bytes.len());
        assert_eq!(cat.ext_crc, t.ext_crc);
        assert_eq!(cat.event_count, 50);
        // Re-parse the extension the trailer points at.
        assert_eq!(decode_extension(ext_bytes).heads, heads);
    }
}

// ---------------------------------------------------------------------------
// bn-11g: the SealPackIdentity section (spec 01 §3.3.3)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod pack_identity_tests {
    use super::*;
    use crate::sealer::decode_trailer;

    fn ident(byte: u8) -> SealPackIdentity {
        SealPackIdentity {
            identity_kind:       SEAL_PACK_IDENTITY_HDRDIR_BLAKE3,
            pack_format_version: 1,
            segment_id:          7,
            identity:            [byte; 32],
        }
    }

    fn summary() -> SealSummary {
        SealSummary {
            segment_id:  7,
            epoch:       42,
            base_pos:    1000,
            batch_count: 5,
            event_count: 50,
            content_len: SEGMENT_HEADER_LEN as u64 + 500,
        }
    }

    #[test]
    fn identity_section_roundtrips_and_is_one_48_byte_entry() {
        let id = ident(0xA7);
        let ext = encode_extension(&[], &[], Some(&id));
        assert_eq!(ext.len(), EXT_SECTION_HDR_LEN + EXT_ENTRY_LEN);
        assert_eq!(rd_u16(&ext, EXT_KIND_OFF), EXT_KIND_SEAL_PACK_IDENTITY);
        assert_eq!(rd_u32(&ext, EXT_ENTRY_COUNT_OFF), 1);
        assert_eq!(rd_u64(&ext, EXT_PAYLOAD_LEN_OFF), EXT_ENTRY_LEN as u64);
        let parsed = decode_extension(&ext);
        assert_eq!(parsed.pack_identity, Some(id));
        assert!(parsed.pack_identity.unwrap().kind_is_known());
        assert_eq!(parsed.pack_identity.unwrap().hex(), "a7".repeat(32));
    }

    /// The two halves of the binding are written together: the section (under
    /// `ext_crc`) and the trailer flag (under `footer_crc`). A writer that
    /// could set one without the other would produce the exact ambiguity
    /// §3.3.3 exists to forbid.
    #[test]
    fn a_named_footer_sets_the_flag_and_a_plain_one_does_not() {
        let id = ident(0x5C);
        let (footer, t) = encode_sealed_footer(&summary(), &[], &[], Some(&id));
        assert!(t.names_seal_pack(), "TrailerFields flag");
        let cat = decode_trailer(&footer).expect("trailer decodes");
        assert!(cat.names_seal_pack(), "decoded trailer flag");
        assert_eq!(cat.flags, FOOTER_FLAG_SEAL_PACK_IDENTITY);
        assert_eq!(cat.ext_len as usize, EXT_SECTION_HDR_LEN + EXT_ENTRY_LEN);
        let ext = &footer[..footer.len() - SEGMENT_TRAILER_LEN];
        assert_eq!(cat.ext_crc, crc32c::crc32c(ext));
        assert_eq!(decode_extension(ext).pack_identity, Some(id));

        // ...and the legacy shape is byte-for-byte the pre-bn-11g footer.
        let (plain, pt) = encode_sealed_footer(&summary(), &[], &[], None);
        assert!(!pt.names_seal_pack());
        assert_eq!(plain.len(), SEGMENT_TRAILER_LEN, "empty extension");
        let plain_cat = decode_trailer(&plain).expect("legacy trailer decodes");
        assert_eq!(plain_cat.flags, 0);
        assert_eq!(plain_cat.ext_len, 0);
        assert_eq!(plain_cat.ext_crc, 0);
        assert!(!plain_cat.names_seal_pack());
    }

    /// A legacy footer parses **unchanged** under the new decoder: the whole
    /// point of riding a reserved flag bit and a new section kind.
    #[test]
    fn a_legacy_footer_parses_identically_and_names_nothing() {
        let s = summary();
        let legacy = crate::sealer::encode_trailer(&TrailerFields::phase3(
            s.segment_id,
            s.epoch,
            s.base_pos,
            s.batch_count,
            s.event_count,
            s.content_len,
        ));
        let (fresh, _) = encode_sealed_footer(&s, &[], &[], None);
        assert_eq!(
            fresh.as_slice(),
            legacy.as_slice(),
            "a no-pack seal must write the pre-bn-11g bytes exactly"
        );
        let cat = decode_trailer(&legacy).expect("decodes");
        assert!(!cat.names_seal_pack());
        assert_eq!(decode_extension(&[]).pack_identity, None);
    }

    /// Identity-bit corruption inside the extension: `ext_crc` no longer
    /// matches, so a reader must treat the identity as unresolvable — while the
    /// trailer flag, in the *other* checksum domain, still says one was
    /// written. That asymmetry is what stops a bit flip being a silent trust
    /// downgrade to the legacy coverage-only policy.
    #[test]
    fn a_flipped_identity_bit_breaks_ext_crc_but_not_the_flag() {
        let id = ident(0x11);
        let (mut footer, _) =
            encode_sealed_footer(&summary(), &[], &[], Some(&id));
        let ext_len = footer.len() - SEGMENT_TRAILER_LEN;
        // Flip a bit inside the stored identity bytes.
        footer[EXT_SECTION_HDR_LEN + 16] ^= 0x01;

        let cat = decode_trailer(&footer).expect("trailer still valid");
        assert!(
            cat.names_seal_pack(),
            "the flag is under footer_crc, untouched by extension damage"
        );
        let ext = &footer[..ext_len];
        assert_ne!(
            crc32c::crc32c(ext),
            cat.ext_crc,
            "the damaged extension must fail its own CRC"
        );
        // And the value that *would* be read is not the one that was written,
        // so trusting it without the CRC check would admit the wrong pack.
        assert_ne!(decode_extension(ext).pack_identity, Some(id));
    }

    /// An unknown `identity_kind` parses (the *section* kind is known) but is
    /// not checkable, so the caller must refuse to trust any pack — §3.3.3
    /// rule 2c. It is deliberately NOT advisory-skipped into `None`, so the
    /// caller can tell "unknown kind" from "no section".
    #[test]
    fn an_unknown_identity_kind_parses_but_is_not_known() {
        let mut id = ident(0x22);
        id.identity_kind = 0xBEEF;
        let ext = encode_extension(&[], &[], Some(&id));
        let parsed = decode_extension(&ext).pack_identity.expect("parsed");
        assert_eq!(parsed.identity_kind, 0xBEEF);
        assert!(!parsed.kind_is_known());
    }

    /// Two identity sections are an unresolvable name, not a choice: the
    /// decoder yields `None` rather than letting first or last win.
    #[test]
    fn duplicate_identity_sections_resolve_to_nothing() {
        let mut ext = encode_extension(&[], &[], Some(&ident(0x01)));
        ext.extend_from_slice(&encode_extension(&[], &[], Some(&ident(0x02))));
        assert_eq!(decode_extension(&ext).pack_identity, None);
    }

    /// A malformed kind-3 section (wrong `entry_count` / `payload_len`) is
    /// likewise unresolvable, and never panics.
    #[test]
    fn a_malformed_identity_section_resolves_to_nothing() {
        for (count, payload_len) in
            [(0u32, EXT_ENTRY_LEN as u64), (2, 96), (1, 47), (1, 0)]
        {
            let mut ext = Vec::new();
            push_section_header(
                &mut ext,
                EXT_KIND_SEAL_PACK_IDENTITY,
                count,
                payload_len,
            );
            ext.resize(EXT_SECTION_HDR_LEN + payload_len as usize, 0xEE);
            assert_eq!(
                decode_extension(&ext).pack_identity,
                None,
                "count={count} payload_len={payload_len}"
            );
        }
    }

    /// The identity coexists with the fold anchors, and an unknown section
    /// between them is still advisory-skipped (§3.3.2 forward compat).
    #[test]
    fn identity_coexists_with_anchors_and_an_unknown_kind() {
        let id = ident(0x33);
        let heads = vec![StreamHeadEntry {
            stream_id:    3,
            last_version: 8,
            head_hash:    [0x44; 32],
        }];
        let known = encode_extension(&heads, &[], Some(&id));
        // Splice an unknown section in front of everything.
        let mut ext = Vec::new();
        push_section_header(&mut ext, 4242, 0, 8);
        ext.extend_from_slice(&[0xEE; 8]);
        ext.extend_from_slice(&known);
        let parsed = decode_extension(&ext);
        assert_eq!(parsed.pack_identity, Some(id));
        assert_eq!(parsed.heads, heads);
    }
}
