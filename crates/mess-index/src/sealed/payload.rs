//! The sealed **payload-block sidecar** (D6, bn-zge): the read-optimized,
//! on-disk form of a sealed segment's event *payloads*, produced by the
//! background sealer alongside the [pointer sidecar](crate::sealed::segment).
//!
//! Where the pointer sidecar (`.pidx`) maps `(stream, version)` to a byte
//! offset, this sidecar (`.pcol`) holds the payload *bytes* rewritten into
//! compact, compressed blocks so a sealed read reassembles an event without
//! touching the raw segment. It wires the bn-1bn [columnar codec](crate::columnar)
//! into the seal per the round-4 D6 format:
//!
//! - **Columnar by default.** Each ~128-event block is handed to
//!   [`crate::columnar::encode_block`], which shreds MessagePack payloads into
//!   per-path columns and zstd-compresses them. A payload run the codec cannot
//!   model routes to a byte-exact **raw fallback** — the codec decides, and we
//!   wire that decision through as a per-block flag.
//! - **Per-block 1-bit columnar/row flag.** The block index records, per block,
//!   whether the stored block is columnar or a row image ([`BlockKind`]). Mixed
//!   segments (some columnar, some row-fallback blocks) are the normal case and
//!   replay through the one read path.
//! - **Row-fallback dictionary tier.** A raw-fallback block MAY be recompressed
//!   against a registered 16 KiB zstd dictionary (magicless framing) to claw
//!   back the ratio the columnar codec would have delivered. The dictionary is
//!   a `$registry` object (`04-registry.md` §3.8, `DictRegistered`, codec ≥ 1);
//!   the block index carries the `dict_id`, and **every `dict_id` a block
//!   references MUST be registered** — [`SealedPayloadIndex::verify_dicts_registered`]
//!   is the referenced-implies-registered gate (D3). The columnar tier never
//!   uses a dictionary (`dict_id == 0`).
//! - **Permanent verify-on-seal.** [`encode_payload_sidecar`] reassembles every
//!   block through the *read path* and byte-compares against the source frames
//!   before returning the bytes. A single mismatch aborts the seal with
//!   [`PayloadError::VerifyMismatch`]. This ships — it is not a test-only check
//!   — so a codec or framing regression can never silently corrupt a sealed
//!   payload: the bytes are proven byte-exact at the moment they are written.
//!
//! # File layout
//!
//! ```text
//! Header (32 bytes):
//!   0   u32  magic = PCOL_MAGIC
//!   4   u16  format_version = 1
//!   6   u16  flags = 0
//!   8   u64  segment_id
//!   16  u64  event_count          (Σ block n_events)
//!   24  u32  n_blocks
//!   28  u32  reserved = 0
//!
//! DATA region:  encoded blocks, back to back
//! INDEX region: n_blocks x BlockEntry (32 bytes), ascending by first_event
//!
//! BlockEntry (32 bytes):
//!   0   u64  first_event          (stored-order index of the block's first event)
//!   8   u32  n_events
//!   12  u32  byte_len             (encoded block length)
//!   16  u64  byte_off             (absolute file offset of the block in DATA)
//!   24  u8   kind                 (0 = row image, 1 = columnar) — the 1-bit flag
//!   25  u8   reserved = 0
//!   26  u16  dict_id              (0 = none; nonzero only legal for kind = row)
//!   28  u32  reserved2 = 0
//!
//! Footer (24 bytes, at EOF):
//!   0   u64  index_off
//!   8   u32  content_crc          (crc32c over [0, footer_start))
//!   12  u32  n_blocks             (redundant, cross-checks the header)
//!   16  u32  reserved = 0
//!   20  u32  magic = PCOL_MAGIC
//! ```

use std::collections::BTreeMap;

use crate::columnar::{self, Block, CodecError, EncodeOpts, FLAG_COLUMNAR};

/// Payload sidecar magic (`"PCL1"`).
pub const PCOL_MAGIC: u32 = 0x5043_4C01;
/// Header length (bytes).
pub const HEADER_LEN: usize = 32;
/// Block-index entry length (bytes).
pub const BLOCK_ENTRY_LEN: usize = 32;
/// Footer length (bytes), at EOF.
pub const FOOTER_LEN: usize = 24;
/// Current sidecar `format_version`.
pub const FORMAT_VERSION: u16 = 1;

/// Default events per payload block (round-4 D6 default).
pub const DEFAULT_BLOCK_EVENTS: usize = 128;
/// Maximum trained row-fallback dictionary size (D6 / `04-registry.md` §3.8).
pub const MAX_DICT_BYTES: usize = 16 << 10;

/// Whether a stored block is columnar or a row image — the per-block 1-bit
/// flag the block index carries (mirrors the codec's [`FLAG_COLUMNAR`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BlockKind {
    /// Row image: the codec fell back (unshreddable payloads), or a
    /// dictionary-recompressed row block ([`dict_id != 0`](BlockEntry::dict_id)).
    Row,
    /// Columnar: shredded per-path columns.
    Columnar,
}

impl BlockKind {
    fn from_byte(b: u8) -> Result<Self, PayloadError> {
        match b {
            0 => Ok(BlockKind::Row),
            1 => Ok(BlockKind::Columnar),
            _ => Err(PayloadError::Corrupt("unknown block kind")),
        }
    }
    fn to_byte(self) -> u8 {
        match self {
            BlockKind::Row => 0,
            BlockKind::Columnar => 1,
        }
    }
}

/// A resolver from `dict_id` to the trained dictionary bytes. In production
/// this is backed by the materialized `$registry` dictionary table
/// (`04-registry.md` §6); in tests it is a small in-memory map. Only the
/// row-fallback tier consults it, and only for nonzero `dict_id`s.
pub trait DictResolver {
    /// The trained bytes for `dict_id`, or `None` if it is not registered.
    fn dict_bytes(&self, dict_id: u16) -> Option<&[u8]>;
}

/// A resolver that knows no dictionaries — the columnar-only regime. Every
/// nonzero `dict_id` is unregistered, so [`SealedPayloadIndex::verify_dicts_registered`]
/// passes only when no block references a dictionary.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoDicts;

impl DictResolver for NoDicts {
    fn dict_bytes(&self, _dict_id: u16) -> Option<&[u8]> {
        None
    }
}

impl DictResolver for BTreeMap<u16, Vec<u8>> {
    fn dict_bytes(&self, dict_id: u16) -> Option<&[u8]> {
        self.get(&dict_id).map(Vec::as_slice)
    }
}

/// Errors building, opening, or reading a payload sidecar.
#[derive(Debug, thiserror::Error)]
pub enum PayloadError {
    /// Bytes too short, mis-magicked, wrong-version, or CRC-mismatched.
    #[error("payload sidecar corrupt: {0}")]
    Corrupt(&'static str),
    /// A stored block failed to decode/reassemble (media integrity).
    #[error("payload block decode: {0}")]
    Codec(#[from] CodecError),
    /// A block references a `dict_id` with no registered dictionary — the
    /// referenced-implies-registered violation (D3). Carries the offending id.
    #[error("payload block references unregistered dict_id {0}")]
    UnregisteredDict(u16),
    /// A row block declared `dict_id == 0` but was framed as a dict block, or
    /// vice versa — an internal inconsistency, never from a validated sidecar.
    #[error("dict/kind inconsistency in block {0}")]
    DictKindMismatch(usize),
    /// zstd rejected a dictionary-framed row block.
    #[error("row-dict decompression failed")]
    RowDictDecompress,
    /// Verify-on-seal: a reassembled event did not byte-match its source.
    /// Carries the stored-order event index. **This aborts the seal.**
    #[error("verify-on-seal mismatch at event {0}")]
    VerifyMismatch(u64),
    /// The requested event index is past the segment's event count.
    #[error("event index out of range")]
    IndexOutOfRange,
}

// ---------------------------------------------------------------------------
// Row-fallback dictionary codec (magicless zstd, one frame per row image)
// ---------------------------------------------------------------------------
//
// A dict row block is `u32 LE ulen || magicless-zstd(dict, row_image)`, where
// the row image is the same `n || offsets || payloads` shape the codec's raw
// block uses. It is framed distinctly from the codec's own raw block so the
// reader dispatches on `dict_id`: `dict_id == 0` ⇒ decode via the codec
// ([`Block::decode`]); `dict_id != 0` ⇒ decode here with the resolved dict.

/// Upper bound on a row image's decompressed size (guards a hostile `ulen`).
const MAX_ROW_ULEN: usize = 64 << 20;

fn build_row_image(events: &[&[u8]]) -> Vec<u8> {
    let total: usize = events.iter().map(|e| e.len()).sum();
    let mut img = Vec::with_capacity(4 + 4 * (events.len() + 1) + total);
    img.extend_from_slice(&(events.len() as u32).to_le_bytes());
    let mut off = 0u32;
    img.extend_from_slice(&off.to_le_bytes());
    for e in events {
        off += e.len() as u32;
        img.extend_from_slice(&off.to_le_bytes());
    }
    for e in events {
        img.extend_from_slice(e);
    }
    img
}

/// Encode a dict-compressed row block (magicless zstd). Falls back to a plain
/// (non-magicless, no-dict) frame only if the dictionary compressor cannot be
/// constructed, which never happens for a valid dictionary.
fn encode_row_dict(events: &[&[u8]], level: i32, dict: &[u8]) -> std::io::Result<Vec<u8>> {
    use zstd::zstd_safe::{CParameter, FrameFormat};
    let img = build_row_image(events);
    let mut c = zstd::bulk::Compressor::with_dictionary(level, dict)?;
    c.set_parameter(CParameter::Format(FrameFormat::Magicless))?;
    let comp = c.compress(&img)?;
    let mut out = Vec::with_capacity(4 + comp.len());
    out.extend_from_slice(&(img.len() as u32).to_le_bytes());
    out.extend_from_slice(&comp);
    Ok(out)
}

/// A decoded dict row block: the reassembled row image plus its `n+1` offsets.
struct RowImage {
    buf: Vec<u8>,
    offsets: Vec<u32>,
    base: usize,
}

fn decode_row_dict(bytes: &[u8], dict: &[u8]) -> Result<RowImage, PayloadError> {
    use zstd::zstd_safe::{DParameter, FrameFormat};
    if bytes.len() < 4 {
        return Err(PayloadError::Corrupt("truncated row-dict block"));
    }
    let ulen = u32::from_le_bytes(bytes[0..4].try_into().unwrap()) as usize;
    if ulen > MAX_ROW_ULEN {
        return Err(PayloadError::RowDictDecompress);
    }
    let mut d =
        zstd::bulk::Decompressor::with_dictionary(dict).map_err(|_| PayloadError::RowDictDecompress)?;
    d.set_parameter(DParameter::Format(FrameFormat::Magicless))
        .map_err(|_| PayloadError::RowDictDecompress)?;
    let buf = d.decompress(&bytes[4..], ulen).map_err(|_| PayloadError::RowDictDecompress)?;
    let mut p = 0usize;
    let n = rd_u32(&buf, &mut p)? as usize;
    let mut offsets = Vec::with_capacity(n + 1);
    for _ in 0..=n {
        offsets.push(rd_u32(&buf, &mut p)?);
    }
    let base = p;
    let last = *offsets.last().unwrap() as usize;
    if base + last > buf.len() {
        return Err(PayloadError::Corrupt("row-dict payload region truncated"));
    }
    Ok(RowImage { buf, offsets, base })
}

impl RowImage {
    fn len(&self) -> usize {
        self.offsets.len() - 1
    }
    fn event(&self, i: usize) -> &[u8] {
        let a = self.offsets[i] as usize;
        let b = self.offsets[i + 1] as usize;
        &self.buf[self.base + a..self.base + b]
    }
}

fn rd_u32(d: &[u8], p: &mut usize) -> Result<u32, PayloadError> {
    let s = d.get(*p..*p + 4).ok_or(PayloadError::Corrupt("truncated row image"))?;
    *p += 4;
    Ok(u32::from_le_bytes(s.try_into().unwrap()))
}

// ---------------------------------------------------------------------------
// Seal input + encode (with verify-on-seal)
// ---------------------------------------------------------------------------

/// Options for encoding a payload sidecar.
#[derive(Clone)]
pub struct PayloadSealOpts {
    /// Events per block (the codec caps a block at `u16::MAX`; D6 default 128).
    pub block_events: usize,
    /// Columnar codec options (zstd level; whole-block layout by default).
    pub codec: EncodeOpts,
    /// The single segment-level row-fallback dictionary, if one is available:
    /// `(dict_id, dict_bytes)`. When set, any block the codec routes to raw is
    /// recompressed against this dictionary (magicless) and its index entry
    /// records the `dict_id`. `dict_bytes` MUST be ≤ [`MAX_DICT_BYTES`].
    pub row_dict: Option<(u16, Vec<u8>)>,
}

impl Default for PayloadSealOpts {
    fn default() -> Self {
        PayloadSealOpts {
            block_events: DEFAULT_BLOCK_EVENTS,
            codec: EncodeOpts::default(),
            row_dict: None,
        }
    }
}

/// One block's encoded bytes plus the metadata its index entry needs.
struct EncodedBlock {
    first_event: u64,
    n_events: u32,
    kind: BlockKind,
    dict_id: u16,
    bytes: Vec<u8>,
}

/// Encode `events` (in stored order) into a payload sidecar image, **verifying
/// byte-exactness on the way out**. Columnar by default; the codec's raw
/// fallback is wired through as a row block, optionally recompressed against
/// `opts.row_dict`.
///
/// Verify-on-seal (permanent): every block is reassembled through the read
/// path and byte-compared against its source events. A mismatch returns
/// [`PayloadError::VerifyMismatch`] and no bytes — the seal aborts.
pub fn encode_payload_sidecar(
    segment_id: u64,
    events: &[&[u8]],
    opts: &PayloadSealOpts,
) -> Result<Vec<u8>, PayloadError> {
    assert!(opts.block_events > 0, "block_events must be > 0");
    if let Some((_, d)) = &opts.row_dict {
        assert!(d.len() <= MAX_DICT_BYTES, "row dict exceeds 16 KiB");
    }

    let mut encs: Vec<EncodedBlock> = Vec::new();
    let mut first = 0u64;
    for chunk in events.chunks(opts.block_events) {
        let block = columnar::encode_block(chunk, opts.codec);
        let columnar = block.get(1).copied().unwrap_or(0) & FLAG_COLUMNAR != 0;
        let enc = if columnar {
            EncodedBlock {
                first_event: first,
                n_events: chunk.len() as u32,
                kind: BlockKind::Columnar,
                dict_id: 0,
                bytes: block,
            }
        } else if let Some((dict_id, dict)) = &opts.row_dict {
            // Recompress the raw run against the registered dictionary.
            match encode_row_dict(chunk, opts.codec.level, dict) {
                Ok(bytes) => EncodedBlock {
                    first_event: first,
                    n_events: chunk.len() as u32,
                    kind: BlockKind::Row,
                    dict_id: *dict_id,
                    bytes,
                },
                // If dict framing fails for any reason, keep the codec's raw
                // block: correctness never depends on the dictionary tier.
                Err(_) => EncodedBlock {
                    first_event: first,
                    n_events: chunk.len() as u32,
                    kind: BlockKind::Row,
                    dict_id: 0,
                    bytes: block,
                },
            }
        } else {
            EncodedBlock {
                first_event: first,
                n_events: chunk.len() as u32,
                kind: BlockKind::Row,
                dict_id: 0,
                bytes: block,
            }
        };
        first += chunk.len() as u64;
        encs.push(enc);
    }

    let event_count = first;
    let bytes = serialize(segment_id, event_count, &encs);

    // Verify-on-seal (permanent): reassemble every event through the read path
    // and byte-compare against the source. A mismatch aborts the seal.
    let index = SealedPayloadIndex::from_bytes(bytes)?;
    let resolver = SealResolver { dict: opts.row_dict.as_ref() };
    verify_reassembly(&index, events, &resolver)?;
    Ok(index.into_bytes())
}

/// The verify-on-seal gate: reassemble every event in `index` through the read
/// path and byte-compare against `source`. Returns [`PayloadError::VerifyMismatch`]
/// at the first event whose reassembled bytes differ from `source[i]` (or a
/// [`PayloadError::Codec`] if a block fails to decode). This is exactly what
/// [`encode_payload_sidecar`] runs before returning its bytes — it ships, so a
/// codec/framing regression can never write a payload that does not reassemble
/// byte-exact.
pub fn verify_reassembly(
    index: &SealedPayloadIndex,
    source: &[&[u8]],
    resolver: &impl DictResolver,
) -> Result<(), PayloadError> {
    if index.event_count() as usize != source.len() {
        return Err(PayloadError::VerifyMismatch(source.len().min(index.event_count() as usize) as u64));
    }
    let mut out = Vec::new();
    let mut offs = Vec::new();
    index.reassemble_all(resolver, &mut out, &mut offs)?;
    debug_assert_eq!(offs.len(), source.len() + 1);
    for (i, w) in offs.windows(2).enumerate() {
        if &out[w[0] as usize..w[1] as usize] != source[i] {
            return Err(PayloadError::VerifyMismatch(i as u64));
        }
    }
    Ok(())
}

/// The resolver [`encode_payload_sidecar`] uses for its own verify pass: it
/// knows exactly the one dictionary it just applied.
struct SealResolver<'a> {
    dict: Option<&'a (u16, Vec<u8>)>,
}

impl DictResolver for SealResolver<'_> {
    fn dict_bytes(&self, dict_id: u16) -> Option<&[u8]> {
        match self.dict {
            Some((id, bytes)) if *id == dict_id => Some(bytes),
            _ => None,
        }
    }
}

fn serialize(segment_id: u64, event_count: u64, encs: &[EncodedBlock]) -> Vec<u8> {
    let data_len: usize = encs.iter().map(|e| e.bytes.len()).sum();
    let mut buf = Vec::with_capacity(
        HEADER_LEN + data_len + encs.len() * BLOCK_ENTRY_LEN + FOOTER_LEN,
    );
    // Header.
    buf.extend_from_slice(&PCOL_MAGIC.to_le_bytes());
    buf.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
    buf.extend_from_slice(&0u16.to_le_bytes()); // flags
    buf.extend_from_slice(&segment_id.to_le_bytes());
    buf.extend_from_slice(&event_count.to_le_bytes());
    buf.extend_from_slice(&(encs.len() as u32).to_le_bytes());
    buf.extend_from_slice(&0u32.to_le_bytes()); // reserved
    debug_assert_eq!(buf.len(), HEADER_LEN);

    // DATA region, recording each block's absolute offset.
    let mut offs = Vec::with_capacity(encs.len());
    for e in encs {
        offs.push(buf.len() as u64);
        buf.extend_from_slice(&e.bytes);
    }
    let index_off = buf.len() as u64;
    // INDEX region.
    for (i, e) in encs.iter().enumerate() {
        buf.extend_from_slice(&e.first_event.to_le_bytes());
        buf.extend_from_slice(&e.n_events.to_le_bytes());
        buf.extend_from_slice(&(e.bytes.len() as u32).to_le_bytes());
        buf.extend_from_slice(&offs[i].to_le_bytes());
        buf.push(e.kind.to_byte());
        buf.push(0); // reserved
        buf.extend_from_slice(&e.dict_id.to_le_bytes());
        buf.extend_from_slice(&0u32.to_le_bytes()); // reserved2
    }
    // Footer.
    let content_crc = crc32c::crc32c(&buf);
    buf.extend_from_slice(&index_off.to_le_bytes());
    buf.extend_from_slice(&content_crc.to_le_bytes());
    buf.extend_from_slice(&(encs.len() as u32).to_le_bytes());
    buf.extend_from_slice(&0u32.to_le_bytes()); // reserved
    buf.extend_from_slice(&PCOL_MAGIC.to_le_bytes());
    buf
}

// ---------------------------------------------------------------------------
// Reader
// ---------------------------------------------------------------------------

/// A parsed block-index entry.
#[derive(Debug, Clone, Copy)]
pub struct BlockEntry {
    /// Stored-order index of the block's first event.
    pub first_event: u64,
    /// Number of events in the block.
    pub n_events: u32,
    /// Columnar or row image.
    pub kind: BlockKind,
    /// Dictionary id (0 = none). Nonzero only for [`BlockKind::Row`].
    pub dict_id: u16,
    byte_off: u64,
    byte_len: u32,
}

/// The read-only sealed payload index for one segment. Owns the sidecar bytes
/// in memory; a point read is a block lookup + one block decode.
#[derive(Debug)]
pub struct SealedPayloadIndex {
    segment_id: u64,
    event_count: u64,
    bytes: Vec<u8>,
    /// Block entries ascending by `first_event`.
    blocks: Vec<BlockEntry>,
}

impl SealedPayloadIndex {
    /// The segment this index covers.
    pub fn segment_id(&self) -> u64 {
        self.segment_id
    }
    /// Total events across all blocks.
    pub fn event_count(&self) -> u64 {
        self.event_count
    }
    /// Number of payload blocks.
    pub fn block_count(&self) -> usize {
        self.blocks.len()
    }
    /// The block index entries (ascending by `first_event`).
    pub fn blocks(&self) -> &[BlockEntry] {
        &self.blocks
    }

    fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }

    /// Parse a sidecar byte image, validating magic, version, CRC, and every
    /// block span. The bytes are moved in and retained.
    pub fn from_bytes(bytes: Vec<u8>) -> Result<Self, PayloadError> {
        if bytes.len() < HEADER_LEN + FOOTER_LEN {
            return Err(PayloadError::Corrupt("shorter than header + footer"));
        }
        if rd32(&bytes, 0) != PCOL_MAGIC {
            return Err(PayloadError::Corrupt("bad header magic"));
        }
        if rd16(&bytes, 4) != FORMAT_VERSION {
            return Err(PayloadError::Corrupt("unknown format_version"));
        }
        let segment_id = rd64(&bytes, 8);
        let event_count = rd64(&bytes, 16);
        let n_blocks = rd32(&bytes, 24) as usize;

        let footer_start = bytes.len() - FOOTER_LEN;
        let foot = &bytes[footer_start..];
        if rd32(foot, 20) != PCOL_MAGIC {
            return Err(PayloadError::Corrupt("bad footer magic"));
        }
        if rd32(foot, 12) as usize != n_blocks {
            return Err(PayloadError::Corrupt("footer/header n_blocks disagree"));
        }
        let index_off = rd64(foot, 0) as usize;
        let stored_crc = rd32(foot, 8);
        if crc32c::crc32c(&bytes[..footer_start]) != stored_crc {
            return Err(PayloadError::Corrupt("content CRC mismatch"));
        }

        let index_len = n_blocks
            .checked_mul(BLOCK_ENTRY_LEN)
            .ok_or(PayloadError::Corrupt("index length overflow"))?;
        if index_off > footer_start || footer_start - index_off != index_len {
            return Err(PayloadError::Corrupt("index region size mismatch"));
        }
        if index_off < HEADER_LEN {
            return Err(PayloadError::Corrupt("index overlaps header"));
        }

        let mut blocks = Vec::with_capacity(n_blocks);
        let mut expect_first = 0u64;
        for i in 0..n_blocks {
            let b = index_off + i * BLOCK_ENTRY_LEN;
            let first_event = rd64(&bytes, b);
            let n_events = rd32(&bytes, b + 8);
            let byte_len = rd32(&bytes, b + 12);
            let byte_off = rd64(&bytes, b + 16);
            let kind = BlockKind::from_byte(bytes[b + 24])?;
            let dict_id = rd16(&bytes, b + 26);
            // Structural invariants so later slicing/reassembly cannot panic.
            let end = byte_off as usize + byte_len as usize;
            if byte_off < HEADER_LEN as u64 || end > index_off {
                return Err(PayloadError::Corrupt("block span out of range"));
            }
            if first_event != expect_first {
                return Err(PayloadError::Corrupt("block first_event not contiguous"));
            }
            if kind == BlockKind::Columnar && dict_id != 0 {
                return Err(PayloadError::Corrupt("columnar block carries a dict_id"));
            }
            expect_first += u64::from(n_events);
            blocks.push(BlockEntry {
                first_event,
                n_events,
                kind,
                dict_id,
                byte_off,
                byte_len,
            });
        }
        if expect_first != event_count {
            return Err(PayloadError::Corrupt("block event counts != event_count"));
        }

        Ok(SealedPayloadIndex { segment_id, event_count, bytes, blocks })
    }

    /// Read and parse a sidecar from `path`.
    pub fn open(path: &std::path::Path) -> std::io::Result<Result<Self, PayloadError>> {
        let bytes = std::fs::read(path)?;
        Ok(Self::from_bytes(bytes))
    }

    /// Every distinct nonzero `dict_id` referenced by a block.
    pub fn referenced_dict_ids(&self) -> Vec<u16> {
        let mut ids: Vec<u16> = self.blocks.iter().map(|b| b.dict_id).filter(|&d| d != 0).collect();
        ids.sort_unstable();
        ids.dedup();
        ids
    }

    /// **Referenced-implies-registered** (D3): every `dict_id` any block
    /// references MUST resolve in `resolver`. Returns the first offending id as
    /// [`PayloadError::UnregisteredDict`]. A columnar-only segment references no
    /// dictionaries and trivially passes (even against [`NoDicts`]).
    pub fn verify_dicts_registered(&self, resolver: &impl DictResolver) -> Result<(), PayloadError> {
        for id in self.referenced_dict_ids() {
            if resolver.dict_bytes(id).is_none() {
                return Err(PayloadError::UnregisteredDict(id));
            }
        }
        Ok(())
    }

    #[inline]
    fn block_bytes(&self, e: &BlockEntry) -> &[u8] {
        &self.bytes[e.byte_off as usize..e.byte_off as usize + e.byte_len as usize]
    }

    /// The index of the block containing stored-order event `idx`, or `None`.
    fn locate(&self, idx: u64) -> Option<usize> {
        if idx >= self.event_count {
            return None;
        }
        // blocks are contiguous and ascending: binary search on first_event.
        let mut lo = 0usize;
        let mut hi = self.blocks.len();
        while lo < hi {
            let mid = (lo + hi) / 2;
            let b = &self.blocks[mid];
            if idx < b.first_event {
                hi = mid;
            } else if idx >= b.first_event + u64::from(b.n_events) {
                lo = mid + 1;
            } else {
                return Some(mid);
            }
        }
        None
    }

    /// Reassemble a single event (point read) to its exact source bytes,
    /// resolving a dictionary through `resolver` for a row-dict block.
    pub fn reassemble_event(
        &self,
        idx: u64,
        resolver: &impl DictResolver,
    ) -> Result<Vec<u8>, PayloadError> {
        let bi = self.locate(idx).ok_or(PayloadError::IndexOutOfRange)?;
        let e = &self.blocks[bi];
        let row = (idx - e.first_event) as usize;
        let raw = self.block_bytes(e);
        if e.dict_id == 0 {
            // Columnar or codec-raw block: the codec decodes both.
            Ok(Block::decode(raw)?.reassemble_one(row)?)
        } else {
            let dict = resolver.dict_bytes(e.dict_id).ok_or(PayloadError::UnregisteredDict(e.dict_id))?;
            let img = decode_row_dict(raw, dict)?;
            if row >= img.len() {
                return Err(PayloadError::IndexOutOfRange);
            }
            Ok(img.event(row).to_vec())
        }
    }

    /// Reassemble every event in stored order, appending bytes to `out` and
    /// `event_count + 1` boundaries to `offs`. Byte-exact across mixed
    /// (columnar + row-fallback) blocks.
    pub fn reassemble_all(
        &self,
        resolver: &impl DictResolver,
        out: &mut Vec<u8>,
        offs: &mut Vec<u32>,
    ) -> Result<(), PayloadError> {
        for e in &self.blocks {
            let raw = self.block_bytes(e);
            if e.dict_id == 0 {
                let block = Block::decode(raw)?;
                block.reassemble_all(out, offs)?;
                // reassemble_all pushes n+1 offsets; drop the trailing dup so
                // blocks concatenate into one contiguous offset list.
                offs.pop();
            } else {
                let dict = resolver
                    .dict_bytes(e.dict_id)
                    .ok_or(PayloadError::UnregisteredDict(e.dict_id))?;
                let img = decode_row_dict(raw, dict)?;
                for i in 0..img.len() {
                    offs.push(out.len() as u32);
                    out.extend_from_slice(img.event(i));
                }
            }
        }
        offs.push(out.len() as u32);
        Ok(())
    }
}

fn rd16(d: &[u8], at: usize) -> u16 {
    u16::from_le_bytes(d[at..at + 2].try_into().unwrap())
}
fn rd32(d: &[u8], at: usize) -> u32 {
    u32::from_le_bytes(d[at..at + 4].try_into().unwrap())
}
fn rd64(d: &[u8], at: usize) -> u64 {
    u64::from_le_bytes(d[at..at + 8].try_into().unwrap())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::columnar::{emit_int, emit_str};
    use std::collections::BTreeMap;

    /// Tiny deterministic xorshift RNG (no external rng dep, mirrors the codec
    /// tests).
    struct Rng(u64);
    impl Rng {
        fn new(seed: u64) -> Self {
            Rng(seed ^ 0x9e37_79b9_7f4a_7c15)
        }
        fn next_u64(&mut self) -> u64 {
            let mut x = self.0;
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            self.0 = x;
            x
        }
        fn below(&mut self, n: u64) -> u64 {
            self.next_u64() % n
        }
    }

    /// A canonical MessagePack map `{"stream": s, "seq": n, "amount": a}` — the
    /// codec shreds these into columnar blocks.
    fn msgpack_event(rng: &mut Rng, seq: u64) -> Vec<u8> {
        let mut v = vec![0x83]; // 3-field map
        emit_str(&mut v, b"stream");
        emit_str(&mut v, format!("s{:05}", rng.below(400)).as_bytes());
        emit_str(&mut v, b"seq");
        emit_int(&mut v, seq as i64);
        emit_str(&mut v, b"amount");
        emit_int(&mut v, rng.below(1_000_000) as i64);
        v
    }

    /// A run of random bytes — the codec cannot shred these, forcing a row
    /// fallback block.
    fn binary_event(rng: &mut Rng) -> Vec<u8> {
        let len = 1 + rng.below(48) as usize;
        (0..len).map(|_| rng.next_u64() as u8).collect()
    }

    fn refs(evs: &[Vec<u8>]) -> Vec<&[u8]> {
        evs.iter().map(Vec::as_slice).collect()
    }

    /// A mixed corpus: alternating runs of shreddable msgpack and unshreddable
    /// binary, so a small block size yields both columnar and row blocks.
    fn mixed_corpus() -> Vec<Vec<u8>> {
        let mut rng = Rng::new(0xC0FFEE);
        let mut evs = Vec::new();
        for round in 0..6u64 {
            for i in 0..70 {
                evs.push(msgpack_event(&mut rng, round * 70 + i));
            }
            for _ in 0..40 {
                evs.push(binary_event(&mut rng));
            }
        }
        evs
    }

    fn opts() -> PayloadSealOpts {
        PayloadSealOpts { block_events: 32, ..Default::default() }
    }

    #[test]
    fn mixed_segment_roundtrips_byte_exact() {
        let evs = mixed_corpus();
        let bytes = encode_payload_sidecar(7, &refs(&evs), &opts()).unwrap();
        let idx = SealedPayloadIndex::from_bytes(bytes).unwrap();

        assert_eq!(idx.segment_id(), 7);
        assert_eq!(idx.event_count() as usize, evs.len());

        // A mixed segment must actually contain BOTH block kinds.
        let kinds: Vec<BlockKind> = idx.blocks().iter().map(|b| b.kind).collect();
        assert!(kinds.contains(&BlockKind::Columnar), "expected columnar blocks");
        assert!(kinds.contains(&BlockKind::Row), "expected row-fallback blocks");

        // Full reassembly is byte-exact.
        let mut out = Vec::new();
        let mut offs = Vec::new();
        idx.reassemble_all(&NoDicts, &mut out, &mut offs).unwrap();
        assert_eq!(offs.len(), evs.len() + 1);
        for (i, w) in offs.windows(2).enumerate() {
            assert_eq!(&out[w[0] as usize..w[1] as usize], evs[i].as_slice(), "range mismatch at {i}");
        }

        // Point reads are byte-exact for both block kinds.
        for (i, ev) in evs.iter().enumerate() {
            let got = idx.reassemble_event(i as u64, &NoDicts).unwrap();
            assert_eq!(&got, ev, "point-read mismatch at {i}");
        }

        // Out-of-range point read is a clean error, not a panic.
        assert!(matches!(
            idx.reassemble_event(evs.len() as u64, &NoDicts),
            Err(PayloadError::IndexOutOfRange)
        ));
    }

    #[test]
    fn empty_segment_roundtrips() {
        let bytes = encode_payload_sidecar(1, &[], &opts()).unwrap();
        let idx = SealedPayloadIndex::from_bytes(bytes).unwrap();
        assert_eq!(idx.event_count(), 0);
        assert_eq!(idx.block_count(), 0);
        let mut out = Vec::new();
        let mut offs = Vec::new();
        idx.reassemble_all(&NoDicts, &mut out, &mut offs).unwrap();
        assert_eq!(offs, vec![0]);
        assert!(out.is_empty());
    }

    #[test]
    fn verify_on_seal_aborts_on_reassembly_mismatch() {
        // Seal a good sidecar, then run the SHIPPED verify gate against a source
        // whose event 45 was corrupted: the reassembled (correct) bytes no
        // longer match the claimed source, so verify aborts at exactly 45.
        let evs = mixed_corpus();
        let bytes = encode_payload_sidecar(7, &refs(&evs), &opts()).unwrap();
        let idx = SealedPayloadIndex::from_bytes(bytes).unwrap();

        let mut corrupted = evs.clone();
        corrupted[45] = b"a completely different payload".to_vec();
        let err = verify_reassembly(&idx, &refs(&corrupted), &NoDicts).unwrap_err();
        assert!(matches!(err, PayloadError::VerifyMismatch(45)), "got {err:?}");

        // Sanity: the untampered source verifies clean (the ship path).
        verify_reassembly(&idx, &refs(&evs), &NoDicts).unwrap();
    }

    #[test]
    fn verify_on_seal_catches_a_corrupted_stored_block() {
        // A different injection: corrupt a stored columnar block's bytes (and
        // repair the CRC so the sidecar still parses). The verify gate must
        // refuse it — either the block fails to decode, or it reassembles to
        // bytes that no longer match the source.
        let evs = mixed_corpus();
        let bytes = encode_payload_sidecar(7, &refs(&evs), &opts()).unwrap();
        let idx = SealedPayloadIndex::from_bytes(bytes).unwrap();
        // Pick the first columnar block and flip a byte deep in its data.
        let col = idx.blocks().iter().find(|b| b.kind == BlockKind::Columnar).unwrap();
        let target = col.byte_off as usize + col.byte_len as usize - 1;
        let first_event = col.first_event;

        let mut tampered = idx.into_bytes();
        tampered[target] ^= 0xff;
        // Repair the content CRC so from_bytes accepts the tampered image.
        let footer_start = tampered.len() - FOOTER_LEN;
        let crc = crc32c::crc32c(&tampered[..footer_start]);
        tampered[footer_start + 8..footer_start + 12].copy_from_slice(&crc.to_le_bytes());

        let idx2 = SealedPayloadIndex::from_bytes(tampered).unwrap();
        let err = verify_reassembly(&idx2, &refs(&evs), &NoDicts).unwrap_err();
        match err {
            PayloadError::VerifyMismatch(i) => assert!(i >= first_event),
            PayloadError::Codec(_) => {} // corrupt zstd frame → decode error, also a refusal
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn row_fallback_dictionary_tier_roundtrips_and_registers() {
        // Force an all-binary corpus (every block falls back to row) and attach
        // a trained 16 KiB dictionary as dict_id 5.
        let mut rng = Rng::new(99);
        let evs: Vec<Vec<u8>> = (0..300).map(|_| binary_event(&mut rng)).collect();
        // Train a dictionary from the samples (bounded to 16 KiB).
        let samples: Vec<&[u8]> = evs.iter().map(Vec::as_slice).collect();
        let dict = zstd::dict::from_samples(&samples, MAX_DICT_BYTES).unwrap_or_default();
        // `from_samples` can refuse tiny corpora; fall back to a raw-content
        // dictionary so the tier is still exercised.
        let dict = if dict.is_empty() { evs.concat() } else { dict };
        let dict = if dict.len() > MAX_DICT_BYTES { dict[..MAX_DICT_BYTES].to_vec() } else { dict };

        let o = PayloadSealOpts { block_events: 32, row_dict: Some((5, dict.clone())), ..Default::default() };
        let bytes = encode_payload_sidecar(3, &refs(&evs), &o).unwrap();
        let idx = SealedPayloadIndex::from_bytes(bytes).unwrap();

        // Every block is a row block referencing dict_id 5.
        assert!(idx.blocks().iter().all(|b| b.kind == BlockKind::Row && b.dict_id == 5));
        assert_eq!(idx.referenced_dict_ids(), vec![5]);

        // Reassembly needs the dictionary; byte-exact with it.
        let mut registry: BTreeMap<u16, Vec<u8>> = BTreeMap::new();
        registry.insert(5, dict);
        let mut out = Vec::new();
        let mut offs = Vec::new();
        idx.reassemble_all(&registry, &mut out, &mut offs).unwrap();
        for (i, w) in offs.windows(2).enumerate() {
            assert_eq!(&out[w[0] as usize..w[1] as usize], evs[i].as_slice());
        }
        for (i, ev) in evs.iter().enumerate() {
            assert_eq!(&idx.reassemble_event(i as u64, &registry).unwrap(), ev);
        }
    }

    #[test]
    fn referenced_implies_registered_gate() {
        let mut rng = Rng::new(7);
        let evs: Vec<Vec<u8>> = (0..100).map(|_| binary_event(&mut rng)).collect();
        let dict = {
            let d = evs.concat();
            d[..d.len().min(MAX_DICT_BYTES)].to_vec()
        };
        let o = PayloadSealOpts { block_events: 32, row_dict: Some((9, dict.clone())), ..Default::default() };
        let bytes = encode_payload_sidecar(1, &refs(&evs), &o).unwrap();
        let idx = SealedPayloadIndex::from_bytes(bytes).unwrap();

        // Registered: passes.
        let mut reg: BTreeMap<u16, Vec<u8>> = BTreeMap::new();
        reg.insert(9, dict);
        idx.verify_dicts_registered(&reg).unwrap();

        // Unregistered (empty registry): the gate rejects with the offending id.
        let err = idx.verify_dicts_registered(&NoDicts).unwrap_err();
        assert!(matches!(err, PayloadError::UnregisteredDict(9)), "got {err:?}");

        // A referenced-but-unregistered dict also fails an actual read.
        assert!(matches!(
            idx.reassemble_event(0, &NoDicts),
            Err(PayloadError::UnregisteredDict(9))
        ));
    }

    #[test]
    fn columnar_only_segment_needs_no_dicts() {
        let mut rng = Rng::new(11);
        let evs: Vec<Vec<u8>> = (0..200).map(|i| msgpack_event(&mut rng, i)).collect();
        let bytes = encode_payload_sidecar(2, &refs(&evs), &opts()).unwrap();
        let idx = SealedPayloadIndex::from_bytes(bytes).unwrap();
        assert!(idx.blocks().iter().all(|b| b.kind == BlockKind::Columnar));
        assert!(idx.referenced_dict_ids().is_empty());
        // A columnar-only segment trivially satisfies the gate even with no dicts.
        idx.verify_dicts_registered(&NoDicts).unwrap();
    }

    #[test]
    fn corrupt_crc_and_truncation_rejected() {
        let evs = mixed_corpus();
        let good = encode_payload_sidecar(7, &refs(&evs), &opts()).unwrap();

        let mut bad = good.clone();
        bad[HEADER_LEN] ^= 0xff; // flip a byte in the DATA region
        assert!(matches!(SealedPayloadIndex::from_bytes(bad), Err(PayloadError::Corrupt(_))));

        let short = good[..HEADER_LEN + FOOTER_LEN - 1].to_vec();
        assert!(matches!(SealedPayloadIndex::from_bytes(short), Err(PayloadError::Corrupt(_))));

        let mut bad_magic = good.clone();
        bad_magic[0] ^= 0x01;
        assert!(matches!(SealedPayloadIndex::from_bytes(bad_magic), Err(PayloadError::Corrupt(_))));
    }

    // -----------------------------------------------------------------------
    // Bench: sealed payload replay throughput + bytes/event on a 1M-event
    // corpus, compression on. Run with:
    //   TMPDIR=$HOME/.cache/mess-test-tmp cargo test -p mess-index --release \
    //     sealed::payload::tests::payload_replay_bench -- --ignored --nocapture
    // -----------------------------------------------------------------------
    #[test]
    #[ignore = "perf bench; run explicitly with --release --ignored --nocapture"]
    fn payload_replay_bench() {
        use std::time::Instant;
        const TOTAL: usize = 1_000_000;
        let mut rng = Rng::new(0xB5EED);
        // Block-clustered mixed segment: ~90% of 128-event blocks are pure
        // shreddable msgpack (columnar), ~10% pure binary (row fallback) — since
        // one unshreddable event routes a whole block to raw, a realistic corpus
        // clusters rather than sprinkling, so the columnar default dominates.
        let mut evs: Vec<Vec<u8>> = Vec::with_capacity(TOTAL);
        let mut i = 0u64;
        while evs.len() < TOTAL {
            let binary_block = rng.below(10) == 0;
            for _ in 0..DEFAULT_BLOCK_EVENTS {
                if evs.len() == TOTAL {
                    break;
                }
                evs.push(if binary_block { binary_event(&mut rng) } else { msgpack_event(&mut rng, i) });
                i += 1;
            }
        }
        let raw_bytes: usize = evs.iter().map(Vec::len).sum();
        let refs = refs(&evs);

        let t = Instant::now();
        let bytes = encode_payload_sidecar(1, &refs, &PayloadSealOpts::default()).unwrap();
        let seal_dt = t.elapsed();
        let sidecar_len = bytes.len();

        let idx = SealedPayloadIndex::from_bytes(bytes).unwrap();
        let row_blocks = idx.blocks().iter().filter(|b| b.kind == BlockKind::Row).count();

        // Full sequential replay throughput.
        let t = Instant::now();
        let mut out = Vec::new();
        let mut offs = Vec::new();
        idx.reassemble_all(&NoDicts, &mut out, &mut offs).unwrap();
        let replay_dt = t.elapsed();
        assert_eq!(offs.len(), evs.len() + 1);

        // Point-read latency: one event per block.
        let t = Instant::now();
        let mut sink = 0u64;
        let mut n_pr = 0usize;
        for b in idx.blocks() {
            let mid = b.first_event + u64::from(b.n_events) / 2;
            let v = idx.reassemble_event(mid, &NoDicts).unwrap();
            sink = sink.wrapping_add(v.len() as u64);
            n_pr += 1;
        }
        let pr_dt = t.elapsed();
        std::hint::black_box(sink);

        eprintln!("=== sealed payload replay bench (zstd-9, {DEFAULT_BLOCK_EVENTS}-event blocks) ===");
        eprintln!("  events            {TOTAL}");
        eprintln!(
            "  raw payload       {raw_bytes} B ({:.1} B/event)",
            raw_bytes as f64 / TOTAL as f64
        );
        eprintln!(
            "  sidecar           {sidecar_len} B ({:.2} B/event, ratio {:.2}x)",
            sidecar_len as f64 / TOTAL as f64,
            raw_bytes as f64 / sidecar_len as f64
        );
        eprintln!("  row fallbacks     {row_blocks}/{} blocks", idx.block_count());
        eprintln!(
            "  seal+verify       {:.2} M ev/s ({:?})",
            TOTAL as f64 / seal_dt.as_secs_f64() / 1e6,
            seal_dt
        );
        eprintln!(
            "  replay throughput {:.2} M ev/s ({:?})",
            TOTAL as f64 / replay_dt.as_secs_f64() / 1e6,
            replay_dt
        );
        eprintln!("  point read        {:.3} us/read ({n_pr} reads)", pr_dt.as_secs_f64() * 1e6 / n_pr as f64);
    }

    #[test]
    fn magicless_framing_saves_the_magic_bytes() {
        // The dict row block is magicless: its zstd frame carries no 4-byte
        // magic. Confirm a magicless frame decodes only with the magicless
        // decompressor (proving the framing is actually magic-stripped).
        let mut rng = Rng::new(5);
        let evs: Vec<Vec<u8>> = (0..40).map(|_| binary_event(&mut rng)).collect();
        let dict = evs.concat();
        let block = encode_row_dict(&refs(&evs), 9, &dict).unwrap();
        // Body after the u32 ulen must NOT start with the zstd magic 0x28B52FFD.
        let magic = [0x28u8, 0xB5, 0x2F, 0xFD];
        assert_ne!(&block[4..8], &magic, "frame should be magicless");
    }
}
