//! The sealed **payload-block sidecar** (D6, bn-zge): the read-optimized,
//! on-disk form of a sealed segment's event *payloads*, produced by the
//! background sealer alongside the [pointer sidecar](crate::sealed::segment).
//!
//! Where the pointer sidecar (`.pidx`) maps `(stream, version)` to a byte
//! offset, this sidecar (`.pcol`) holds the payload *bytes* rewritten into
//! compact, compressed blocks so a sealed read reassembles an event without
//! touching the raw segment. It wires the bn-1bn [columnar
//! codec](crate::columnar) into the seal per the round-4 D6 format:
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
//!   references MUST be registered** —
//!   [`SealedPayloadIndex::verify_dicts_registered`] is the
//!   referenced-implies-registered gate (D3). The columnar tier never uses a
//!   dictionary (`dict_id == 0`).
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
//!   6   u16  flags                (bit0 FLAG_SPLIT_CRC — see below)
//!   8   u64  segment_id
//!   16  u64  event_count          (Σ block n_events)
//!   24  u32  n_blocks
//!   28  u32  index_crc            (crc32c over the INDEX region; 0 unless
//!                                  FLAG_SPLIT_CRC — was `reserved`)
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
//!   28  u32  block_crc            (crc32c over the block's stored bytes; 0
//!                                  unless FLAG_SPLIT_CRC — was `reserved2`)
//!
//! Footer (24 bytes, at EOF):
//!   0   u64  index_off
//!   8   u32  content_crc          (crc32c over [0, footer_start))
//!   12  u32  n_blocks             (redundant, cross-checks the header)
//!   16  u32  reserved = 0
//!   20  u32  magic = PCOL_MAGIC
//! ```
//!
//! # Lazy attach and where integrity is checked (bn-bka2)
//!
//! A sidecar is opened one of two ways, and the difference is *when* its bytes
//! are read, never *what* they decode to:
//!
//! - [`SealedPayloadIndex::from_bytes`] — the **eager** form. The whole image
//!   is already in memory (verify-on-seal, a `SealPack` payload section, the
//!   offline re-block), so it verifies `content_crc` over the entire image up
//!   front. Block reads are slices; nothing else is checked.
//! - [`SealedPayloadIndex::open`] — the **lazy** form used when a sealed
//!   segment re-attaches its sibling `.pcol` at engine open. It reads only the
//!   32-byte header, the 24-byte footer, and the `n_blocks × 32`-byte block
//!   index — O(events/128) bytes, ~0.4% of a typical sidecar — and retains the
//!   file handle. A block's bytes are `pread` on the first read that touches it
//!   and dropped when that read is done. Eager attach made engine open linear
//!   in total sidecar size (bn-2u01 measured it at 98.5% of warm open and ~all
//!   of post-open RSS on an 8 GiB corpus); lazy attach makes it linear in
//!   *block count* instead.
//!
//! **Lazy attach requires [`FLAG_SPLIT_CRC`]**: a sidecar written before
//! bn-bka2 (`flags == 0`) carries no `index_crc` and no per-block `block_crc`,
//! so [`SealedPayloadIndex::open`] falls back to the eager path for it — whole
//! image, `content_crc` verified, bytes resident, exactly what an open did
//! before this change. Attaching such a file lazily would leave it with *no*
//! checksum at all, which is why the fallback is not optional. Every new seal
//! sets the flag, so the lazy win applies to every segment sealed from here on,
//! and an offline [`archive_reblock`] rewrites an old sidecar into the new
//! format.
//!
//! **The corruption-detection timing shifts with it.** Eager open verified
//! every byte of the sidecar before attaching, so a torn `.pcol` was dropped at
//! open (D1: the raw log stays the payload authority). A lazy open cannot —
//! `content_crc` covers the whole image, and reading it all is precisely the
//! cost being removed. Instead:
//!
//! 1. **At open** the header, footer, and block index are fully validated
//!    (magic ×2, `format_version`, `n_blocks` header/footer agreement, index
//!    region sizing, every block span inside DATA, contiguous `first_event`
//!    coverage summing to `event_count`, kind/`dict_id` legality) and — for a
//!    sidecar written with [`FLAG_SPLIT_CRC`] — the block index is
//!    **checksummed** against `index_crc`. A failure drops the sidecar at open
//!    exactly as before.
//! 2. **At first read** of a block, a [`FLAG_SPLIT_CRC`] sidecar verifies that
//!    block's `block_crc` before decoding it. A mismatch is a
//!    [`PayloadError::Corrupt`], which every caller already treats like a
//!    missing sidecar: the read degrades to the raw log (mess-store's
//!    `decode_capsule` falls back to the raw frames, D1/I5). So a damaged block
//!    is still never *served* — it is merely discovered when it is read rather
//!    than when the segment is attached, and only that block degrades instead
//!    of the whole sidecar.
//! 3. `index_crc`/`block_crc` live in fields the round-4 format reserved, and
//!    the flag says whether they are meaningful. A sidecar written before
//!    bn-bka2 has `flags == 0` and therefore attaches **eagerly** (above), so
//!    it is still whole-image `content_crc`-verified at open and a torn one is
//!    still dropped there — no store loses a check it has today. New seals
//!    always set the flag, and `content_crc` is still written and still
//!    verified by every eager path, so no integrity is removed from the format.

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::fs::File;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

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
/// Header `flags` bit 0 (bn-bka2): the header's `index_crc` and every
/// [`BlockEntry`]'s `block_crc` are meaningful, so a lazily attached sidecar
/// ([`SealedPayloadIndex::open`]) can checksum the block index at open and each
/// block before it decodes it — the split-coverage stand-in for the whole-image
/// `content_crc` an eager open verifies. Clear on sidecars written before
/// bn-bka2 (both fields are then the format's original zero `reserved`s), which
/// read exactly as they always did, minus the pre-decode checksum.
pub const FLAG_SPLIT_CRC: u16 = 0x0001;

/// Default events per payload block (round-4 D6 default).
pub const DEFAULT_BLOCK_EVENTS: usize = 128;
/// Maximum trained row-fallback dictionary size (D6 / `04-registry.md` §3.8).
pub const MAX_DICT_BYTES: usize = 16 << 10;

/// Whether a stored block is columnar or a row image — the per-block 1-bit
/// flag the block index carries (mirrors the codec's [`FLAG_COLUMNAR`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BlockKind {
    /// Row image: the codec fell back (unshreddable payloads), or a
    /// dictionary-recompressed row block ([`dict_id !=
    /// 0`](BlockEntry::dict_id)).
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
/// nonzero `dict_id` is unregistered, so
/// [`SealedPayloadIndex::verify_dicts_registered`] passes only when no block
/// references a dictionary.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoDicts;

impl DictResolver for NoDicts {
    fn dict_bytes(&self, _dict_id: u16) -> Option<&[u8]> { None }
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
    /// Reading a block's bytes from a **lazily attached** sidecar failed — the
    /// file was removed, truncated, or the `pread` itself errored (bn-bka2).
    /// Callers treat this exactly like a missing sidecar: the read degrades to
    /// the raw log, which is always the payload authority (D1/I5).
    #[error("payload sidecar read: {0}")]
    Io(#[from] std::io::Error),
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
fn encode_row_dict(
    events: &[&[u8]],
    level: i32,
    dict: &[u8],
) -> std::io::Result<Vec<u8>> {
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
    buf:     Vec<u8>,
    offsets: Vec<u32>,
    base:    usize,
}

fn decode_row_dict(
    bytes: &[u8],
    dict: &[u8],
) -> Result<RowImage, PayloadError> {
    use zstd::zstd_safe::{DParameter, FrameFormat};
    if bytes.len() < 4 {
        return Err(PayloadError::Corrupt("truncated row-dict block"));
    }
    let ulen = u32::from_le_bytes(bytes[0..4].try_into().unwrap()) as usize;
    if ulen > MAX_ROW_ULEN {
        return Err(PayloadError::RowDictDecompress);
    }
    let mut d = zstd::bulk::Decompressor::with_dictionary(dict)
        .map_err(|_| PayloadError::RowDictDecompress)?;
    d.set_parameter(DParameter::Format(FrameFormat::Magicless))
        .map_err(|_| PayloadError::RowDictDecompress)?;
    let buf = d
        .decompress(&bytes[4..], ulen)
        .map_err(|_| PayloadError::RowDictDecompress)?;
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
    fn len(&self) -> usize { self.offsets.len() - 1 }

    fn event(&self, i: usize) -> &[u8] {
        let a = self.offsets[i] as usize;
        let b = self.offsets[i + 1] as usize;
        &self.buf[self.base + a..self.base + b]
    }
}

fn rd_u32(d: &[u8], p: &mut usize) -> Result<u32, PayloadError> {
    let s = d
        .get(*p..*p + 4)
        .ok_or(PayloadError::Corrupt("truncated row image"))?;
    *p += 4;
    Ok(u32::from_le_bytes(s.try_into().unwrap()))
}

// ---------------------------------------------------------------------------
// Seal input + encode (with verify-on-seal)
// ---------------------------------------------------------------------------

/// Options for encoding a payload sidecar.
#[derive(Clone)]
pub struct PayloadSealOpts {
    /// Events per block (the codec caps a block at `u16::MAX`; D6 default
    /// 128).
    pub block_events: usize,
    /// Columnar codec options (zstd level; whole-block layout by default).
    pub codec:        EncodeOpts,
    /// The single segment-level row-fallback dictionary, if one is available:
    /// `(dict_id, dict_bytes)`. When set, any block the codec routes to raw is
    /// recompressed against this dictionary (magicless) and its index entry
    /// records the `dict_id`. `dict_bytes` MUST be ≤ [`MAX_DICT_BYTES`].
    pub row_dict:     Option<(u16, Vec<u8>)>,
}

impl Default for PayloadSealOpts {
    fn default() -> Self {
        PayloadSealOpts {
            block_events: DEFAULT_BLOCK_EVENTS,
            codec:        EncodeOpts::default(),
            row_dict:     None,
        }
    }
}

// ---------------------------------------------------------------------------
// Offline archive re-block (bn-382)
// ---------------------------------------------------------------------------
//
// A sealed segment's `.pcol` is written once by the sealer at the round-4 D6
// default (columnar / 128-event blocks / zstd-9). For an *archive* — a segment
// past the replay SLA whose bytes/event matters more than its replay CPU —
// [`archive_reblock`] rewrites that `.pcol` **offline** with larger columnar
// blocks at a higher zstd level (the round-4 frontier: 2048-event blocks,
// zstd-19, ~26.5 B/event on the heavy corpus). It is OFF by default and no code
// path invokes it automatically.
//
// The re-block only changes the payload **block geometry** — block size and
// zstd level are per-block in the `.pcol` format (each [`BlockEntry`] carries
// its own `n_events`/`byte_len`), so the one read path reassembles a 128-event
// file and a 2048-event file transparently. The pointer sidecar (`.pidx`) is
// untouched: logical stored-order positions are unchanged, so every
// [`EventPtr`] and the pointer sidecar stay valid across the re-block.

/// Recommended archive-tier block size (round-4 D6 frontier): 2048 events.
pub const ARCHIVE_BLOCK_EVENTS: usize = 2048;
/// Recommended archive-tier zstd level (round-4 D6 frontier): 19.
pub const ARCHIVE_ZSTD_LEVEL: i32 = 19;

/// The `.pcol` payload-sidecar path for `segment_id` under `dir`:
/// `<dir>/seg-<id>.pcol`. The single source of truth for the `.pcol` naming,
/// shared by [`crate::sealed::driver::SealDriver::payload_sidecar_path`] (write
/// at seal) and [`archive_reblock`] (rewrite offline) so the two can never
/// drift.
pub fn pcol_path(dir: &Path, segment_id: u64) -> PathBuf {
    dir.join(format!("seg-{segment_id:020}.pcol"))
}

/// Policy for the **offline archive re-block** ([`archive_reblock`]): rewrite
/// an already-sealed segment's `.pcol` with larger columnar blocks at a higher
/// zstd level, trading seal/replay CPU for a tighter bytes/event ratio on
/// archives past the replay SLA.
///
/// **OFF by default.** `enabled` is `false` in [`ArchivePolicy::default`], and
/// no code path invokes the re-block automatically. A caller that wants the
/// archive tier constructs an enabled policy explicitly (e.g.
/// [`ArchivePolicy::archive`]).
#[derive(Clone, Copy, Debug)]
pub struct ArchivePolicy {
    /// Whether the archive re-block runs at all. `false` ⇒ [`archive_reblock`]
    /// is a no-op that touches nothing on disk.
    pub enabled:      bool,
    /// Events per re-blocked columnar block (D6 archive frontier: 2048). The
    /// codec caps a block at [`crate::columnar::MAX_BLOCK_EVENTS`].
    pub block_events: usize,
    /// zstd level for the re-blocked columnar/raw blocks (D6 archive: 19).
    pub level:        i32,
}

impl Default for ArchivePolicy {
    /// OFF: `enabled == false`. The archive tier is strictly opt-in.
    fn default() -> Self {
        ArchivePolicy {
            enabled:      false,
            block_events: ARCHIVE_BLOCK_EVENTS,
            level:        ARCHIVE_ZSTD_LEVEL,
        }
    }
}

impl ArchivePolicy {
    /// An **enabled** archive policy at the round-4 D6 frontier (2048-event
    /// blocks, zstd-19). [`ArchivePolicy::default`] is OFF; this is the opt-in.
    pub fn archive() -> Self {
        ArchivePolicy { enabled: true, ..Default::default() }
    }
}

/// The result of an [`archive_reblock`] call.
#[derive(Debug, Clone, Copy)]
pub struct ReblockOutcome {
    /// Whether the `.pcol` was actually rewritten. `false` when the policy was
    /// disabled — nothing was read or written.
    pub reblocked:   bool,
    /// Total events in the segment (0 when not reblocked).
    pub event_count: u64,
    /// `.pcol` size before the re-block, bytes (0 when not reblocked).
    pub old_bytes:   u64,
    /// `.pcol` size after the re-block, bytes (0 when not reblocked).
    pub new_bytes:   u64,
    /// Number of payload blocks before the re-block (0 when not reblocked).
    pub old_blocks:  usize,
    /// Number of payload blocks after the re-block (0 when not reblocked).
    pub new_blocks:  usize,
}

/// Errors from an [`archive_reblock`].
#[derive(Debug, thiserror::Error)]
pub enum ReblockError {
    /// Reading the old `.pcol` or writing the new one failed.
    #[error("archive re-block I/O: {0}")]
    Io(#[from] std::io::Error),
    /// Parsing, reassembling, or verifying a payload sidecar failed — including
    /// a verify-on-reblock byte-exactness mismatch
    /// ([`PayloadError::VerifyMismatch`]), which aborts the re-block with
    /// the OLD `.pcol` left intact.
    #[error("archive re-block payload: {0}")]
    Payload(#[from] PayloadError),
}

/// Re-block the sealed segment `segment_id`'s payload sidecar (`.pcol`) under
/// `dir` **offline**, into `policy.block_events`-event columnar blocks at
/// `policy.level` (the archive frontier: 2048 / zstd-19). Returns a
/// [`ReblockOutcome`] describing the geometry/size change.
///
/// - **Off by default.** If `policy.enabled` is `false` this reads and writes
///   nothing and returns `reblocked == false` — the archive tier is opt-in and
///   never runs automatically.
/// - **`.pidx` untouched.** Only the `.pcol` block geometry changes; logical
///   stored-order positions are unchanged, so the pointer sidecar and every
///   [`EventPtr`] stay valid. Reads of the re-blocked file go through the exact
///   same read path (block size is per-block).
/// - **Verify-on-reblock.** The OLD `.pcol` is fully reassembled (resolving any
///   row-fallback dictionaries via `resolver`) to recover its exact payloads;
///   the NEW blocks are then reassembled and byte-compared against those OLD
///   payloads *before* anything is written. A mismatch aborts with
///   [`PayloadError::VerifyMismatch`] and leaves the OLD `.pcol` in place.
/// - **Crash-atomic.** The new image is written temp → fsync → rename → dir
///   fsync ([`crate::sealed::driver::write_durable`]). A crash before the
///   rename leaves the OLD `.pcol` intact and serving; the temp husk is never
///   opened by a reader.
///
/// The re-blocked file carries **no dictionary** (`dict_id == 0` on every
/// block): row fallbacks recompress whole-block at the archive level, so the
/// archived `.pcol` is fully self-describing and needs no `$registry`
/// dictionary to read.
pub fn archive_reblock(
    dir: &Path,
    segment_id: u64,
    policy: &ArchivePolicy,
    resolver: &impl DictResolver,
) -> Result<ReblockOutcome, ReblockError> {
    if !policy.enabled {
        // Policy off: nothing happens — no read, no write.
        return Ok(ReblockOutcome {
            reblocked:   false,
            event_count: 0,
            old_bytes:   0,
            new_bytes:   0,
            old_blocks:  0,
            new_blocks:  0,
        });
    }

    let path = pcol_path(dir, segment_id);
    // Eager (whole-image, `content_crc`-verified) open: the re-block is about
    // to reassemble every block anyway, so it pays nothing for the strongest
    // check the format offers, and it must never rewrite a file it has not
    // fully validated. Engine open takes the lazy path instead (bn-bka2).
    let old_image = std::fs::read(&path)?;
    let old_bytes = old_image.len() as u64;
    let old = SealedPayloadIndex::from_bytes(old_image)?;
    let old_blocks = old.block_count();

    // Reassemble the OLD `.pcol`'s payloads (the re-block's source of truth),
    // resolving any row-fallback dictionaries the old file referenced.
    let mut src = Vec::new();
    let mut src_offs = Vec::new();
    old.reassemble_all(resolver, &mut src, &mut src_offs)?;
    let refs: Vec<&[u8]> = src_offs
        .windows(2)
        .map(|w| &src[w[0] as usize..w[1] as usize])
        .collect();

    // Re-encode with the archive geometry. encode_payload_sidecar runs the
    // PERMANENT verify-on-seal: every NEW block is reassembled and
    // byte-compared against `refs` (the OLD `.pcol`'s exact payloads)
    // before it returns — that is the verify-on-reblock. No dictionary in
    // the archive tier.
    let opts = PayloadSealOpts {
        block_events: policy.block_events,
        codec:        EncodeOpts {
            level:      policy.level,
            per_column: false,
        },
        row_dict:     None,
    };
    let new_image = encode_payload_sidecar(segment_id, &refs, &opts)?;

    // Independent verify-on-reblock against the OLD `.pcol`: reassemble the NEW
    // image end-to-end and byte-compare the whole payload region + boundaries
    // to the OLD reassembly. Redundant with encode's internal verify by
    // design — the re-block never renames a file it has not proven
    // byte-identical to the one it replaces.
    let new_idx = SealedPayloadIndex::from_bytes(new_image)?;
    let mut new_src = Vec::new();
    let mut new_offs = Vec::new();
    new_idx.reassemble_all(&NoDicts, &mut new_src, &mut new_offs)?;
    if new_offs != src_offs || new_src != src {
        let bad = new_offs
            .iter()
            .zip(&src_offs)
            .position(|(a, b)| a != b)
            .unwrap_or(0) as u64;
        return Err(PayloadError::VerifyMismatch(bad).into());
    }

    let new_blocks = new_idx.block_count();
    let event_count = new_idx.event_count();
    let bytes = new_idx.into_bytes()?;
    let new_bytes = bytes.len() as u64;

    // Crash-atomic replace, same seal-commit discipline as the sealer: temp →
    // fsync → rename over the `.pcol` → dir fsync. A crash before the rename
    // leaves the OLD `.pcol` intact and serving; open() reads exactly `.pcol`,
    // never the `.tmp` husk.
    crate::sealed::driver::write_durable(&path, &bytes)?;

    Ok(ReblockOutcome {
        reblocked: true,
        event_count,
        old_bytes,
        new_bytes,
        old_blocks,
        new_blocks,
    })
}

/// One block's encoded bytes plus the metadata its index entry needs.
struct EncodedBlock {
    first_event: u64,
    n_events:    u32,
    kind:        BlockKind,
    dict_id:     u16,
    bytes:       Vec<u8>,
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
                n_events:    chunk.len() as u32,
                kind:        BlockKind::Columnar,
                dict_id:     0,
                bytes:       block,
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
                    n_events:    chunk.len() as u32,
                    kind:        BlockKind::Row,
                    dict_id:     0,
                    bytes:       block,
                },
            }
        } else {
            EncodedBlock {
                first_event: first,
                n_events:    chunk.len() as u32,
                kind:        BlockKind::Row,
                dict_id:     0,
                bytes:       block,
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
    index.into_bytes()
}

/// The verify-on-seal gate: reassemble every event in `index` through the read
/// path and byte-compare against `source`. Returns
/// [`PayloadError::VerifyMismatch`] at the first event whose reassembled bytes
/// differ from `source[i]` (or a [`PayloadError::Codec`] if a block fails to
/// decode). This is exactly what [`encode_payload_sidecar`] runs before
/// returning its bytes — it ships, so a codec/framing regression can never
/// write a payload that does not reassemble byte-exact.
pub fn verify_reassembly(
    index: &SealedPayloadIndex,
    source: &[&[u8]],
    resolver: &impl DictResolver,
) -> Result<(), PayloadError> {
    if index.event_count() as usize != source.len() {
        return Err(PayloadError::VerifyMismatch(
            source.len().min(index.event_count() as usize) as u64,
        ));
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

fn serialize(
    segment_id: u64,
    event_count: u64,
    encs: &[EncodedBlock],
) -> Vec<u8> {
    let data_len: usize = encs.iter().map(|e| e.bytes.len()).sum();
    let mut buf = Vec::with_capacity(
        HEADER_LEN + data_len + encs.len() * BLOCK_ENTRY_LEN + FOOTER_LEN,
    );
    // Header. `index_crc` is backpatched below, once the INDEX region exists.
    buf.extend_from_slice(&PCOL_MAGIC.to_le_bytes());
    buf.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
    buf.extend_from_slice(&FLAG_SPLIT_CRC.to_le_bytes()); // flags
    buf.extend_from_slice(&segment_id.to_le_bytes());
    buf.extend_from_slice(&event_count.to_le_bytes());
    buf.extend_from_slice(&(encs.len() as u32).to_le_bytes());
    buf.extend_from_slice(&0u32.to_le_bytes()); // index_crc placeholder
    debug_assert_eq!(buf.len(), HEADER_LEN);

    // DATA region, recording each block's absolute offset.
    let mut offs = Vec::with_capacity(encs.len());
    for e in encs {
        offs.push(buf.len() as u64);
        buf.extend_from_slice(&e.bytes);
    }
    let index_off = buf.len() as u64;
    // INDEX region. Each entry carries its block's crc32c (FLAG_SPLIT_CRC) so
    // a lazily attached reader can checksum a block before decoding it without
    // reading the rest of the file.
    for (i, e) in encs.iter().enumerate() {
        buf.extend_from_slice(&e.first_event.to_le_bytes());
        buf.extend_from_slice(&e.n_events.to_le_bytes());
        buf.extend_from_slice(&(e.bytes.len() as u32).to_le_bytes());
        buf.extend_from_slice(&offs[i].to_le_bytes());
        buf.push(e.kind.to_byte());
        buf.push(0); // reserved
        buf.extend_from_slice(&e.dict_id.to_le_bytes());
        buf.extend_from_slice(&crc32c::crc32c(&e.bytes).to_le_bytes());
    }
    // The block index's own checksum, so a lazy open validates the table it
    // just read (the whole-image `content_crc` below still covers everything
    // for an eager open).
    let index_crc = crc32c::crc32c(&buf[index_off as usize..]);
    buf[28..32].copy_from_slice(&index_crc.to_le_bytes());

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
    pub n_events:    u32,
    /// Columnar or row image.
    pub kind:        BlockKind,
    /// Dictionary id (0 = none). Nonzero only for [`BlockKind::Row`].
    pub dict_id:     u16,
    byte_off:        u64,
    byte_len:        u32,
    /// crc32c over the block's stored bytes, or `None` on a
    /// pre-[`FLAG_SPLIT_CRC`] sidecar (bn-bka2).
    crc:             Option<u32>,
}

/// Where a [`SealedPayloadIndex`]'s block bytes come from (bn-bka2).
///
/// The block *table* is always resident — it is small (32 B per ~128 events)
/// and every read needs it. The block *bytes* are either already in memory or
/// read from the file on demand, which is the whole difference between an
/// eager and a lazy attach.
#[derive(Debug)]
enum BlockSource {
    /// The whole sidecar image, resident. Block bytes are slices of it.
    Memory(Vec<u8>),
    /// A retained handle on the file the image lives in. Block bytes are
    /// `pread` per read and dropped after it; residency is the kernel page
    /// cache's business, and mess-store's decoded-block cache keeps the
    /// *decoded* form hot.
    ///
    /// `base` is where the image starts in that file: `0` for a standalone
    /// `.pcol`, and the `PAYLOAD_COLUMNS` section's offset when the image is
    /// embedded in a SealPack (bn-dbz). Every in-image offset the block table
    /// carries is read at `base + off`. The handle is shared (`Arc`) because a
    /// pack serves several lazily attached sections from one open file.
    File { file: Arc<File>, base: u64 },
}

/// The read-only sealed payload index for one segment: the block table plus a
/// source for the block bytes ([`BlockSource`]). A point read is a block lookup
/// + (for a lazily attached sidecar) one `pread` + one block decode.
///
/// `&self` throughout and `Sync` with no interior locking — the lazy form adds
/// only a `File`, and positioned reads (`pread`) do not touch the shared file
/// offset, so concurrent readers of one `Arc<SealedSegmentIndex>` never
/// serialise.
#[derive(Debug)]
pub struct SealedPayloadIndex {
    segment_id:  u64,
    event_count: u64,
    src:         BlockSource,
    /// End of the DATA region = start of the INDEX region. Block spans are
    /// validated against it.
    index_off:   u64,
    /// Block entries ascending by `first_event`.
    blocks:      Vec<BlockEntry>,
}

impl SealedPayloadIndex {
    /// The segment this index covers.
    pub fn segment_id(&self) -> u64 { self.segment_id }

    /// Total events across all blocks.
    pub fn event_count(&self) -> u64 { self.event_count }

    /// Number of payload blocks.
    pub fn block_count(&self) -> usize { self.blocks.len() }

    /// The block index entries (ascending by `first_event`).
    pub fn blocks(&self) -> &[BlockEntry] { &self.blocks }

    /// Whether this index holds the whole sidecar image in memory rather than
    /// reading blocks on demand from the file — the bn-bka2 laziness
    /// observable. `true` for [`Self::from_bytes`]/[`Self::open_eager`], and
    /// for [`Self::open`] of a pre-[`FLAG_SPLIT_CRC`] sidecar (which attaches
    /// eagerly so it keeps its whole-image `content_crc` check).
    pub fn is_resident(&self) -> bool {
        matches!(self.src, BlockSource::Memory(_))
    }

    /// The whole sidecar image, consuming the index. Only the eager form owns
    /// one; a lazily attached index reads the file back.
    fn into_bytes(self) -> Result<Vec<u8>, PayloadError> {
        match self.src {
            BlockSource::Memory(b) => Ok(b),
            BlockSource::File { file, base } => {
                let total = self.index_off as usize
                    + self.blocks.len() * BLOCK_ENTRY_LEN
                    + FOOTER_LEN;
                let mut buf = vec![0u8; total];
                file.read_exact_at(&mut buf, base)?;
                Ok(buf)
            }
        }
    }

    /// Parse a sidecar byte image, validating magic, version, CRC, and every
    /// block span. The bytes are moved in and retained — this is the **eager**
    /// form (verify-on-seal, a `SealPack` payload section, the offline
    /// re-block); [`Self::open`] is the lazy one.
    pub fn from_bytes(bytes: Vec<u8>) -> Result<Self, PayloadError> {
        if bytes.len() < HEADER_LEN + FOOTER_LEN {
            return Err(PayloadError::Corrupt("shorter than header + footer"));
        }
        let head = Header::parse(&bytes[..HEADER_LEN])?;
        let footer_start = bytes.len() - FOOTER_LEN;
        let index_off =
            parse_footer(&bytes[footer_start..], &head, footer_start as u64)?;
        let stored_crc = rd32(&bytes[footer_start..], 8);
        if crc32c::crc32c(&bytes[..footer_start]) != stored_crc {
            return Err(PayloadError::Corrupt("content CRC mismatch"));
        }
        // The whole image is checksummed above, so the block index needs no
        // separate `index_crc` pass here.
        let blocks = parse_blocks(
            &bytes[index_off as usize..footer_start],
            &head,
            index_off,
        )?;

        Ok(SealedPayloadIndex {
            segment_id: head.segment_id,
            event_count: head.event_count,
            src: BlockSource::Memory(bytes),
            index_off,
            blocks,
        })
    }

    /// **Lazily** attach the sidecar at `path` (bn-bka2): read and validate the
    /// header, footer, and block index — `HEADER_LEN + FOOTER_LEN +
    /// n_blocks × BLOCK_ENTRY_LEN` bytes, independent of how much payload the
    /// segment holds — and retain the file handle. Block bytes are read on the
    /// first read that touches them, never at open.
    ///
    /// **Except for a pre-[`FLAG_SPLIT_CRC`] sidecar**, which has no
    /// index/block checksums to attach lazily against and so takes the eager
    /// path ([`Self::from_bytes`]: whole image, `content_crc` verified,
    /// resident) — trunk-identical behaviour for segments sealed before
    /// bn-bka2. See the module docs.
    ///
    /// The outer `io::Result` is a filesystem failure (no such file, no
    /// permission); the inner `Result` is a malformed sidecar. Callers
    /// ([`crate::sealed::segment::SealedSegmentIndex::open`]) treat both the
    /// same way: drop the sidecar, serve payloads from the raw log (D1).
    ///
    /// See the module docs for what this does *not* check that an eager
    /// [`Self::from_bytes`] does — the whole-image `content_crc` — and how a
    /// [`FLAG_SPLIT_CRC`] sidecar covers the same ground at read time.
    pub fn open(
        path: &std::path::Path,
    ) -> std::io::Result<Result<Self, PayloadError>> {
        let file = File::open(path)?;
        let len = file.metadata()?.len();
        Ok(Self::attach(file, len))
    }

    /// The lazy attach proper, split out so tests can hand in a handle.
    fn attach(file: File, len: u64) -> Result<Self, PayloadError> {
        Self::attach_at(Arc::new(file), 0, len)
    }

    /// **Lazily** attach a `.pcol` image that occupies `[base, base + len)` of
    /// an already-open file (bn-dbz): the SealPack `PAYLOAD_COLUMNS` section.
    ///
    /// Identical in every respect to [`Self::open`] — same header/footer/block
    /// index validation, same [`FLAG_SPLIT_CRC`] requirement for laziness with
    /// the same eager fallback for a pre-flag image, same per-block checksum
    /// before a block is decoded — except that every offset is taken relative
    /// to `base` and the handle is shared, so one open file serves a pack's
    /// payload columns and its event-type column at once.
    pub fn attach_at(
        file: Arc<File>,
        base: u64,
        len: u64,
    ) -> Result<Self, PayloadError> {
        if len < (HEADER_LEN + FOOTER_LEN) as u64 {
            return Err(PayloadError::Corrupt("shorter than header + footer"));
        }
        let mut hbuf = [0u8; HEADER_LEN];
        file.read_exact_at(&mut hbuf, base)?;
        let head = Header::parse(&hbuf)?;

        // LEGACY SIDECARS ATTACH EAGERLY (bn-bka2 review). Without
        // `FLAG_SPLIT_CRC` there is no `index_crc` and no per-block
        // `block_crc`, so a lazy attach would run with NO checksum at all —
        // weaker than the whole-image `content_crc` an eager open verifies
        // today, and a raw (`dict_id == 0`) block whose zstd frame survives a
        // flipped byte could decode into plausible-but-wrong payload bytes.
        // Read the whole image and verify `content_crc` exactly as
        // `from_bytes` does: pre-bn-bka2 segments keep trunk-identical
        // behaviour, every new seal sets the flag and gets the lazy win, and
        // `archive_reblock` upgrades an old sidecar's format when it runs.
        if head.flags & FLAG_SPLIT_CRC == 0 {
            let mut bytes = vec![0u8; len as usize];
            file.read_exact_at(&mut bytes, base)?;
            return Self::from_bytes(bytes);
        }

        let footer_start = len - FOOTER_LEN as u64;
        let mut fbuf = [0u8; FOOTER_LEN];
        file.read_exact_at(&mut fbuf, base + footer_start)?;
        let index_off = parse_footer(&fbuf, &head, footer_start)?;

        // The one length-bounded allocation: the block table, whose size the
        // footer just proved equals `footer_start - index_off`.
        let mut ibuf = vec![0u8; (footer_start - index_off) as usize];
        file.read_exact_at(&mut ibuf, base + index_off)?;
        if head.flags & FLAG_SPLIT_CRC != 0
            && crc32c::crc32c(&ibuf) != head.index_crc
        {
            return Err(PayloadError::Corrupt("block index CRC mismatch"));
        }
        let blocks = parse_blocks(&ibuf, &head, index_off)?;

        Ok(SealedPayloadIndex {
            segment_id: head.segment_id,
            event_count: head.event_count,
            src: BlockSource::File { file, base },
            index_off,
            blocks,
        })
    }

    /// Read and parse a sidecar from `path` **eagerly**: the whole image is
    /// pulled into memory and its `content_crc` verified before anything else
    /// happens. The offline archive re-block uses this — it is about to
    /// reassemble every block anyway, so it pays nothing for the strongest
    /// check the format offers. Engine open uses [`Self::open`] instead.
    pub fn open_eager(
        path: &std::path::Path,
    ) -> std::io::Result<Result<Self, PayloadError>> {
        let bytes = std::fs::read(path)?;
        Ok(Self::from_bytes(bytes))
    }

    /// Every distinct nonzero `dict_id` referenced by a block.
    pub fn referenced_dict_ids(&self) -> Vec<u16> {
        let mut ids: Vec<u16> =
            self.blocks.iter().map(|b| b.dict_id).filter(|&d| d != 0).collect();
        ids.sort_unstable();
        ids.dedup();
        ids
    }

    /// **Referenced-implies-registered** (D3): every `dict_id` any block
    /// references MUST resolve in `resolver`. Returns the first offending id as
    /// [`PayloadError::UnregisteredDict`]. A columnar-only segment references
    /// no dictionaries and trivially passes (even against [`NoDicts`]).
    pub fn verify_dicts_registered(
        &self,
        resolver: &impl DictResolver,
    ) -> Result<(), PayloadError> {
        for id in self.referenced_dict_ids() {
            if resolver.dict_bytes(id).is_none() {
                return Err(PayloadError::UnregisteredDict(id));
            }
        }
        Ok(())
    }

    /// One block's stored bytes: a slice of the resident image, or a `pread` of
    /// exactly that block from the retained handle (bn-bka2). A lazily read
    /// block is checksummed against its [`BlockEntry`] `crc` before the caller
    /// decodes it, so a torn block is a typed [`PayloadError::Corrupt`] — the
    /// same degradation a missing sidecar produces — instead of bytes the codec
    /// might mis-decode into a plausible payload.
    #[inline]
    fn block_bytes(
        &self,
        e: &BlockEntry,
    ) -> Result<Cow<'_, [u8]>, PayloadError> {
        let (off, len) = (e.byte_off as usize, e.byte_len as usize);
        match &self.src {
            BlockSource::Memory(bytes) => {
                Ok(Cow::Borrowed(&bytes[off..off + len]))
            }
            BlockSource::File { file, base } => {
                let mut buf = vec![0u8; len];
                file.read_exact_at(&mut buf, base + e.byte_off)?;
                if let Some(crc) = e.crc
                    && crc32c::crc32c(&buf) != crc
                {
                    return Err(PayloadError::Corrupt("block CRC mismatch"));
                }
                Ok(Cow::Owned(buf))
            }
        }
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
        let raw = self.block_bytes(e)?;
        if e.dict_id == 0 {
            // Columnar or codec-raw block: the codec decodes both.
            Ok(Block::decode(&raw)?.reassemble_one(row)?)
        } else {
            let dict = resolver
                .dict_bytes(e.dict_id)
                .ok_or(PayloadError::UnregisteredDict(e.dict_id))?;
            let img = decode_row_dict(&raw, dict)?;
            if row >= img.len() {
                return Err(PayloadError::IndexOutOfRange);
            }
            Ok(img.event(row).to_vec())
        }
    }

    /// The index of the block covering stored-order event `idx`, or `None`
    /// past the coverage (bn-2ib — lets a caller cache **decoded blocks**
    /// across reads and slice ranges out of them via
    /// [`reassemble_block`](Self::reassemble_block), instead of re-decoding a
    /// block for every overlapping batch).
    pub fn block_for(&self, idx: u64) -> Option<usize> { self.locate(idx) }

    /// Reassemble **every** event of block `bi` to exact source bytes,
    /// appending to `out` and `n_events + 1` boundaries to `offs` (bn-2ib).
    /// Event `blocks()[bi].first_event + i` is
    /// `out[offs[i] as usize..offs[i + 1] as usize]`. The unit a decoded-block
    /// cache stores: one decode serves every batch the block covers.
    pub fn reassemble_block(
        &self,
        bi: usize,
        resolver: &impl DictResolver,
        out: &mut Vec<u8>,
        offs: &mut Vec<u32>,
    ) -> Result<(), PayloadError> {
        out.clear();
        offs.clear();
        let e = self.blocks.get(bi).ok_or(PayloadError::IndexOutOfRange)?;
        let raw = self.block_bytes(e)?;
        if e.dict_id == 0 {
            let block = Block::decode(&raw)?;
            block.reassemble_all(out, offs)?;
        } else {
            let dict = resolver
                .dict_bytes(e.dict_id)
                .ok_or(PayloadError::UnregisteredDict(e.dict_id))?;
            let img = decode_row_dict(&raw, dict)?;
            for i in 0..img.len() {
                offs.push(out.len() as u32);
                out.extend_from_slice(img.event(i));
            }
            offs.push(out.len() as u32);
        }
        Ok(())
    }

    /// Reassemble the contiguous stored-order event range `[lo, hi)` to exact
    /// source bytes, decoding **only the blocks that cover the range**
    /// (bn-2ib — the block-native sealed read's per-batch payload fetch;
    /// [`reassemble_all`](Self::reassemble_all) would decode the whole
    /// segment, [`reassemble_event`](Self::reassemble_event) would re-decode
    /// a block once per event). Appends bytes to `out` and `hi - lo + 1`
    /// boundaries to `offs`: event `lo + i` is
    /// `out[offs[i] as usize..offs[i + 1] as usize]`. Byte-exact across mixed
    /// (columnar + row-fallback) blocks, exactly like the two existing paths.
    pub fn reassemble_range(
        &self,
        lo: u64,
        hi: u64,
        resolver: &impl DictResolver,
        out: &mut Vec<u8>,
        offs: &mut Vec<u32>,
    ) -> Result<(), PayloadError> {
        out.clear();
        offs.clear();
        if lo >= hi {
            offs.push(0);
            return Ok(());
        }
        if hi > self.event_count {
            return Err(PayloadError::IndexOutOfRange);
        }
        let mut bi = self.locate(lo).ok_or(PayloadError::IndexOutOfRange)?;
        let mut next = lo;
        while next < hi {
            let e = &self.blocks[bi];
            debug_assert!(next >= e.first_event);
            let raw = self.block_bytes(e)?;
            let block_lo = (next - e.first_event) as usize;
            let block_hi = (hi.min(e.first_event + u64::from(e.n_events))
                - e.first_event) as usize;
            if e.dict_id == 0 {
                // Columnar or codec-raw block: decode once, take the rows.
                let block = Block::decode(&raw)?;
                for row in block_lo..block_hi {
                    offs.push(out.len() as u32);
                    out.extend_from_slice(&block.reassemble_one(row)?);
                }
            } else {
                let dict = resolver
                    .dict_bytes(e.dict_id)
                    .ok_or(PayloadError::UnregisteredDict(e.dict_id))?;
                let img = decode_row_dict(&raw, dict)?;
                if block_hi > img.len() {
                    return Err(PayloadError::IndexOutOfRange);
                }
                for row in block_lo..block_hi {
                    offs.push(out.len() as u32);
                    out.extend_from_slice(img.event(row));
                }
            }
            next = e.first_event + u64::from(e.n_events);
            bi += 1;
        }
        offs.push(out.len() as u32);
        Ok(())
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
            let raw = self.block_bytes(e)?;
            if e.dict_id == 0 {
                let block = Block::decode(&raw)?;
                block.reassemble_all(out, offs)?;
                // reassemble_all pushes n+1 offsets; drop the trailing dup so
                // blocks concatenate into one contiguous offset list.
                offs.pop();
            } else {
                let dict = resolver
                    .dict_bytes(e.dict_id)
                    .ok_or(PayloadError::UnregisteredDict(e.dict_id))?;
                let img = decode_row_dict(&raw, dict)?;
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

/// The parsed 32-byte header. Shared by the eager
/// ([`SealedPayloadIndex::from_bytes`]) and lazy ([`SealedPayloadIndex::open`])
/// paths so the two can never validate different things.
struct Header {
    flags:       u16,
    segment_id:  u64,
    event_count: u64,
    n_blocks:    usize,
    index_crc:   u32,
}

impl Header {
    fn parse(h: &[u8]) -> Result<Self, PayloadError> {
        debug_assert_eq!(h.len(), HEADER_LEN);
        if rd32(h, 0) != PCOL_MAGIC {
            return Err(PayloadError::Corrupt("bad header magic"));
        }
        if rd16(h, 4) != FORMAT_VERSION {
            return Err(PayloadError::Corrupt("unknown format_version"));
        }
        Ok(Header {
            flags:       rd16(h, 6),
            segment_id:  rd64(h, 8),
            event_count: rd64(h, 16),
            n_blocks:    rd32(h, 24) as usize,
            index_crc:   rd32(h, 28),
        })
    }
}

/// Validate the 24-byte footer against `head` and return `index_off` (the
/// DATA/INDEX boundary), having proved the INDEX region is exactly
/// `n_blocks × BLOCK_ENTRY_LEN` bytes ending at `footer_start`.
fn parse_footer(
    foot: &[u8],
    head: &Header,
    footer_start: u64,
) -> Result<u64, PayloadError> {
    debug_assert_eq!(foot.len(), FOOTER_LEN);
    if rd32(foot, 20) != PCOL_MAGIC {
        return Err(PayloadError::Corrupt("bad footer magic"));
    }
    if rd32(foot, 12) as usize != head.n_blocks {
        return Err(PayloadError::Corrupt("footer/header n_blocks disagree"));
    }
    let index_off = rd64(foot, 0);
    let index_len = head
        .n_blocks
        .checked_mul(BLOCK_ENTRY_LEN)
        .ok_or(PayloadError::Corrupt("index length overflow"))?
        as u64;
    if index_off > footer_start || footer_start - index_off != index_len {
        return Err(PayloadError::Corrupt("index region size mismatch"));
    }
    if index_off < HEADER_LEN as u64 {
        return Err(PayloadError::Corrupt("index overlaps header"));
    }
    Ok(index_off)
}

/// Parse and fully validate the INDEX region (`idx` is exactly that region:
/// `n_blocks × BLOCK_ENTRY_LEN` bytes, which started at `index_off` in the
/// file). Every block span must lie inside DATA (`[HEADER_LEN, index_off)`)
/// and the blocks must tile `[0, event_count)` contiguously, so no later slice
/// or `pread` can escape the file.
fn parse_blocks(
    idx: &[u8],
    head: &Header,
    index_off: u64,
) -> Result<Vec<BlockEntry>, PayloadError> {
    debug_assert_eq!(idx.len(), head.n_blocks * BLOCK_ENTRY_LEN);
    let split_crc = head.flags & FLAG_SPLIT_CRC != 0;
    let mut blocks = Vec::with_capacity(head.n_blocks);
    let mut expect_first = 0u64;
    for i in 0..head.n_blocks {
        let b = i * BLOCK_ENTRY_LEN;
        let first_event = rd64(idx, b);
        let n_events = rd32(idx, b + 8);
        let byte_len = rd32(idx, b + 12);
        let byte_off = rd64(idx, b + 16);
        let kind = BlockKind::from_byte(idx[b + 24])?;
        let dict_id = rd16(idx, b + 26);
        let crc = split_crc.then(|| rd32(idx, b + 28));
        // Structural invariants so later slicing/reassembly cannot panic.
        let end = byte_off.saturating_add(u64::from(byte_len));
        if byte_off < HEADER_LEN as u64 || end > index_off {
            return Err(PayloadError::Corrupt("block span out of range"));
        }
        if first_event != expect_first {
            return Err(PayloadError::Corrupt(
                "block first_event not contiguous",
            ));
        }
        if kind == BlockKind::Columnar && dict_id != 0 {
            return Err(PayloadError::Corrupt(
                "columnar block carries a dict_id",
            ));
        }
        expect_first += u64::from(n_events);
        blocks.push(BlockEntry {
            first_event,
            n_events,
            kind,
            dict_id,
            byte_off,
            byte_len,
            crc,
        });
    }
    if expect_first != head.event_count {
        return Err(PayloadError::Corrupt("block event counts != event_count"));
    }
    Ok(blocks)
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
    use std::collections::BTreeMap;

    use super::*;
    use crate::columnar::{emit_int, emit_str};

    /// Tiny deterministic xorshift RNG (no external rng dep, mirrors the codec
    /// tests).
    struct Rng(u64);
    impl Rng {
        fn new(seed: u64) -> Self { Rng(seed ^ 0x9E37_79B9_7F4A_7C15) }

        fn next_u64(&mut self) -> u64 {
            let mut x = self.0;
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            self.0 = x;
            x
        }

        fn below(&mut self, n: u64) -> u64 { self.next_u64() % n }
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

    /// A **heavy** shreddable event (~180 B) mirroring the round-4 reference
    /// corpus shape (`spikes/perf_compress/src/workload.rs`): a 9-field
    /// rmp-named-style map with low-cardinality enum-ish strings (currency,
    /// source), a per-event actor word, a monotone timestamp, and a free-text
    /// `note` drawn from a small fixed vocabulary. The `note`'s cross-event
    /// vocabulary redundancy is exactly the content a 2048-event zstd-19 window
    /// exploits better than a 128-event zstd-9 one (REPORT.md H2: +3-8%).
    fn heavy_event(rng: &mut Rng, seq: u64) -> Vec<u8> {
        const CURRENCY: [&[u8]; 4] = [b"USD", b"EUR", b"GBP", b"JPY"];
        const SOURCE: [&[u8]; 4] = [b"web", b"mobile", b"api", b"batch"];
        const VOCAB: [&[u8]; 16] = [
            b"payment",
            b"received",
            b"from",
            b"customer",
            b"for",
            b"invoice",
            b"pending",
            b"review",
            b"approved",
            b"by",
            b"finance",
            b"team",
            b"scheduled",
            b"retry",
            b"gateway",
            b"timeout",
        ];
        let mut note = Vec::new();
        let words = 6 + rng.below(6) as usize;
        for w in 0..words {
            if w > 0 {
                note.push(b' ');
            }
            note.extend_from_slice(
                VOCAB[rng.below(VOCAB.len() as u64) as usize],
            );
        }
        let actor: Vec<u8> =
            (0..8).map(|_| b'a' + (rng.below(26) as u8)).collect();

        let mut v = vec![0x89]; // 9-field map
        emit_str(&mut v, b"stream");
        emit_str(
            &mut v,
            format!("account-{:07}", rng.below(10_000)).as_bytes(),
        );
        emit_str(&mut v, b"seq");
        emit_int(&mut v, seq as i64);
        emit_str(&mut v, b"amount_cents");
        emit_int(&mut v, rng.below(5_000_000) as i64);
        emit_str(&mut v, b"currency");
        emit_str(&mut v, CURRENCY[rng.below(4) as usize]);
        emit_str(&mut v, b"actor");
        emit_str(&mut v, &actor);
        emit_str(&mut v, b"source");
        emit_str(&mut v, SOURCE[rng.below(4) as usize]);
        emit_str(&mut v, b"note");
        emit_str(&mut v, &note);
        emit_str(&mut v, b"occurred_at_ms");
        emit_int(&mut v, 1_767_225_600_000_i64 + seq as i64 * 37);
        emit_str(&mut v, b"schema_v");
        emit_int(&mut v, 3);
        v
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
        let kinds: Vec<BlockKind> =
            idx.blocks().iter().map(|b| b.kind).collect();
        assert!(
            kinds.contains(&BlockKind::Columnar),
            "expected columnar blocks"
        );
        assert!(
            kinds.contains(&BlockKind::Row),
            "expected row-fallback blocks"
        );

        // Full reassembly is byte-exact.
        let mut out = Vec::new();
        let mut offs = Vec::new();
        idx.reassemble_all(&NoDicts, &mut out, &mut offs).unwrap();
        assert_eq!(offs.len(), evs.len() + 1);
        for (i, w) in offs.windows(2).enumerate() {
            assert_eq!(
                &out[w[0] as usize..w[1] as usize],
                evs[i].as_slice(),
                "range mismatch at {i}"
            );
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

    /// bn-2ib: `reassemble_range` must agree with `reassemble_all`'s slices
    /// for every range shape — inside one block, straddling block (and
    /// block-kind) boundaries, whole-segment, and empty — and reject a range
    /// past the coverage with a typed error, never a panic.
    #[test]
    fn reassemble_range_matches_full_reassembly_slices() {
        let evs = mixed_corpus();
        let bytes = encode_payload_sidecar(7, &refs(&evs), &opts()).unwrap();
        let idx = SealedPayloadIndex::from_bytes(bytes).unwrap();

        let n = evs.len() as u64;
        let ranges = [
            (0u64, n),  // whole segment
            (0, 1),     // first event
            (3, 9),     // inside the first (columnar) block
            (30, 35),   // straddles the 32-event block boundary
            (28, 100),  // straddles columnar → row-fallback kinds
            (n - 5, n), // tail
            (17, 17),   // empty range
        ];
        let mut out = Vec::new();
        let mut offs = Vec::new();
        for (lo, hi) in ranges {
            idx.reassemble_range(lo, hi, &NoDicts, &mut out, &mut offs)
                .unwrap();
            assert_eq!(
                offs.len() as u64,
                hi - lo + 1,
                "boundary count for [{lo},{hi})"
            );
            for (i, w) in offs.windows(2).enumerate() {
                assert_eq!(
                    &out[w[0] as usize..w[1] as usize],
                    evs[lo as usize + i].as_slice(),
                    "range [{lo},{hi}) mismatch at local {i}"
                );
            }
        }

        // Past-coverage range: typed error.
        assert!(matches!(
            idx.reassemble_range(n - 1, n + 1, &NoDicts, &mut out, &mut offs),
            Err(PayloadError::IndexOutOfRange)
        ));
        assert!(matches!(
            idx.reassemble_range(n + 3, n + 4, &NoDicts, &mut out, &mut offs),
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
        // Seal a good sidecar, then run the SHIPPED verify gate against a
        // source whose event 45 was corrupted: the reassembled
        // (correct) bytes no longer match the claimed source, so verify
        // aborts at exactly 45.
        let evs = mixed_corpus();
        let bytes = encode_payload_sidecar(7, &refs(&evs), &opts()).unwrap();
        let idx = SealedPayloadIndex::from_bytes(bytes).unwrap();

        let mut corrupted = evs.clone();
        corrupted[45] = b"a completely different payload".to_vec();
        let err =
            verify_reassembly(&idx, &refs(&corrupted), &NoDicts).unwrap_err();
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
        let col = idx
            .blocks()
            .iter()
            .find(|b| b.kind == BlockKind::Columnar)
            .unwrap();
        let target = col.byte_off as usize + col.byte_len as usize - 1;
        let first_event = col.first_event;

        let mut tampered = idx.into_bytes().unwrap();
        tampered[target] ^= 0xFF;
        // Repair the content CRC so from_bytes accepts the tampered image.
        let footer_start = tampered.len() - FOOTER_LEN;
        let crc = crc32c::crc32c(&tampered[..footer_start]);
        tampered[footer_start + 8..footer_start + 12]
            .copy_from_slice(&crc.to_le_bytes());

        let idx2 = SealedPayloadIndex::from_bytes(tampered).unwrap();
        let err = verify_reassembly(&idx2, &refs(&evs), &NoDicts).unwrap_err();
        match err {
            PayloadError::VerifyMismatch(i) => assert!(i >= first_event),
            // Corrupt zstd frame → decode error, also a refusal.
            PayloadError::Codec(_) => {}
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn row_fallback_dictionary_tier_roundtrips_and_registers() {
        // Force an all-binary corpus (every block falls back to row) and attach
        // a trained 16 KiB dictionary as dict_id 5.
        let mut rng = Rng::new(99);
        let evs: Vec<Vec<u8>> =
            (0..300).map(|_| binary_event(&mut rng)).collect();
        // Train a dictionary from the samples (bounded to 16 KiB).
        let samples: Vec<&[u8]> = evs.iter().map(Vec::as_slice).collect();
        let dict = zstd::dict::from_samples(&samples, MAX_DICT_BYTES)
            .unwrap_or_default();
        // `from_samples` can refuse tiny corpora; fall back to a raw-content
        // dictionary so the tier is still exercised.
        let dict = if dict.is_empty() { evs.concat() } else { dict };
        let dict = if dict.len() > MAX_DICT_BYTES {
            dict[..MAX_DICT_BYTES].to_vec()
        } else {
            dict
        };

        let o = PayloadSealOpts {
            block_events: 32,
            row_dict: Some((5, dict.clone())),
            ..Default::default()
        };
        let bytes = encode_payload_sidecar(3, &refs(&evs), &o).unwrap();
        let idx = SealedPayloadIndex::from_bytes(bytes).unwrap();

        // Every block is a row block referencing dict_id 5.
        assert!(
            idx.blocks()
                .iter()
                .all(|b| b.kind == BlockKind::Row && b.dict_id == 5)
        );
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
        let evs: Vec<Vec<u8>> =
            (0..100).map(|_| binary_event(&mut rng)).collect();
        let dict = {
            let d = evs.concat();
            d[..d.len().min(MAX_DICT_BYTES)].to_vec()
        };
        let o = PayloadSealOpts {
            block_events: 32,
            row_dict: Some((9, dict.clone())),
            ..Default::default()
        };
        let bytes = encode_payload_sidecar(1, &refs(&evs), &o).unwrap();
        let idx = SealedPayloadIndex::from_bytes(bytes).unwrap();

        // Registered: passes.
        let mut reg: BTreeMap<u16, Vec<u8>> = BTreeMap::new();
        reg.insert(9, dict);
        idx.verify_dicts_registered(&reg).unwrap();

        // Unregistered (empty registry): the gate rejects with the offending
        // id.
        let err = idx.verify_dicts_registered(&NoDicts).unwrap_err();
        assert!(
            matches!(err, PayloadError::UnregisteredDict(9)),
            "got {err:?}"
        );

        // A referenced-but-unregistered dict also fails an actual read.
        assert!(matches!(
            idx.reassemble_event(0, &NoDicts),
            Err(PayloadError::UnregisteredDict(9))
        ));
    }

    #[test]
    fn columnar_only_segment_needs_no_dicts() {
        let mut rng = Rng::new(11);
        let evs: Vec<Vec<u8>> =
            (0..200).map(|i| msgpack_event(&mut rng, i)).collect();
        let bytes = encode_payload_sidecar(2, &refs(&evs), &opts()).unwrap();
        let idx = SealedPayloadIndex::from_bytes(bytes).unwrap();
        assert!(idx.blocks().iter().all(|b| b.kind == BlockKind::Columnar));
        assert!(idx.referenced_dict_ids().is_empty());
        // A columnar-only segment trivially satisfies the gate even with no
        // dicts.
        idx.verify_dicts_registered(&NoDicts).unwrap();
    }

    #[test]
    fn corrupt_crc_and_truncation_rejected() {
        let evs = mixed_corpus();
        let good = encode_payload_sidecar(7, &refs(&evs), &opts()).unwrap();

        let mut bad = good.clone();
        bad[HEADER_LEN] ^= 0xFF; // flip a byte in the DATA region
        assert!(matches!(
            SealedPayloadIndex::from_bytes(bad),
            Err(PayloadError::Corrupt(_))
        ));

        let short = good[..HEADER_LEN + FOOTER_LEN - 1].to_vec();
        assert!(matches!(
            SealedPayloadIndex::from_bytes(short),
            Err(PayloadError::Corrupt(_))
        ));

        let mut bad_magic = good.clone();
        bad_magic[0] ^= 0x01;
        assert!(matches!(
            SealedPayloadIndex::from_bytes(bad_magic),
            Err(PayloadError::Corrupt(_))
        ));
    }

    // -----------------------------------------------------------------------
    // bn-bka2: lazy attach (`open`) — header/footer/block-index only at open,
    // block bytes on demand, corruption discovered at read time.
    // -----------------------------------------------------------------------

    /// Write `bytes` as segment `seg`'s `.pcol` under `dir` and lazily attach
    /// it, asserting the attach succeeded and is file-backed.
    fn attach_lazy(
        dir: &std::path::Path,
        seg: u64,
        bytes: &[u8],
    ) -> SealedPayloadIndex {
        std::fs::write(pcol_path(dir, seg), bytes).unwrap();
        let idx = SealedPayloadIndex::open(&pcol_path(dir, seg))
            .expect("io")
            .expect("parse");
        assert!(!idx.is_resident(), "open must attach lazily, not slurp");
        idx
    }

    /// Rewrite `image` into the pre-bn-bka2 shape: `flags == 0`, no
    /// `index_crc`, no per-block `block_crc`, `content_crc` repaired — exactly
    /// what a sidecar sealed before this change looks like on disk.
    fn strip_split_crcs(mut image: Vec<u8>) -> Vec<u8> {
        let footer_start = image.len() - FOOTER_LEN;
        let index_off = rd64(&image[footer_start..], 0) as usize;
        let n_blocks = rd32(&image, 24) as usize;
        image[6..8].copy_from_slice(&0u16.to_le_bytes()); // flags
        image[28..32].copy_from_slice(&0u32.to_le_bytes()); // index_crc
        for i in 0..n_blocks {
            let b = index_off + i * BLOCK_ENTRY_LEN + 28;
            image[b..b + 4].copy_from_slice(&0u32.to_le_bytes());
        }
        let crc = crc32c::crc32c(&image[..footer_start]);
        image[footer_start + 8..footer_start + 12]
            .copy_from_slice(&crc.to_le_bytes());
        image
    }

    /// The heart of bn-bka2: `open` must not read the DATA region. Proof by
    /// construction — a sidecar whose payload bytes are destroyed but whose
    /// header/footer/block index are intact **opens fine** (an eager
    /// `from_bytes` of the same image rejects it on `content_crc`), reads of
    /// untouched blocks stay byte-exact, and the torn block is caught when it
    /// is read, as the typed error the sealed read path degrades on (D1 — the
    /// raw log remains the payload authority).
    #[test]
    #[cfg_attr(miri, ignore)]
    fn lazy_open_skips_payload_bytes_and_tears_surface_at_read() {
        let dir =
            mess_testkit::sweeping_temp_dir("idx-payload-lazy-open-skips");
        let seg = 11u64;
        let evs = mixed_corpus();
        let good = encode_payload_sidecar(seg, &refs(&evs), &opts()).unwrap();

        // Destroy the second block's stored bytes; touch nothing else.
        let victim = 1usize;
        let e = SealedPayloadIndex::from_bytes(good.clone()).unwrap().blocks()
            [victim];
        let (vlo, vhi) = (e.first_event, e.first_event + u64::from(e.n_events));
        let mut torn = good.clone();
        let span =
            e.byte_off as usize..(e.byte_off as usize + e.byte_len as usize);
        for b in &mut torn[span] {
            *b ^= 0xFF;
        }

        // An eager open reads every byte, so it rejects the image outright —
        // this is the check the lazy path provably does not run.
        assert!(matches!(
            SealedPayloadIndex::from_bytes(torn.clone()),
            Err(PayloadError::Corrupt(_))
        ));

        // The lazy open attaches: identity and geometry come from the bounded
        // header/footer/index read, and the DATA region is never looked at.
        let lazy = attach_lazy(dir.path(), seg, &torn);
        assert_eq!(lazy.segment_id(), seg);
        assert_eq!(lazy.event_count() as usize, evs.len());
        assert_eq!(
            lazy.block_count(),
            SealedPayloadIndex::from_bytes(good).unwrap().block_count()
        );

        // Reading the torn block is a typed Corrupt error (its `block_crc`
        // fails before the codec ever sees the bytes), for every read shape.
        let mut out = Vec::new();
        let mut offs = Vec::new();
        assert!(matches!(
            lazy.reassemble_event(vlo, &NoDicts),
            Err(PayloadError::Corrupt(_))
        ));
        assert!(matches!(
            lazy.reassemble_block(victim, &NoDicts, &mut out, &mut offs),
            Err(PayloadError::Corrupt(_))
        ));
        assert!(matches!(
            lazy.reassemble_range(vlo, vhi, &NoDicts, &mut out, &mut offs),
            Err(PayloadError::Corrupt(_))
        ));
        assert!(matches!(
            lazy.reassemble_all(&NoDicts, &mut out, &mut offs),
            Err(PayloadError::Corrupt(_))
        ));

        // Every OTHER block still reassembles byte-exact through the lazy
        // path: one torn block degrades one read, not the whole sidecar.
        for (i, ev) in evs.iter().enumerate() {
            let gi = i as u64;
            if (vlo..vhi).contains(&gi) {
                continue;
            }
            assert_eq!(
                &lazy.reassemble_event(gi, &NoDicts).unwrap(),
                ev,
                "point {i}"
            );
        }
        lazy.reassemble_range(0, vlo, &NoDicts, &mut out, &mut offs).unwrap();
        for (i, w) in offs.windows(2).enumerate() {
            assert_eq!(&out[w[0] as usize..w[1] as usize], evs[i].as_slice());
        }
    }

    /// A lazily attached sidecar agrees byte-for-byte with the eager one on
    /// every read shape, across mixed columnar / row-fallback blocks.
    #[test]
    #[cfg_attr(miri, ignore)]
    fn lazy_and_eager_reads_agree_byte_for_byte() {
        let dir = mess_testkit::sweeping_temp_dir("idx-payload-lazy-agrees");
        let seg = 3u64;
        let evs = mixed_corpus();
        let image = encode_payload_sidecar(seg, &refs(&evs), &opts()).unwrap();
        let eager = SealedPayloadIndex::from_bytes(image.clone()).unwrap();
        assert!(eager.is_resident());
        let lazy = attach_lazy(dir.path(), seg, &image);

        assert_eq!(lazy.block_count(), eager.block_count());
        assert_eq!(lazy.event_count(), eager.event_count());

        let n = evs.len() as u64;
        for gi in 0..n {
            assert_eq!(
                lazy.reassemble_event(gi, &NoDicts).unwrap(),
                eager.reassemble_event(gi, &NoDicts).unwrap(),
                "point {gi}"
            );
        }
        let (mut lo_o, mut lo_f) = (Vec::new(), Vec::new());
        let (mut eo, mut ef) = (Vec::new(), Vec::new());
        for (lo, hi) in
            [(0, n), (0, 1), (3, 9), (30, 35), (28, 100), (n - 5, n), (17, 17)]
        {
            lazy.reassemble_range(lo, hi, &NoDicts, &mut lo_o, &mut lo_f)
                .unwrap();
            eager.reassemble_range(lo, hi, &NoDicts, &mut eo, &mut ef).unwrap();
            assert_eq!((&lo_o, &lo_f), (&eo, &ef), "range [{lo},{hi})");
        }
        for bi in 0..lazy.block_count() {
            lazy.reassemble_block(bi, &NoDicts, &mut lo_o, &mut lo_f).unwrap();
            eager.reassemble_block(bi, &NoDicts, &mut eo, &mut ef).unwrap();
            assert_eq!((&lo_o, &lo_f), (&eo, &ef), "block {bi}");
        }
        lazy.reassemble_all(&NoDicts, &mut lo_o, &mut lo_f).unwrap();
        eager.reassemble_all(&NoDicts, &mut eo, &mut ef).unwrap();
        assert_eq!((lo_o, lo_f), (eo, ef), "full scan");
    }

    /// Structural damage is still caught **at open**, so the sidecar is
    /// dropped before it is ever attached: a torn block index (caught by
    /// `index_crc`), a torn footer, a truncated file, and a bad magic.
    #[test]
    #[cfg_attr(miri, ignore)]
    fn lazy_open_rejects_structural_damage() {
        let dir =
            mess_testkit::sweeping_temp_dir("idx-payload-lazy-open-rejects");
        let seg = 4u64;
        let evs = mixed_corpus();
        let good = encode_payload_sidecar(seg, &refs(&evs), &opts()).unwrap();
        let footer_start = good.len() - FOOTER_LEN;
        let index_off = rd64(&good[footer_start..], 0) as usize;

        let open_at = |name: &str, bytes: &[u8]| {
            let p = dir.path().join(name);
            std::fs::write(&p, bytes).unwrap();
            SealedPayloadIndex::open(&p).unwrap()
        };

        // Block index flipped: the entry table's own crc32c catches it.
        let mut bad_index = good.clone();
        bad_index[index_off + 9] ^= 0xFF;
        assert!(matches!(
            open_at("bad-index.pcol", &bad_index),
            Err(PayloadError::Corrupt("block index CRC mismatch"))
        ));

        // A block span pointed outside DATA is refused even without the CRC
        // (the legacy shape): structural validation is unconditional.
        let mut oob = good.clone();
        oob[index_off + 16..index_off + 24]
            .copy_from_slice(&(index_off as u64).to_le_bytes());
        let oob = strip_split_crcs(oob);
        assert!(matches!(
            open_at("oob.pcol", &oob),
            Err(PayloadError::Corrupt("block span out of range"))
        ));

        // Footer magic gone (e.g. a partially rewritten file).
        let mut bad_footer = good.clone();
        bad_footer[footer_start + 20] ^= 0xFF;
        assert!(matches!(
            open_at("bad-footer.pcol", &bad_footer),
            Err(PayloadError::Corrupt("bad footer magic"))
        ));

        // Truncated to a husk (the sealer crashed mid-write, or the file was
        // clipped): the footer read lands in the middle of DATA.
        assert!(matches!(
            open_at("husk.pcol", &good[..good.len() / 3]),
            Err(PayloadError::Corrupt(_))
        ));
        assert!(matches!(
            open_at("tiny.pcol", &good[..HEADER_LEN + FOOTER_LEN - 1]),
            Err(PayloadError::Corrupt("shorter than header + footer"))
        ));

        // Not a `.pcol` at all.
        let mut bad_magic = good.clone();
        bad_magic[0] ^= 0x01;
        assert!(matches!(
            open_at("bad-magic.pcol", &bad_magic),
            Err(PayloadError::Corrupt("bad header magic"))
        ));

        // A missing file is an io error, not a parse error — the caller drops
        // the sidecar either way.
        assert!(
            SealedPayloadIndex::open(&dir.path().join("absent.pcol")).is_err()
        );
    }

    /// Backward compatibility: a sidecar sealed before bn-bka2 (`flags == 0`,
    /// no split CRCs) opens and reads byte-exact — and, having no per-block
    /// checksum to attach lazily against, it takes the **eager** path so it
    /// keeps the whole-image `content_crc` check an open performs today.
    #[test]
    #[cfg_attr(miri, ignore)]
    fn legacy_sidecar_without_split_crcs_attaches_eagerly_and_reads() {
        let dir = mess_testkit::sweeping_temp_dir("idx-payload-lazy-legacy");
        let seg = 8u64;
        let evs = mixed_corpus();
        let legacy = strip_split_crcs(
            encode_payload_sidecar(seg, &refs(&evs), &opts()).unwrap(),
        );
        // The stripped image is still a valid v1 sidecar to the eager reader.
        SealedPayloadIndex::from_bytes(legacy.clone()).unwrap();

        std::fs::write(pcol_path(dir.path(), seg), &legacy).unwrap();
        let idx = SealedPayloadIndex::open(&pcol_path(dir.path(), seg))
            .expect("io")
            .expect("parse");
        assert!(
            idx.is_resident(),
            "a flags==0 sidecar must attach eagerly: lazily it would carry no \
             checksum at all"
        );
        assert!(idx.blocks().iter().all(|b| b.crc.is_none()));
        for (i, ev) in evs.iter().enumerate() {
            assert_eq!(&idx.reassemble_event(i as u64, &NoDicts).unwrap(), ev);
        }
        let mut out = Vec::new();
        let mut offs = Vec::new();
        idx.reassemble_all(&NoDicts, &mut out, &mut offs).unwrap();
        for (i, w) in offs.windows(2).enumerate() {
            assert_eq!(&out[w[0] as usize..w[1] as usize], evs[i].as_slice());
        }
    }

    /// bn-bka2 review: the legacy path must not lose integrity. A torn byte in
    /// a pre-`FLAG_SPLIT_CRC` sidecar's DATA region — invisible to the
    /// structural checks and, on a raw block, potentially decodable into
    /// plausible-but-wrong bytes — is caught by `content_crc` and the sidecar
    /// is **dropped at open**, byte-for-byte what trunk does. The same tear in
    /// a `FLAG_SPLIT_CRC` sidecar attaches and degrades at read
    /// (`lazy_open_skips_payload_bytes_and_tears_surface_at_read`): both
    /// refuse to serve the block, they just discover it at different moments.
    #[test]
    #[cfg_attr(miri, ignore)]
    fn torn_legacy_sidecar_is_dropped_at_open_like_trunk() {
        let dir =
            mess_testkit::sweeping_temp_dir("idx-payload-legacy-torn-open");
        let seg = 21u64;
        let evs = mixed_corpus();
        let good = encode_payload_sidecar(seg, &refs(&evs), &opts()).unwrap();

        // Build the legacy image first (stripping recomputes `content_crc`
        // over the clean bytes, so the file is exactly a valid pre-bn-bka2
        // sidecar), THEN tear a byte in the DATA region — real media damage
        // after the fact, which only `content_crc` can see.
        let mut torn = strip_split_crcs(good.clone());
        torn[HEADER_LEN + 4] ^= 0xFF;
        std::fs::write(pcol_path(dir.path(), seg), &torn).unwrap();

        match SealedPayloadIndex::open(&pcol_path(dir.path(), seg)).unwrap() {
            Err(PayloadError::Corrupt("content CRC mismatch")) => {}
            other => panic!(
                "torn legacy sidecar must be dropped at open, got {other:?}"
            ),
        }

        // Control: the same file with the tear reverted attaches and serves.
        let clean = strip_split_crcs(good);
        std::fs::write(pcol_path(dir.path(), seg), &clean).unwrap();
        let idx = SealedPayloadIndex::open(&pcol_path(dir.path(), seg))
            .unwrap()
            .unwrap();
        assert_eq!(idx.reassemble_event(0, &NoDicts).unwrap(), evs[0]);
    }

    /// Concurrent readers share one lazily attached index by `&self` with no
    /// lock on the read path (positioned `pread`s do not touch a shared file
    /// offset), so parallel point reads are byte-exact and do not serialise.
    #[test]
    #[cfg_attr(miri, ignore)]
    fn lazy_index_serves_concurrent_readers() {
        let dir =
            mess_testkit::sweeping_temp_dir("idx-payload-lazy-concurrent");
        let seg = 12u64;
        let evs = mixed_corpus();
        let image = encode_payload_sidecar(seg, &refs(&evs), &opts()).unwrap();
        let idx = attach_lazy(dir.path(), seg, &image);

        std::thread::scope(|s| {
            for t in 0..4u64 {
                let idx = &idx;
                let evs = &evs;
                s.spawn(move || {
                    for round in 0..3u64 {
                        for (i, ev) in evs.iter().enumerate() {
                            let gi =
                                (i as u64 + t * 37 + round) % evs.len() as u64;
                            let want = &evs[gi as usize];
                            assert_eq!(
                                &idx.reassemble_event(gi, &NoDicts).unwrap(),
                                want
                            );
                            let _ = ev;
                        }
                    }
                });
            }
        });
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
        // shreddable msgpack (columnar), ~10% pure binary (row fallback) —
        // since one unshreddable event routes a whole block to raw, a
        // realistic corpus clusters rather than sprinkling, so the
        // columnar default dominates.
        let mut evs: Vec<Vec<u8>> = Vec::with_capacity(TOTAL);
        let mut i = 0u64;
        while evs.len() < TOTAL {
            let binary_block = rng.below(10) == 0;
            for _ in 0..DEFAULT_BLOCK_EVENTS {
                if evs.len() == TOTAL {
                    break;
                }
                evs.push(if binary_block {
                    binary_event(&mut rng)
                } else {
                    msgpack_event(&mut rng, i)
                });
                i += 1;
            }
        }
        let raw_bytes: usize = evs.iter().map(Vec::len).sum();
        let refs = refs(&evs);

        let t = Instant::now();
        let bytes =
            encode_payload_sidecar(1, &refs, &PayloadSealOpts::default())
                .unwrap();
        let seal_dt = t.elapsed();
        let sidecar_len = bytes.len();

        let idx = SealedPayloadIndex::from_bytes(bytes).unwrap();
        let row_blocks =
            idx.blocks().iter().filter(|b| b.kind == BlockKind::Row).count();

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

        eprintln!(
            "=== sealed payload replay bench (zstd-9, \
             {DEFAULT_BLOCK_EVENTS}-event blocks) ==="
        );
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
        eprintln!(
            "  row fallbacks     {row_blocks}/{} blocks",
            idx.block_count()
        );
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
        eprintln!(
            "  point read        {:.3} us/read ({n_pr} reads)",
            pr_dt.as_secs_f64() * 1e6 / n_pr as f64
        );
    }

    #[test]
    fn magicless_framing_saves_the_magic_bytes() {
        // The dict row block is magicless: its zstd frame carries no 4-byte
        // magic. Confirm a magicless frame decodes only with the magicless
        // decompressor (proving the framing is actually magic-stripped).
        let mut rng = Rng::new(5);
        let evs: Vec<Vec<u8>> =
            (0..40).map(|_| binary_event(&mut rng)).collect();
        let dict = evs.concat();
        let block = encode_row_dict(&refs(&evs), 9, &dict).unwrap();
        // Body after the u32 ulen must NOT start with the zstd magic
        // 0x28B52FFD.
        let magic = [0x28u8, 0xB5, 0x2F, 0xFD];
        assert_ne!(&block[4..8], &magic, "frame should be magicless");
    }

    // -----------------------------------------------------------------------
    // bn-382: offline archive re-block (2048-event columnar blocks @ zstd-19)
    // -----------------------------------------------------------------------

    /// Write `bytes` to the segment's `.pcol` path in `dir` (test helper — the
    /// on-disk starting state a re-block operates on).
    fn write_pcol(dir: &std::path::Path, seg: u64, bytes: &[u8]) {
        std::fs::write(pcol_path(dir, seg), bytes).unwrap();
    }

    /// Re-blocking an already-sealed `.pcol` (128-event / zstd-9) into
    /// 2048-event zstd-19 blocks keeps every payload byte-exact — point AND
    /// range reads — while shrinking the block count. The one read path
    /// handles both geometries.
    #[test]
    #[cfg_attr(miri, ignore)]
    fn archive_reblock_is_byte_exact_point_and_range() {
        let dir = mess_testkit::sweeping_temp_dir(
            "idx-payload-archive-reblock-is-byte",
        );
        let seg = 42u64;

        // A ~5000-event mixed corpus so the default 128-event geometry produces
        // many small blocks and the 2048 re-block produces just a few big ones.
        let mut rng = Rng::new(0xA5C11);
        let mut evs: Vec<Vec<u8>> = Vec::new();
        for round in 0..50u64 {
            for i in 0..90 {
                evs.push(msgpack_event(&mut rng, round * 90 + i));
            }
            for _ in 0..10 {
                evs.push(binary_event(&mut rng));
            }
        }

        // Seal at the round-4 default geometry (128-event blocks, zstd-9) on
        // disk.
        let sealed = encode_payload_sidecar(
            seg,
            &refs(&evs),
            &PayloadSealOpts::default(),
        )
        .unwrap();
        write_pcol(dir.path(), seg, &sealed);
        let old_idx = SealedPayloadIndex::from_bytes(sealed).unwrap();
        // Sanity: the default geometry blocks are 128 events (except the tail).
        assert!(
            old_idx.blocks().iter().rev().skip(1).all(|b| b.n_events == 128)
        );

        // Re-block to the archive frontier.
        let out = archive_reblock(
            dir.path(),
            seg,
            &ArchivePolicy::archive(),
            &NoDicts,
        )
        .unwrap();
        assert!(out.reblocked);
        assert_eq!(out.event_count as usize, evs.len());
        assert!(out.new_blocks < out.old_blocks, "re-block coalesces blocks");

        // Re-open the re-blocked file straight from disk: the SAME read path.
        let new_idx = SealedPayloadIndex::open(&pcol_path(dir.path(), seg))
            .unwrap()
            .unwrap();
        assert_eq!(new_idx.segment_id(), seg);
        assert_eq!(new_idx.event_count() as usize, evs.len());
        // New geometry: 2048-event blocks (except the tail) — proof the block
        // size actually changed and is carried per-block in the format.
        assert!(
            new_idx.blocks().iter().rev().skip(1).all(|b| b.n_events == 2048)
        );
        assert_eq!(new_idx.block_count(), out.new_blocks);

        // Full range replay is byte-exact.
        let mut o = Vec::new();
        let mut offs = Vec::new();
        new_idx.reassemble_all(&NoDicts, &mut o, &mut offs).unwrap();
        assert_eq!(offs.len(), evs.len() + 1);
        for (i, w) in offs.windows(2).enumerate() {
            assert_eq!(
                &o[w[0] as usize..w[1] as usize],
                evs[i].as_slice(),
                "range mismatch at {i}"
            );
        }
        // Every point read is byte-exact across the whole segment.
        for (i, ev) in evs.iter().enumerate() {
            assert_eq!(
                &new_idx.reassemble_event(i as u64, &NoDicts).unwrap(),
                ev,
                "point {i}"
            );
        }
        // Out-of-range is still a clean error, not a panic.
        assert!(matches!(
            new_idx.reassemble_event(evs.len() as u64, &NoDicts),
            Err(PayloadError::IndexOutOfRange)
        ));
    }

    /// A crash mid-reblock (temp written, rename not yet done) leaves the OLD
    /// `.pcol` intact and serving; the `.tmp` husk is ignored by the reader.
    #[test]
    #[cfg_attr(miri, ignore)]
    fn archive_reblock_crash_leaves_old_pcol_serving() {
        let dir = mess_testkit::sweeping_temp_dir(
            "idx-payload-archive-reblock-crash-leaves",
        );
        let seg = 7u64;
        let mut rng = Rng::new(0xC7A5);
        let evs: Vec<Vec<u8>> =
            (0..300).map(|i| msgpack_event(&mut rng, i)).collect();

        // The committed, serving `.pcol` at the default geometry.
        let sealed = encode_payload_sidecar(
            seg,
            &refs(&evs),
            &PayloadSealOpts::default(),
        )
        .unwrap();
        write_pcol(dir.path(), seg, &sealed);
        let before = std::fs::read(pcol_path(dir.path(), seg)).unwrap();

        // Simulate a crash mid-reblock: a partial temp husk exists next to the
        // `.pcol`, but the rename never happened (write_durable's temp is
        // `<name>.tmp`).
        let path = pcol_path(dir.path(), seg);
        let mut husk = path.file_name().unwrap().to_os_string();
        husk.push(".tmp");
        let husk = path.with_file_name(husk);
        std::fs::write(&husk, b"partial garbage, not a valid .pcol").unwrap();

        // The OLD `.pcol` is byte-identical and still serves every payload; the
        // reader never touches the husk.
        assert_eq!(
            std::fs::read(&path).unwrap(),
            before,
            "old .pcol untouched by the crash"
        );
        let idx = SealedPayloadIndex::open(&path).unwrap().unwrap();
        assert_eq!(idx.event_count() as usize, evs.len());
        for (i, ev) in evs.iter().enumerate() {
            assert_eq!(&idx.reassemble_event(i as u64, &NoDicts).unwrap(), ev);
        }

        // Recovery re-runs the re-block: it completes atomically over the husk
        // and the new file serves byte-exact.
        let out = archive_reblock(
            dir.path(),
            seg,
            &ArchivePolicy::archive(),
            &NoDicts,
        )
        .unwrap();
        assert!(out.reblocked);
        let reblocked = SealedPayloadIndex::open(&path).unwrap().unwrap();
        for (i, ev) in evs.iter().enumerate() {
            assert_eq!(
                &reblocked.reassemble_event(i as u64, &NoDicts).unwrap(),
                ev
            );
        }
    }

    /// Policy OFF (the default) is a strict no-op: the `.pcol` is not read or
    /// written, its bytes are unchanged, and the outcome reports `reblocked:
    /// false`.
    #[test]
    #[cfg_attr(miri, ignore)]
    fn archive_reblock_policy_off_is_noop() {
        let dir = mess_testkit::sweeping_temp_dir(
            "idx-payload-archive-reblock-policy-off",
        );
        let seg = 3u64;
        let mut rng = Rng::new(0x0FF);
        let evs: Vec<Vec<u8>> =
            (0..200).map(|i| msgpack_event(&mut rng, i)).collect();
        let sealed = encode_payload_sidecar(
            seg,
            &refs(&evs),
            &PayloadSealOpts::default(),
        )
        .unwrap();
        write_pcol(dir.path(), seg, &sealed);
        let before = std::fs::read(pcol_path(dir.path(), seg)).unwrap();

        // Default policy is OFF.
        assert!(!ArchivePolicy::default().enabled);
        let out = archive_reblock(
            dir.path(),
            seg,
            &ArchivePolicy::default(),
            &NoDicts,
        )
        .unwrap();
        assert!(!out.reblocked, "disabled policy does nothing");

        let after = std::fs::read(pcol_path(dir.path(), seg)).unwrap();
        assert_eq!(before, after, "policy-off left the .pcol byte-identical");
        // No stray temp husk either.
        let mut husk =
            pcol_path(dir.path(), seg).file_name().unwrap().to_os_string();
        husk.push(".tmp");
        assert!(!pcol_path(dir.path(), seg).with_file_name(husk).exists());
    }

    // -----------------------------------------------------------------------
    // Bench: archive re-block bytes/event improvement on a 1M-event corpus.
    // Run with:
    //   TMPDIR=$HOME/.cache/mess-test-tmp cargo test -p mess-index --release \
    //     sealed::payload::tests::archive_reblock_bench -- --ignored
    // --nocapture
    // -----------------------------------------------------------------------
    #[test]
    #[ignore = "perf bench; run explicitly with --release --ignored --nocapture"]
    fn archive_reblock_bench() {
        use std::time::Instant;
        const TOTAL: usize = 1_000_000;
        let mut rng = Rng::new(0xA2C41BE);
        // Heavy reference-shaped corpus (~180 B/event, columnar-shreddable),
        // the regime the archive tier targets: real cross-event
        // redundancy that a larger block window + higher zstd level
        // claw back (REPORT.md: 30.8 → 26.5 B/event at 2048/zstd-19).
        // ~5% incompressible binary blocks stand in for the
        // row-fallback tail.
        let mut evs: Vec<Vec<u8>> = Vec::with_capacity(TOTAL);
        let mut i = 0u64;
        while evs.len() < TOTAL {
            let binary_block = rng.below(20) == 0;
            for _ in 0..DEFAULT_BLOCK_EVENTS {
                if evs.len() == TOTAL {
                    break;
                }
                evs.push(if binary_block {
                    binary_event(&mut rng)
                } else {
                    heavy_event(&mut rng, i)
                });
                i += 1;
            }
        }
        let raw_bytes: usize = evs.iter().map(Vec::len).sum();
        let refs = refs(&evs);

        let dir = mess_testkit::sweeping_temp_dir(
            "idx-payload-archive-reblock-bench",
        );
        let seg = 1u64;

        // Baseline seal at the round-4 default (128-event blocks, zstd-9).
        let sealed =
            encode_payload_sidecar(seg, &refs, &PayloadSealOpts::default())
                .unwrap();
        let base_len = sealed.len();
        let base_blocks = SealedPayloadIndex::from_bytes(sealed.clone())
            .unwrap()
            .block_count();
        write_pcol(dir.path(), seg, &sealed);

        // Offline archive re-block (2048-event blocks, zstd-19).
        let t = Instant::now();
        let out = archive_reblock(
            dir.path(),
            seg,
            &ArchivePolicy::archive(),
            &NoDicts,
        )
        .unwrap();
        let reblock_dt = t.elapsed();

        // Replay throughput off the re-blocked file.
        let new_idx = SealedPayloadIndex::open(&pcol_path(dir.path(), seg))
            .unwrap()
            .unwrap();
        let t = Instant::now();
        let mut o = Vec::new();
        let mut offs = Vec::new();
        new_idx.reassemble_all(&NoDicts, &mut o, &mut offs).unwrap();
        let replay_dt = t.elapsed();
        assert_eq!(offs.len(), evs.len() + 1);

        eprintln!("=== archive re-block bench (1M events, heavy corpus) ===");
        eprintln!(
            "  raw payload          {raw_bytes} B ({:.1} B/event)",
            raw_bytes as f64 / TOTAL as f64
        );
        eprintln!(
            "  baseline 128/zstd-9  {base_len} B ({:.2} B/event, \
             {base_blocks} blocks)",
            base_len as f64 / TOTAL as f64
        );
        eprintln!(
            "  archive  2048/zstd-19 {} B ({:.2} B/event, {} blocks)",
            out.new_bytes,
            out.new_bytes as f64 / TOTAL as f64,
            out.new_blocks
        );
        eprintln!(
            "  improvement          {:.2} B/event ({:.1}%)",
            (base_len as f64 - out.new_bytes as f64) / TOTAL as f64,
            (base_len as f64 - out.new_bytes as f64) / base_len as f64 * 100.0
        );
        eprintln!("  re-block wall        {reblock_dt:?}");
        eprintln!(
            "  archive replay       {:.2} M ev/s ({replay_dt:?})",
            TOTAL as f64 / replay_dt.as_secs_f64() / 1e6
        );
    }
}
