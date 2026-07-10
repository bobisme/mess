//! Columnar payload codec for the sealed tier (bn-1bn).
//!
//! This is a **standalone codec stage**: it turns a run of event payloads
//! (opaque `&[u8]`, in practice rmp-serde named-mode MessagePack — see
//! `01-log-format.md` codec_id 1) into a compact, self-describing **columnar
//! block**, and reassembles the block back to the **byte-exact** original
//! payloads. It owns the columnar block byte format; the sealer that emits
//! these blocks alongside the pointer index (bn-zge) is a separate bone and
//! this module carries no sealer state.
//!
//! Round-4 default sealed tier is columnar / 128-event blocks / zstd-9 / no
//! dicts (`spikes/perf_compress`, proven byte-exact on 1M events). This module
//! ports that shredder/reassembler as a codec: skeleton interning, per-path
//! columns, delta/varint int columns, zstd per block (or per column).
//!
//! # Byte-exactness is the acceptance bar
//!
//! Reassembly reproduces the original payload bytes **exactly** — a memcmp
//! passes for every event, always. Two disciplines make that sound over
//! *arbitrary* input, not just the reference corpus:
//!
//! 1. **Structural bytes are stored verbatim.** Map/array headers and map keys
//!    are copied into the per-skeleton *literal* runs, so their exact encoding
//!    (including non-canonical container/key headers) round-trips untouched.
//! 2. **Scalars are canonicality-checked at shred time.** Integer and string
//!    *values* are re-emitted from decoded form with minimal MessagePack
//!    encodings ([`emit_int`]/[`emit_str`], matching what rmp-serde emits). If
//!    an event's original scalar encoding is *not* the minimal one — or uses a
//!    marker this codec does not model (floats, bin, ext, u64 > i64::MAX, …),
//!    or is not valid MessagePack at all — the shred fails and the whole block
//!    falls back to a [raw block](#raw-block-flag_columnar-clear) that stores
//!    the payloads verbatim. **Fallback is never an error and never lossy.**
//!
//! So every block is byte-exact by construction: shreddable blocks re-emit
//! canonical bytes that equal the (canonical) originals; everything else is
//! stored raw. The shredder never panics on adversarial bytes — malformed,
//! truncated, or deeply nested input routes to raw fallback.
//!
//! # Block byte format (version 1)
//!
//! Every block begins with a 2-byte frame header:
//!
//! ```text
//! byte 0: VERSION   (= 1, COLUMNAR_VERSION)
//! byte 1: FLAGS
//!         bit0 FLAG_COLUMNAR  1 = columnar, 0 = raw fallback
//!         bit1 FLAG_PERCOL    1 = per-column zstd (point-read layout),
//!                             0 = whole-block zstd; ignored when raw
//!         bits2-7 reserved, MUST be 0
//! ```
//!
//! ## Raw block (FLAG_COLUMNAR clear)
//!
//! ```text
//! u32 LE ulen          uncompressed row-image length
//! zstd frame           zstd(level) of the row image:
//!    u16 LE n_events
//!    u32 LE off[0..=n_events]   (n_events+1 offsets, off[0] = 0)
//!    bytes              concatenated event payloads
//! ```
//!
//! ## Columnar whole-block (FLAG_COLUMNAR, FLAG_PERCOL clear)
//!
//! ```text
//! u32 LE ulen          uncompressed columnar-image length
//! zstd frame           zstd(level) of the columnar image (below)
//! ```
//!
//! ## Columnar per-column (FLAG_COLUMNAR | FLAG_PERCOL)
//!
//! Point-read layout: the structural preamble is stored uncompressed and each
//! column is its own zstd frame, so reassembling a single row decompresses
//! only the columns that row's skeleton references.
//!
//! ```text
//! u16 LE n_events
//! u16 LE n_skels
//! u16 LE n_cols
//! skeleton table       (see below)
//! skel ids             n_events x u16 LE
//! col directory        n_cols x { u8 kind, u8 enc, u32 LE clen, u32 LE ulen }
//! col data             n_cols zstd frames, directory order
//! ```
//!
//! ## Columnar image (the whole-block payload, uncompressed)
//!
//! ```text
//! u16 LE n_events
//! u16 LE n_skels
//! u16 LE n_cols
//! skeleton table:
//!   for each of n_skels:
//!     u16 LE n_ops
//!     for each op:
//!       u8 tag                       0 = Lit, 1 = Val
//!       Lit: u32 LE len, len bytes   verbatim structural bytes
//!       Val: u16 LE col, u8 kind     pull one value from column `col`
//! skel ids: n_events x u16 LE        skeleton id per event
//! col directory: n_cols x { u8 kind, u8 enc, u32 LE data_len }
//!                                    col id is the directory position (dense)
//! col data: concatenated, directory order
//! ```
//!
//! Column ids are dense (`0..n_cols`) and implicit by directory position, so a
//! skeleton `Val`'s `col` indexes the directory directly. Column encodings:
//!
//! - `K_INT`: zigzag-varint sequence, `ENC_RAW` (each value) or `ENC_DELTA`
//!   (successive differences), whichever is smaller for that column.
//! - `K_STR`: `(varint len, bytes)` per value.
//! - `K_BOOL`: one byte (0/1) per value.
//!
//! The version byte lets bn-zge's sealer dispatch on format; the reserved flag
//! bits and reserved column kinds/encodings leave room for later additions
//! (e.g. float columns) without a format break.

mod reassemble;
mod shred;

#[cfg(test)]
mod tests;

pub use reassemble::{Block, CodecError};
pub use shred::{BlockEncoder, EncodeOpts, encode_block};

/// Block format version (frame header byte 0).
pub const COLUMNAR_VERSION: u8 = 1;

/// FLAGS bit 0: set = columnar block, clear = raw fallback block.
pub const FLAG_COLUMNAR: u8 = 0b0000_0001;
/// FLAGS bit 1: set = per-column zstd (point-read layout), clear = whole-block.
pub const FLAG_PERCOL: u8 = 0b0000_0010;
/// Bits that must be zero in a v1 frame header's FLAGS byte.
pub const FLAG_RESERVED_MASK: u8 = !(FLAG_COLUMNAR | FLAG_PERCOL);

/// Column kind: signed integer (zigzag varint, raw or delta).
pub const K_INT: u8 = 0;
/// Column kind: string / utf8 bytes (`varint len || bytes`).
pub const K_STR: u8 = 1;
/// Column kind: boolean (one 0/1 byte per value).
pub const K_BOOL: u8 = 2;

/// Integer column encoding: each value zigzag-varint'd independently.
pub const ENC_RAW: u8 = 0;
/// Integer column encoding: successive differences zigzag-varint'd.
pub const ENC_DELTA: u8 = 1;

/// Largest event count a single block may hold (n_events is a u16).
pub const MAX_BLOCK_EVENTS: usize = u16::MAX as usize;

// ---------------------------------------------------------------------------
// varint / zigzag  (ported verbatim from spikes/perf_compress/src/columnar.rs)
// ---------------------------------------------------------------------------

/// LEB128 unsigned varint append.
#[inline]
pub fn write_varint(out: &mut Vec<u8>, mut v: u64) {
    loop {
        if v < 0x80 {
            out.push(v as u8);
            return;
        }
        out.push((v as u8 & 0x7F) | 0x80);
        v >>= 7;
    }
}

/// LEB128 unsigned varint read. Returns `None` on truncation or overlong
/// (> 10 byte) encodings rather than panicking — the codec must survive
/// adversarial block bytes.
#[inline]
pub fn read_varint(d: &[u8], p: &mut usize) -> Option<u64> {
    let mut v = 0u64;
    let mut shift = 0u32;
    loop {
        let b = *d.get(*p)?;
        *p += 1;
        if shift >= 64 {
            return None;
        }
        v |= ((b & 0x7F) as u64) << shift;
        if b & 0x80 == 0 {
            return Some(v);
        }
        shift += 7;
    }
}

/// Byte length of `v` as an unsigned varint.
#[inline]
pub fn varint_len(v: u64) -> usize {
    if v == 0 { 1 } else { (64 - v.leading_zeros() as usize).div_ceil(7) }
}

/// Zigzag encode `i64 -> u64`.
#[inline]
pub fn zz(v: i64) -> u64 { ((v << 1) ^ (v >> 63)) as u64 }

/// Zigzag decode `u64 -> i64`.
#[inline]
pub fn unzz(u: u64) -> i64 { ((u >> 1) as i64) ^ -((u & 1) as i64) }

// ---------------------------------------------------------------------------
// Minimal MessagePack scalar encoders (mirror rmp's write_uint/write_sint/
// write_str_len — the canonical bytes reassembly re-emits).
// ---------------------------------------------------------------------------

/// Append the minimal MessagePack integer encoding of `v`.
#[inline]
pub fn emit_int(out: &mut Vec<u8>, v: i64) {
    if v >= 0 {
        let u = v as u64;
        if u < 0x80 {
            out.push(u as u8);
        } else if u < 0x100 {
            out.push(0xCC);
            out.push(u as u8);
        } else if u < 0x1_0000 {
            out.push(0xCD);
            out.extend_from_slice(&(u as u16).to_be_bytes());
        } else if u < 0x1_0000_0000 {
            out.push(0xCE);
            out.extend_from_slice(&(u as u32).to_be_bytes());
        } else {
            out.push(0xCF);
            out.extend_from_slice(&u.to_be_bytes());
        }
    } else if v >= -32 {
        out.push(v as u8);
    } else if v >= -128 {
        out.push(0xD0);
        out.push(v as u8);
    } else if v >= -32768 {
        out.push(0xD1);
        out.extend_from_slice(&(v as i16).to_be_bytes());
    } else if v >= -(1i64 << 31) {
        out.push(0xD2);
        out.extend_from_slice(&(v as i32).to_be_bytes());
    } else {
        out.push(0xD3);
        out.extend_from_slice(&v.to_be_bytes());
    }
}

/// Append the minimal MessagePack string header for a payload of `len` bytes.
#[inline]
pub fn emit_str_header(out: &mut Vec<u8>, len: usize) {
    if len < 32 {
        out.push(0xA0 | len as u8);
    } else if len < 256 {
        out.push(0xD9);
        out.push(len as u8);
    } else if len < 0x1_0000 {
        out.push(0xDA);
        out.extend_from_slice(&(len as u16).to_be_bytes());
    } else {
        out.push(0xDB);
        out.extend_from_slice(&(len as u32).to_be_bytes());
    }
}

/// Append the minimal MessagePack string encoding (header + `s`).
#[inline]
pub fn emit_str(out: &mut Vec<u8>, s: &[u8]) {
    emit_str_header(out, s.len());
    out.extend_from_slice(s);
}
