//! The **consolidated SealPack** (bn-3of, Spike I): one immutable,
//! typed-section file per sealed segment that replaces the `.pidx` / `.filter`
//! / `.pcol` sidecar trio with a single artifact (design §11, research/04 §6).
//!
//! # Why one file
//!
//! The sidecar trio has three opens, three installs, three validation surfaces,
//! and three mismatch cases per sealed segment. A SealPack folds every
//! *rebuildable* read accelerator (pointers, skip tables, stream directory,
//! membership filter, per-event type ids, columnar payloads) into one
//! typed-section container so the install is one temp→verify→fsync→rename→
//! dir-fsync sequence and the reopen is one open. Parity (`.par`) stays a
//! separate artifact (§11.5) — its repair policy and failure domain differ.
//!
//! Like every sidecar it replaces, the pack is **advisory** (D-FMT-3, spec 01
//! §3.3.2): the raw log segment is truth. A missing, truncated, or corrupt pack
//! — or any single corrupt mandatory section — falls the reader back to a raw
//! segment scan, losing only speed. Corruption of an *optional* section (the
//! filter, the type ids, the payload columns) degrades **locally**: that
//! accelerator is dropped and the rest of the pack serves normally.
//!
//! # Byte layout
//!
//! ```text
//! Header (64 bytes)
//! SectionDirectory   (n_sections × SectionRef, 48 bytes each)
//! Section bytes...   (in directory order; directory offsets are absolute)
//! Trailer (40 bytes) (whole-pack blake3 hash binds header+directory+sections)
//! ```
//!
//! ## Header (64 bytes)
//!
//! ```text
//!   0   u32  magic = PACK_MAGIC
//!   4   u16  format_version = 1
//!   6   u16  header_flags = 0
//!   8   u64  segment_id
//!   16  u64  base_pos
//!   24  u64  event_count
//!   32  u32  n_streams
//!   36  u32  n_sections
//!   40  u64  directory_off   (= HEADER_LEN)
//!   48  u64  sections_off
//!   56  u64  reserved = 0
//! ```
//!
//! ## SectionRef (48 bytes, research/04 §6.1)
//!
//! ```text
//!   0   u16  kind
//!   2   u16  version
//!   4   u32  flags
//!   8   u64  offset                (absolute file offset of the section bytes)
//!   16  u64  length                (stored length)
//!   24  u64  uncompressed_length
//!   32  u32  crc32c                (over the stored section bytes)
//!   36  u16  codec_id              (section-kind-specific; e.g. directory repr)
//!   38  u16  reserved
//!   40  u64  content_hash_prefix   (first 8 bytes of blake3(section bytes))
//! ```
//!
//! ## Trailer (40 bytes)
//!
//! ```text
//!   0   [u8;32]  pack_hash = blake3(bytes[0 .. sections_off])   (header + directory ONLY)
//!   32  u32      reserved = 0
//!   36  u32      magic = PACK_MAGIC
//! ```
//!
//! The pack hash deliberately covers **only the header + section directory**,
//! NOT the section bodies (review F1). The directory already commits to every
//! section's `crc32c` + `content_hash_prefix`, so the trailer hash still binds
//! the whole pack's identity **transitively** — a flipped section byte fails
//! that section's own directory-committed checksums, and a flipped directory
//! or header byte fails the trailer hash. What the narrower scope buys is
//! **real local degradation**: a bit flip inside an OPTIONAL section (filter,
//! event-type ids, payload columns, stats) fails only that section's CRC at
//! open, so the section is dropped and the rest of the pack keeps serving —
//! whereas a whole-bytes hash would reject the entire pack and drop the
//! segment to a full raw scan for any single flipped bit anywhere. A corrupt
//! MANDATORY section still rejects the whole pack (the reader cannot resolve
//! without it).
//!
//! # Sections
//!
//! | kind | name | mandatory | notes |
//! |---|---|---|---|
//! | 1 | `STREAM_DIRECTORY` | yes | codec-tagged: sorted (0) or bitrank (1), §12.6 |
//! | 2 | `POINTER_BLOCKS` | yes | packed varint-delta blocks (reuses `ptr_block`) |
//! | 3 | `POINTER_SKIPS` | yes | intra-block skip tables |
//! | 4 | `GLOBAL_OFFSET_INDEX` | no (reserved) | derivable from pointer blocks |
//! | 5 | `STREAM_FILTER` | no | `BinaryFuse16` bytes (reuses `filter`) |
//! | 6 | `EVENT_TYPE_IDS` | no | per-event type ids (new — bn-3fn carry-forward) |
//! | 7 | `PAYLOAD_COLUMNS` | no | columnar `.pcol` bytes (reuses `payload`) |
//! | 8 | `ROW_FALLBACK_BLOCKS` | no (reserved) | folded inside `PAYLOAD_COLUMNS` today |
//! | 9 | `SEGMENT_EFFECT` | no (reserved) | Spike D effect image (§9.7) |
//! | 10 | `REGISTRY_DELTA` | no (reserved) | §9.5 |
//! | 11 | `STATS` | no | informational summary |
//!
//! Unknown section kinds are skipped by readers (forward compat, D-FMT-3).

use crate::sealed::filter::SegmentFilter;
use crate::sealed::ptr_block::{
    BatchPtr, SkipEntry, encode_ptr_block, encode_skips,
};
use crate::sealed::segment::{SealBatch, SealStream};

/// Pack magic (`"MSP1"`).
pub const PACK_MAGIC: u32 = 0x3150__534D;
/// Header length (bytes).
pub const HEADER_LEN: usize = 64;
/// Directory-entry (`SectionRef`) length (bytes).
pub const SECTION_REF_LEN: usize = 48;
/// Trailer length (bytes), at EOF.
pub const TRAILER_LEN: usize = 40;
/// Current pack `format_version`.
pub const FORMAT_VERSION: u16 = 1;

/// The consolidated SealPack path for `segment_id` under `dir`:
/// `<dir>/seg-<id:020>.seal`. The single source of truth for the `.seal`
/// naming, shared by the sealer (write) and the reopen loader (read) so the two
/// can never target different files.
pub fn seal_pack_path(
    dir: &std::path::Path,
    segment_id: u64,
) -> std::path::PathBuf {
    dir.join(format!("seg-{segment_id:020}.seal"))
}

// -- Section kinds ---------------------------------------------------------

/// Codec-tagged per-stream directory (sorted or bitrank). Mandatory.
pub const KIND_STREAM_DIRECTORY: u16 = 1;
/// Packed varint-delta pointer blocks. Mandatory.
pub const KIND_POINTER_BLOCKS: u16 = 2;
/// Intra-block skip tables. Mandatory.
pub const KIND_POINTER_SKIPS: u16 = 3;
/// Global-position offset index. Reserved (derivable today).
pub const KIND_GLOBAL_OFFSET_INDEX: u16 = 4;
/// `BinaryFuse16` stream-id membership filter. Optional.
pub const KIND_STREAM_FILTER: u16 = 5;
/// Per-event `event_type_id` column. Optional (bn-3fn carry-forward #1).
pub const KIND_EVENT_TYPE_IDS: u16 = 6;
/// Columnar payload blocks (`.pcol` image). Optional.
pub const KIND_PAYLOAD_COLUMNS: u16 = 7;
/// Row-fallback payload blocks. Reserved (folded into `PAYLOAD_COLUMNS`).
pub const KIND_ROW_FALLBACK_BLOCKS: u16 = 8;
/// Algebraic segment effect (Spike D). Reserved.
pub const KIND_SEGMENT_EFFECT: u16 = 9;
/// Registry delta. Reserved.
pub const KIND_REGISTRY_DELTA: u16 = 10;
/// Informational stats summary. Optional.
pub const KIND_STATS: u16 = 11;

// -- Directory codecs (STREAM_DIRECTORY.codec_id) -------------------------

/// Sorted 56-byte records, ascending by stream id (the always-correct
/// fallback).
pub const DIRCODEC_SORTED: u16 = 0;
/// Bitvector + rank over the shifted `[min, max]` universe (§12.2, Spike H).
/// Selected when the universe is dense (`U <= 8n`); the bit position IS the
/// stream id, so no key copy is stored.
pub const DIRCODEC_BITRANK: u16 = 1;

/// A sorted-directory record: 56 bytes. Offsets are **relative** to the start
/// of the `POINTER_BLOCKS` / `POINTER_SKIPS` sections.
const DIR_SORTED_REC_LEN: usize = 56;
/// A bitrank-directory entry record: 48 bytes (no stream id — the bit position
/// is the key).
const DIR_BITRANK_REC_LEN: usize = 48;

// -------------------------------------------------------------------------
// Errors
// -------------------------------------------------------------------------

/// Why parsing/validating a SealPack failed.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum PackError {
    /// The bytes are too short, mis-magicked, wrong-version, or a mandatory
    /// structural invariant (whole-pack hash, directory bounds, mandatory
    /// section CRC) failed. The reader falls back to a raw segment scan.
    #[error("pack corrupt: {0}")]
    Corrupt(&'static str),
}

// -------------------------------------------------------------------------
// Little-endian helpers
// -------------------------------------------------------------------------

fn put_u16(buf: &mut Vec<u8>, v: u16) {
    buf.extend_from_slice(&v.to_le_bytes());
}
fn put_u32(buf: &mut Vec<u8>, v: u32) {
    buf.extend_from_slice(&v.to_le_bytes());
}
fn put_u64(buf: &mut Vec<u8>, v: u64) {
    buf.extend_from_slice(&v.to_le_bytes());
}

fn rd_u16(d: &[u8], at: usize) -> u16 {
    u16::from_le_bytes(d[at..at + 2].try_into().unwrap())
}
fn rd_u32(d: &[u8], at: usize) -> u32 {
    u32::from_le_bytes(d[at..at + 4].try_into().unwrap())
}
fn rd_u64(d: &[u8], at: usize) -> u64 {
    u64::from_le_bytes(d[at..at + 8].try_into().unwrap())
}

/// First 8 bytes of the blake3 hash of `bytes`, as a `u64` (the section
/// `content_hash_prefix`).
fn hash_prefix(bytes: &[u8]) -> u64 {
    let h = blake3::hash(bytes);
    u64::from_le_bytes(h.as_bytes()[..8].try_into().unwrap())
}

// -------------------------------------------------------------------------
// A stream directory entry with section-relative offsets (the intermediate
// the reader hands back to `segment.rs`, which turns the relative offsets into
// absolute pack offsets and builds its private `DirEntry`).
// -------------------------------------------------------------------------

/// A parsed directory entry with offsets **relative** to the `POINTER_BLOCKS`
/// / `POINTER_SKIPS` section starts. `pub(crate)` so `segment.rs` can rebase
/// them onto absolute pack offsets when constructing the read index.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DirEntryRaw {
    pub stream_id:     u64,
    pub first_version: u64,
    pub last_version:  u64,
    pub ptr_off:       u64,
    pub ptr_len:       u32,
    pub n_batches:     u32,
    pub skip_off:      u64,
    pub skip_len:      u32,
}

// -------------------------------------------------------------------------
// Encode
// -------------------------------------------------------------------------

/// Everything the sealer hands the pack encoder. Borrows so the driver keeps
/// ownership of its already-computed sidecar pieces.
pub struct PackInput<'a> {
    /// The segment being sealed.
    pub segment_id:     u64,
    /// The segment's A1 base position.
    pub base_pos:       u64,
    /// Per-stream batch lists, ascending by stream id.
    pub streams:        &'a [SealStream],
    /// Per-event `event_type_id` in stored (global-position) order; empty to
    /// omit the `EVENT_TYPE_IDS` section. Length (when non-empty) must equal
    /// the segment's event count.
    pub event_type_ids: &'a [u32],
    /// The built membership filter, or `None` to omit `STREAM_FILTER`.
    pub filter:         Option<&'a SegmentFilter>,
    /// Pre-encoded, already verify-on-seal columnar payload bytes (the `.pcol`
    /// image), or `None` to omit `PAYLOAD_COLUMNS`.
    pub payload_bytes:  Option<&'a [u8]>,
}

/// One section staged for the directory + body layout.
struct StagedSection {
    kind:     u16,
    version:  u16,
    codec_id: u16,
    bytes:    Vec<u8>,
}

/// The total event count of a stream set (Σ frame_count).
fn event_count_of(streams: &[SealStream]) -> u64 {
    streams
        .iter()
        .flat_map(|s| s.batches.iter())
        .map(|b| u64::from(b.frame_count))
        .sum()
}

/// Serialize a SealPack byte image. Pure: no I/O — the driver writes and fsyncs
/// the bytes. Streams MUST be ascending by `stream_id`, each stream's batches
/// version-ascending (the `SealInput` contract).
pub fn encode_pack(input: &PackInput) -> Vec<u8> {
    // Encode per-stream pointer blocks + skip tables, tracking section-relative
    // spans, exactly as the legacy `.pidx` did — reusing the Kani-proven
    // per-stream codecs.
    struct Enc {
        stream_id:     u64,
        first_version: u64,
        last_version:  u64,
        n_batches:     u32,
        block:         Vec<u8>,
        skips:         Vec<u8>,
    }
    let mut encs: Vec<Enc> = Vec::with_capacity(input.streams.len());
    for s in &*input.streams {
        debug_assert!(!s.batches.is_empty());
        let ptrs: Vec<BatchPtr> =
            s.batches.iter().map(SealBatch::as_batch_ptr).collect();
        let mut skips: Vec<SkipEntry> = Vec::new();
        let block = encode_ptr_block(&ptrs, &mut skips);
        let last = ptrs.last().unwrap();
        encs.push(Enc {
            stream_id: s.stream_id,
            first_version: ptrs[0].first_version,
            last_version: last.last_version(),
            n_batches: ptrs.len() as u32,
            block,
            skips: encode_skips(&skips),
        });
    }

    // POINTER_BLOCKS + POINTER_SKIPS section bytes, plus each stream's relative
    // offset within them.
    let mut ptr_section = Vec::new();
    let mut skip_section = Vec::new();
    let mut ptr_offs = Vec::with_capacity(encs.len());
    let mut skip_offs = Vec::with_capacity(encs.len());
    for e in &encs {
        ptr_offs.push(ptr_section.len() as u64);
        ptr_section.extend_from_slice(&e.block);
        skip_offs.push(skip_section.len() as u64);
        skip_section.extend_from_slice(&e.skips);
    }

    // STREAM_DIRECTORY: choose sorted vs bitrank (§12.6). The chooser is a
    // deterministic density test; the chosen codec is recorded in `codec_id`
    // so a static representation never affects correctness.
    let dir_entries: Vec<DirEntryRaw> = encs
        .iter()
        .enumerate()
        .map(|(i, e)| DirEntryRaw {
            stream_id:     e.stream_id,
            first_version: e.first_version,
            last_version:  e.last_version,
            ptr_off:       ptr_offs[i],
            ptr_len:       e.block.len() as u32,
            n_batches:     e.n_batches,
            skip_off:      skip_offs[i],
            skip_len:      e.skips.len() as u32,
        })
        .collect();
    let (dir_codec, dir_bytes) = encode_stream_directory(&dir_entries);

    // Stage the mandatory sections then the optional ones.
    let mut staged: Vec<StagedSection> = vec![
        StagedSection {
            kind:     KIND_STREAM_DIRECTORY,
            version:  1,
            codec_id: dir_codec,
            bytes:    dir_bytes,
        },
        StagedSection {
            kind:     KIND_POINTER_BLOCKS,
            version:  1,
            codec_id: 0,
            bytes:    ptr_section,
        },
        StagedSection {
            kind:     KIND_POINTER_SKIPS,
            version:  1,
            codec_id: 0,
            bytes:    skip_section,
        },
    ];
    if let Some(f) = input.filter {
        staged.push(StagedSection {
            kind:     KIND_STREAM_FILTER,
            version:  1,
            codec_id: 0,
            bytes:    f.to_bytes(),
        });
    }
    if !input.event_type_ids.is_empty() {
        staged.push(StagedSection {
            kind:     KIND_EVENT_TYPE_IDS,
            version:  1,
            codec_id: 0,
            bytes:    encode_event_types(input.event_type_ids),
        });
    }
    if let Some(p) = input.payload_bytes {
        staged.push(StagedSection {
            kind:     KIND_PAYLOAD_COLUMNS,
            version:  1,
            codec_id: 0,
            bytes:    p.to_vec(),
        });
    }
    staged.push(StagedSection {
        kind:     KIND_STATS,
        version:  1,
        codec_id: 0,
        bytes:    encode_stats(input, &dir_entries),
    });

    let event_count = event_count_of(input.streams);
    let n_streams = encs.len() as u32;
    let n_sections = staged.len() as u32;

    let dir_len = staged.len() * SECTION_REF_LEN;
    let sections_off = (HEADER_LEN + dir_len) as u64;

    // Header.
    let mut buf: Vec<u8> = Vec::new();
    put_u32(&mut buf, PACK_MAGIC);
    put_u16(&mut buf, FORMAT_VERSION);
    put_u16(&mut buf, 0); // header_flags
    put_u64(&mut buf, input.segment_id);
    put_u64(&mut buf, input.base_pos);
    put_u64(&mut buf, event_count);
    put_u32(&mut buf, n_streams);
    put_u32(&mut buf, n_sections);
    put_u64(&mut buf, HEADER_LEN as u64); // directory_off
    put_u64(&mut buf, sections_off);
    put_u64(&mut buf, 0); // reserved
    debug_assert_eq!(buf.len(), HEADER_LEN);

    // Compute each section's absolute offset up front so the directory can be
    // laid out before the section bodies.
    let mut offset = sections_off;
    let mut refs: Vec<(u64, u32, u32, u64)> = Vec::with_capacity(staged.len());
    for s in &staged {
        let len = s.bytes.len() as u64;
        let crc = crc32c::crc32c(&s.bytes);
        let hp = hash_prefix(&s.bytes);
        refs.push((offset, len as u32, crc, hp));
        offset += len;
    }

    // Directory.
    for (s, &(off, len, crc, hp)) in staged.iter().zip(refs.iter()) {
        put_u16(&mut buf, s.kind);
        put_u16(&mut buf, s.version);
        put_u32(&mut buf, 0); // flags
        put_u64(&mut buf, off);
        put_u64(&mut buf, u64::from(len));
        put_u64(&mut buf, u64::from(len)); // uncompressed_length (== length; codecs are opaque)
        put_u32(&mut buf, crc);
        put_u16(&mut buf, s.codec_id);
        put_u16(&mut buf, 0); // reserved
        put_u64(&mut buf, hp);
    }
    debug_assert_eq!(buf.len() as u64, sections_off);

    // Trailer hash scope (review F1): header + directory ONLY. The directory
    // commits every section's crc32c + content_hash_prefix, so this still
    // binds the full pack identity transitively while letting a corrupt
    // OPTIONAL section fail only its own CRC at open (local degradation)
    // instead of rejecting the whole pack.
    let pack_hash = blake3::hash(&buf);
    debug_assert_eq!(buf.len() as u64, sections_off);

    // Section bodies.
    for s in &staged {
        buf.extend_from_slice(&s.bytes);
    }

    // Trailer.
    buf.extend_from_slice(pack_hash.as_bytes());
    put_u32(&mut buf, 0); // reserved
    put_u32(&mut buf, PACK_MAGIC);
    buf
}

/// Encode `STREAM_DIRECTORY`, choosing the representation per §12.6. Returns
/// `(codec_id, bytes)`.
fn encode_stream_directory(entries: &[DirEntryRaw]) -> (u16, Vec<u8>) {
    if should_use_bitrank(entries) {
        (DIRCODEC_BITRANK, encode_dir_bitrank(entries))
    } else {
        (DIRCODEC_SORTED, encode_dir_sorted(entries))
    }
}

/// The §12.6 density chooser: bitrank when the stream-id universe is dense
/// enough (`U <= 8n`) and large enough to matter (`n >= 8`), else sorted. The
/// `U <= 8n` cap bounds the bitvector at `n/8` bytes, so bitrank never blows up
/// memory relative to the always-present entry column.
fn should_use_bitrank(entries: &[DirEntryRaw]) -> bool {
    let n = entries.len();
    if n < 8 {
        return false;
    }
    // Entries are ascending by stream_id (encoder invariant).
    let min = entries[0].stream_id;
    let max = entries[n - 1].stream_id;
    let u = (max - min).saturating_add(1);
    u <= 8u64.saturating_mul(n as u64)
}

fn encode_dir_sorted(entries: &[DirEntryRaw]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(entries.len() * DIR_SORTED_REC_LEN);
    for e in entries {
        put_u64(&mut buf, e.stream_id);
        put_u64(&mut buf, e.first_version);
        put_u64(&mut buf, e.last_version);
        put_u64(&mut buf, e.ptr_off);
        put_u32(&mut buf, e.ptr_len);
        put_u32(&mut buf, e.n_batches);
        put_u64(&mut buf, e.skip_off);
        put_u32(&mut buf, e.skip_len);
        put_u32(&mut buf, 0); // reserved -> 56 bytes
    }
    buf
}

/// Bitrank image: `min u64 | n_words u64 | words[n_words] u64 | entries[n]×48`.
/// The rank directories are derived on open with one popcount pass (§12.2).
fn encode_dir_bitrank(entries: &[DirEntryRaw]) -> Vec<u8> {
    let n = entries.len();
    let min = entries[0].stream_id;
    let max = entries[n - 1].stream_id;
    let u = (max - min + 1) as usize;
    let n_words = u.div_ceil(64);
    let mut words = vec![0u64; n_words];
    for e in entries {
        let i = (e.stream_id - min) as usize;
        words[i / 64] |= 1u64 << (i % 64);
    }
    let mut buf =
        Vec::with_capacity(16 + n_words * 8 + n * DIR_BITRANK_REC_LEN);
    put_u64(&mut buf, min);
    put_u64(&mut buf, n_words as u64);
    for &w in &words {
        put_u64(&mut buf, w);
    }
    for e in entries {
        put_u64(&mut buf, e.first_version);
        put_u64(&mut buf, e.last_version);
        put_u64(&mut buf, e.ptr_off);
        put_u32(&mut buf, e.ptr_len);
        put_u32(&mut buf, e.n_batches);
        put_u64(&mut buf, e.skip_off);
        put_u32(&mut buf, e.skip_len);
        put_u32(&mut buf, 0); // reserved -> 48 bytes
    }
    buf
}

/// Encode the `EVENT_TYPE_IDS` section: a dictionary of distinct type ids plus
/// per-event indices into it, with the index width chosen by the distinct
/// count.
///
/// # Why dictionary + width-adaptive indices
///
/// Real stores carry O(10) distinct event types across a segment's ~1M events,
/// so a raw `u32`-per-event column would spend 4 B/event advertising a handful
/// of repeated values. A dictionary collapses that to a 1-byte index/event
/// (≤256 distinct) — a 4× shrink — while keeping the lookup O(1) (one indexed
/// load + one dictionary load). The width widens to 2 or 4 bytes for
/// pathological universes so the encoding stays exact for any input. The
/// section is optional and small next to the payload columns, so the modest
/// dictionary header is free.
///
/// ```text
///   0   u32  event_count
///   4   u32  dict_len
///   8   u8   index_width (1 | 2 | 4)
///   9   u8   reserved
///   10  u16  reserved
///   12  dict:    dict_len × u32   (distinct type ids, first-seen order)
///   ..  indices: event_count × index_width bytes
/// ```
pub fn encode_event_types(type_ids: &[u32]) -> Vec<u8> {
    // Build a first-seen-order dictionary.
    let mut dict: Vec<u32> = Vec::new();
    let mut index_of: std::collections::HashMap<u32, u32> =
        std::collections::HashMap::new();
    let mut indices: Vec<u32> = Vec::with_capacity(type_ids.len());
    for &t in type_ids {
        let idx = *index_of.entry(t).or_insert_with(|| {
            let i = dict.len() as u32;
            dict.push(t);
            i
        });
        indices.push(idx);
    }
    let width: u8 = if dict.len() <= u8::MAX as usize + 1 {
        1
    } else if dict.len() <= u16::MAX as usize + 1 {
        2
    } else {
        4
    };

    let mut buf = Vec::with_capacity(
        12 + dict.len() * 4 + type_ids.len() * width as usize,
    );
    put_u32(&mut buf, type_ids.len() as u32);
    put_u32(&mut buf, dict.len() as u32);
    buf.push(width);
    buf.push(0);
    put_u16(&mut buf, 0);
    for &t in &dict {
        put_u32(&mut buf, t);
    }
    for &i in &indices {
        match width {
            1 => buf.push(i as u8),
            2 => put_u16(&mut buf, i as u16),
            _ => put_u32(&mut buf, i),
        }
    }
    buf
}

/// Decode the `EVENT_TYPE_IDS` section back into the per-event type id column.
pub fn decode_event_types(body: &[u8]) -> Result<Vec<u32>, PackError> {
    if body.len() < 12 {
        return Err(PackError::Corrupt("event-type section too short"));
    }
    let event_count = rd_u32(body, 0) as usize;
    let dict_len = rd_u32(body, 4) as usize;
    let width = body[8] as usize;
    if width != 1 && width != 2 && width != 4 {
        return Err(PackError::Corrupt("event-type bad index width"));
    }
    let dict_start = 12usize;
    let dict_bytes =
        dict_len.checked_mul(4).ok_or(PackError::Corrupt("dict overflow"))?;
    let idx_start = dict_start
        .checked_add(dict_bytes)
        .ok_or(PackError::Corrupt("event-type layout overflow"))?;
    let idx_bytes = event_count
        .checked_mul(width)
        .ok_or(PackError::Corrupt("event-type index overflow"))?;
    let end = idx_start
        .checked_add(idx_bytes)
        .ok_or(PackError::Corrupt("event-type layout overflow"))?;
    if body.len() < end {
        return Err(PackError::Corrupt("event-type section truncated"));
    }
    let mut dict = Vec::with_capacity(dict_len);
    for i in 0..dict_len {
        dict.push(rd_u32(body, dict_start + i * 4));
    }
    let mut out = Vec::with_capacity(event_count);
    for i in 0..event_count {
        let at = idx_start + i * width;
        let idx = match width {
            1 => body[at] as usize,
            2 => rd_u16(body, at) as usize,
            _ => rd_u32(body, at) as usize,
        };
        let t = *dict
            .get(idx)
            .ok_or(PackError::Corrupt("event-type index out of range"))?;
        out.push(t);
    }
    Ok(out)
}

/// A small informational summary; not consulted on the read path (readers may
/// skip it entirely), so it is deliberately compact and best-effort.
fn encode_stats(input: &PackInput, entries: &[DirEntryRaw]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(40);
    put_u64(&mut buf, event_count_of(input.streams));
    put_u32(&mut buf, entries.len() as u32);
    let (min, max) = match (entries.first(), entries.last()) {
        (Some(a), Some(b)) => (a.stream_id, b.stream_id),
        _ => (0, 0),
    };
    put_u64(&mut buf, min);
    put_u64(&mut buf, max);
    put_u32(&mut buf, input.event_type_ids.len() as u32);
    put_u32(&mut buf, 0); // reserved
    buf
}

// -------------------------------------------------------------------------
// Decode / verify
// -------------------------------------------------------------------------

/// A validated section reference: `kind`, `codec_id`, and the absolute byte
/// span within the pack.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ValidSection {
    pub kind:     u16,
    pub codec_id: u16,
    pub offset:   usize,
    pub length:   usize,
    /// Whether the section's `crc32c` verified. An optional section that fails
    /// CRC is retained in the directory but flagged so readers drop it locally
    /// (local degradation); a mandatory section that fails CRC makes
    /// [`parse_pack`] return [`PackError::Corrupt`].
    pub crc_ok:   bool,
}

/// The parsed, structurally-verified pack directory. Mandatory sections
/// (`STREAM_DIRECTORY`, `POINTER_BLOCKS`, `POINTER_SKIPS`) are guaranteed
/// present and CRC-valid on `Ok`.
pub(crate) struct ParsedPack {
    pub segment_id:  u64,
    pub base_pos:    u64,
    pub event_count: u64,
    pub n_streams:   u32,
    pub sections:    Vec<ValidSection>,
}

impl ParsedPack {
    /// The first section of `kind` whose CRC verified, as an absolute span.
    pub fn section(&self, kind: u16) -> Option<ValidSection> {
        self.sections.iter().find(|s| s.kind == kind && s.crc_ok).copied()
    }
}

/// Validate a SealPack byte image: magic/version, whole-pack blake3 hash,
/// directory bounds, and every section's `crc32c` + `content_hash_prefix`.
/// Mandatory-section corruption (or any structural fault) is fatal; an optional
/// section that fails its CRC is kept but flagged `crc_ok=false`.
pub(crate) fn parse_pack(bytes: &[u8]) -> Result<ParsedPack, PackError> {
    if bytes.len() < HEADER_LEN + TRAILER_LEN {
        return Err(PackError::Corrupt("shorter than header + trailer"));
    }
    if rd_u32(bytes, 0) != PACK_MAGIC {
        return Err(PackError::Corrupt("bad header magic"));
    }
    if rd_u16(bytes, 4) != FORMAT_VERSION {
        return Err(PackError::Corrupt("unknown format_version"));
    }
    let segment_id = rd_u64(bytes, 8);
    let base_pos = rd_u64(bytes, 16);
    let event_count = rd_u64(bytes, 24);
    let n_streams = rd_u32(bytes, 32);
    let n_sections = rd_u32(bytes, 36) as usize;
    let directory_off = rd_u64(bytes, 40) as usize;
    let sections_off = rd_u64(bytes, 48) as usize;

    let trailer_start = bytes.len() - TRAILER_LEN;
    let trailer = &bytes[trailer_start..];
    if rd_u32(trailer, 36) != PACK_MAGIC {
        return Err(PackError::Corrupt("bad trailer magic"));
    }

    if directory_off != HEADER_LEN {
        return Err(PackError::Corrupt("directory_off != header len"));
    }
    let dir_len = n_sections
        .checked_mul(SECTION_REF_LEN)
        .ok_or(PackError::Corrupt("directory length overflow"))?;
    let dir_end = directory_off
        .checked_add(dir_len)
        .ok_or(PackError::Corrupt("directory end overflow"))?;
    if sections_off != dir_end || sections_off > trailer_start {
        return Err(PackError::Corrupt("sections_off inconsistent"));
    }

    // Pack hash covers header + directory ONLY (review F1): the directory's
    // per-section crc32c + content_hash_prefix bind the section bodies
    // transitively, and a corrupt OPTIONAL section must fail its OWN checksum
    // (local degradation), not this pack-wide one.
    let got = blake3::hash(&bytes[..sections_off]);
    if got.as_bytes() != &trailer[0..32] {
        return Err(PackError::Corrupt("pack header/directory hash mismatch"));
    }

    let mut sections = Vec::with_capacity(n_sections);
    for i in 0..n_sections {
        let b = directory_off + i * SECTION_REF_LEN;
        let kind = rd_u16(bytes, b);
        let codec_id = rd_u16(bytes, b + 36);
        let offset = rd_u64(bytes, b + 8) as usize;
        let length = rd_u64(bytes, b + 16) as usize;
        let crc = rd_u32(bytes, b + 32);
        let hp = rd_u64(bytes, b + 40);
        let end = offset
            .checked_add(length)
            .ok_or(PackError::Corrupt("section span overflow"))?;
        if offset < sections_off || end > trailer_start {
            return Err(PackError::Corrupt("section span out of range"));
        }
        let body = &bytes[offset..end];
        // A section verifies when BOTH its crc32c and its content-hash prefix
        // match (the two are independent so a fault is caught even if one
        // algorithm collides).
        let crc_ok = crc32c::crc32c(body) == crc && hash_prefix(body) == hp;
        sections.push(ValidSection { kind, codec_id, offset, length, crc_ok });
    }

    // Mandatory sections must be present and CRC-valid, else the whole pack is
    // untrustworthy and the reader must raw-scan.
    for &kind in
        &[KIND_STREAM_DIRECTORY, KIND_POINTER_BLOCKS, KIND_POINTER_SKIPS]
    {
        if !sections.iter().any(|s| s.kind == kind && s.crc_ok) {
            return Err(PackError::Corrupt("missing/corrupt mandatory section"));
        }
    }

    Ok(ParsedPack { segment_id, base_pos, event_count, n_streams, sections })
}

/// Decode a `STREAM_DIRECTORY` section body (codec `DIRCODEC_SORTED` or
/// `DIRCODEC_BITRANK`) into directory entries with section-relative offsets.
pub(crate) fn decode_stream_directory(
    codec_id: u16,
    body: &[u8],
    n_streams: usize,
) -> Result<Vec<DirEntryRaw>, PackError> {
    match codec_id {
        DIRCODEC_SORTED => decode_dir_sorted(body, n_streams),
        DIRCODEC_BITRANK => decode_dir_bitrank(body, n_streams),
        _ => Err(PackError::Corrupt("unknown directory codec")),
    }
}

fn decode_dir_sorted(
    body: &[u8],
    n_streams: usize,
) -> Result<Vec<DirEntryRaw>, PackError> {
    let want = n_streams
        .checked_mul(DIR_SORTED_REC_LEN)
        .ok_or(PackError::Corrupt("directory length overflow"))?;
    if body.len() != want {
        return Err(PackError::Corrupt("sorted directory size mismatch"));
    }
    let mut out = Vec::with_capacity(n_streams);
    for i in 0..n_streams {
        let b = i * DIR_SORTED_REC_LEN;
        out.push(DirEntryRaw {
            stream_id:     rd_u64(body, b),
            first_version: rd_u64(body, b + 8),
            last_version:  rd_u64(body, b + 16),
            ptr_off:       rd_u64(body, b + 24),
            ptr_len:       rd_u32(body, b + 32),
            n_batches:     rd_u32(body, b + 36),
            skip_off:      rd_u64(body, b + 40),
            skip_len:      rd_u32(body, b + 48),
        });
    }
    Ok(out)
}

fn decode_dir_bitrank(
    body: &[u8],
    n_streams: usize,
) -> Result<Vec<DirEntryRaw>, PackError> {
    if body.len() < 16 {
        return Err(PackError::Corrupt("short bitrank directory"));
    }
    let min = rd_u64(body, 0);
    let n_words = rd_u64(body, 8) as usize;
    let words_bytes = n_words
        .checked_mul(8)
        .ok_or(PackError::Corrupt("bitrank words overflow"))?;
    let entries_bytes = n_streams
        .checked_mul(DIR_BITRANK_REC_LEN)
        .ok_or(PackError::Corrupt("bitrank entries overflow"))?;
    let want = 16usize
        .checked_add(words_bytes)
        .and_then(|v| v.checked_add(entries_bytes))
        .ok_or(PackError::Corrupt("bitrank layout overflow"))?;
    if body.len() != want {
        return Err(PackError::Corrupt("bitrank directory size mismatch"));
    }
    // Walk set bits in ascending order (matching encode order) and pair each
    // with its entry record.
    let words_start = 16;
    let ecol_start = 16 + words_bytes;
    let mut out = Vec::with_capacity(n_streams);
    let mut slot = 0usize;
    let mut popcount = 0usize;
    for w in 0..n_words {
        let mut word = rd_u64(body, words_start + w * 8);
        popcount += word.count_ones() as usize;
        while word != 0 {
            let b = word.trailing_zeros() as u64;
            let stream_id = min + (w as u64) * 64 + b;
            let rec = ecol_start + slot * DIR_BITRANK_REC_LEN;
            if slot >= n_streams {
                return Err(PackError::Corrupt("bitrank popcount > n_streams"));
            }
            out.push(DirEntryRaw {
                stream_id,
                first_version: rd_u64(body, rec),
                last_version: rd_u64(body, rec + 8),
                ptr_off: rd_u64(body, rec + 16),
                ptr_len: rd_u32(body, rec + 24),
                n_batches: rd_u32(body, rec + 28),
                skip_off: rd_u64(body, rec + 32),
                skip_len: rd_u32(body, rec + 40),
            });
            slot += 1;
            word &= word - 1;
        }
    }
    if popcount != n_streams {
        return Err(PackError::Corrupt("bitrank popcount != n_streams"));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stream(id: u64, batches: &[(u64, u32, u64, u64)]) -> SealStream {
        SealStream {
            stream_id: id,
            batches:   batches
                .iter()
                .map(|&(v, fc, g, off)| SealBatch {
                    first_version:    v,
                    frame_count:      fc,
                    first_global_pos: g,
                    offset:           off,
                })
                .collect(),
        }
    }

    #[test]
    fn event_types_round_trip_small_dict() {
        let ids = vec![7u32, 7, 3, 7, 9, 3, 3];
        let enc = encode_event_types(&ids);
        // dict {7,3,9} -> width 1; header 12 + dict 12 + 7 indices = 31 bytes
        assert_eq!(enc.len(), 12 + 3 * 4 + 7);
        assert_eq!(decode_event_types(&enc).unwrap(), ids);
    }

    #[test]
    fn event_types_round_trip_empty() {
        let enc = encode_event_types(&[]);
        assert_eq!(decode_event_types(&enc).unwrap(), Vec::<u32>::new());
    }

    #[test]
    fn event_types_wide_dict_uses_u16() {
        let ids: Vec<u32> = (0..1000u32).collect(); // 1000 distinct -> width 2
        let enc = encode_event_types(&ids);
        assert_eq!(enc[8], 2, "width");
        assert_eq!(decode_event_types(&enc).unwrap(), ids);
    }

    #[test]
    fn directory_sorted_round_trips() {
        let entries: Vec<DirEntryRaw> = (0..4u64)
            .map(|i| DirEntryRaw {
                stream_id:     i * 1000,
                first_version: 0,
                last_version:  i,
                ptr_off:       i * 10,
                ptr_len:       10,
                n_batches:     1,
                skip_off:      i * 2,
                skip_len:      2,
            })
            .collect();
        // Sparse (U/n huge) -> sorted.
        let (codec, bytes) = encode_stream_directory(&entries);
        assert_eq!(codec, DIRCODEC_SORTED);
        let back =
            decode_stream_directory(codec, &bytes, entries.len()).unwrap();
        assert_eq!(back, entries);
    }

    #[test]
    fn directory_bitrank_selected_and_round_trips() {
        // 16 dense contiguous stream ids -> U/n = 1 -> bitrank.
        let entries: Vec<DirEntryRaw> = (0..16u64)
            .map(|i| DirEntryRaw {
                stream_id:     100 + i,
                first_version: i,
                last_version:  i + 5,
                ptr_off:       i * 7,
                ptr_len:       7,
                n_batches:     2,
                skip_off:      i * 3,
                skip_len:      3,
            })
            .collect();
        let (codec, bytes) = encode_stream_directory(&entries);
        assert_eq!(codec, DIRCODEC_BITRANK);
        let back =
            decode_stream_directory(codec, &bytes, entries.len()).unwrap();
        assert_eq!(back, entries);
    }

    #[test]
    fn pack_parse_validates_and_finds_sections() {
        let streams = vec![
            stream(10, &[(0, 3, 1000, 4096), (3, 2, 1003, 8192)]),
            stream(20, &[(0, 1, 1005, 12288)]),
        ];
        let type_ids = vec![1u32, 1, 1, 2, 5];
        let input = PackInput {
            segment_id:     7,
            base_pos:       1000,
            streams:        &streams,
            event_type_ids: &type_ids,
            filter:         None,
            payload_bytes:  None,
        };
        let bytes = encode_pack(&input);
        let parsed = parse_pack(&bytes).unwrap();
        assert_eq!(parsed.segment_id, 7);
        assert_eq!(parsed.base_pos, 1000);
        assert_eq!(parsed.event_count, 6);
        assert!(parsed.section(KIND_STREAM_DIRECTORY).is_some());
        assert!(parsed.section(KIND_POINTER_BLOCKS).is_some());
        assert!(parsed.section(KIND_POINTER_SKIPS).is_some());
        assert!(parsed.section(KIND_EVENT_TYPE_IDS).is_some());
        // No filter / payload staged.
        assert!(parsed.section(KIND_STREAM_FILTER).is_none());
        assert!(parsed.section(KIND_PAYLOAD_COLUMNS).is_none());

        let evsec = parsed.section(KIND_EVENT_TYPE_IDS).unwrap();
        let body = &bytes[evsec.offset..evsec.offset + evsec.length];
        assert_eq!(decode_event_types(body).unwrap(), type_ids);
    }

    /// Review F1 semantics: a flip in the header or section directory fails
    /// the trailer hash (whole pack rejected); a flip in a MANDATORY section
    /// body fails that section's directory-committed CRC (whole pack rejected
    /// — the reader cannot resolve without it); a flip in an OPTIONAL section
    /// body is caught by ITS OWN CRC only — the pack still parses and just
    /// that section is dropped (local degradation).
    #[test]
    fn hash_scope_header_directory_fatal_optional_sections_local() {
        let streams = vec![stream(10, &[(0, 3, 1000, 4096)])];
        let type_ids = vec![1u32, 1, 2];
        let input = PackInput {
            segment_id:     1,
            base_pos:       0,
            streams:        &streams,
            event_type_ids: &type_ids,
            filter:         None,
            payload_bytes:  None,
        };
        let good = encode_pack(&input);
        let parsed = parse_pack(&good).unwrap();

        // (a) header + directory flips: trailer hash rejects the pack.
        for at in [8usize, HEADER_LEN, HEADER_LEN + 4] {
            let mut b = good.clone();
            b[at] ^= 0xFF;
            assert!(
                parse_pack(&b).is_err(),
                "header/directory flip at {at} must reject the pack"
            );
        }

        // (b) mandatory section body flip: that section's CRC rejects the
        // whole pack (unresolvable without it).
        let pb = parsed.section(KIND_POINTER_BLOCKS).unwrap();
        let mut b = good.clone();
        b[pb.offset] ^= 0xFF;
        assert!(
            matches!(
                parse_pack(&b),
                Err(PackError::Corrupt("missing/corrupt mandatory section"))
            ),
            "mandatory section flip must reject the pack"
        );

        // (c) OPTIONAL section body flip (EVENT_TYPE_IDS): pack still parses;
        // only that section is dropped; mandatory sections keep verifying.
        // NO trailer repair — this is a real single-bit-rot scenario.
        let ev = parsed.section(KIND_EVENT_TYPE_IDS).unwrap();
        let mut b = good.clone();
        b[ev.offset] ^= 0xFF;
        let reparsed = parse_pack(&b).expect(
            "optional-section corruption must not reject the whole pack",
        );
        assert!(
            reparsed.section(KIND_EVENT_TYPE_IDS).is_none(),
            "corrupt optional section must be dropped"
        );
        assert!(reparsed.section(KIND_STREAM_DIRECTORY).is_some());
        assert!(reparsed.section(KIND_POINTER_BLOCKS).is_some());
        assert!(reparsed.section(KIND_POINTER_SKIPS).is_some());
    }
}
