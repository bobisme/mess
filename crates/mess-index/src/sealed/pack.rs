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
//! The trailer's `pack_hash` is also this pack's stable **identity**
//! ([`PackIdentity`], bn-11g): the value a sealed segment's footer records to
//! name the exact pack it accepted (spec 01 §3.3.3). Naming it costs a reader
//! nothing — `open_pack` recomputes and verifies this hash before it will serve
//! a single pointer, so the identity a reader compares against the footer is
//! always one it derived from the bytes in front of it, never one the pack
//! asserted about itself. See [`PackIdentity`] for why the narrow hash scope is
//! still a whole-pack identity.
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
//!
//! # What a reopen reads, and what it does not (bn-dbz)
//!
//! A pack is opened one of two ways, and the difference is *when* section
//! bytes are read, never what they decode to:
//!
//! - [`parse_pack`] — the **eager** form: the whole image is already in memory
//!   (the sealer's parse-back, an offline verifier, a test), so every section's
//!   `crc32c` + `content_hash_prefix` is checked up front.
//! - [`PackDirectory::attach`] — the **lazy** form used by
//!   [`SealedSegmentIndex::open_pack`](crate::sealed::segment::SealedSegmentIndex::open_pack)
//!   at engine open. It preads the 64-byte header, the 40-byte trailer, and the
//!   `n_sections × 48`-byte directory, verifies the trailer's blake3 over
//!   header+directory, and stops. The reader then pulls in only the sections a
//!   *pointer* resolution needs — `STREAM_DIRECTORY`, `POINTER_BLOCKS`,
//!   `POINTER_SKIPS`, and the small `STREAM_FILTER` — each verified against its
//!   directory-committed checksums as it is read. `PAYLOAD_COLUMNS` and
//!   `EVENT_TYPE_IDS`, the two sections that scale with the segment's *content*
//!   rather than its stream count, stay on disk behind the retained file handle.
//!
//! Reading the whole pack at open made reopen residency linear in total sealed
//! bytes: bn-2u01 measured `open_pack` materializing the entire pack **plus** a
//! second copy of the payload columns **plus** a `Vec<u32>` per event from
//! `EVENT_TYPE_IDS` (~1.5 GB at 385M events), and Spike J measured +18%/+36%
//! reopen RSS at 2M/10M events with SealPack on. Lazy open makes it linear in
//! stream count instead.
//!
//! Integrity moves with the laziness rather than eroding, exactly as it did for
//! the `.pcol` sidecar in bn-bka2:
//!
//! - The trailer hash covers header + directory, so the directory's per-section
//!   `crc32c` + `content_hash_prefix` are themselves trustworthy without
//!   reading a single section body.
//! - A **mandatory** section is read and fully verified at open; a failure
//!   rejects the pack and the reader raw-scans, as before.
//! - `PAYLOAD_COLUMNS` is handed to
//!   [`SealedPayloadIndex::attach_at`](crate::sealed::payload::SealedPayloadIndex::attach_at),
//!   whose own `FLAG_SPLIT_CRC` block index + per-block CRCs are verified at
//!   attach and at first block read respectively.
//! - `EVENT_TYPE_IDS` written with [`ETFLAG_BLOCK_CRC`] carries a per-block
//!   checksum table (see [`encode_event_types`]) verified at first touch of
//!   that block. A section written *without* the flag has only a whole-section
//!   CRC, so it is read whole and verified at open — trunk-identical behaviour
//!   for any pack sealed before bn-dbz, on the bn-bka2 legacy ruling.
//!
//! Damage found late is the same typed degradation the read path already
//! handles: the accelerator drops and the raw log answers (D1/I5).

use std::fs::File;
use std::os::unix::fs::FileExt;
use std::sync::Arc;

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
/// fallback, and the tie-break winner — see [`should_use_bitrank`]).
pub const DIRCODEC_SORTED: u16 = 0;
/// Bitvector + rank over the shifted `[min, max]` universe (§12.2, Spike H).
/// The bit position IS the stream id, so no key copy is stored; selected when
/// that trade is strictly smaller on disk (see [`should_use_bitrank`]).
pub const DIRCODEC_BITRANK: u16 = 1;

/// The short name of a `STREAM_DIRECTORY` codec id, for operator-facing
/// reports (`mess inspect`'s per-segment `dir_codec`). Unknown ids render as
/// `"unknown"` rather than panicking — a future codec must still be
/// *describable* by an older tool.
#[must_use]
pub const fn dircodec_name(codec_id: u16) -> &'static str {
    match codec_id {
        DIRCODEC_SORTED => "sorted",
        DIRCODEC_BITRANK => "bitrank",
        _ => "unknown",
    }
}

/// A sorted-directory record: 56 bytes. Offsets are **relative** to the start
/// of the `POINTER_BLOCKS` / `POINTER_SKIPS` sections.
const DIR_SORTED_REC_LEN: usize = 56;
/// A bitrank-directory entry record: 48 bytes (no stream id — the bit position
/// is the key).
const DIR_BITRANK_REC_LEN: usize = 48;

// -------------------------------------------------------------------------
// Identity (bn-11g)
// -------------------------------------------------------------------------

/// A SealPack's **stable identity**: `blake3(header ++ section directory)` —
/// the exact 32 bytes the pack carries in its trailer and that every open
/// (eager [`parse_pack`] or lazy [`parse_pack_directory`]) recomputes and
/// verifies before the pack may answer anything.
///
/// # Why this is a whole-pack identity
///
/// The hash covers 64 + `n_sections × 48` bytes and no section body, yet two
/// packs with equal identities agree on every byte a reader can use. The
/// directory commits each section's `crc32c` **and** `content_hash_prefix`, and
/// every read path checks a section's bytes against those before using them
/// (eagerly at open for the mandatory sections, at first touch for the
/// file-backed ones). So a changed body fails its directory-committed
/// checksums, and a changed directory or header fails this hash. Identity is
/// transitive through the directory, exactly as the trailer's integrity
/// guarantee is (review F1).
///
/// # Why it is the right thing for a footer to name
///
/// It is *stable*: it depends only on the pack's content, not on where the file
/// sits, when it was written, or what it is called. It is *free*: the open path
/// already computes it. And it is *specific* in the way coverage is not — a
/// stale pack from an earlier seal of the same segment range, a pack copied
/// from another store, and a same-coverage substitute all have different
/// identities, while all three match `segment_id`/`base_pos`/`event_count`.
/// See spec 01 §3.3.3 / D-FMT-10.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct PackIdentity([u8; 32]);

impl PackIdentity {
    /// Wrap raw identity bytes (e.g. decoded from a segment footer).
    #[must_use]
    pub fn from_bytes(bytes: [u8; 32]) -> Self { PackIdentity(bytes) }

    /// The raw identity bytes.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8; 32] { &self.0 }

    /// Lowercase hex — the operator-facing rendering (`mess verify`, the
    /// refutation log lines).
    #[must_use]
    pub fn hex(&self) -> String {
        use std::fmt::Write as _;
        let mut s = String::with_capacity(64);
        for b in &self.0 {
            let _ = write!(s, "{b:02x}");
        }
        s
    }
}

impl std::fmt::Debug for PackIdentity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "PackIdentity({})", self.hex())
    }
}

impl std::fmt::Display for PackIdentity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.hex())
    }
}

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

/// On-disk length of the `DIRCODEC_SORTED` image for `n` entries.
const fn dir_sorted_len(n: usize) -> u64 {
    (n as u64).saturating_mul(DIR_SORTED_REC_LEN as u64)
}

/// On-disk length of the `DIRCODEC_BITRANK` image for `n` entries spanning a
/// universe of `u` stream ids: `min | n_words | words[ceil(u/64)] |
/// entries[n]`.
const fn dir_bitrank_len(n: usize, u: u64) -> u64 {
    16u64
        .saturating_add(u.div_ceil(64).saturating_mul(8))
        .saturating_add((n as u64).saturating_mul(DIR_BITRANK_REC_LEN as u64))
}

/// The §12.6 representation chooser, as a **deterministic byte-cost
/// comparison**: emit whichever directory image is strictly smaller, and
/// `DIRCODEC_SORTED` on a tie.
///
/// # Why byte cost and not a density ratio (bn-we9x)
///
/// The rule used to be `n >= 8 && U <= 8n` — Spike H's *lookup* crossover,
/// carried over from a tournament that compared in-memory directory layouts.
/// `bn-dcr` corrected that premise: `STREAM_DIRECTORY` is a **serialization
/// codec only**. Both codecs rehydrate into the identical
/// `HashMap<u64, DirEntry>`, so the codec cannot affect lookup speed at all;
/// what it does affect is bytes on disk (and the `pread` + decode that reads
/// them). The only defensible criterion left is therefore the one design.md
/// §12.6 actually prescribes — compare the encodings' byte costs — and the
/// old ratio was measurably too conservative for it: 8 of 39 real
/// engine-written segment directories (`U/n` 9.3–19.1) were paying 6–14%
/// more directory bytes than necessary.
///
/// # The trade, stated exactly
///
/// Bitrank drops the 8-byte `stream_id` from every record (56 → 48 B) and
/// spends the savings on a `ceil(U/64)`-word bitvector plus a 16-byte header.
/// So it wins **exactly** when the bitvector costs less than the key column it
/// replaced, i.e. `16 + 8*ceil(U/64) < 8n`, i.e. `U <= 64*(n - 3)`. That
/// makes the size bound self-evident and strictly stronger than any ratio cap:
/// **the chosen image is never larger than the sorted image**, so the
/// bitvector can never blow up relative to the always-present entry column.
///
/// Cost of the extra region, measured on the same 39 real directories
/// (ABBA-ordered paired decode, median of 41): total directory decode
/// 273.2 µs → 277.0 µs — **+3.8 µs across the whole corpus** — for
/// −48,880 bytes. Bitrank decode is 1.14–1.38× sorted's, and it buys back far
/// more than that in bytes not read.
///
/// # Determinism
///
/// A pure function of `(n, min, max)` with an integer comparison and no
/// floating point, no tie ambiguity (`<`, so equal cost picks sorted), and no
/// dependence on hasher seed, iteration order, or wall clock. Identical
/// directory content therefore always yields the identical codec and the
/// identical bytes, which is what re-seal byte-stability (and so pack
/// identity) rests on.
fn should_use_bitrank(entries: &[DirEntryRaw]) -> bool {
    let n = entries.len();
    if n == 0 {
        // Empty segment: there is no `[min, max]`, and the sorted image is the
        // empty body. The deterministic simple fallback.
        return false;
    }
    // Entries are ascending by stream_id (encoder invariant).
    let min = entries[0].stream_id;
    let max = entries[n - 1].stream_id;
    let u = (max - min).saturating_add(1);
    dir_bitrank_len(n, u) < dir_sorted_len(n)
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
///
/// The production call site is [`encode_stream_directory`], which reaches here
/// only after [`should_use_bitrank`] has proved this image is smaller than the
/// sorted one — so `U <= 64*(n-3)` and the `as usize` / `vec![0u64; n_words]`
/// below are bounded by `n`, never by the raw stream-id span.
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
/// count, plus (bn-dbz) a per-block checksum table so a reader can serve a
/// point read without materializing — or even reading — the whole column.
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
/// # Why a per-block checksum table (bn-dbz)
///
/// "Small next to the payload columns" is not the same as small: 1 B/event is
/// 385 MB at 385M events, and the reader used to *decode* it into a
/// `Vec<u32>` (4 B/event, ~1.5 GB) at open. Keeping the column on disk and
/// `pread`ing the piece a read actually needs removes both, but the section's
/// directory-committed `crc32c` covers the whole body — verifying it would mean
/// reading everything, which is the cost being removed. So the body carries its
/// own split coverage, the same shape `.pcol` gained in bn-bka2:
///
/// - a **prologue CRC** over the 12-byte header + dictionary, verified when the
///   column is attached; and
/// - one CRC per fixed run of [`ET_BLOCK_EVENTS`] index entries, verified
///   before that block's bytes are used.
///
/// [`ETFLAG_BLOCK_CRC`] in the formerly-reserved `flags` byte says the table is
/// present. A body written without it (a pack sealed before bn-dbz) is read
/// whole and whole-section-CRC verified, exactly as it always was.
///
/// The table is appended *after* the index region and the header keeps its
/// original field layout, so a pre-bn-dbz [`decode_event_types`] reads a new
/// body correctly and ignores the trailing bytes.
///
/// ```text
///   0   u32  event_count
///   4   u32  dict_len
///   8   u8   index_width (1 | 2 | 4)
///   9   u8   flags            (bit0 ETFLAG_BLOCK_CRC — was `reserved`)
///   10  u16  block_shift      (events per CRC block = 1 << block_shift;
///                              0 unless ETFLAG_BLOCK_CRC — was `reserved`)
///   12  dict:    dict_len × u32   (distinct type ids, first-seen order)
///   ..  indices: event_count × index_width bytes
///   ..  crcs:    (1 + n_blocks) × u32   (only when ETFLAG_BLOCK_CRC)
///                [0]     crc32c(header ++ dict)          — the prologue
///                [1 + i] crc32c(block i's index bytes)
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

    let n_blocks = type_ids.len().div_ceil(ET_BLOCK_EVENTS);
    let mut buf = Vec::with_capacity(
        ET_HEADER_LEN
            + dict.len() * 4
            + type_ids.len() * width as usize
            + (1 + n_blocks) * 4,
    );
    put_u32(&mut buf, type_ids.len() as u32);
    put_u32(&mut buf, dict.len() as u32);
    buf.push(width);
    buf.push(ETFLAG_BLOCK_CRC);
    put_u16(&mut buf, ET_BLOCK_SHIFT);
    for &t in &dict {
        put_u32(&mut buf, t);
    }
    // The prologue (header + dictionary) is what an attach reads and keeps
    // resident; checksum it before the index region starts.
    let prologue_crc = crc32c::crc32c(&buf);
    let idx_start = buf.len();
    for &i in &indices {
        match width {
            1 => buf.push(i as u8),
            2 => put_u16(&mut buf, i as u16),
            _ => put_u32(&mut buf, i),
        }
    }
    // One CRC per ET_BLOCK_EVENTS run of index entries, so a lazily attached
    // reader checksums exactly the bytes it preads.
    let mut crcs: Vec<u32> = Vec::with_capacity(1 + n_blocks);
    crcs.push(prologue_crc);
    let block_bytes = ET_BLOCK_EVENTS * width as usize;
    for b in 0..n_blocks {
        let lo = idx_start + b * block_bytes;
        let hi = (lo + block_bytes).min(buf.len());
        crcs.push(crc32c::crc32c(&buf[lo..hi]));
    }
    for c in crcs {
        put_u32(&mut buf, c);
    }
    buf
}

/// The `EVENT_TYPE_IDS` body's fixed header length (bytes).
pub const ET_HEADER_LEN: usize = 12;
/// `EVENT_TYPE_IDS` body `flags` bit 0 (bn-dbz): the body carries the trailing
/// per-block `crc32c` table described on [`encode_event_types`], so a reader
/// can attach the column **file-backed** and checksum each `pread` before using
/// it. Clear on a body written before bn-dbz (the field was `reserved = 0`),
/// which is therefore read whole and verified against the section's own
/// directory-committed `crc32c`, exactly as it always was.
pub const ETFLAG_BLOCK_CRC: u8 = 0x01;
/// `log2` of the per-CRC-block event count for [`ETFLAG_BLOCK_CRC`] bodies.
///
/// 1024 events is 1 KiB of index at the common `width = 1`. The block size is
/// a latency/residency knob and 1024 is where both flatten out: measured on the
/// `pack_read_bench` 1M-event pack, a random point read costs p50 741 ns at
/// 4096 events/block (the `crc32c` dominates), **260 ns at 1024**, and 201 ns
/// at 256 — while the resident CRC table grows 4× per step down, and the
/// `pread` syscall floor (~200 ns) caps what the last step can buy. At 1024 the
/// table is 4 B per 1024 events (1.5 MB at 385M events) against the 4 B/event
/// (1.5 GB) of decoded column it replaces.
pub const ET_BLOCK_SHIFT: u16 = 10;
/// Events per CRC block (see [`ET_BLOCK_SHIFT`]).
pub const ET_BLOCK_EVENTS: usize = 1 << ET_BLOCK_SHIFT;

/// The `EVENT_TYPE_IDS` body's fixed-header fields plus the derived region
/// offsets, parsed from the first [`ET_HEADER_LEN`] bytes alone — so a reader
/// can lay the section out before reading (or without ever reading) the
/// dictionary and index regions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct EtLayout {
    pub event_count: usize,
    pub dict_len:    usize,
    pub width:       usize,
    pub flags:       u8,
    pub block_shift: u32,
    /// Offset of the dictionary within the section body.
    pub dict_start:  usize,
    /// Offset of the per-event index region within the section body.
    pub idx_start:   usize,
    /// Offset of the trailing CRC table, and one past the index region.
    pub crc_start:   usize,
    /// Total body length implied by the header (index region, plus the CRC
    /// table when [`ETFLAG_BLOCK_CRC`] is set).
    pub body_len:    usize,
    /// Number of CRC-covered index blocks (0 without [`ETFLAG_BLOCK_CRC`]).
    pub n_blocks:    usize,
}

impl EtLayout {
    /// Parse the fixed header. Every derived offset is overflow-checked, so a
    /// caller may use them to bound `pread`s against a section length without
    /// re-validating the arithmetic.
    pub(crate) fn parse(head: &[u8]) -> Result<Self, PackError> {
        if head.len() < ET_HEADER_LEN {
            return Err(PackError::Corrupt("event-type section too short"));
        }
        let event_count = rd_u32(head, 0) as usize;
        let dict_len = rd_u32(head, 4) as usize;
        let width = head[8] as usize;
        if width != 1 && width != 2 && width != 4 {
            return Err(PackError::Corrupt("event-type bad index width"));
        }
        let flags = head[9];
        let block_shift = u32::from(rd_u16(head, 10));
        let dict_start = ET_HEADER_LEN;
        let dict_bytes = dict_len
            .checked_mul(4)
            .ok_or(PackError::Corrupt("dict overflow"))?;
        let idx_start = dict_start
            .checked_add(dict_bytes)
            .ok_or(PackError::Corrupt("event-type layout overflow"))?;
        let idx_bytes = event_count
            .checked_mul(width)
            .ok_or(PackError::Corrupt("event-type index overflow"))?;
        let crc_start = idx_start
            .checked_add(idx_bytes)
            .ok_or(PackError::Corrupt("event-type layout overflow"))?;
        let (n_blocks, body_len) = if flags & ETFLAG_BLOCK_CRC != 0 {
            // A zero/absurd shift would make the block count meaningless; the
            // encoder only ever writes ET_BLOCK_SHIFT, but a corrupt header
            // must not divide by zero or allocate wildly.
            if block_shift == 0 || block_shift > 32 {
                return Err(PackError::Corrupt("event-type bad block shift"));
            }
            let per = 1usize << block_shift;
            let n = event_count.div_ceil(per);
            let table = n
                .checked_add(1)
                .and_then(|k| k.checked_mul(4))
                .ok_or(PackError::Corrupt("event-type crc table overflow"))?;
            (
                n,
                crc_start
                    .checked_add(table)
                    .ok_or(PackError::Corrupt("event-type layout overflow"))?,
            )
        } else {
            (0, crc_start)
        };
        Ok(EtLayout {
            event_count,
            dict_len,
            width,
            flags,
            block_shift,
            dict_start,
            idx_start,
            crc_start,
            body_len,
            n_blocks,
        })
    }

    /// The index-region byte span of CRC block `b`, relative to the body.
    fn block_span(&self, b: usize) -> (usize, usize) {
        let per_block = (1usize << self.block_shift) * self.width;
        let lo = self.idx_start + b * per_block;
        ((lo), (lo + per_block).min(self.crc_start))
    }
}

/// Read one index entry at body offset `at`.
#[inline]
fn rd_index(buf: &[u8], at: usize, width: usize) -> usize {
    match width {
        1 => buf[at] as usize,
        2 => rd_u16(buf, at) as usize,
        _ => rd_u32(buf, at) as usize,
    }
}

/// Decode the `EVENT_TYPE_IDS` section back into the per-event type id column.
/// Tolerates a body longer than the header implies (a bn-dbz body's trailing
/// CRC table), so the two encodings share one whole-body decoder.
pub fn decode_event_types(body: &[u8]) -> Result<Vec<u32>, PackError> {
    let l = EtLayout::parse(body)?;
    if body.len() < l.crc_start {
        return Err(PackError::Corrupt("event-type section truncated"));
    }
    let mut dict = Vec::with_capacity(l.dict_len);
    for i in 0..l.dict_len {
        dict.push(rd_u32(body, l.dict_start + i * 4));
    }
    let mut out = Vec::with_capacity(l.event_count);
    for i in 0..l.event_count {
        let idx = rd_index(body, l.idx_start + i * l.width, l.width);
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
    pub segment_id:     u64,
    pub base_pos:       u64,
    pub event_count:    u64,
    pub n_streams:      u32,
    /// This pack's verified [`PackIdentity`] (bn-11g) — the trailer hash that
    /// [`parse_pack_directory`] just recomputed over the header + directory.
    pub identity:       PackIdentity,
    /// The pack's own `format_version` (bn-11g), for the footer cross-check.
    pub format_version: u16,
    pub sections:       Vec<ValidSection>,
}

impl ParsedPack {
    /// The first section of `kind` whose CRC verified, as an absolute span.
    pub fn section(&self, kind: u16) -> Option<ValidSection> {
        self.sections.iter().find(|s| s.kind == kind && s.crc_ok).copied()
    }
}

/// A section's directory entry: where its bytes are and what they must hash to.
/// Unlike [`ValidSection`] this carries the *committed* checksums rather than
/// the verdict, because a lazily attached pack (bn-dbz) checks a section's
/// bytes when it reads them, not at open.
#[derive(Debug, Clone, Copy)]
pub(crate) struct SectionRef {
    pub kind:        u16,
    pub codec_id:    u16,
    /// Absolute byte offset of the section body within the pack file.
    pub offset:      usize,
    pub length:      usize,
    pub crc:         u32,
    pub hash_prefix: u64,
}

impl SectionRef {
    /// Whether `body` is exactly this section's committed bytes. Both the
    /// `crc32c` and the blake3 `content_hash_prefix` must match — they are
    /// independent, so a fault is caught even if one algorithm collides.
    pub fn verify(&self, body: &[u8]) -> bool {
        body.len() == self.length
            && crc32c::crc32c(body) == self.crc
            && hash_prefix(body) == self.hash_prefix
    }
}

/// A pack's header + section directory, structurally validated and bound by the
/// trailer's blake3 hash — everything a reader needs to *locate* and *check*
/// any section without having read one (bn-dbz).
pub(crate) struct PackDirectory {
    pub segment_id:     u64,
    pub base_pos:       u64,
    pub event_count:    u64,
    pub n_streams:      u32,
    /// This pack's verified [`PackIdentity`] (bn-11g). Set only after the
    /// trailer hash matched, so holding one is proof the header + directory
    /// are exactly the bytes the identity names.
    pub identity:       PackIdentity,
    /// The pack's own `format_version`, for the footer's cross-check
    /// (spec 01 §3.3.3: a pack whose header disagrees is not the named pack).
    pub format_version: u16,
    pub sections:       Vec<SectionRef>,
}

impl PackDirectory {
    /// The first section of `kind`, or `None`.
    pub fn section(&self, kind: u16) -> Option<SectionRef> {
        self.sections.iter().find(|s| s.kind == kind).copied()
    }
}

/// The header fields a reader needs before it can even size the directory.
#[derive(Debug, Clone, Copy)]
pub(crate) struct PackHeader {
    pub segment_id:     u64,
    pub base_pos:       u64,
    pub event_count:    u64,
    pub n_streams:      u32,
    pub n_sections:     usize,
    /// First byte after the directory = first section body byte.
    pub sections_off:   usize,
    /// The pack's `format_version` (always [`FORMAT_VERSION`] today; carried
    /// so the footer's identity cross-check can compare it — bn-11g).
    pub format_version: u16,
}

impl PackHeader {
    /// Directory length in bytes.
    pub fn dir_len(&self) -> usize { self.n_sections * SECTION_REF_LEN }
}

/// Parse and self-check a pack's 64-byte header: magic, `format_version`, and
/// the directory/section offsets' internal consistency. Reads nothing else, so
/// a lazy open can size its next two `pread`s from the result.
pub(crate) fn parse_pack_header(head: &[u8]) -> Result<PackHeader, PackError> {
    if head.len() < HEADER_LEN {
        return Err(PackError::Corrupt("shorter than header"));
    }
    if rd_u32(head, 0) != PACK_MAGIC {
        return Err(PackError::Corrupt("bad header magic"));
    }
    if rd_u16(head, 4) != FORMAT_VERSION {
        return Err(PackError::Corrupt("unknown format_version"));
    }
    let n_sections = rd_u32(head, 36) as usize;
    let directory_off = rd_u64(head, 40) as usize;
    let sections_off = rd_u64(head, 48) as usize;
    if directory_off != HEADER_LEN {
        return Err(PackError::Corrupt("directory_off != header len"));
    }
    let dir_len = n_sections
        .checked_mul(SECTION_REF_LEN)
        .ok_or(PackError::Corrupt("directory length overflow"))?;
    let dir_end = directory_off
        .checked_add(dir_len)
        .ok_or(PackError::Corrupt("directory end overflow"))?;
    if sections_off != dir_end {
        return Err(PackError::Corrupt("sections_off inconsistent"));
    }
    Ok(PackHeader {
        segment_id: rd_u64(head, 8),
        base_pos: rd_u64(head, 16),
        event_count: rd_u64(head, 24),
        n_streams: rd_u32(head, 32),
        n_sections,
        sections_off,
        format_version: rd_u16(head, 4),
    })
}

/// Validate the trailer against `head` + `dir` and decode the section
/// directory, bounds-checking every span against the file — **without reading a
/// single section body** (bn-dbz).
///
/// The trailer's blake3 covers header + directory only (review F1), so this is
/// the complete integrity check for those two regions; each section's own
/// `crc32c` + `content_hash_prefix` then rides in the (now-trusted) directory,
/// to be checked by whoever reads that section's bytes.
pub(crate) fn parse_pack_directory(
    head: &[u8],
    dir: &[u8],
    trailer: &[u8],
    file_len: u64,
) -> Result<PackDirectory, PackError> {
    let h = parse_pack_header(head)?;
    if dir.len() != h.dir_len() || trailer.len() != TRAILER_LEN {
        return Err(PackError::Corrupt("directory/trailer size mismatch"));
    }
    if rd_u32(trailer, 36) != PACK_MAGIC {
        return Err(PackError::Corrupt("bad trailer magic"));
    }
    if file_len < (HEADER_LEN + TRAILER_LEN) as u64 {
        return Err(PackError::Corrupt("shorter than header + trailer"));
    }
    let trailer_start = (file_len - TRAILER_LEN as u64) as usize;
    if h.sections_off > trailer_start {
        return Err(PackError::Corrupt("sections_off inconsistent"));
    }

    // Pack hash covers header + directory ONLY (review F1): the directory's
    // per-section crc32c + content_hash_prefix bind the section bodies
    // transitively, and a corrupt OPTIONAL section must fail its OWN checksum
    // (local degradation), not this pack-wide one.
    let mut hasher = blake3::Hasher::new();
    hasher.update(&head[..HEADER_LEN]);
    hasher.update(dir);
    let computed = *hasher.finalize().as_bytes();
    if computed != trailer[0..32] {
        return Err(PackError::Corrupt("pack header/directory hash mismatch"));
    }
    // bn-11g: the identity is the value just RECOMPUTED, never the stored one.
    // They are equal here by construction, and taking the computed side keeps
    // that true by construction rather than by reading order.
    let identity = PackIdentity::from_bytes(computed);

    let mut sections = Vec::with_capacity(h.n_sections);
    for i in 0..h.n_sections {
        let b = i * SECTION_REF_LEN;
        let offset = rd_u64(dir, b + 8) as usize;
        let length = rd_u64(dir, b + 16) as usize;
        let end = offset
            .checked_add(length)
            .ok_or(PackError::Corrupt("section span overflow"))?;
        if offset < h.sections_off || end > trailer_start {
            return Err(PackError::Corrupt("section span out of range"));
        }
        sections.push(SectionRef {
            kind: rd_u16(dir, b),
            codec_id: rd_u16(dir, b + 36),
            offset,
            length,
            crc: rd_u32(dir, b + 32),
            hash_prefix: rd_u64(dir, b + 40),
        });
    }
    Ok(PackDirectory {
        segment_id: h.segment_id,
        base_pos: h.base_pos,
        event_count: h.event_count,
        n_streams: h.n_streams,
        identity,
        format_version: h.format_version,
        sections,
    })
}

/// Validate a SealPack byte image: magic/version, whole-pack blake3 hash,
/// directory bounds, and every section's `crc32c` + `content_hash_prefix`.
/// Mandatory-section corruption (or any structural fault) is fatal; an optional
/// section that fails its CRC is kept but flagged `crc_ok=false`.
///
/// This is the **eager** form (the sealer's parse-back, an offline verifier, a
/// test). Engine open goes through [`parse_pack_directory`] instead and checks
/// each section as it reads it.
pub(crate) fn parse_pack(bytes: &[u8]) -> Result<ParsedPack, PackError> {
    if bytes.len() < HEADER_LEN + TRAILER_LEN {
        return Err(PackError::Corrupt("shorter than header + trailer"));
    }
    let h = parse_pack_header(bytes)?;
    if h.sections_off > bytes.len() - TRAILER_LEN {
        return Err(PackError::Corrupt("sections_off inconsistent"));
    }
    let dir = parse_pack_directory(
        &bytes[..HEADER_LEN],
        &bytes[HEADER_LEN..h.sections_off],
        &bytes[bytes.len() - TRAILER_LEN..],
        bytes.len() as u64,
    )?;

    let sections: Vec<ValidSection> = dir
        .sections
        .iter()
        .map(|s| ValidSection {
            kind:     s.kind,
            codec_id: s.codec_id,
            offset:   s.offset,
            length:   s.length,
            crc_ok:   s.verify(&bytes[s.offset..s.offset + s.length]),
        })
        .collect();

    // Mandatory sections must be present and CRC-valid, else the whole pack is
    // untrustworthy and the reader must raw-scan.
    for &kind in
        &[KIND_STREAM_DIRECTORY, KIND_POINTER_BLOCKS, KIND_POINTER_SKIPS]
    {
        if !sections.iter().any(|s| s.kind == kind && s.crc_ok) {
            return Err(PackError::Corrupt("missing/corrupt mandatory section"));
        }
    }

    Ok(ParsedPack {
        segment_id: dir.segment_id,
        base_pos: dir.base_pos,
        event_count: dir.event_count,
        n_streams: dir.n_streams,
        identity: dir.identity,
        format_version: dir.format_version,
        sections,
    })
}

// -------------------------------------------------------------------------
// The file-backed event-type column (bn-dbz)
// -------------------------------------------------------------------------

/// Where an [`EventTypeColumn`]'s per-event index bytes come from.
///
/// The dictionary is always resident (it is O(distinct types), not O(events),
/// and every read needs it). The index region — 1 B/event in the common case —
/// is either already in memory or `pread` per read, which is the whole
/// difference between an eager and a lazy attach.
#[derive(Debug)]
enum EtSource {
    /// The whole section body, resident: the eager pack parse, or a body
    /// written before [`ETFLAG_BLOCK_CRC`] (which has no per-block checksums to
    /// attach lazily against, so it keeps the whole-section check it has
    /// today).
    Memory(Vec<u8>),
    /// The pack file plus the section's absolute offset. An index block is
    /// `pread` per read, checksummed against the body's CRC table, and dropped.
    File {
        file: Arc<File>,
        base: u64,
        /// `[0]` is the prologue CRC (verified at attach); `[1 + b]` covers
        /// index block `b`.
        crcs: Vec<u32>,
    },
}

/// One segment's `EVENT_TYPE_IDS` column: the resident dictionary plus a source
/// for the per-event indices ([`EtSource`]).
///
/// A point read is a dictionary lookup plus — for a file-backed column — one
/// `pread` of the covering [`ET_BLOCK_EVENTS`]-entry block and one `crc32c`
/// over it. `&self` throughout and `Sync` with no interior locking: positioned
/// reads do not touch the shared file offset, so concurrent readers of one
/// `Arc<SealedSegmentIndex>` never serialise.
#[derive(Debug)]
pub(crate) struct EventTypeColumn {
    layout: EtLayout,
    dict:   Vec<u32>,
    src:    EtSource,
}

impl EventTypeColumn {
    /// Total events the column covers.
    pub fn event_count(&self) -> u64 { self.layout.event_count as u64 }

    /// Whether the per-event index region is held in memory rather than read on
    /// demand — the bn-dbz laziness observable.
    pub fn is_resident(&self) -> bool {
        matches!(self.src, EtSource::Memory(_))
    }

    /// Build an **eager** column over an already-verified section body.
    pub fn from_body(body: Vec<u8>) -> Result<Self, PackError> {
        let layout = EtLayout::parse(&body)?;
        if body.len() < layout.crc_start {
            return Err(PackError::Corrupt("event-type section truncated"));
        }
        let dict = read_dict(&body[layout.dict_start..], layout.dict_len)?;
        Ok(EventTypeColumn { layout, dict, src: EtSource::Memory(body) })
    }

    /// **Lazily** attach the `EVENT_TYPE_IDS` section `sec` of the pack behind
    /// `file`: read the fixed header, the dictionary, and the trailing
    /// per-block CRC table — bytes proportional to the *distinct type
    /// count* and the *block count*, not to the event count — and retain
    /// the handle.
    ///
    /// A body without [`ETFLAG_BLOCK_CRC`] (a pack sealed before bn-dbz) has no
    /// per-block checksums, so attaching it lazily would run with no integrity
    /// check on the bytes actually read. It is instead read whole and verified
    /// against the section's directory-committed `crc32c` +
    /// `content_hash_prefix` — byte-for-byte the check the eager path makes —
    /// exactly the bn-bka2 legacy ruling for pre-`FLAG_SPLIT_CRC` `.pcol`s.
    pub fn attach(
        file: &Arc<File>,
        sec: &SectionRef,
    ) -> Result<Self, PackError> {
        let base = sec.offset as u64;
        let mut head = [0u8; ET_HEADER_LEN];
        if sec.length < ET_HEADER_LEN {
            return Err(PackError::Corrupt("event-type section too short"));
        }
        pread(file, &mut head, base)?;
        let layout = EtLayout::parse(&head)?;
        if layout.body_len != sec.length {
            return Err(PackError::Corrupt("event-type body/section mismatch"));
        }

        if layout.flags & ETFLAG_BLOCK_CRC == 0 {
            let mut body = vec![0u8; sec.length];
            pread(file, &mut body, base)?;
            if !sec.verify(&body) {
                return Err(PackError::Corrupt("event-type section CRC"));
            }
            return Self::from_body(body);
        }

        // Prologue = header + dictionary; the CRC table's first entry covers
        // it.
        let mut prologue = vec![0u8; layout.idx_start];
        pread(file, &mut prologue, base)?;
        let mut table = vec![0u8; (1 + layout.n_blocks) * 4];
        pread(file, &mut table, base + layout.crc_start as u64)?;
        let crcs: Vec<u32> =
            table.chunks_exact(4).map(|c| rd_u32(c, 0)).collect();
        if crc32c::crc32c(&prologue) != crcs[0] {
            return Err(PackError::Corrupt("event-type prologue CRC"));
        }
        let dict = read_dict(&prologue[layout.dict_start..], layout.dict_len)?;
        Ok(EventTypeColumn {
            layout,
            dict,
            src: EtSource::File { file: Arc::clone(file), base, crcs },
        })
    }

    /// The type ids of the stored-order events in `[lo, hi)`, or `None` if the
    /// range is out of bounds or any byte it needs fails its checksum / cannot
    /// be read. `None` is the caller's cue to decode the raw batch instead
    /// (D1: the log is the authority), which is exactly what an absent section
    /// already means.
    ///
    /// A file-backed column reads each covering [`ET_BLOCK_EVENTS`] block once,
    /// so a whole batch costs one `pread` in the overwhelmingly common case
    /// rather than one per event.
    pub fn range(&self, lo: u64, hi: u64) -> Option<Vec<u32>> {
        if hi < lo || hi > self.layout.event_count as u64 {
            return None;
        }
        let (lo, hi) = (lo as usize, hi as usize);
        let mut out = Vec::with_capacity(hi - lo);
        match &self.src {
            EtSource::Memory(body) => {
                for i in lo..hi {
                    let at = self.layout.idx_start + i * self.layout.width;
                    let idx = rd_index(body, at, self.layout.width);
                    out.push(*self.dict.get(idx)?);
                }
            }
            EtSource::File { file, base, crcs } => {
                if lo == hi {
                    return Some(out);
                }
                let per = 1usize << self.layout.block_shift;
                let mut buf: Vec<u8> = Vec::new();
                for b in (lo / per)..=((hi - 1) / per) {
                    let (blo, bhi) = self.layout.block_span(b);
                    buf.resize(bhi - blo, 0);
                    pread(file, &mut buf, base + blo as u64).ok()?;
                    if crc32c::crc32c(&buf) != *crcs.get(1 + b)? {
                        return None;
                    }
                    let first = lo.max(b * per);
                    let last = hi.min((b + 1) * per);
                    for i in first..last {
                        let at = (i - b * per) * self.layout.width;
                        let idx = rd_index(&buf, at, self.layout.width);
                        out.push(*self.dict.get(idx)?);
                    }
                }
            }
        }
        Some(out)
    }

    /// The type id of the stored-order event at `idx`, or `None` (see
    /// [`Self::range`]).
    pub fn get(&self, idx: u64) -> Option<u32> {
        self.range(idx, idx.checked_add(1)?)?.pop()
    }
}

/// `pread` exactly `buf.len()` bytes at `off`, as a [`PackError`].
fn pread(file: &File, buf: &mut [u8], off: u64) -> Result<(), PackError> {
    file.read_exact_at(buf, off)
        .map_err(|_| PackError::Corrupt("pack read failed"))
}

/// Decode `dict_len` `u32`s from the head of `buf`.
fn read_dict(buf: &[u8], dict_len: usize) -> Result<Vec<u32>, PackError> {
    if buf.len() < dict_len * 4 {
        return Err(PackError::Corrupt("event-type dictionary truncated"));
    }
    Ok((0..dict_len).map(|i| rd_u32(buf, i * 4)).collect())
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
        // dict {7,3,9} -> width 1; header 12 + dict 12 + 7 indices = 31 bytes,
        // plus the bn-dbz CRC table (prologue + one block) = 8 bytes.
        assert_eq!(enc.len(), 12 + 3 * 4 + 7 + 2 * 4);
        assert_eq!(enc[9], ETFLAG_BLOCK_CRC, "block-CRC flag set");
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

    // -- §12.6 chooser: byte-optimality, exactness, determinism (bn-we9x) ---

    /// `n` distinct ascending ids spanning **exactly** `[base, base + u - 1]`,
    /// clustered by a seeded splitmix64 rather than evenly spread — real
    /// interned stream-id sets arrive in bursts, and clustering is what moves
    /// the bitvector's word occupancy around without moving `U`.
    fn ids_spanning(n: usize, u: u64, base: u64, seed: u64) -> Vec<u64> {
        assert!(u >= n as u64, "universe too small for n distinct ids");
        let mut s = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
        let mut next = || {
            s = s.wrapping_add(0x9E37_79B9_7F4A_7C15);
            let mut z = s;
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            z ^ (z >> 31)
        };
        // Both endpoints are pinned so the span is exactly `u`; the interior is
        // drawn (and de-duplicated) from the open interval.
        let mut set = std::collections::BTreeSet::new();
        set.insert(0u64);
        if n > 1 {
            set.insert(u - 1);
        }
        while set.len() < n {
            set.insert(next() % u);
        }
        set.into_iter().take(n).map(|v| base + v).collect()
    }

    fn entries_for(ids: &[u64]) -> Vec<DirEntryRaw> {
        ids.iter()
            .enumerate()
            .map(|(i, &id)| DirEntryRaw {
                stream_id:     id,
                first_version: i as u64,
                last_version:  i as u64 + 3,
                ptr_off:       (i * 64) as u64,
                ptr_len:       64,
                n_batches:     2,
                skip_off:      (i * 8) as u64,
                skip_len:      8,
            })
            .collect()
    }

    /// The shapes every chooser test sweeps: the four real-corpus stream
    /// counts (`n` = 109 / 994 / 2022 / 8056 came off engine-written
    /// segments), the small-`n` tail, and `U/n` ratios walking from fully
    /// dense through the byte break-even (~64) out to the sparsest real
    /// segment (93.3).
    fn chooser_shapes() -> Vec<(usize, u64)> {
        let mut out = Vec::new();
        for n in [1usize, 2, 3, 4, 5, 7, 8, 9, 16, 63, 64, 65, 109, 994, 2022] {
            for ratio in [1u64, 2, 4, 8, 10, 13, 19, 32, 48, 61, 64, 67, 93] {
                out.push((n, (n as u64) * ratio));
            }
        }
        out
    }

    /// The AC's exactness clause on the shapes that matter: on every real and
    /// threshold-edge shape, **both** codecs round-trip
    /// `decode(encode(x)) == x` — including the one the chooser declined, so a
    /// future rule change cannot silently ship an inexact encoder.
    #[test]
    fn both_directory_codecs_are_exact_on_every_shape() {
        for (n, u) in chooser_shapes() {
            for (base, seed) in [(0u64, 1u64), (1 << 40, 7)] {
                let ids = ids_spanning(n, u, base, seed);
                // A single id spans 1 whatever `u` asked for.
                let span = ids[n - 1] - ids[0] + 1;
                let e = entries_for(&ids);
                let sorted = encode_dir_sorted(&e);
                assert_eq!(sorted.len() as u64, dir_sorted_len(n));
                assert_eq!(
                    decode_stream_directory(DIRCODEC_SORTED, &sorted, n)
                        .unwrap(),
                    e,
                    "sorted inexact at n={n} u={u}"
                );
                let bitrank = encode_dir_bitrank(&e);
                assert_eq!(bitrank.len() as u64, dir_bitrank_len(n, span));
                assert_eq!(
                    decode_stream_directory(DIRCODEC_BITRANK, &bitrank, n)
                        .unwrap(),
                    e,
                    "bitrank inexact at n={n} u={u}"
                );
            }
        }
    }

    /// The chooser's whole contract in one assertion: the emitted image is
    /// **never larger than the alternative**. Checked against both encoders
    /// materialized, so this is the measured product and not a restatement of
    /// the cost formulas.
    #[test]
    fn chooser_always_emits_the_smaller_image() {
        for (n, u) in chooser_shapes() {
            let e = entries_for(&ids_spanning(n, u, 0, 3));
            let (codec, bytes) = encode_stream_directory(&e);
            let sorted = encode_dir_sorted(&e);
            let bitrank = encode_dir_bitrank(&e);
            let best = sorted.len().min(bitrank.len());
            assert_eq!(
                bytes.len(),
                best,
                "chooser picked {codec} ({} B) over {best} B at n={n} u={u}",
                bytes.len()
            );
            // Ties go to sorted: the simpler codec, and the faster to decode.
            if sorted.len() == bitrank.len() {
                assert_eq!(codec, DIRCODEC_SORTED, "tie at n={n} u={u}");
            }
            assert_eq!(
                decode_stream_directory(codec, &bytes, n).unwrap(),
                e,
                "chosen codec inexact at n={n} u={u}"
            );
        }
        // The empty segment: the deterministic simple fallback, empty body.
        let (codec, bytes) = encode_stream_directory(&[]);
        assert_eq!(codec, DIRCODEC_SORTED);
        assert!(bytes.is_empty());
        assert_eq!(
            decode_stream_directory(codec, &bytes, 0).unwrap(),
            Vec::<DirEntryRaw>::new()
        );
    }

    /// Walk `U` one id at a time across the byte break-even and pin that the
    /// codec flips **exactly once, in one direction, at `U == 64*(n-3)`**.
    ///
    /// This is the pathological-flip check the re-seal story needs: near the
    /// threshold the decision must be a clean monotone step, not an
    /// oscillation that would make two near-identical segments (or two
    /// re-seals of drifting content) alternate representations — and, because
    /// the rule is "emit the smaller image", even the flip itself never costs
    /// more than the 8-byte tie.
    #[test]
    fn chooser_flips_once_at_the_byte_break_even() {
        for n in [8usize, 64, 109, 994] {
            let break_even = 64 * (n as u64 - 3);
            let mut prev_bitrank = true;
            for u in (break_even - 130)..=(break_even + 130) {
                let e = entries_for(&ids_spanning(n, u, 0, u));
                let (codec, bytes) = encode_stream_directory(&e);
                let is_bitrank = codec == DIRCODEC_BITRANK;
                assert_eq!(
                    is_bitrank,
                    u <= break_even,
                    "codec at n={n} u={u} (break-even {break_even})"
                );
                assert!(
                    prev_bitrank || !is_bitrank,
                    "codec oscillated back to bitrank at n={n} u={u}"
                );
                prev_bitrank = is_bitrank;
                assert_eq!(
                    decode_stream_directory(codec, &bytes, n).unwrap(),
                    e,
                    "inexact at the threshold, n={n} u={u}"
                );
            }
            assert!(!prev_bitrank, "never left bitrank at n={n}");
        }
    }

    /// Re-seal byte stability: the chooser is a pure function of
    /// `(n, min, max)`, so identical directory content must yield the
    /// identical codec **and** byte-identical section bytes — every time,
    /// from independently built inputs. Pack identity (the trailer hash over
    /// header + directory, which commits every section's `crc32c` +
    /// `content_hash_prefix`) rests on exactly this.
    #[test]
    fn chooser_is_deterministic_and_re_encode_is_byte_identical() {
        for (n, u) in chooser_shapes() {
            let ids = ids_spanning(n, u, 0, 11);
            let (codec, bytes) = encode_stream_directory(&entries_for(&ids));
            for _ in 0..3 {
                // Rebuild the entries from scratch each round so nothing can
                // be carried over in an allocation or a cached layout.
                let (c, b) = encode_stream_directory(&entries_for(&ids));
                assert_eq!(c, codec, "codec drifted at n={n} u={u}");
                assert_eq!(b, bytes, "bytes drifted at n={n} u={u}");
            }
        }
    }

    /// The same determinism claim at whole-pack scale, on threshold-edge
    /// shapes: two `encode_pack` calls over equal input are byte-identical,
    /// which is what makes a re-seal of unchanged content produce the same
    /// pack identity.
    #[test]
    fn pack_bytes_are_stable_across_threshold_edge_reseals() {
        for n in [8usize, 64, 200] {
            let break_even = 64 * (n as u64 - 3);
            for u in [break_even - 1, break_even, break_even + 1] {
                let streams: Vec<SealStream> = ids_spanning(n, u, 0, 5)
                    .into_iter()
                    .enumerate()
                    .map(|(i, id)| stream(id, &[(0, 2, 1000 + i as u64, 4096)]))
                    .collect();
                let mk = || PackInput {
                    segment_id:     3,
                    base_pos:       0,
                    streams:        &streams,
                    event_type_ids: &[],
                    filter:         None,
                    payload_bytes:  None,
                };
                let a = encode_pack(&mk());
                let b = encode_pack(&mk());
                assert_eq!(a, b, "pack bytes drifted at n={n} u={u}");
                let idx = parse_pack(&a).unwrap();
                let sec = idx.section(KIND_STREAM_DIRECTORY).unwrap();
                assert_eq!(
                    sec.codec_id == DIRCODEC_BITRANK,
                    u <= break_even,
                    "pack-level codec at n={n} u={u}"
                );
            }
        }
    }

    #[test]
    fn dircodec_names_are_stable() {
        assert_eq!(dircodec_name(DIRCODEC_SORTED), "sorted");
        assert_eq!(dircodec_name(DIRCODEC_BITRANK), "bitrank");
        assert_eq!(dircodec_name(9999), "unknown");
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

    // ---------------------------------------------------------------------
    // bn-dbz: the file-backed event-type column
    // ---------------------------------------------------------------------

    /// The pre-bn-dbz `EVENT_TYPE_IDS` body: identical header layout with
    /// `flags`/`block_shift` still zero `reserved`s, and no trailing CRC table.
    /// This is byte-for-byte what a pack sealed before bn-dbz carries, so a
    /// column attached over it exercises the legacy path exactly.
    fn encode_event_types_legacy(type_ids: &[u32]) -> Vec<u8> {
        let mut dict: Vec<u32> = Vec::new();
        let mut indices: Vec<u32> = Vec::with_capacity(type_ids.len());
        for &t in type_ids {
            let i = dict.iter().position(|&d| d == t).unwrap_or_else(|| {
                dict.push(t);
                dict.len() - 1
            });
            indices.push(i as u32);
        }
        assert!(dict.len() <= 256, "test corpora stay in the width-1 regime");
        let mut buf = Vec::new();
        put_u32(&mut buf, type_ids.len() as u32);
        put_u32(&mut buf, dict.len() as u32);
        buf.push(1); // width
        buf.push(0); // flags: pre-bn-dbz reserved
        put_u16(&mut buf, 0); // block_shift: pre-bn-dbz reserved
        for &t in &dict {
            put_u32(&mut buf, t);
        }
        for &i in &indices {
            buf.push(i as u8);
        }
        buf
    }

    /// Write `body` into a scratch file at `base` (with junk in front, so the
    /// base-offset arithmetic is genuinely exercised) and hand back the open
    /// handle plus the directory entry a pack would carry for it.
    fn section_file(
        dir: &std::path::Path,
        name: &str,
        base: usize,
        body: &[u8],
    ) -> (Arc<File>, SectionRef) {
        let path = dir.join(name);
        let mut image = vec![0xA5u8; base];
        image.extend_from_slice(body);
        image.extend_from_slice(&[0x5Au8; 16]); // trailing junk
        std::fs::write(&path, &image).unwrap();
        let sec = SectionRef {
            kind:        KIND_EVENT_TYPE_IDS,
            codec_id:    0,
            offset:      base,
            length:      body.len(),
            crc:         crc32c::crc32c(body),
            hash_prefix: hash_prefix(body),
        };
        (Arc::new(File::open(&path).unwrap()), sec)
    }

    /// A body written before bn-dbz has no per-block checksums, so attaching it
    /// lazily would read bytes with nothing to check them against. It must
    /// therefore attach EAGERLY — whole body, section `crc32c` +
    /// `content_hash_prefix` verified — which is byte-for-byte what an open did
    /// before this change (the bn-bka2 legacy ruling). Reads are unaffected.
    #[test]
    #[cfg_attr(miri, ignore)]
    fn legacy_event_type_body_attaches_eagerly_and_reads() {
        let dir = mess_testkit::sweeping_temp_dir("pack-et-legacy");
        let ids: Vec<u32> = (0..5000u32).map(|i| (i % 7) + 1).collect();
        let body = encode_event_types_legacy(&ids);
        // The whole-body decoder handles both encodings.
        assert_eq!(decode_event_types(&body).unwrap(), ids);

        let (file, sec) = section_file(dir.path(), "legacy.bin", 97, &body);
        let col = EventTypeColumn::attach(&file, &sec).unwrap();
        assert!(
            col.is_resident(),
            "a flags==0 body must attach eagerly: lazily it would carry no \
             checksum at all"
        );
        assert_eq!(col.event_count(), ids.len() as u64);
        for (i, &t) in ids.iter().enumerate() {
            assert_eq!(col.get(i as u64), Some(t));
        }
        assert_eq!(col.range(10, 4200).unwrap(), ids[10..4200]);
    }

    /// The legacy path must not lose integrity: a torn byte in a pre-bn-dbz
    /// body's index region is caught by the section's whole-body `crc32c` and
    /// the column is DROPPED AT ATTACH, exactly as it was before bn-dbz.
    #[test]
    #[cfg_attr(miri, ignore)]
    fn torn_legacy_event_type_body_is_dropped_at_attach() {
        let dir = mess_testkit::sweeping_temp_dir("pack-et-legacy-torn");
        let ids: Vec<u32> = (0..5000u32).map(|i| (i % 7) + 1).collect();
        let mut body = encode_event_types_legacy(&ids);
        let (_, sec) = section_file(dir.path(), "clean.bin", 97, &body);
        // Tear a byte in the index region AFTER computing the committed
        // checksums — real media damage, invisible to any structural check.
        let torn_at = body.len() - 100;
        body[torn_at] ^= 0xFF;
        let path = dir.path().join("torn.bin");
        let mut image = vec![0xA5u8; 97];
        image.extend_from_slice(&body);
        std::fs::write(&path, &image).unwrap();
        let file = Arc::new(File::open(&path).unwrap());
        assert!(
            EventTypeColumn::attach(&file, &sec).is_err(),
            "a torn legacy body must be dropped at attach"
        );
    }

    /// A bn-dbz body attaches file-backed and answers every point and range
    /// read identically to the eager, whole-body column — across CRC-block
    /// boundaries, which is where a blocked reader can go wrong.
    #[test]
    #[cfg_attr(miri, ignore)]
    fn lazy_event_type_column_matches_eager_across_block_boundaries() {
        let dir = mess_testkit::sweeping_temp_dir("pack-et-lazy");
        // Two and a bit CRC blocks.
        let n = ET_BLOCK_EVENTS * 2 + 37;
        let ids: Vec<u32> = (0..n as u32).map(|i| (i % 11) * 3 + 2).collect();
        let body = encode_event_types(&ids);
        let (file, sec) = section_file(dir.path(), "lazy.bin", 4096, &body);

        let lazy = EventTypeColumn::attach(&file, &sec).unwrap();
        assert!(!lazy.is_resident(), "a bn-dbz body must attach file-backed");
        let eager = EventTypeColumn::from_body(body).unwrap();
        assert!(eager.is_resident());

        for i in 0..n as u64 {
            assert_eq!(lazy.get(i), Some(ids[i as usize]), "point read {i}");
            assert_eq!(lazy.get(i), eager.get(i));
        }
        assert_eq!(lazy.get(n as u64), None, "out of range");
        // Ranges that stay inside one block, straddle one boundary, and span
        // every block including the short tail.
        let b = ET_BLOCK_EVENTS as u64;
        for (lo, hi) in [
            (0u64, 10u64),
            (b - 6, b + 10),
            (b + 1, 2 * b - 1),
            (0, n as u64),
            (2 * b, n as u64),
            (7, 7),
        ] {
            assert_eq!(
                lazy.range(lo, hi).unwrap(),
                ids[lo as usize..hi as usize],
                "range {lo}..{hi}"
            );
            assert_eq!(lazy.range(lo, hi), eager.range(lo, hi));
        }
        assert_eq!(lazy.range(0, n as u64 + 1), None, "range past the end");
    }

    /// The direct laziness proof: `attach` never reads the index region, so a
    /// body whose index bytes are garbage still attaches — and the damage
    /// surfaces at the read of the affected BLOCK, as a `None` the caller
    /// degrades on, while every other block keeps answering exactly.
    #[test]
    #[cfg_attr(miri, ignore)]
    fn lazy_event_type_block_tear_surfaces_at_read_and_stays_local() {
        let dir = mess_testkit::sweeping_temp_dir("pack-et-lazy-torn");
        let n = ET_BLOCK_EVENTS * 3;
        let ids: Vec<u32> = (0..n as u32).map(|i| (i % 5) + 1).collect();
        let mut body = encode_event_types(&ids);
        let layout = EtLayout::parse(&body).unwrap();
        // Tear one byte inside CRC block 1's index bytes.
        body[layout.idx_start + ET_BLOCK_EVENTS + 5] ^= 0xFF;
        let (file, mut sec) = section_file(dir.path(), "torn.bin", 8, &body);
        // Recompute the section checksums over the torn body: the pack
        // directory would commit whatever was written. This isolates the
        // per-block table as the ONLY thing that can catch the tear — the
        // situation a lazy open is actually in.
        sec.crc = crc32c::crc32c(&body);
        sec.hash_prefix = hash_prefix(&body);

        let col = EventTypeColumn::attach(&file, &sec)
            .expect("attach must not read the index region");
        assert!(!col.is_resident());
        // Block 0 and block 2 are untouched and exact.
        assert_eq!(col.get(0), Some(ids[0]));
        assert_eq!(col.range(0, 128).unwrap(), ids[0..128]);
        let b2 = ET_BLOCK_EVENTS as u64 * 2;
        assert_eq!(col.get(b2), Some(ids[b2 as usize]));
        // Block 1 refuses to serve — the caller falls back to the raw batch.
        assert_eq!(col.get(ET_BLOCK_EVENTS as u64 + 5), None);
        assert_eq!(col.get(ET_BLOCK_EVENTS as u64), None);
        // A range that merely touches the torn block refuses as a whole.
        assert_eq!(col.range(0, ET_BLOCK_EVENTS as u64 + 1), None);
    }

    /// A torn PROLOGUE (header + dictionary) is caught at attach: it is the one
    /// part a lazy column keeps resident, so it is checked exactly once, up
    /// front, against the CRC table's first entry.
    #[test]
    #[cfg_attr(miri, ignore)]
    fn lazy_event_type_prologue_tear_is_caught_at_attach() {
        let dir = mess_testkit::sweeping_temp_dir("pack-et-prologue-torn");
        let ids: Vec<u32> = (0..1000u32).map(|i| (i % 9) + 4).collect();
        let mut body = encode_event_types(&ids);
        let layout = EtLayout::parse(&body).unwrap();
        body[layout.dict_start + 1] ^= 0xFF; // a dictionary byte
        let (file, mut sec) = section_file(dir.path(), "torn.bin", 0, &body);
        sec.crc = crc32c::crc32c(&body);
        sec.hash_prefix = hash_prefix(&body);
        assert_eq!(
            EventTypeColumn::attach(&file, &sec).unwrap_err(),
            PackError::Corrupt("event-type prologue CRC")
        );
    }

    /// A header that disagrees with the directory's section length is refused
    /// at attach: the header is what every subsequent `pread` is bounded by, so
    /// it must be pinned to something the trailer hash already covers.
    #[test]
    #[cfg_attr(miri, ignore)]
    fn event_type_header_must_agree_with_the_section_length() {
        let dir = mess_testkit::sweeping_temp_dir("pack-et-badlen");
        let ids: Vec<u32> = (0..100u32).map(|i| i % 3).collect();
        let body = encode_event_types(&ids);
        let (file, mut sec) = section_file(dir.path(), "b.bin", 0, &body);
        sec.length -= 1;
        assert_eq!(
            EventTypeColumn::attach(&file, &sec).unwrap_err(),
            PackError::Corrupt("event-type body/section mismatch")
        );
    }
}

// -------------------------------------------------------------------------
// bn-11g — the pack identity (spec 01 §3.3.3)
// -------------------------------------------------------------------------

#[cfg(test)]
mod identity_tests {
    use super::*;
    use crate::sealed::segment::{SealBatch, SealStream};

    fn streams(
        stream_id: u64,
        frame_count: u32,
        offset: u64,
    ) -> Vec<SealStream> {
        vec![SealStream {
            stream_id,
            batches: vec![SealBatch {
                first_version: 0,
                frame_count,
                first_global_pos: 0,
                offset,
            }],
        }]
    }

    fn build(
        segment_id: u64,
        base_pos: u64,
        s: &[SealStream],
        type_ids: &[u32],
    ) -> Vec<u8> {
        encode_pack(&PackInput {
            segment_id,
            base_pos,
            streams: s,
            event_type_ids: type_ids,
            filter: None,
            payload_bytes: None,
        })
    }

    fn identity_of(bytes: &[u8]) -> PackIdentity {
        parse_pack(bytes).expect("valid pack").identity
    }

    /// The identity a reader derives is exactly the pack's own trailer hash —
    /// the value `open_pack` already recomputes. Naming it in the footer
    /// therefore costs a reader nothing beyond the comparison.
    #[test]
    fn identity_is_the_trailer_hash_over_header_and_directory() {
        let s = streams(10, 4, 4096);
        let bytes = build(7, 1000, &s, &[]);
        let id = identity_of(&bytes);

        let trailer = &bytes[bytes.len() - TRAILER_LEN..];
        assert_eq!(id.as_bytes(), &trailer[..32], "the stored trailer hash");

        let h = parse_pack_header(&bytes).unwrap();
        let recomputed = blake3::hash(&bytes[..h.sections_off]);
        assert_eq!(
            id.as_bytes(),
            recomputed.as_bytes(),
            "blake3(header ++ directory), nothing else"
        );
        assert_eq!(id.hex().len(), 64);
    }

    /// Deterministic: the same input yields the same identity, so a re-seal of
    /// identical content is not spuriously refuted.
    #[test]
    fn identical_input_yields_an_identical_identity() {
        let s = streams(10, 4, 4096);
        assert_eq!(
            identity_of(&build(7, 1000, &s, &[])),
            identity_of(&build(7, 1000, &s, &[]))
        );
    }

    /// The three substitutions coverage cannot see. Each of these packs
    /// matches — or can be made to match — a segment's `segment_id`/`base_pos`
    /// coverage, and each has a different identity.
    #[test]
    fn different_content_yields_a_different_identity() {
        let base = build(7, 1000, &streams(10, 4, 4096), &[]);
        let base_id = identity_of(&base);

        // A "stale replacement": same segment, same coverage, different
        // pointer content (an earlier seal of the same range at other offsets).
        let stale = build(7, 1000, &streams(10, 4, 8192), &[]);
        assert_eq!(
            parse_pack(&stale).unwrap().segment_id,
            parse_pack(&base).unwrap().segment_id,
            "coverage is identical..."
        );
        assert_eq!(
            parse_pack(&stale).unwrap().event_count,
            parse_pack(&base).unwrap().event_count
        );
        assert_ne!(identity_of(&stale), base_id, "...the identity is not");

        // A "copied wrong pack": a different segment's pack.
        assert_ne!(
            identity_of(&build(8, 1000, &streams(10, 4, 4096), &[])),
            base_id
        );
        // An extra optional section changes the directory, hence the identity.
        assert_ne!(
            identity_of(&build(7, 1000, &streams(10, 4, 4096), &[1, 1, 2, 3])),
            base_id
        );
    }

    /// A flipped bit anywhere in the header or directory fails the trailer hash
    /// outright — there is no "wrong but parseable" identity to compare.
    #[test]
    fn a_flipped_header_or_directory_bit_fails_the_hash_not_the_compare() {
        let s = streams(10, 4, 4096);
        for at in [8usize, 24, HEADER_LEN + 8, HEADER_LEN + 32] {
            let mut bytes = build(7, 1000, &s, &[]);
            bytes[at] ^= 0x01;
            assert!(
                matches!(
                    parse_pack(&bytes).err(),
                    Some(PackError::Corrupt(
                        "pack header/directory hash mismatch"
                    ))
                ),
                "byte {at} must fail the trailer hash"
            );
        }
        // And flipping the STORED identity itself is caught the same way: the
        // recomputed hash no longer matches, so no pack is produced at all.
        let mut bytes = build(7, 1000, &s, &[]);
        let at = bytes.len() - TRAILER_LEN;
        bytes[at] ^= 0x80;
        assert!(parse_pack(&bytes).is_err());
    }
}
