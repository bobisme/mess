//! On-disk constants and field offsets for the v3 log format.
//!
//! Every value here is transcribed **from the byte tables of
//! [`docs/spec/01-log-format.md`]**, which is normative. Where the spike
//! dialects (`spikes/perf_group_commit`, format v2) differ, the spec wins —
//! most consequentially D-FMT-8 (the split-coverage `batch_crc`, see
//! [`crate::crc`]) and the mandatory A9 `segment_epoch` in every batch header,
//! neither of which any spike header carried.
//!
//! [`docs/spec/01-log-format.md`]: ../../../../docs/spec/01-log-format.md

// ---------------------------------------------------------------------------
// Magic numbers (§4.1, §3.2, §3.3.1)
// ---------------------------------------------------------------------------

/// `SegmentHeader.magic` (§3.2).
pub const SEGMENT_MAGIC: u32 = 0x5E60_1EAD;
/// `BatchHeader.magic` (§4.1).
pub const HEADER_MAGIC: u32 = 0xBA7C_4EAD;
/// `CommitMarker.magic` (§4.1).
pub const MARKER_MAGIC: u32 = 0xC0AA_17ED;
/// `SegmentFooter` trailer `magic` (§3.3.1). Unused until Phase 4 sealing,
/// interned here so the family of magics lives in one place.
pub const FOOTER_MAGIC: u32 = 0x5EA1_F007;

/// The normative on-disk format version (§2, D-FMT-1). Distinct from the
/// spikes' v1 (`1`) and v2 (`2`).
pub const FORMAT_VERSION: u16 = 3;

// ---------------------------------------------------------------------------
// SegmentHeader (§3.2) — fixed 52 bytes at offset 0
// ---------------------------------------------------------------------------

/// `SEGMENT_HEADER_LEN` (§3.2). The first `BatchHeader` begins at offset 52.
pub const SEGMENT_HEADER_LEN: usize = 52;

/// Byte offset of `SegmentHeader.header_crc`; its coverage is `[0, 48)`.
pub const SEGMENT_HEADER_CRC_OFF: usize = 48;

// SegmentHeader field offsets (§3.2 table).
pub(crate) const SH_MAGIC_OFF: usize = 0;
pub(crate) const SH_FORMAT_VERSION_OFF: usize = 4;
pub(crate) const SH_FLAGS_OFF: usize = 6;
pub(crate) const SH_SEGMENT_ID_OFF: usize = 8;
pub(crate) const SH_BASE_POS_OFF: usize = 16;
pub(crate) const SH_EPOCH_OFF: usize = 24;
pub(crate) const SH_CREATED_UNIX_NANOS_OFF: usize = 32;
pub(crate) const SH_PREV_SEGMENT_EPOCH_OFF: usize = 40;

// ---------------------------------------------------------------------------
// BatchHeader (§4.2) — fixed 72 bytes
// ---------------------------------------------------------------------------

/// `HEADER_LEN` (§4.1): fixed BatchHeader length.
pub const HEADER_LEN: usize = 72;

/// `HEADER_CRC_OFF` (§4.2): offset of `batch_crc`; its 4 bytes `[68, 72)` are
/// the first field the split CRC coverage skips over (R4, §5.2).
pub const HEADER_CRC_OFF: usize = 68;

// BatchHeader field offsets (§4.2 table).
pub(crate) const BH_MAGIC_OFF: usize = 0;
pub(crate) const BH_FORMAT_VERSION_OFF: usize = 4;
pub(crate) const BH_FLAGS_OFF: usize = 6;
pub(crate) const BH_FRAME_COUNT_OFF: usize = 8;
pub(crate) const BH_BATCH_ID_OFF: usize = 12;
pub(crate) const BH_TOTAL_LEN_OFF: usize = 20;
pub(crate) const BH_FIRST_GLOBAL_POS_OFF: usize = 28;
pub(crate) const BH_SEGMENT_EPOCH_OFF: usize = 36;
pub(crate) const BH_STREAM_ID_OFF: usize = 44;
pub(crate) const BH_CATEGORY_ID_OFF: usize = 52;
pub(crate) const BH_FIRST_STREAM_VERSION_OFF: usize = 60;

// ---------------------------------------------------------------------------
// flags bitfield (§4.2.1)
// ---------------------------------------------------------------------------

/// `flags` bit 0: a 32-byte `crypto_chain` (§4.4) follows the header.
pub const FLAG_CRYPTO_CHAIN: u16 = 1 << 0;
/// The mask of flag bits defined in v3. Any bit outside this mask set on read
/// MUST be rejected (§4.2.1); an encoder MUST never set one.
pub const FLAGS_KNOWN_MASK: u16 = FLAG_CRYPTO_CHAIN;

// ---------------------------------------------------------------------------
// crypto_chain (§4.4)
// ---------------------------------------------------------------------------

/// `CHAIN_LEN` (§4.1): crypto-chain value length when present, at offset
/// `HEADER_LEN`.
pub const CHAIN_LEN: usize = 32;

// ---------------------------------------------------------------------------
// EventSubframe (§4.3) — fixed 28-byte header + `compressed_len` payload
// ---------------------------------------------------------------------------

/// `SUBFRAME_HDR_LEN` (§4.1): fixed EventSubframe header length.
pub const SUBFRAME_HDR_LEN: usize = 28;

// EventSubframe header field offsets (§4.3 table).
pub(crate) const SF_EVENT_TYPE_ID_OFF: usize = 0;
pub(crate) const SF_SCHEMA_VERSION_OFF: usize = 4;
pub(crate) const SF_CODEC_ID_OFF: usize = 6;
pub(crate) const SF_COMPRESSION_ID_OFF: usize = 8;
pub(crate) const SF_SUBFRAME_FLAGS_OFF: usize = 9;
pub(crate) const SF_DICT_ID_OFF: usize = 10;
pub(crate) const SF_UNCOMPRESSED_LEN_OFF: usize = 12;
pub(crate) const SF_COMPRESSED_LEN_OFF: usize = 16;
pub(crate) const SF_METADATA_LEN_OFF: usize = 20;
pub(crate) const SF_DATA_LEN_OFF: usize = 24;

// ---------------------------------------------------------------------------
// CommitMarker (§4.5) — fixed 16 bytes
// ---------------------------------------------------------------------------

/// `MARKER_LEN` (§4.1): CommitMarker length.
pub const MARKER_LEN: usize = 16;

// CommitMarker field offsets, relative to the marker's first byte (§4.5).
pub(crate) const CM_MAGIC_OFF: usize = 0;
pub(crate) const CM_TOTAL_LEN_ECHO_OFF: usize = 4;
pub(crate) const CM_BATCH_CRC_ECHO_OFF: usize = 12;

// ---------------------------------------------------------------------------
// Size bounds (§4.1)
// ---------------------------------------------------------------------------

/// `MIN_BATCH_LEN` = `HEADER_LEN + MARKER_LEN` = 88 (§4.1). The A2 lower bound;
/// never actually valid (A5 forbids zero frames).
pub const MIN_BATCH_LEN: u64 = (HEADER_LEN + MARKER_LEN) as u64;

/// `MAX_BATCH_LEN` = 64 MiB (§4.1, D-FMT-4): the A2 sanity cap, ¼ of a segment.
pub const MAX_BATCH_LEN: u64 = 64 * 1024 * 1024;

/// `SEGMENT_SIZE` = 256 MiB (§3.1). A batch MUST NOT be placed such that it
/// would extend beyond this (A8). Configurable per-writer for tests.
pub const SEGMENT_SIZE: u64 = 256 * 1024 * 1024;

// ---------------------------------------------------------------------------
// SegmentFooter trailer (§3.3.1) — fixed 100 bytes at end-of-file (bn-sbt)
// ---------------------------------------------------------------------------
//
// The trailer is the fixed part of the seal: it occupies the final
// `SEGMENT_TRAILER_LEN` bytes of a sealed segment so a fast-path reader
// `pread`s exactly these bytes from EOF without first knowing the file length
// (R2, §02 §8.3). Its `footer_crc` covers only `[0, 96)`; the variable-length
// extension region that precedes it (§3.3.2) is covered separately by
// `ext_crc` so the fast path can validate and use the catalog fields without
// reading the extension. In Phase 3 the extension is always empty
// (`ext_len == 0`, `ext_crc == 0`); the `StreamHeadTable` is Phase 5.

/// `SEGMENT_TRAILER_LEN` (§3.3.1): the fixed trailer occupies the final 100
/// bytes of a sealed segment file (R2 pread-from-EOF).
pub const SEGMENT_TRAILER_LEN: usize = 100;

/// Byte offset of the trailer's `footer_crc`; its coverage is the single range
/// `[0, 96)` (nothing follows it — §5.2).
pub const SEGMENT_FOOTER_CRC_OFF: usize = 96;

// SegmentFooter trailer field offsets, relative to the trailer's first byte
// (§3.3.1 table).
pub(crate) const FT_MAGIC_OFF: usize = 0;
pub(crate) const FT_FORMAT_VERSION_OFF: usize = 4;
pub(crate) const FT_FLAGS_OFF: usize = 6;
pub(crate) const FT_SEGMENT_ID_OFF: usize = 8;
pub(crate) const FT_EPOCH_OFF: usize = 16;
pub(crate) const FT_BASE_POS_OFF: usize = 24;
pub(crate) const FT_BATCH_COUNT_OFF: usize = 32;
pub(crate) const FT_EVENT_COUNT_OFF: usize = 40;
pub(crate) const FT_END_POS_OFF: usize = 48;
pub(crate) const FT_SEALED_LEN_OFF: usize = 56;
pub(crate) const FT_EXT_OFFSET_OFF: usize = 64;
pub(crate) const FT_EXT_LEN_OFF: usize = 72;
pub(crate) const FT_EXT_CRC_OFF: usize = 80;
pub(crate) const FT_REPAIR_SIDECAR_KIND_OFF: usize = 84;
pub(crate) const FT_RESERVED_OFF: usize = 86;
pub(crate) const FT_REPAIR_SIDECAR_REF_OFF: usize = 88;

// ---------------------------------------------------------------------------
// SegmentFooter extension region — typed sections (§3.3.2) (bn-sbt)
// ---------------------------------------------------------------------------
//
// The extension region is a sequence of self-delimiting typed sections. In
// Phase 3 the writer emits none (`ext_len == 0`), but the section-header layout
// and the known section kinds are interned here so the format is complete and
// forward-compatible: a reader walks sections by hopping `EXT_SECTION_HDR_LEN +
// payload_len` and MUST advisory-skip any unknown `kind` (§3.3.2).

/// `EXT_SECTION_HDR_LEN` (§3.3.2): the fixed section-header length that
/// precedes each extension section's payload.
pub const EXT_SECTION_HDR_LEN: usize = 16;

// Extension section-header field offsets, relative to the section's first byte
// (§3.3.2 table). `pub` because this crate owns the extension byte layout
// (§3.3.2) and Phase 5 (`StreamHeadTable`, bn-fold) writes/reads sections
// through these; nothing in Phase 3 emits a section (`ext_len == 0`).

/// Offset of a section header's `kind` (§3.3.2). `0` is reserved.
pub const EXT_KIND_OFF: usize = 0;
/// Offset of a section header's `section_flags` (§3.3.2; MUST be `0` in v3).
pub const EXT_SECTION_FLAGS_OFF: usize = 2;
/// Offset of a section header's `entry_count` (§3.3.2).
pub const EXT_ENTRY_COUNT_OFF: usize = 4;
/// Offset of a section header's `payload_len` (§3.3.2). The next section begins
/// `payload_len` bytes past the end of this header.
pub const EXT_PAYLOAD_LEN_OFF: usize = 8;

/// Extension section kind `1` — `StreamHeadTable` (§3.3.2). Not written in
/// Phase 3 (Phase 5). Entry size is 48 bytes.
pub const EXT_KIND_STREAM_HEAD_TABLE: u16 = 1;
/// Extension section kind `2` — `SnapshotAnchorList` (§3.3.2). Not written in
/// Phase 3 (Phase 5). Entry size is 48 bytes.
pub const EXT_KIND_SNAPSHOT_ANCHOR_LIST: u16 = 2;
/// Entry size (bytes) of both known extension section kinds (§3.3.2).
pub const EXT_ENTRY_LEN: usize = 48;
