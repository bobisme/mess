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
