//! On-disk constants and field offsets for the **v4 commit-capsule** format
//! (Spike E, bn-9mw) — atomic control+event commit capsules.
//!
//! Every value here is transcribed from the byte-level sketch in
//! [`notes/mess-asterism/research/09-wire-format-v4-sketch.md`], with the §23
//! open decisions **resolved** as documented in
//! [`spikes/capsule_v4_prelude/REPORT.md`]. The resolutions baked into these
//! constants:
//!
//! - **§23.1 header size = 96 bytes** (every `u64` field 8-byte aligned).
//! - **§23.2 `header_crc` dropped**: the four bytes at offset 84 are a
//!   MUST-BE-ZERO reserved word ([`CH_RESERVED_HDR_OFF`]). The full-capsule CRC
//!   is authoritative; the A2-style caps ([`MAX_CAPSULE_LEN`],
//!   [`MAX_CONTROL_LEN`], [`MAX_CONTROL_COUNT`]) already bound every allocation
//!   before the full CRC runs, exactly as v3 does with no header CRC.
//! - **§23.3 marker = 32 bytes** with explicit `batch_id_echo` +
//!   `total_len_echo` + `capsule_crc_echo` — the full 64-bit length echo binds
//!   zero-event ordering under sector reorder.
//!
//! v4 reuses the v3 52-byte `SegmentHeader` family unchanged except for
//! `format_version = 4` ([`FORMAT_VERSION_V4`]); v3 and v4 capsules never
//! coexist in one segment (§2), and the v4 event subframe is the v3 28-byte
//! subframe verbatim (§15), so [`crate::format`]'s `SF_*` / `SUBFRAME_HDR_LEN`
//! are reused directly.

// ---------------------------------------------------------------------------
// Segment version (§2)
// ---------------------------------------------------------------------------

/// A v4 segment stamps `SegmentHeader.format_version = 4`. The segment-header
/// byte layout is otherwise the v3 family
/// ([`crate::format::SEGMENT_HEADER_LEN`] etc.); recovery dispatches on this
/// field.
pub const FORMAT_VERSION_V4: u16 = 4;

// ---------------------------------------------------------------------------
// Magic numbers (§4, §17)
// ---------------------------------------------------------------------------

/// `CapsuleHeader.magic` (§4).
pub const CAPSULE_MAGIC: u32 = 0xCA95_4EAD;
/// `CommitMarker.magic` (§17).
pub const CAPSULE_MARKER_MAGIC: u32 = 0xCA95_17ED;

// ---------------------------------------------------------------------------
// CapsuleHeader (§4) — fixed 96 bytes
// ---------------------------------------------------------------------------

/// Fixed `CapsuleHeader` length (§23.1 resolved to 96).
pub const CAPSULE_HEADER_LEN: usize = 96;

pub(crate) const CH_MAGIC_OFF: usize = 0;
pub(crate) const CH_FORMAT_VERSION_OFF: usize = 4;
pub(crate) const CH_FLAGS_OFF: usize = 6;
pub(crate) const CH_EVENT_COUNT_OFF: usize = 8;
pub(crate) const CH_CONTROL_COUNT_OFF: usize = 12;
pub(crate) const CH_BATCH_ID_OFF: usize = 16;
pub(crate) const CH_TOTAL_LEN_OFF: usize = 24;
pub(crate) const CH_FIRST_GLOBAL_POS_OFF: usize = 32;
pub(crate) const CH_SEGMENT_EPOCH_OFF: usize = 40;
pub(crate) const CH_STREAM_ID_OFF: usize = 48;
pub(crate) const CH_CATEGORY_ID_OFF: usize = 56;
pub(crate) const CH_FIRST_STREAM_VERSION_OFF: usize = 64;
pub(crate) const CH_CONTROL_LEN_OFF: usize = 72;
pub(crate) const CH_EVENT_REGION_LEN_OFF: usize = 76;
/// `capsule_crc` (§4): split-coverage CRC32C, excluded from its own coverage.
pub const CAPSULE_HEADER_CRC_OFF: usize = 80;
/// §23.2: the dropped `header_crc` slot, now a MUST-BE-ZERO reserved word.
pub(crate) const CH_RESERVED_HDR_OFF: usize = 84;
pub(crate) const CH_LOGICAL_FLAGS_OFF: usize = 88;
pub(crate) const CH_RESERVED_OFF: usize = 92;

// ---------------------------------------------------------------------------
// flags bitfield (§5) — physical layout
// ---------------------------------------------------------------------------

/// `flags` bit 0: a 32-byte crypto chain entry follows the header (§16).
pub const FLAG_CRYPTO_CHAIN: u16 = 1 << 0;
/// The mask of physical `flags` bits defined in v4. Any bit outside this set
/// rejects the capsule (§5, requirement 9). `CONTROL_COMPRESSED` (bit 1) is
/// deliberately NOT defined: §23 resolves controls to uncompressed-only.
pub const FLAGS_KNOWN_MASK: u16 = FLAG_CRYPTO_CHAIN;

// ---------------------------------------------------------------------------
// logical_flags bitfield (§5) — engine semantics, redundant cross-checks
// ---------------------------------------------------------------------------

/// `logical_flags` bit 0: `event_count == 0` (a control-only capsule). MUST
/// agree with the parsed `event_count`.
pub const LFLAG_CONTROL_ONLY: u32 = 1 << 0;
/// `logical_flags` bit 1: exactly one dedupe control is present. Redundant
/// hint; MUST agree with the parsed controls.
pub const LFLAG_HAS_DEDUPE: u32 = 1 << 1;
/// `logical_flags` bit 2: control records introduce IDs used by this capsule.
/// Advisory; never a substitute for the prelude-first resolution check.
pub const LFLAG_REGISTRY_INTRODUCES_IDS: u32 = 1 << 2;
/// Known `logical_flags` mask; any bit outside it rejects the capsule (§5).
pub const LOGICAL_FLAGS_KNOWN_MASK: u32 =
    LFLAG_CONTROL_ONLY | LFLAG_HAS_DEDUPE | LFLAG_REGISTRY_INTRODUCES_IDS;

// ---------------------------------------------------------------------------
// crypto_chain (§16)
// ---------------------------------------------------------------------------

/// Crypto-chain entry length when present, at offset [`CAPSULE_HEADER_LEN`].
/// Same 32-byte width as v3 ([`crate::format::CHAIN_LEN`]).
pub const CAPSULE_CHAIN_LEN: usize = 32;

// ---------------------------------------------------------------------------
// ControlRecord TLV framing (§7)
// ---------------------------------------------------------------------------

/// Fixed control-record TLV header: `kind u16 | version u16 | payload_len u32`.
/// Each record occupies `CONTROL_TLV_HDR_LEN + payload_len` bytes; the
/// `control_count` records tile `control_len` exactly (§7).
pub const CONTROL_TLV_HDR_LEN: usize = 8;

pub(crate) const CT_KIND_OFF: usize = 0;
pub(crate) const CT_VERSION_OFF: usize = 2;
pub(crate) const CT_PAYLOAD_LEN_OFF: usize = 4;

/// The one control version this spike encodes/decodes. §23.6: all v4 controls
/// are critical; an unknown `(kind, version)` rejects the capsule (recovery
/// stops), never skips.
pub const CONTROL_VERSION_V1: u16 = 1;

// Control kinds (§9). The spike implements the registry, dedupe, checkpoint,
// and snapshot kinds; the migration kinds (0x0040+) are reserved.
/// `StreamRegisteredV1` (§10).
pub const CTL_STREAM_REGISTERED: u16 = 0x0001;
/// `EventTypeRegisteredV1` (§11).
pub const CTL_EVENT_TYPE_REGISTERED: u16 = 0x0002;
/// `CategoryRegisteredV1` (§9).
pub const CTL_CATEGORY_REGISTERED: u16 = 0x0003;
/// `DedupeKeyV1` (§12).
pub const CTL_DEDUPE_KEY: u16 = 0x0010;
/// `SnapshotInstalledV1` (§13), hash-presence flagged per review V4.
pub const CTL_SNAPSHOT_INSTALLED: u16 = 0x0020;
/// `ProjectionCheckpointV1` (§14).
pub const CTL_PROJECTION_CHECKPOINT: u16 = 0x0030;

// DedupeKeyV1 scope kinds (§12).
/// Dedupe scope: the key is unique within one stream.
pub const DEDUPE_SCOPE_STREAM: u8 = 1;
/// Dedupe scope: the key is unique globally.
pub const DEDUPE_SCOPE_GLOBAL: u8 = 2;

// SnapshotInstalledV1 hash-presence bits (§13, review V4).
pub const SNAP_HAS_STATE_HASH: u8 = 1 << 0;
pub const SNAP_HAS_EVENT_PREFIX_HASH: u8 = 1 << 1;
pub const SNAP_HAS_BLOB_HASH: u8 = 1 << 2;
pub const SNAP_HASH_PRESENCE_KNOWN_MASK: u8 =
    SNAP_HAS_STATE_HASH | SNAP_HAS_EVENT_PREFIX_HASH | SNAP_HAS_BLOB_HASH;

// ---------------------------------------------------------------------------
// CommitMarker (§17) — fixed 32 bytes (§23.3 resolved)
// ---------------------------------------------------------------------------

/// Fixed `CommitMarker` length (§23.3 resolved to 32).
pub const CAPSULE_MARKER_LEN: usize = 32;

pub(crate) const CM_MAGIC_OFF: usize = 0;
// marker_flags at offset 4 and marker_reserved at offset 28 are written as
// zero words directly by the encoder; their offsets are documented in the
// layout table above (§17) and need no named constant here.
pub(crate) const CM_BATCH_ID_ECHO_OFF: usize = 8;
pub(crate) const CM_TOTAL_LEN_ECHO_OFF: usize = 16;
pub(crate) const CM_CAPSULE_CRC_ECHO_OFF: usize = 24;

// ---------------------------------------------------------------------------
// Size bounds and caps (§6, §19)
// ---------------------------------------------------------------------------

/// The smallest possible capsule: header + marker, i.e. a control-only capsule
/// with exactly one zero-payload control would still be larger. Never itself a
/// valid capsule (§6: `control_count + event_count >= 1`), but the A2 lower
/// bound a `total_len` must clear.
pub const MIN_CAPSULE_LEN: u64 =
    (CAPSULE_HEADER_LEN + CAPSULE_MARKER_LEN) as u64;

/// `MAX_CAPSULE_LEN` = 64 MiB (§19, retained from v3's `MAX_BATCH_LEN`): the A2
/// sanity cap validated before any allocation.
pub const MAX_CAPSULE_LEN: u64 = 64 * 1024 * 1024;

/// `MAX_CONTROL_LEN` = 1 MiB (§19): the cap on the whole control TLV region.
pub const MAX_CONTROL_LEN: u32 = 1024 * 1024;

/// `MAX_CONTROL_COUNT` = 4096 (§19).
pub const MAX_CONTROL_COUNT: u32 = 4096;

/// `MAX_NAME_LEN` = 65_535 bytes (§19): a name is a `Bytes16` (u16 length).
pub const MAX_NAME_LEN: usize = u16::MAX as usize;

/// `MAX_DEDUPE_KEY_LEN` = 1 MiB (§19, §23.10). A dedupe key is a `Bytes32`.
pub const MAX_DEDUPE_KEY_LEN: usize = 1024 * 1024;

/// `MAX_STATE_REF_LEN` = 1 MiB: a `ProjectionCheckpointV1` state ref
/// (`Bytes32`) cap, mirroring the dedupe-key cap (§23.10).
pub const MAX_STATE_REF_LEN: usize = 1024 * 1024;
