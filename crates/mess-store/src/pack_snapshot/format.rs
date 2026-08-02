//! On-disk byte layouts for the pack snapshot sidecar (ADR 0002 §1).
//!
//! Every artifact here is **self-describing** — magic, format version, store
//! UUID where identity matters, an explicit length, a CRC32C, and (for the
//! things a torn write can bisect) a trailing commit marker. Nothing in this
//! module performs I/O; it is pure encode/decode so the framing can be
//! property-tested without a filesystem.
//!
//! # Why CRC32C and why no new dependency
//!
//! `mess-log` already owns the project's CRC32C primitive
//! ([`mess_log::crc::crc32c_two`], the hardware-accelerated Castagnoli
//! polynomial the log format mandates). `mess-store` already depends on
//! `mess-log`, so the sidecar reuses it and adds **zero** new crates: see
//! [`crc32`].
//!
//! # The reserved temporary suffix
//!
//! Staging files are written as `<final-name>.tmp` and renamed into place.
//! Discovery parses candidate names *structurally* ([`parse_root_name`],
//! [`parse_pack_name`]) and only ever accepts the exact final suffixes, so a
//! `.tmp` file is never a discovery candidate — not by convention, but because
//! its name cannot parse.
//!
//! # Format versions and what a bump means
//!
//! [`RECORD_FORMAT`] and [`ROOT_FORMAT`] are `2`: `bn-2gns` widened both to
//! carry the stable [`SnapshotCompatibility`] identity. A decoder that meets a
//! format it does not understand returns `None`, which every caller turns into
//! a **miss** — so every sidecar written by an earlier build is simply not
//! found, and the aggregate is rebuilt by replay. That is the whole migration
//! story, and it is the law working, not a gap in it: snapshots are discardable
//! acceleration.

use std::fmt;

use crate::snapshot::{
    SnapshotCompatibility, SnapshotCoverage, SnapshotTrust, StableSnapshotId,
};

/// Reserved suffix for every staging file. Never a discovery candidate.
pub const TMP_SUFFIX: &str = ".tmp";

/// Suffix of a sealed, immutable pack.
pub const SEALED_SUFFIX: &str = ".pack";
/// Suffix of the active, footerless build pack.
pub const OPEN_SUFFIX: &str = ".open";
/// Suffix of a final root descriptor.
pub const ROOT_SUFFIX: &str = ".root";

/// Name of the store-identity file (created once, under the writer lock).
pub const IDENTITY_FILE: &str = "IDENTITY";

/// Commit marker closing a frame that a torn write must not be able to fake.
/// (`b"MCMT"` little-endian.)
pub const COMMIT_MARKER: u32 = u32::from_le_bytes(*b"MCMT");

/// CRC32C (Castagnoli) of `bytes`.
///
/// Delegates to `mess-log`'s CRC primitive so the sidecar introduces no new
/// dependency and uses the same hardware-accelerated polynomial as the log.
#[inline]
#[must_use]
pub fn crc32(bytes: &[u8]) -> u32 { mess_log::crc::crc32c_two(bytes, &[]) }

/// A 128-bit random store identity.
///
/// Deliberately **not** an RFC-4122 UUID: the sidecar needs an unforgeable
/// namespace token, not a versioned UUID, and minting one from OS entropy
/// costs no dependency. It is rendered as 32 lowercase hex digits in every
/// filename so an operator can eyeball which artifacts belong together.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct StoreUuid(pub [u8; 16]);

impl StoreUuid {
    /// Render as 32 lowercase hex digits.
    #[must_use]
    pub fn to_hex(self) -> String {
        let mut s = String::with_capacity(32);
        for b in self.0 {
            use std::fmt::Write as _;
            let _ = write!(s, "{b:02x}");
        }
        s
    }

    /// Parse 32 lowercase hex digits.
    #[must_use]
    pub fn from_hex(s: &str) -> Option<Self> {
        if s.len() != 32 {
            return None;
        }
        let mut out = [0u8; 16];
        for (i, byte) in out.iter_mut().enumerate() {
            *byte = u8::from_str_radix(s.get(i * 2..i * 2 + 2)?, 16).ok()?;
        }
        Some(StoreUuid(out))
    }
}

impl fmt::Debug for StoreUuid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "StoreUuid({})", self.to_hex())
    }
}

impl fmt::Display for StoreUuid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_hex())
    }
}

// ---------------------------------------------------------------------------
// Coverage and identity on the wire
// ---------------------------------------------------------------------------

/// Wire tag for [`SnapshotCoverage::Empty`].
const COVERAGE_EMPTY: u8 = 0;
/// Wire tag for [`SnapshotCoverage::Through`].
const COVERAGE_THROUGH: u8 = 1;

/// Encode a coverage as `tag(1) || version(8)`.
///
/// The tag is what keeps `Empty` and `Through(0)` distinct on disk, exactly as
/// the lattice keeps them distinct in memory: the version byte for `Empty` is
/// written as zero and ignored on the way back in.
fn put_coverage(out: &mut Vec<u8>, coverage: SnapshotCoverage) {
    match coverage {
        SnapshotCoverage::Empty => {
            out.push(COVERAGE_EMPTY);
            out.extend_from_slice(&0u64.to_le_bytes());
        }
        SnapshotCoverage::Through(n) => {
            out.push(COVERAGE_THROUGH);
            out.extend_from_slice(&n.to_le_bytes());
        }
    }
}

/// Decode a coverage. An unknown tag is `None` — a miss, never a guess.
fn coverage_from_parts(tag: u8, version: u64) -> Option<SnapshotCoverage> {
    match tag {
        COVERAGE_EMPTY => Some(SnapshotCoverage::Empty),
        COVERAGE_THROUGH => Some(SnapshotCoverage::Through(version)),
        _ => None,
    }
}

/// Decode a stored trust-mode byte.
///
/// ADR 0002 §1: exactly one semantic hash, or an unknown mode, is invalid and
/// becomes a miss. This build writes and understands only
/// [`TRUST_UNVERIFIED_CACHE`], so every other byte is `None`.
#[must_use]
pub fn trust_from_u8(mode: u8) -> Option<SnapshotTrust> {
    (mode == TRUST_UNVERIFIED_CACHE).then_some(SnapshotTrust::UnverifiedCache)
}

/// Encode a stable id as `len(1) || bytes`. The length cap is
/// [`StableSnapshotId::MAX_LEN`], so one byte is always enough and a corrupt
/// length can never exceed the frame.
fn put_id(out: &mut Vec<u8>, id: StableSnapshotId) {
    let bytes = id.as_bytes();
    out.push(bytes.len() as u8);
    out.extend_from_slice(bytes);
}

/// Encoded size of `id`.
fn id_len(id: StableSnapshotId) -> usize { 1 + id.as_bytes().len() }

/// Encode a complete compatibility identity.
fn put_compat(out: &mut Vec<u8>, compat: &SnapshotCompatibility) {
    put_id(out, compat.aggregate_schema_id);
    out.extend_from_slice(&compat.fold_version.to_le_bytes());
    put_id(out, compat.codec_id);
    out.extend_from_slice(&compat.codec_version.to_le_bytes());
}

/// Encoded size of `compat`.
fn compat_len(compat: &SnapshotCompatibility) -> usize {
    id_len(compat.aggregate_schema_id) + 4 + id_len(compat.codec_id) + 4
}

// ---------------------------------------------------------------------------
// Identity file
// ---------------------------------------------------------------------------

const IDENTITY_MAGIC: [u8; 4] = *b"MSID";
const IDENTITY_FORMAT: u16 = 1;
/// magic(4) + format(2) + reserved(2) + uuid(16) + reserved_ids(8) + crc(4)
pub const IDENTITY_LEN: usize = 4 + 2 + 2 + 16 + 8 + 4;

/// Encode the identity file body.
///
/// `reserved_ids` is the durably reserved high-water of the shared
/// pack/root id counter: every id strictly below it may already have been
/// handed out, so a reopen must never issue one again. Reserving in batches
/// is what makes "monotone, never reused" survive even the deletion of every
/// artifact that would otherwise witness the counter (see
/// `sidecar::IdAllocator`).
#[must_use]
pub fn encode_identity(uuid: StoreUuid, reserved_ids: u64) -> Vec<u8> {
    let mut out = Vec::with_capacity(IDENTITY_LEN);
    out.extend_from_slice(&IDENTITY_MAGIC);
    out.extend_from_slice(&IDENTITY_FORMAT.to_le_bytes());
    out.extend_from_slice(&0u16.to_le_bytes());
    out.extend_from_slice(&uuid.0);
    out.extend_from_slice(&reserved_ids.to_le_bytes());
    let crc = crc32(&out);
    out.extend_from_slice(&crc.to_le_bytes());
    out
}

/// Decode the identity file into `(uuid, reserved_ids)`. `None` =
/// absent-shaped, corrupt, or a format this binary does not understand — all
/// of which mean "no usable identity".
#[must_use]
pub fn decode_identity(raw: &[u8]) -> Option<(StoreUuid, u64)> {
    if raw.len() != IDENTITY_LEN || raw[0..4] != IDENTITY_MAGIC {
        return None;
    }
    if u16::from_le_bytes(raw[4..6].try_into().ok()?) != IDENTITY_FORMAT {
        return None;
    }
    let want = u32::from_le_bytes(raw[IDENTITY_LEN - 4..].try_into().ok()?);
    if crc32(&raw[..IDENTITY_LEN - 4]) != want {
        return None;
    }
    let uuid = StoreUuid(raw[8..24].try_into().ok()?);
    let reserved_ids = u64::from_le_bytes(raw[24..32].try_into().ok()?);
    Some((uuid, reserved_ids))
}

// ---------------------------------------------------------------------------
// Pack header
// ---------------------------------------------------------------------------

const PACK_MAGIC: [u8; 4] = *b"MPK1";
const PACK_FORMAT: u16 = 1;
/// magic(4) + format(2) + reserved(2) + uuid(16) + sequence(8) + crc(4)
pub const PACK_HEADER_LEN: u64 = 4 + 2 + 2 + 16 + 8 + 4;

/// Encode the fixed header every pack (open or sealed) starts with.
#[must_use]
pub fn encode_pack_header(uuid: StoreUuid, sequence: u64) -> Vec<u8> {
    let mut out = Vec::with_capacity(PACK_HEADER_LEN as usize);
    out.extend_from_slice(&PACK_MAGIC);
    out.extend_from_slice(&PACK_FORMAT.to_le_bytes());
    out.extend_from_slice(&0u16.to_le_bytes());
    out.extend_from_slice(&uuid.0);
    out.extend_from_slice(&sequence.to_le_bytes());
    let crc = crc32(&out);
    out.extend_from_slice(&crc.to_le_bytes());
    out
}

/// Decode a pack header, returning `(uuid, sequence)`.
#[must_use]
pub fn decode_pack_header(raw: &[u8]) -> Option<(StoreUuid, u64)> {
    let n = PACK_HEADER_LEN as usize;
    if raw.len() < n || raw[0..4] != PACK_MAGIC {
        return None;
    }
    if u16::from_le_bytes(raw[4..6].try_into().ok()?) != PACK_FORMAT {
        return None;
    }
    let want = u32::from_le_bytes(raw[n - 4..n].try_into().ok()?);
    if crc32(&raw[..n - 4]) != want {
        return None;
    }
    let uuid = StoreUuid(raw[8..24].try_into().ok()?);
    let seq = u64::from_le_bytes(raw[24..32].try_into().ok()?);
    Some((uuid, seq))
}

// ---------------------------------------------------------------------------
// Record frames
// ---------------------------------------------------------------------------

const FRAME_MAGIC: [u8; 4] = *b"MRC1";
/// magic(4) + total_len(4) + body_len(4) + header_crc(4)
pub const FRAME_HEADER_LEN: usize = 16;
/// body_crc(4) + commit marker(4)
pub const FRAME_TRAILER_LEN: usize = 8;

/// Hard ceiling on one frame, so a garbage length can never make a reader
/// allocate wildly or walk off the end of a pack (ADR: "bounded records").
pub const MAX_FRAME_LEN: u32 = 64 * 1024 * 1024;

/// A validated frame read out of a pack.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame {
    /// The record body bytes.
    pub body:      Vec<u8>,
    /// CRC32C of the body — the record identity a discovery leaf binds.
    pub body_crc:  u32,
    /// Total on-disk length of the frame.
    pub total_len: u32,
}

/// Frame `body` into a complete, independently commit-framed record.
#[must_use]
pub fn encode_frame(body: &[u8]) -> Vec<u8> {
    let total = FRAME_HEADER_LEN + body.len() + FRAME_TRAILER_LEN;
    let mut out = Vec::with_capacity(total);
    out.extend_from_slice(&FRAME_MAGIC);
    out.extend_from_slice(&(total as u32).to_le_bytes());
    out.extend_from_slice(&(body.len() as u32).to_le_bytes());
    let header_crc = crc32(&out);
    out.extend_from_slice(&header_crc.to_le_bytes());
    out.extend_from_slice(body);
    out.extend_from_slice(&crc32(body).to_le_bytes());
    out.extend_from_slice(&COMMIT_MARKER.to_le_bytes());
    out
}

/// The declared total length of a frame starting at `raw[0]`, if the frame
/// *header* alone is well-formed and bounded. Used by the tail scanner to step
/// forward without trusting the body.
#[must_use]
pub fn peek_frame_len(raw: &[u8]) -> Option<u32> {
    if raw.len() < FRAME_HEADER_LEN || raw[0..4] != FRAME_MAGIC {
        return None;
    }
    let total = u32::from_le_bytes(raw[4..8].try_into().ok()?);
    let body_len = u32::from_le_bytes(raw[8..12].try_into().ok()?);
    let want = u32::from_le_bytes(raw[12..16].try_into().ok()?);
    if crc32(&raw[0..12]) != want {
        return None;
    }
    if total > MAX_FRAME_LEN {
        return None;
    }
    let expect = (FRAME_HEADER_LEN as u64)
        + u64::from(body_len)
        + (FRAME_TRAILER_LEN as u64);
    if u64::from(total) != expect {
        return None;
    }
    Some(total)
}

/// Fully validate and decode a frame occupying exactly `raw`.
///
/// Every failure mode — bad magic, bad header CRC, an out-of-bounds length, a
/// body CRC mismatch, a missing commit marker — returns `None`, which the
/// caller turns into "torn tail" (writer) or "miss, replay" (reader). A frame
/// is never partially trusted.
#[must_use]
pub fn decode_frame(raw: &[u8]) -> Option<Frame> {
    let total = peek_frame_len(raw)? as usize;
    if raw.len() < total {
        return None;
    }
    let body_len = total - FRAME_HEADER_LEN - FRAME_TRAILER_LEN;
    let body = &raw[FRAME_HEADER_LEN..FRAME_HEADER_LEN + body_len];
    let body_crc =
        u32::from_le_bytes(raw[total - 8..total - 4].try_into().ok()?);
    let commit = u32::from_le_bytes(raw[total - 4..total].try_into().ok()?);
    if commit != COMMIT_MARKER || crc32(body) != body_crc {
        return None;
    }
    Some(Frame { body: body.to_vec(), body_crc, total_len: total as u32 })
}

// ---------------------------------------------------------------------------
// Record body
// ---------------------------------------------------------------------------

/// Record body format. `2` since `bn-2gns` widened the body to carry the
/// stable [`SnapshotCompatibility`] identity; a `1` body no longer decodes and
/// is therefore a clean miss.
pub const RECORD_FORMAT: u16 = 2;

/// The only trust mode this build writes: physical integrity only, no semantic
/// fold proof. ADR 0002's `CertifiedSnapshotRef` needs both semantic hashes and
/// the snapshot-side fold-chain wiring; a record claiming certification without
/// them would be a lie, so none is written.
pub const TRUST_UNVERIFIED_CACHE: u8 = 0;

/// The decoded, self-describing snapshot record.
///
/// It carries the stream **name** and the complete
/// [`SnapshotCompatibility`], so the pair that forms the lookup key is
/// recoverable from the record alone — no side map, and a leaf that disagrees
/// with the record it names is detectable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    /// The canonical stream identity at the `Backend` seam.
    pub stream_name:   String,
    /// The complete compatibility identity; the other half of the key.
    pub compatibility: SnapshotCompatibility,
    /// What prefix of the stream this record summarizes.
    pub coverage:      SnapshotCoverage,
    /// Semantic trust mode ([`TRUST_UNVERIFIED_CACHE`] today).
    pub trust_mode:    u8,
    /// The encoded aggregate state.
    pub state:         Vec<u8>,
}

/// fmt(2) trust(1) cov_tag(1) cov_version(8) name_len(2) state_len(4)
const RECORD_FIXED: usize = 2 + 1 + 1 + 8 + 2 + 4;

/// Encode a record body.
#[must_use]
pub fn encode_record(rec: &Record) -> Vec<u8> {
    let mut out = Vec::with_capacity(
        RECORD_FIXED
            + compat_len(&rec.compatibility)
            + rec.stream_name.len()
            + rec.state.len(),
    );
    out.extend_from_slice(&RECORD_FORMAT.to_le_bytes());
    out.push(rec.trust_mode);
    put_coverage(&mut out, rec.coverage);
    out.extend_from_slice(&(rec.stream_name.len() as u16).to_le_bytes());
    out.extend_from_slice(&(rec.state.len() as u32).to_le_bytes());
    put_compat(&mut out, &rec.compatibility);
    out.extend_from_slice(rec.stream_name.as_bytes());
    out.extend_from_slice(&rec.state);
    out
}

/// Decode a record body. `None` = unknown format or malformed — a miss.
#[must_use]
pub fn decode_record(raw: &[u8]) -> Option<Record> {
    if raw.len() < RECORD_FIXED {
        return None;
    }
    if u16::from_le_bytes(raw[0..2].try_into().ok()?) != RECORD_FORMAT {
        return None;
    }
    let trust_mode = raw[2];
    let coverage = coverage_from_parts(
        raw[3],
        u64::from_le_bytes(raw[4..12].try_into().ok()?),
    )?;
    let name_len = u16::from_le_bytes(raw[12..14].try_into().ok()?) as usize;
    let state_len = u32::from_le_bytes(raw[14..18].try_into().ok()?) as usize;

    let mut c = Cur { b: raw, i: RECORD_FIXED };
    let compatibility = c.compat()?;
    let name = std::str::from_utf8(c.take(name_len)?).ok()?.to_owned();
    let state = c.take(state_len)?.to_vec();
    // Trailing garbage means the body is not exactly what we wrote.
    if c.i != raw.len() {
        return None;
    }
    Some(Record {
        stream_name: name,
        compatibility,
        coverage,
        trust_mode,
        state,
    })
}

// ---------------------------------------------------------------------------
// Sealed-pack index + footer
// ---------------------------------------------------------------------------

const FOOTER_MAGIC: [u8; 4] = *b"MPKF";
const FOOTER_FORMAT: u16 = 1;
/// magic(4) fmt(2) rsv(2) index_off(8) index_len(8) count(8) crc(4) commit(4)
pub const FOOTER_LEN: usize = 4 + 2 + 2 + 8 + 8 + 8 + 4 + 4;

/// One `(offset, frame_len)` pair in a sealed pack's index.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IndexEntry {
    /// Byte offset of the frame within the pack.
    pub offset:    u64,
    /// Total frame length.
    pub frame_len: u32,
}

/// Encode `index || footer`, the bytes a roll appends before sealing.
#[must_use]
pub fn encode_index_and_footer(
    index_offset: u64,
    entries: &[IndexEntry],
) -> Vec<u8> {
    let mut index = Vec::with_capacity(4 + entries.len() * 12 + 4);
    index.extend_from_slice(&(entries.len() as u32).to_le_bytes());
    for e in entries {
        index.extend_from_slice(&e.offset.to_le_bytes());
        index.extend_from_slice(&e.frame_len.to_le_bytes());
    }
    let icrc = crc32(&index);
    index.extend_from_slice(&icrc.to_le_bytes());

    let index_len = index.len() as u64;
    let mut out = index;
    out.extend_from_slice(&FOOTER_MAGIC);
    out.extend_from_slice(&FOOTER_FORMAT.to_le_bytes());
    out.extend_from_slice(&0u16.to_le_bytes());
    out.extend_from_slice(&index_offset.to_le_bytes());
    out.extend_from_slice(&index_len.to_le_bytes());
    out.extend_from_slice(&(entries.len() as u64).to_le_bytes());
    let footer_start = out.len() - (FOOTER_LEN - 8);
    let crc = crc32(&out[footer_start..]);
    out.extend_from_slice(&crc.to_le_bytes());
    out.extend_from_slice(&COMMIT_MARKER.to_le_bytes());
    out
}

/// A validated sealed-pack footer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Footer {
    /// Offset of the index blob.
    pub index_offset: u64,
    /// Length of the index blob.
    pub index_len:    u64,
    /// Number of records in the pack.
    pub record_count: u64,
}

/// Decode the trailing footer of a sealed pack (`raw` = its last
/// [`FOOTER_LEN`] bytes). `None` = the seal never completed or is corrupt.
#[must_use]
pub fn decode_footer(raw: &[u8]) -> Option<Footer> {
    if raw.len() != FOOTER_LEN || raw[0..4] != FOOTER_MAGIC {
        return None;
    }
    if u16::from_le_bytes(raw[4..6].try_into().ok()?) != FOOTER_FORMAT {
        return None;
    }
    let commit = u32::from_le_bytes(raw[FOOTER_LEN - 4..].try_into().ok()?);
    if commit != COMMIT_MARKER {
        return None;
    }
    let want = u32::from_le_bytes(
        raw[FOOTER_LEN - 8..FOOTER_LEN - 4].try_into().ok()?,
    );
    if crc32(&raw[..FOOTER_LEN - 8]) != want {
        return None;
    }
    Some(Footer {
        index_offset: u64::from_le_bytes(raw[8..16].try_into().ok()?),
        index_len:    u64::from_le_bytes(raw[16..24].try_into().ok()?),
        record_count: u64::from_le_bytes(raw[24..32].try_into().ok()?),
    })
}

// ---------------------------------------------------------------------------
// Root descriptor
// ---------------------------------------------------------------------------

const ROOT_MAGIC: [u8; 4] = *b"MRT1";
/// Root descriptor format. `2` since `bn-2gns` widened the leaf key to the
/// stable identity; a `1` descriptor no longer decodes, so an older sidecar's
/// roots are all rejected and the store simply has no snapshots.
pub const ROOT_FORMAT: u16 = 2;

/// Publication mode recorded in a root descriptor.
///
/// A `Buffered` descriptor can never be mistaken for an acknowledged
/// complete-closure `Durable` descriptor after restart, because the mode is a
/// CRC-covered field of the descriptor itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SaveMode {
    /// Discardable cache write: ordered and atomically visible, no barrier.
    #[default]
    Buffered,
    /// Acknowledged only after the covering closure and directory entries are
    /// synced.
    Durable,
}

impl SaveMode {
    fn as_u8(self) -> u8 {
        match self {
            SaveMode::Buffered => 0,
            SaveMode::Durable => 1,
        }
    }

    fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(SaveMode::Buffered),
            1 => Some(SaveMode::Durable),
            _ => None,
        }
    }
}

/// One head in a root descriptor: the exact `(PackId, offset, length, hash)`
/// leaf ADR 0002 requires, plus the complete key a lookup filters on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RootEntry {
    /// Stream name — the first half of the head key.
    pub stream_name:   String,
    /// The complete compatibility identity — the second half of the key.
    ///
    /// Stored once per *distinct* identity in the root's intern table and
    /// referenced by index, so a million heads of one aggregate cost one copy
    /// of the ids, not a million.
    pub compatibility: SnapshotCompatibility,
    /// What prefix of the stream this head summarizes.
    pub coverage:      SnapshotCoverage,
    /// Which pack the record lives in.
    pub pack_seq:      u64,
    /// Byte offset of the frame in that pack.
    pub offset:        u64,
    /// Total frame length.
    pub frame_len:     u32,
    /// CRC32C of the record body — binds the leaf to exact bytes.
    pub record_crc:    u32,
}

/// A decoded root descriptor — the complete, independently resolvable
/// discovery state of one generation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Root {
    /// Store identity this root belongs to.
    pub uuid:            StoreUuid,
    /// Monotone, never-reused generation number.
    pub generation:      u64,
    /// Publication mode this root was acknowledged under.
    pub mode:            SaveMode,
    /// Active build pack sequence, if any.
    pub active_pack:     Option<u64>,
    /// Validated byte length of the active pack at publication time.
    pub active_pack_len: u64,
    /// Sealed packs reachable from this root.
    pub sealed_packs:    Vec<u64>,
    /// Every head.
    pub entries:         Vec<RootEntry>,
}

const ROOT_FIXED: usize = 4 + 2 + 1 + 1 + 16 + 8 + 8 + 8 + 4 + 4 + 4;
const NO_ACTIVE_PACK: u64 = u64::MAX;

/// Encode a root descriptor.
#[must_use]
pub fn encode_root(root: &Root) -> Vec<u8> {
    // Intern the distinct identities. Deployments have a handful; heads have
    // millions. Sorted + deduped so the table is canonical for a given set.
    let mut compats: Vec<SnapshotCompatibility> =
        root.entries.iter().map(|e| e.compatibility).collect();
    compats.sort_unstable();
    compats.dedup();
    let index_of = |c: &SnapshotCompatibility| -> u32 {
        compats.partition_point(|x| x < c) as u32
    };

    let mut out = Vec::with_capacity(ROOT_FIXED + root.entries.len() * 48);
    out.extend_from_slice(&ROOT_MAGIC);
    out.extend_from_slice(&ROOT_FORMAT.to_le_bytes());
    out.push(root.mode.as_u8());
    out.push(0);
    out.extend_from_slice(&root.uuid.0);
    out.extend_from_slice(&root.generation.to_le_bytes());
    out.extend_from_slice(
        &root.active_pack.unwrap_or(NO_ACTIVE_PACK).to_le_bytes(),
    );
    out.extend_from_slice(&root.active_pack_len.to_le_bytes());
    out.extend_from_slice(&(root.sealed_packs.len() as u32).to_le_bytes());
    out.extend_from_slice(&(compats.len() as u32).to_le_bytes());
    out.extend_from_slice(&(root.entries.len() as u32).to_le_bytes());
    for seq in &root.sealed_packs {
        out.extend_from_slice(&seq.to_le_bytes());
    }
    for compat in &compats {
        put_compat(&mut out, compat);
    }
    for e in &root.entries {
        out.extend_from_slice(&e.pack_seq.to_le_bytes());
        out.extend_from_slice(&e.offset.to_le_bytes());
        out.extend_from_slice(&e.frame_len.to_le_bytes());
        out.extend_from_slice(&e.record_crc.to_le_bytes());
        out.extend_from_slice(&index_of(&e.compatibility).to_le_bytes());
        put_coverage(&mut out, e.coverage);
        out.extend_from_slice(&(e.stream_name.len() as u16).to_le_bytes());
        out.extend_from_slice(e.stream_name.as_bytes());
    }
    let crc = crc32(&out);
    out.extend_from_slice(&crc.to_le_bytes());
    out.extend_from_slice(&COMMIT_MARKER.to_le_bytes());
    out
}

/// A bounded cursor over a byte slice — every read is length-checked, so a
/// corrupt descriptor can only produce `None`, never a panic or a wild read.
struct Cur<'a> {
    b: &'a [u8],
    i: usize,
}

impl<'a> Cur<'a> {
    fn take(&mut self, n: usize) -> Option<&'a [u8]> {
        let s = self.b.get(self.i..self.i.checked_add(n)?)?;
        self.i += n;
        Some(s)
    }

    fn u8(&mut self) -> Option<u8> { Some(self.take(1)?[0]) }

    fn u16(&mut self) -> Option<u16> {
        Some(u16::from_le_bytes(self.take(2)?.try_into().ok()?))
    }

    fn u32(&mut self) -> Option<u32> {
        Some(u32::from_le_bytes(self.take(4)?.try_into().ok()?))
    }

    fn u64(&mut self) -> Option<u64> {
        Some(u64::from_le_bytes(self.take(8)?.try_into().ok()?))
    }

    /// A length-prefixed stable id. The length byte is validated against the
    /// cap *and* the alphabet, so a corrupt id is a miss rather than a value
    /// that no author could ever have written.
    fn id(&mut self) -> Option<StableSnapshotId> {
        let len = self.u8()? as usize;
        if len > StableSnapshotId::MAX_LEN {
            return None;
        }
        StableSnapshotId::parse(std::str::from_utf8(self.take(len)?).ok()?).ok()
    }

    fn compat(&mut self) -> Option<SnapshotCompatibility> {
        Some(SnapshotCompatibility {
            aggregate_schema_id: self.id()?,
            fold_version:        self.u32()?,
            codec_id:            self.id()?,
            codec_version:       self.u32()?,
        })
    }

    fn coverage(&mut self) -> Option<SnapshotCoverage> {
        let tag = self.u8()?;
        coverage_from_parts(tag, self.u64()?)
    }
}

/// Decode a root descriptor. `None` = truncated, corrupt, uncommitted, or a
/// format this binary does not understand; the opener then falls back to an
/// older root (and ultimately to "no snapshots", i.e. replay).
#[must_use]
pub fn decode_root(raw: &[u8]) -> Option<Root> {
    if raw.len() < ROOT_FIXED + 8 || raw[0..4] != ROOT_MAGIC {
        return None;
    }
    let commit = u32::from_le_bytes(raw[raw.len() - 4..].try_into().ok()?);
    if commit != COMMIT_MARKER {
        return None;
    }
    let want =
        u32::from_le_bytes(raw[raw.len() - 8..raw.len() - 4].try_into().ok()?);
    if crc32(&raw[..raw.len() - 8]) != want {
        return None;
    }

    let mut c = Cur { b: &raw[..raw.len() - 8], i: 4 };
    if c.u16()? != ROOT_FORMAT {
        return None;
    }
    let mode = SaveMode::from_u8(c.u8()?)?;
    let _reserved = c.u8()?;
    let uuid = StoreUuid(c.take(16)?.try_into().ok()?);
    let generation = c.u64()?;
    let active = c.u64()?;
    let active_pack_len = c.u64()?;
    let sealed_count = c.u32()? as usize;
    let compat_count = c.u32()? as usize;
    let entry_count = c.u32()? as usize;

    // Bound every count against the remaining bytes before reserving, so a
    // corrupt count cannot drive a huge allocation.
    let remaining = c.b.len().saturating_sub(c.i);
    if sealed_count.saturating_mul(8) > remaining {
        return None;
    }
    let mut sealed_packs = Vec::with_capacity(sealed_count);
    for _ in 0..sealed_count {
        sealed_packs.push(c.u64()?);
    }
    // Smallest possible identity is two one-byte ids plus two versions.
    const COMPAT_MIN: usize = 1 + 1 + 4 + 1 + 1 + 4;
    if compat_count.saturating_mul(COMPAT_MIN) > c.b.len().saturating_sub(c.i) {
        return None;
    }
    let mut compats = Vec::with_capacity(compat_count);
    for _ in 0..compat_count {
        compats.push(c.compat()?);
    }
    // Smallest possible entry is the fixed part with a zero-length name.
    const ENTRY_FIXED: usize = 8 + 8 + 4 + 4 + 4 + 1 + 8 + 2;
    if entry_count.saturating_mul(ENTRY_FIXED) > c.b.len().saturating_sub(c.i) {
        return None;
    }
    let mut entries = Vec::with_capacity(entry_count);
    for _ in 0..entry_count {
        let pack_seq = c.u64()?;
        let offset = c.u64()?;
        let frame_len = c.u32()?;
        let record_crc = c.u32()?;
        let compatibility = *compats.get(c.u32()? as usize)?;
        let coverage = c.coverage()?;
        let name_len = c.u16()? as usize;
        let name = std::str::from_utf8(c.take(name_len)?).ok()?.to_owned();
        entries.push(RootEntry {
            stream_name: name,
            compatibility,
            coverage,
            pack_seq,
            offset,
            frame_len,
            record_crc,
        });
    }
    // Trailing garbage means the descriptor is not exactly what we wrote.
    if c.i != c.b.len() {
        return None;
    }

    Some(Root {
        uuid,
        generation,
        mode,
        active_pack: (active != NO_ACTIVE_PACK).then_some(active),
        active_pack_len,
        sealed_packs,
        entries,
    })
}

// ---------------------------------------------------------------------------
// Filenames
// ---------------------------------------------------------------------------

/// Filename of a pack: `pack-<uuid>-<seq>.open` / `.pack`.
#[must_use]
pub fn pack_name(uuid: StoreUuid, seq: u64, sealed: bool) -> String {
    let suffix = if sealed { SEALED_SUFFIX } else { OPEN_SUFFIX };
    format!("pack-{}-{seq:016x}{suffix}", uuid.to_hex())
}

/// Filename of a root descriptor: `root-<uuid>-<generation>.root`.
#[must_use]
pub fn root_name(uuid: StoreUuid, generation: u64) -> String {
    format!("root-{}-{generation:016x}{ROOT_SUFFIX}", uuid.to_hex())
}

/// Parse a pack filename, returning `(uuid, sequence, sealed)`.
///
/// Structural: a `.tmp` staging name cannot parse, so it is never a discovery
/// candidate.
#[must_use]
pub fn parse_pack_name(name: &str) -> Option<(StoreUuid, u64, bool)> {
    let (stem, sealed) = match name.strip_suffix(SEALED_SUFFIX) {
        Some(s) => (s, true),
        None => (name.strip_suffix(OPEN_SUFFIX)?, false),
    };
    let rest = stem.strip_prefix("pack-")?;
    let (uuid_hex, seq_hex) = rest.split_at_checked(32)?;
    let uuid = StoreUuid::from_hex(uuid_hex)?;
    let seq_hex = seq_hex.strip_prefix('-')?;
    if seq_hex.len() != 16 {
        return None;
    }
    let seq = u64::from_str_radix(seq_hex, 16).ok()?;
    Some((uuid, seq, sealed))
}

/// Parse a root filename, returning `(uuid, generation)`.
#[must_use]
pub fn parse_root_name(name: &str) -> Option<(StoreUuid, u64)> {
    let stem = name.strip_suffix(ROOT_SUFFIX)?;
    let rest = stem.strip_prefix("root-")?;
    let (uuid_hex, gen_hex) = rest.split_at_checked(32)?;
    let uuid = StoreUuid::from_hex(uuid_hex)?;
    let gen_hex = gen_hex.strip_prefix('-')?;
    if gen_hex.len() != 16 {
        return None;
    }
    let generation = u64::from_str_radix(gen_hex, 16).ok()?;
    Some((uuid, generation))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn uuid() -> StoreUuid { StoreUuid([7u8; 16]) }

    fn compat(fold: u32) -> SnapshotCompatibility {
        SnapshotCompatibility {
            aggregate_schema_id: StableSnapshotId::new("test.agg"),
            fold_version:        fold,
            codec_id:            StableSnapshotId::new("test.codec"),
            codec_version:       1,
        }
    }

    #[test]
    fn coverage_survives_the_wire_with_empty_below_through_zero() {
        for cov in [
            SnapshotCoverage::Empty,
            SnapshotCoverage::Through(0),
            SnapshotCoverage::Through(u64::MAX),
        ] {
            let mut raw = Vec::new();
            put_coverage(&mut raw, cov);
            let mut c = Cur { b: &raw, i: 0 };
            assert_eq!(c.coverage(), Some(cov));
        }
        // The tag, not the version, is what separates the two: Empty writes a
        // zero version, and Through(0) writes the same version under a
        // different tag.
        let mut empty = Vec::new();
        put_coverage(&mut empty, SnapshotCoverage::Empty);
        let mut zero = Vec::new();
        put_coverage(&mut zero, SnapshotCoverage::Through(0));
        assert_ne!(empty, zero);
        assert_eq!(coverage_from_parts(2, 0), None, "unknown tag is a miss");
    }

    #[test]
    fn an_unknown_trust_mode_is_a_miss() {
        assert_eq!(
            trust_from_u8(TRUST_UNVERIFIED_CACHE),
            Some(SnapshotTrust::UnverifiedCache)
        );
        assert_eq!(trust_from_u8(1), None, "certified is not written yet");
        assert_eq!(trust_from_u8(200), None);
    }

    #[test]
    fn a_corrupt_identity_byte_is_a_miss_not_a_forged_id() {
        let mut raw = Vec::new();
        put_compat(&mut raw, &compat(1));
        // An id length past the cap.
        let mut too_long = raw.clone();
        too_long[0] = 200;
        assert_eq!(Cur { b: &too_long, i: 0 }.compat(), None);
        // A byte outside the canonical alphabet.
        let mut not_canonical = raw.clone();
        not_canonical[1] = b'A';
        assert_eq!(Cur { b: &not_canonical, i: 0 }.compat(), None);
        assert_eq!(Cur { b: &raw, i: 0 }.compat(), Some(compat(1)));
    }

    #[test]
    fn identity_roundtrip_and_corruption() {
        let raw = encode_identity(uuid(), 4096);
        assert_eq!(raw.len(), IDENTITY_LEN);
        assert_eq!(decode_identity(&raw), Some((uuid(), 4096)));

        let mut bad = raw.clone();
        bad[10] ^= 0xFF;
        assert_eq!(decode_identity(&bad), None, "crc must catch a flip");
        assert_eq!(decode_identity(&raw[..IDENTITY_LEN - 1]), None);
        let mut magic = raw.clone();
        magic[0] = b'X';
        assert_eq!(decode_identity(&magic), None);
    }

    #[test]
    fn pack_header_roundtrip_and_corruption() {
        let raw = encode_pack_header(uuid(), 42);
        assert_eq!(raw.len() as u64, PACK_HEADER_LEN);
        assert_eq!(decode_pack_header(&raw), Some((uuid(), 42)));
        let mut bad = raw.clone();
        bad[25] ^= 1;
        assert_eq!(decode_pack_header(&bad), None);
    }

    #[test]
    fn frame_roundtrip() {
        let body = b"a snapshot record".to_vec();
        let raw = encode_frame(&body);
        let f = decode_frame(&raw).expect("valid frame");
        assert_eq!(f.body, body);
        assert_eq!(f.total_len as usize, raw.len());
        assert_eq!(f.body_crc, crc32(&body));
    }

    #[test]
    fn every_frame_truncation_is_rejected() {
        // A torn tail can leave ANY prefix of the frame on disk. Not one of
        // them may decode: that is what "independently commit-framed" buys.
        let raw = encode_frame(b"payload payload payload");
        for n in 0..raw.len() {
            assert!(
                decode_frame(&raw[..n]).is_none(),
                "prefix of {n} bytes must not decode"
            );
        }
        assert!(decode_frame(&raw).is_some());
    }

    #[test]
    fn frame_bit_flips_are_rejected() {
        let raw = encode_frame(b"payload");
        for i in 0..raw.len() {
            let mut bad = raw.clone();
            bad[i] ^= 0x80;
            assert!(
                decode_frame(&bad).is_none(),
                "flipping byte {i} must invalidate the frame"
            );
        }
    }

    #[test]
    fn frame_length_is_bounded() {
        let mut raw = encode_frame(b"x");
        // Forge a header claiming a colossal frame, with a valid header CRC.
        let huge = MAX_FRAME_LEN + 1;
        raw[4..8].copy_from_slice(&huge.to_le_bytes());
        let body_len = huge - (FRAME_HEADER_LEN + FRAME_TRAILER_LEN) as u32;
        raw[8..12].copy_from_slice(&body_len.to_le_bytes());
        let crc = crc32(&raw[0..12]);
        raw[12..16].copy_from_slice(&crc.to_le_bytes());
        assert_eq!(peek_frame_len(&raw), None, "bounded records: reject");
    }

    #[test]
    fn record_roundtrip() {
        let rec = Record {
            stream_name:   "orders-42".into(),
            compatibility: compat(3),
            coverage:      SnapshotCoverage::Through(17),
            trust_mode:    TRUST_UNVERIFIED_CACHE,
            state:         vec![1, 2, 3, 4],
        };
        let raw = encode_record(&rec);
        assert_eq!(decode_record(&raw), Some(rec.clone()));
        assert_eq!(decode_record(&raw[..raw.len() - 1]), None);
        let mut wrong_fmt = raw.clone();
        wrong_fmt[0] = 0xFE;
        assert_eq!(
            decode_record(&wrong_fmt),
            None,
            "unknown format is a miss, never a guess"
        );
    }

    #[test]
    fn a_record_body_written_by_the_previous_format_never_decodes() {
        // The exact v1 body layout, frozen here so the law survives the code
        // that used to write it: fmt(2) trust(1) flags(1) fold(4) version(8)
        // ptr(8) name_len(2) state_len(4) || name || state.
        let mut v1 = Vec::new();
        v1.extend_from_slice(&1u16.to_le_bytes());
        v1.push(TRUST_UNVERIFIED_CACHE);
        v1.push(0);
        v1.extend_from_slice(&1u32.to_le_bytes());
        v1.extend_from_slice(&17u64.to_le_bytes());
        v1.extend_from_slice(&0u64.to_le_bytes());
        v1.extend_from_slice(&9u16.to_le_bytes());
        v1.extend_from_slice(&4u32.to_le_bytes());
        v1.extend_from_slice(b"orders-42");
        v1.extend_from_slice(&[1, 2, 3, 4]);
        assert_eq!(
            decode_record(&v1),
            None,
            "a pre-identity record is a clean miss, never a wrong hit"
        );
    }

    #[test]
    fn every_record_truncation_and_flip_is_a_miss() {
        let raw = encode_record(&Record {
            stream_name:   "s".into(),
            compatibility: compat(1),
            coverage:      SnapshotCoverage::Empty,
            trust_mode:    TRUST_UNVERIFIED_CACHE,
            state:         vec![9],
        });
        for n in 0..raw.len() {
            assert!(decode_record(&raw[..n]).is_none(), "prefix {n} decoded");
        }
        assert!(decode_record(&raw).is_some());
    }

    #[test]
    fn footer_roundtrip_and_incomplete_seal() {
        let entries = vec![
            IndexEntry { offset: 36, frame_len: 100 },
            IndexEntry { offset: 136, frame_len: 50 },
        ];
        let blob = encode_index_and_footer(36, &entries);
        let footer = decode_footer(&blob[blob.len() - FOOTER_LEN..])
            .expect("valid footer");
        assert_eq!(footer.record_count, 2);
        assert_eq!(footer.index_offset, 36);

        // A seal interrupted before the commit marker must not validate.
        let mut torn = blob.clone();
        let n = torn.len();
        torn[n - 1] ^= 0xFF;
        assert_eq!(decode_footer(&torn[n - FOOTER_LEN..]), None);
    }

    fn sample_root() -> Root {
        Root {
            uuid:            uuid(),
            generation:      9,
            mode:            SaveMode::Durable,
            active_pack:     Some(2),
            active_pack_len: 4096,
            sealed_packs:    vec![0, 1],
            entries:         vec![
                RootEntry {
                    stream_name:   "a".into(),
                    compatibility: compat(1),
                    coverage:      SnapshotCoverage::Empty,
                    pack_seq:      0,
                    offset:        36,
                    frame_len:     64,
                    record_crc:    0xABCD,
                },
                RootEntry {
                    stream_name:   "stream/with/slashes".into(),
                    compatibility: compat(2),
                    coverage:      SnapshotCoverage::Through(77),
                    pack_seq:      2,
                    offset:        100,
                    frame_len:     4096,
                    record_crc:    0x1234,
                },
            ],
        }
    }

    #[test]
    fn root_roundtrip() {
        let root = sample_root();
        let raw = encode_root(&root);
        assert_eq!(decode_root(&raw), Some(root));
    }

    #[test]
    fn identities_are_interned_once_however_many_heads_share_them() {
        let one = |n: usize| Root {
            entries: (0..n)
                .map(|i| RootEntry {
                    stream_name:   format!("s-{i:04}"),
                    compatibility: compat(1),
                    coverage:      SnapshotCoverage::Through(i as u64),
                    pack_seq:      0,
                    offset:        i as u64 * 64,
                    frame_len:     64,
                    record_crc:    i as u32,
                })
                .collect(),
            ..sample_root()
        };
        let small = encode_root(&one(1)).len();
        let big = encode_root(&one(101)).len();
        // 100 extra heads must not carry 100 extra copies of the ids: the
        // per-head cost is the leaf (37 bytes) plus its 6-byte name.
        assert!(
            big - small <= 100 * 64,
            "per-head cost {} is carrying the identity bytes",
            (big - small) / 100
        );
        assert_eq!(decode_root(&encode_root(&one(101))), Some(one(101)));
    }

    #[test]
    fn a_root_written_by_the_previous_format_never_decodes() {
        // Frozen v1 root prologue: magic, format 1, mode, reserved. The format
        // check rejects it before any field is interpreted, so an older
        // sidecar's roots are all unusable and the store has no snapshots.
        let mut v1 = Vec::new();
        v1.extend_from_slice(&ROOT_MAGIC);
        v1.extend_from_slice(&1u16.to_le_bytes());
        v1.push(0);
        v1.push(0);
        v1.extend_from_slice(&uuid().0);
        v1.extend_from_slice(&1u64.to_le_bytes());
        v1.extend_from_slice(&NO_ACTIVE_PACK.to_le_bytes());
        v1.extend_from_slice(&0u64.to_le_bytes());
        v1.extend_from_slice(&0u32.to_le_bytes());
        v1.extend_from_slice(&0u32.to_le_bytes());
        let crc = crc32(&v1);
        v1.extend_from_slice(&crc.to_le_bytes());
        v1.extend_from_slice(&COMMIT_MARKER.to_le_bytes());
        assert_eq!(decode_root(&v1), None);
    }

    #[test]
    fn a_leaf_naming_a_missing_intern_slot_is_a_miss() {
        let root = sample_root();
        let mut raw = encode_root(&root);
        // The first leaf's compat index sits after the sealed-pack list and
        // the intern table; find it by re-encoding with a forged index.
        let idx = raw
            .windows(4)
            .position(|w| w == 0xABCDu32.to_le_bytes())
            .expect("record_crc marks the leaf")
            + 4;
        raw[idx..idx + 4].copy_from_slice(&999u32.to_le_bytes());
        let n = raw.len();
        let crc = crc32(&raw[..n - 8]);
        raw[n - 8..n - 4].copy_from_slice(&crc.to_le_bytes());
        assert_eq!(decode_root(&raw), None);
    }

    #[test]
    fn root_rejects_every_truncation_and_flip() {
        let raw = encode_root(&sample_root());
        for n in 0..raw.len() {
            assert!(decode_root(&raw[..n]).is_none(), "prefix {n} must fail");
        }
        for i in 0..raw.len() {
            let mut bad = raw.clone();
            bad[i] ^= 0x40;
            assert!(decode_root(&bad).is_none(), "flip at {i} must fail");
        }
    }

    #[test]
    fn root_mode_survives_and_buffered_never_reads_as_durable() {
        let mut root = sample_root();
        root.mode = SaveMode::Buffered;
        let decoded = decode_root(&encode_root(&root)).expect("decodes");
        assert_eq!(decoded.mode, SaveMode::Buffered);
    }

    #[test]
    fn root_with_corrupt_counts_does_not_over_allocate() {
        let mut raw = encode_root(&sample_root());
        // Forge a colossal entry count and repair the CRC so only the
        // bounds check can reject it.
        let off = 4 + 2 + 1 + 1 + 16 + 8 + 8 + 8 + 4 + 4;
        raw[off..off + 4].copy_from_slice(&u32::MAX.to_le_bytes());
        let n = raw.len();
        let crc = crc32(&raw[..n - 8]);
        raw[n - 8..n - 4].copy_from_slice(&crc.to_le_bytes());
        assert_eq!(decode_root(&raw), None);
    }

    #[test]
    fn filenames_roundtrip_and_tmp_is_never_a_candidate() {
        let u = uuid();
        let open = pack_name(u, 5, false);
        assert_eq!(parse_pack_name(&open), Some((u, 5, false)));
        let sealed = pack_name(u, 5, true);
        assert_eq!(parse_pack_name(&sealed), Some((u, 5, true)));
        let root = root_name(u, 12);
        assert_eq!(parse_root_name(&root), Some((u, 12)));

        // The reserved staging suffix cannot parse as anything.
        assert_eq!(parse_pack_name(&format!("{open}{TMP_SUFFIX}")), None);
        assert_eq!(parse_pack_name(&format!("{sealed}{TMP_SUFFIX}")), None);
        assert_eq!(parse_root_name(&format!("{root}{TMP_SUFFIX}")), None);

        // Neither can junk.
        assert_eq!(parse_root_name("root-short-0.root"), None);
        assert_eq!(parse_pack_name("pack.open"), None);
        assert_eq!(parse_pack_name("LOCK"), None);
        assert_eq!(parse_root_name("IDENTITY"), None);
    }

    #[test]
    fn pack_names_sort_by_sequence_lexicographically() {
        // Zero-padded hex keeps directory listings and any lexicographic
        // scan in sequence order, which the discovery scan relies on.
        let u = uuid();
        let mut names: Vec<String> =
            [10u64, 2, 1, 255].iter().map(|s| pack_name(u, *s, true)).collect();
        names.sort();
        let seqs: Vec<u64> = names
            .iter()
            .map(|n| parse_pack_name(n).expect("parse").1)
            .collect();
        assert_eq!(seqs, vec![1, 2, 10, 255]);
    }
}
