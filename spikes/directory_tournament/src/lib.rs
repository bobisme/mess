//! Spike H (bn-2fp): static directory tournament.
//!
//! The incumbent sealed-segment stream directory is a `HashMap<u64, DirEntry>`
//! rebuilt from a serialized 56-byte-record DIR region on every open
//! (`crates/mess-index/src/sealed/segment.rs`), plus an ascending
//! `Vec<u64>` of stream ids for deterministic iteration. This spike pits that
//! shape against the static representations design.md §12 / research/03 §4-6
//! nominate, under one trait, over one canonical sorted
//! `(stream_id, 48-byte entry payload)` vector per dataset.
//!
//! Candidates (see `candidates/`):
//! - H0  `HashMap` + SipHash (std default) — the incumbent, byte-identical
//!   serialized DIR region, same rebuild-on-open cost.
//! - H0f `HashMap` + foldhash — the cheapest possible "just swap the hasher"
//!   counterfactual.
//! - H1  sorted key array + entry array, binary search — the baseline every
//!   clever structure must beat.
//! - H2  bitvector + rank (superblock 512 / subblock 64, hardware popcount) —
//!   the dense-universe design (§12.2), admissible when U/n <= 8.
//! - H3  partitioned Elias–Fano (256 keys/partition) — the sparse-monotone
//!   design (§12.3).
//! - H4  PtrHash (the published `ptr_hash` crate) + packed key verify (§12.4).
//! - H5  cache-line 7-key bins + overflow — an approximation of the k-PHF
//!   cache-line-bin idea WITHOUT the paper's construction (labelled as such).
//!
//! Admission (the bone): a representation wins only if it beats the incumbent
//! by >=20% on at least one important size/density region without blowing the
//! seal-time construction budget AND has a deterministic fallback; otherwise
//! the verdict is the D10-style negative result.

pub mod candidates;
pub mod datasets;
pub mod gen_real;
pub mod timing;

// ---------------------------------------------------------------------------
// The canonical entry payload
// ---------------------------------------------------------------------------

/// The 48-byte per-stream directory payload — the incumbent's `DirEntry`
/// minus the `stream_id` key (56-byte on-disk record = 8-byte key + this).
/// Field-for-field identical content to
/// `mess_index::sealed::segment::DirEntry`.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Entry {
    pub first_version: u64,
    pub last_version:  u64,
    pub ptr_off:       u64,
    pub skip_off:      u64,
    pub ptr_len:       u32,
    pub n_batches:     u32,
    pub skip_len:      u32,
    pub reserved:      u32,
}

const _: () = assert!(std::mem::size_of::<Entry>() == 48);

impl Entry {
    /// Write in the incumbent's 56-byte DIR-record field order (with key).
    pub fn write_record(&self, key: u64, out: &mut Vec<u8>) {
        out.extend_from_slice(&key.to_le_bytes());
        out.extend_from_slice(&self.first_version.to_le_bytes());
        out.extend_from_slice(&self.last_version.to_le_bytes());
        out.extend_from_slice(&self.ptr_off.to_le_bytes());
        out.extend_from_slice(&self.ptr_len.to_le_bytes());
        out.extend_from_slice(&self.n_batches.to_le_bytes());
        out.extend_from_slice(&self.skip_off.to_le_bytes());
        out.extend_from_slice(&self.skip_len.to_le_bytes());
        out.extend_from_slice(&self.reserved.to_le_bytes());
    }

    /// Parse the incumbent's 56-byte DIR record; returns `(key, entry)`.
    pub fn read_record(b: &[u8]) -> (u64, Entry) {
        (
            rd_u64(b, 0),
            Entry {
                first_version: rd_u64(b, 8),
                last_version:  rd_u64(b, 16),
                ptr_off:       rd_u64(b, 24),
                ptr_len:       rd_u32(b, 32),
                n_batches:     rd_u32(b, 36),
                skip_off:      rd_u64(b, 40),
                skip_len:      rd_u32(b, 48),
                reserved:      rd_u32(b, 52),
            },
        )
    }
}

/// The incumbent's DIR record length.
pub const RECORD_LEN: usize = 56;

#[inline]
pub fn rd_u32(d: &[u8], at: usize) -> u32 {
    u32::from_le_bytes(d[at..at + 4].try_into().unwrap())
}
#[inline]
pub fn rd_u64(d: &[u8], at: usize) -> u64 {
    u64::from_le_bytes(d[at..at + 8].try_into().unwrap())
}

// ---------------------------------------------------------------------------
// The tournament trait
// ---------------------------------------------------------------------------

/// Errors from opening a serialized directory image.
#[derive(Debug)]
pub enum OpenError {
    Corrupt(&'static str),
}

impl std::fmt::Display for OpenError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OpenError::Corrupt(m) => write!(f, "directory corrupt: {m}"),
        }
    }
}
impl std::error::Error for OpenError {}

/// One exact static directory representation, built from the canonical sorted
/// `(stream_id, Entry)` vector. Every implementation must be EXACT: present
/// keys return exactly their entry, absent keys return `None` (MPHF/bin
/// candidates verify the full key).
pub trait ExactDirectory: Sized {
    /// Short table name (also the CSV candidate id).
    const NAME: &'static str;
    /// Envelope kind byte for the serialized image.
    const KIND: u8;

    /// Build from the canonical sorted, strictly-ascending, non-empty vector.
    fn build(pairs: &[(u64, Entry)]) -> Self;

    /// Exact point lookup.
    fn lookup(&self, key: u64) -> Option<&Entry>;

    /// All pairs, ascending by key (used by replay + the exactness tests; not
    /// on the point-lookup hot path).
    fn iter_pairs(&self) -> Vec<(u64, Entry)>;

    /// The seal-time on-disk image (CRC-framed envelope; see [`envelope`]).
    fn serialize(&self) -> Vec<u8>;

    /// Parse a serialized image — the open/parse cost the incumbent pays via
    /// `SealedSegmentIndex::from_bytes`. Must reject corrupt bytes.
    fn open(bytes: &[u8]) -> Result<Self, OpenError>;

    /// Bytes on disk.
    fn serialized_bytes(&self) -> usize { self.serialize().len() }

    /// Analytic resident bytes after open (heap allocations + self).
    fn resident_bytes(&self) -> usize;
}

// ---------------------------------------------------------------------------
// Serialized-image envelope (shared by all candidates)
// ---------------------------------------------------------------------------

/// Envelope: `magic u32 | kind u8 | ver u8 | rsvd u16 | n u64 | body | crc u32`
/// with `crc32c` over everything before the trailing CRC — same checksum
/// family as the real sidecar, so the corrupt-bytes tests exercise the same
/// reject path shape.
pub mod envelope {
    use super::OpenError;

    pub const MAGIC: u32 = 0x5452_4944; // "DIRT"
    pub const HDR: usize = 16;

    pub fn begin(kind: u8, n: u64) -> Vec<u8> {
        let mut v = Vec::new();
        v.extend_from_slice(&MAGIC.to_le_bytes());
        v.push(kind);
        v.push(1); // version
        v.extend_from_slice(&0u16.to_le_bytes());
        v.extend_from_slice(&n.to_le_bytes());
        v
    }

    pub fn finish(mut v: Vec<u8>) -> Vec<u8> {
        let crc = crc32c::crc32c(&v);
        v.extend_from_slice(&crc.to_le_bytes());
        v
    }

    /// Validate the envelope; returns `(n, body)`.
    pub fn open(bytes: &[u8], kind: u8) -> Result<(u64, &[u8]), OpenError> {
        if bytes.len() < HDR + 4 {
            return Err(OpenError::Corrupt("shorter than envelope"));
        }
        if super::rd_u32(bytes, 0) != MAGIC {
            return Err(OpenError::Corrupt("bad magic"));
        }
        if bytes[4] != kind {
            return Err(OpenError::Corrupt("wrong candidate kind"));
        }
        if bytes[5] != 1 {
            return Err(OpenError::Corrupt("unknown version"));
        }
        let crc_at = bytes.len() - 4;
        if crc32c::crc32c(&bytes[..crc_at]) != super::rd_u32(bytes, crc_at) {
            return Err(OpenError::Corrupt("content CRC mismatch"));
        }
        Ok((super::rd_u64(bytes, 8), &bytes[HDR..crc_at]))
    }
}

/// Debug-only canonical-input check shared by all `build`s.
pub fn assert_canonical(pairs: &[(u64, Entry)]) {
    assert!(!pairs.is_empty(), "directory is never empty");
    debug_assert!(
        pairs.windows(2).all(|w| w[0].0 < w[1].0),
        "keys must be strictly ascending"
    );
}
