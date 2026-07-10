//! Key/value byte encodings for the metadata tables and the small typed
//! values they carry.
//!
//! Two rules govern every encoding here, both load-bearing:
//!
//! - **Keys that must range-scan in id order are big-endian.** fjall (like any
//!   LSM) orders keys by their raw bytes; `u64::to_be_bytes` makes numeric id
//!   order equal byte order, so `stream_id` prefixes and the dedupe FIFO `seq`
//!   index iterate in the order recovery and eviction expect.
//! - **Values are little-endian, fixed-width, length-explicit.** They are never
//!   range-scanned, so endianness is a free choice; LE matches the project-wide
//!   default (D2). Every decoder validates length and rejects a short/garbled
//!   slice loudly rather than reading past the end — a corrupt fjall value is a
//!   bug or a torn write, never a silent zero.

/// A writer-interned stream id (`u64`, per registry decision D-REG-G). The
/// key space of every per-stream table (`stream_heads`, `snapshot_heads`, the
/// dedupe window) is keyed by this.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct StreamId(pub u64);

/// A stream's head: the position of its last event both within the stream and
/// across the whole log.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Head {
    /// 0-based position of the stream's last event within the stream (its
    /// optimistic-concurrency version).
    pub version:         u64,
    /// Global log position of that last event (watermark units).
    pub global_position: u64,
}

/// A stream's latest snapshot pointer.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct SnapshotHead {
    /// The stream version this snapshot folds up to (inclusive).
    pub covered_version: u64,
    /// Global log position at which this snapshot was taken.
    pub global_position: u64,
    /// Opaque reference to the snapshot bytes (a blob pointer / content hash);
    /// meaningless to this crate, carried verbatim.
    pub snapshot_ref:    Vec<u8>,
}

/// A value read out of a metadata table did not decode.
#[derive(Debug, thiserror::Error)]
pub enum DecodeError {
    /// A fixed-width value was shorter than its schema requires.
    #[error(
        "metadata value too short: table {table}, need >= {need} bytes, got \
         {got}"
    )]
    TooShort {
        /// Which table's value failed to decode.
        table: &'static str,
        /// Minimum byte length the schema requires.
        need:  usize,
        /// Byte length actually present.
        got:   usize,
    },
    /// A stored key or value was structurally invalid (bn-20b: a malformed
    /// interner key/name — a wrong-width id key, or a name that is not UTF-8).
    #[error("metadata corrupt: table {table}: {reason}")]
    Corrupt {
        /// Which table's entry failed to decode.
        table:  &'static str,
        /// What was wrong.
        reason: String,
    },
}

// ---- keys -------------------------------------------------------------

/// Per-stream table key: the id, big-endian so scans run in id order.
#[must_use]
pub fn stream_key(id: StreamId) -> [u8; 8] { id.0.to_be_bytes() }

/// Dedupe-window primary key: `stream_id (8B BE) || dedupe_key`. The
/// fixed-width id prefix makes the concatenation unambiguous for any
/// `dedupe_key` bytes.
#[must_use]
pub fn dedupe_key(stream: StreamId, key: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(8 + key.len());
    out.extend_from_slice(&stream.0.to_be_bytes());
    out.extend_from_slice(key);
    out
}

/// Dedupe FIFO order-index key: the insertion `seq`, big-endian so the oldest
/// entry is always the lexicographically first key.
#[must_use]
pub fn order_key(seq: u64) -> [u8; 8] { seq.to_be_bytes() }

// ---- values -----------------------------------------------------------

/// Encode a [`Head`] as `version (8B LE) || global_position (8B LE)`.
#[must_use]
pub fn encode_head(h: Head) -> [u8; 16] {
    let mut b = [0u8; 16];
    b[0..8].copy_from_slice(&h.version.to_le_bytes());
    b[8..16].copy_from_slice(&h.global_position.to_le_bytes());
    b
}

/// Decode a [`Head`]; rejects a slice shorter than 16 bytes.
pub fn decode_head(v: &[u8]) -> Result<Head, DecodeError> {
    if v.len() < 16 {
        return Err(DecodeError::TooShort {
            table: "stream_heads",
            need:  16,
            got:   v.len(),
        });
    }
    Ok(Head {
        version:         u64::from_le_bytes(v[0..8].try_into().unwrap()),
        global_position: u64::from_le_bytes(v[8..16].try_into().unwrap()),
    })
}

/// Encode a [`SnapshotHead`] as
/// `covered_version (8B LE) || global_position (8B LE) || snapshot_ref`.
#[must_use]
pub fn encode_snapshot(s: &SnapshotHead) -> Vec<u8> {
    let mut out = Vec::with_capacity(16 + s.snapshot_ref.len());
    out.extend_from_slice(&s.covered_version.to_le_bytes());
    out.extend_from_slice(&s.global_position.to_le_bytes());
    out.extend_from_slice(&s.snapshot_ref);
    out
}

/// Decode a [`SnapshotHead`]; the trailing bytes past the fixed 16-byte header
/// are the opaque `snapshot_ref`.
pub fn decode_snapshot(v: &[u8]) -> Result<SnapshotHead, DecodeError> {
    if v.len() < 16 {
        return Err(DecodeError::TooShort {
            table: "snapshot_heads",
            need:  16,
            got:   v.len(),
        });
    }
    Ok(SnapshotHead {
        covered_version: u64::from_le_bytes(v[0..8].try_into().unwrap()),
        global_position: u64::from_le_bytes(v[8..16].try_into().unwrap()),
        snapshot_ref:    v[16..].to_vec(),
    })
}

/// Encode a bare position (checkpoint value, high-water value) as 8B LE.
#[must_use]
pub fn encode_pos(pos: u64) -> [u8; 8] { pos.to_le_bytes() }

/// Decode a bare position; rejects a slice shorter than 8 bytes.
pub fn decode_pos(table: &'static str, v: &[u8]) -> Result<u64, DecodeError> {
    if v.len() < 8 {
        return Err(DecodeError::TooShort { table, need: 8, got: v.len() });
    }
    Ok(u64::from_le_bytes(v[0..8].try_into().unwrap()))
}

/// Encode a dedupe primary value as `position (8B LE) || seq (8B LE)`. The
/// `seq` lets an overwrite locate and replace its stale FIFO order-index
/// entry so the live-entry count stays exact.
#[must_use]
pub fn encode_dedupe(position: u64, seq: u64) -> [u8; 16] {
    let mut b = [0u8; 16];
    b[0..8].copy_from_slice(&position.to_le_bytes());
    b[8..16].copy_from_slice(&seq.to_le_bytes());
    b
}

/// Decode a dedupe primary value into `(position, seq)`.
pub fn decode_dedupe(v: &[u8]) -> Result<(u64, u64), DecodeError> {
    if v.len() < 16 {
        return Err(DecodeError::TooShort {
            table: "dedupe",
            need:  16,
            got:   v.len(),
        });
    }
    Ok((
        u64::from_le_bytes(v[0..8].try_into().unwrap()),
        u64::from_le_bytes(v[8..16].try_into().unwrap()),
    ))
}
