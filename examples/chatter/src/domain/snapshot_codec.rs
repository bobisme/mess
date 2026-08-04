//! A tiny, **dependency-free** length-prefixed byte codec shared by the two
//! aggregates' [`Snapshottable`](mess_store::Snapshottable) state blobs.
//!
//! Same choice `examples/social` makes and for the same reason:
//! `Snapshottable` expresses its state codec as plain `encode_state` /
//! `decode_state` methods rather than a `serde` bound, precisely so an
//! aggregate author picks the representation. Both aggregates here are
//! **bounded** — a handful of scalars and short strings, no matter how deep
//! the stream gets — so a hand-rolled length-prefixed format is a few lines
//! with an obvious wire shape.
//!
//! # Format
//!
//! Little-endian throughout. `bool`/`u8` are one byte; `u64` is 8 bytes; a
//! `str` is a `u32` length prefix followed by its UTF-8 bytes. Every `read_*`
//! path is **total**: a truncated, over-long, non-UTF-8, or trailing-byte blob
//! yields a [`StateCodecError`], never a panic — a corrupt snapshot must
//! degrade to full replay, not crash the load.

use mess_store::{StableSnapshotId, StateCodecError};

/// The stable name of *this* codec, shared by both aggregates. Change the wire
/// shape below and bump [`CODEC_VERSION`] once: every aggregate's snapshots
/// become a different identity together, miss, and rebuild. No `FOLD_VERSION`
/// moves, because no *fold* changed.
pub(crate) const CODEC_ID: StableSnapshotId =
    StableSnapshotId::new("chatter.length-prefixed");

/// The wire-shape version of [`CODEC_ID`]. See its docs for the bump rule.
pub(crate) const CODEC_VERSION: u32 = 1;

/// Append a `bool` as one byte.
pub(crate) fn put_bool(out: &mut Vec<u8>, b: bool) { out.push(u8::from(b)) }

/// Append a `u64` as 8 little-endian bytes.
pub(crate) fn put_u64(out: &mut Vec<u8>, v: u64) {
    out.extend_from_slice(&v.to_le_bytes())
}

/// Append a `str` as a `u32` length prefix followed by its UTF-8 bytes.
pub(crate) fn put_str(out: &mut Vec<u8>, s: &str) {
    // Every string here is length-capped far below `u32::MAX`, so the cast
    // cannot truncate in practice; the explicit width keeps the wire format
    // fixed regardless of platform `usize`.
    out.extend_from_slice(&(s.len() as u32).to_le_bytes());
    out.extend_from_slice(s.as_bytes());
}

/// A total, bounds-checked cursor over a state blob.
pub(crate) struct Reader<'a> {
    bytes: &'a [u8],
    pos:   usize,
}

impl<'a> Reader<'a> {
    pub(crate) fn new(bytes: &'a [u8]) -> Self { Self { bytes, pos: 0 } }

    fn take(&mut self, n: usize) -> Result<&'a [u8], StateCodecError> {
        let end = self
            .pos
            .checked_add(n)
            .ok_or_else(|| err("state blob length overflow"))?;
        let slice = self
            .bytes
            .get(self.pos..end)
            .ok_or_else(|| err("state blob ended mid-field"))?;
        self.pos = end;
        Ok(slice)
    }

    pub(crate) fn read_bool(&mut self) -> Result<bool, StateCodecError> {
        Ok(self.take(1)?[0] != 0)
    }

    pub(crate) fn read_u64(&mut self) -> Result<u64, StateCodecError> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().expect("8 bytes")))
    }

    pub(crate) fn read_str(&mut self) -> Result<String, StateCodecError> {
        let len = u32::from_le_bytes(self.take(4)?.try_into().expect("4 bytes"))
            as usize;
        let raw = self.take(len)?;
        String::from_utf8(raw.to_vec())
            .map_err(|_| err("state blob string field is not valid UTF-8"))
    }

    /// Assert the whole blob was consumed — trailing bytes mean a shape
    /// mismatch (e.g. a blob written by a different, incompatible fold).
    pub(crate) fn finish(self) -> Result<(), StateCodecError> {
        if self.pos == self.bytes.len() {
            Ok(())
        } else {
            Err(err("state blob has trailing bytes after the decoded state"))
        }
    }
}

fn err(msg: &str) -> StateCodecError { StateCodecError(msg.to_string()) }

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fields_round_trip() {
        let mut out = Vec::new();
        put_bool(&mut out, true);
        put_u64(&mut out, 9_876_543_210);
        put_str(&mut out, "general 🎉");
        let mut r = Reader::new(&out);
        assert!(r.read_bool().unwrap());
        assert_eq!(r.read_u64().unwrap(), 9_876_543_210);
        assert_eq!(r.read_str().unwrap(), "general 🎉");
        r.finish().unwrap();
    }

    #[test]
    fn truncated_blob_errors_not_panics() {
        let mut out = Vec::new();
        put_str(&mut out, "hello");
        let short = &out[..out.len() - 1];
        assert!(Reader::new(short).read_str().is_err());
        assert!(Reader::new(&[]).read_u64().is_err());
    }

    #[test]
    fn trailing_bytes_error() {
        let mut out = Vec::new();
        put_bool(&mut out, true);
        out.push(0xFF);
        let mut r = Reader::new(&out);
        assert!(r.read_bool().is_ok());
        assert!(r.finish().is_err());
    }
}
