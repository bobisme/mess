//! A tiny, **dependency-free** length-prefixed byte codec shared by the four
//! aggregates' [`Snapshottable`](mess_store::Snapshottable) state blobs.
//!
//! # Why hand-rolled and not serde
//!
//! `mess-store`'s [`Snapshottable`](mess_store::Snapshottable) deliberately
//! expresses its state codec as plain `encode_state` / `decode_state` methods
//! rather than a `serde` bound, precisely so an aggregate author picks the
//! representation and the store pulls in no serialization format crate (see the
//! trait's own docs). Post-`bn-jes` every aggregate's state is **bounded** — a
//! handful of scalars and short strings — so a hand-rolled length-prefixed
//! format is a few lines, has an obvious wire shape, and adds **zero
//! dependencies**. That tiny-blob property is the whole point of the bounded
//! remodel: a snapshot here is bytes, never an O(crowd) set.
//!
//! # Format
//!
//! Little-endian throughout. `bool`/`u8` are one byte; a `str` is a `u32`
//! length prefix followed by its UTF-8 bytes; an [`Id`] is its 26-char
//! `Display` string encoded as a `str` (its `FromStr` round-trips it — the same
//! contract [`crate::parse_pair`] relies on); an `Option<Id>` is a one-byte tag
//! (`0` absent, `1` present) then the id when present. Every `decode_*` path is
//! **total**: a truncated, over-long, non-UTF-8, unparseable, or trailing-byte
//! blob yields a [`StateCodecError`], never a panic — a corrupt snapshot must
//! degrade to full replay, not crash the load.

use mess_store::{StableSnapshotId, StateCodecError};

use crate::Id;

/// The stable name of *this* codec, shared by all four aggregates.
///
/// `bn-2gns`: [`Snapshottable`](mess_store::Snapshottable) separates the
/// aggregate's identity from its state codec's, and this module is exactly why.
/// The four aggregates fold four different things but serialize through one
/// hand-rolled format, so one name and one version cover all of them. Change
/// the wire shape below — a new tag, a different length width, a reordered
/// field — and bump [`CODEC_VERSION`] once: every aggregate's snapshots become
/// a different identity together, miss, and rebuild. No `FOLD_VERSION` moves,
/// because no *fold* changed.
pub(crate) const CODEC_ID: StableSnapshotId =
    StableSnapshotId::new("social.length-prefixed");

/// The wire-shape version of [`CODEC_ID`]. See its docs for the bump rule.
pub(crate) const CODEC_VERSION: u32 = 1;

/// Append a `bool` as one byte.
pub(crate) fn put_bool(out: &mut Vec<u8>, b: bool) { out.push(u8::from(b)); }

/// Append a `str` as a `u32` length prefix followed by its UTF-8 bytes.
pub(crate) fn put_str(out: &mut Vec<u8>, s: &str) {
    // A post body / handle is length-capped far below `u32::MAX`, so the cast
    // cannot truncate in practice; the explicit width keeps the wire format
    // fixed regardless of platform `usize`.
    out.extend_from_slice(&(s.len() as u32).to_le_bytes());
    out.extend_from_slice(s.as_bytes());
}

/// Append an [`Id`] as its `Display` string (see the module format note).
pub(crate) fn put_id(out: &mut Vec<u8>, id: Id) {
    put_str(out, &id.to_string());
}

/// Append an `Option<Id>` as a one-byte presence tag then the id when present.
pub(crate) fn put_opt_id(out: &mut Vec<u8>, id: Option<Id>) {
    match id {
        Some(i) => {
            out.push(1);
            put_id(out, i);
        }
        None => out.push(0),
    }
}

/// A total, bounds-checked cursor over a state blob. Every reader method
/// advances the cursor and returns [`StateCodecError`] rather than panicking on
/// a malformed blob.
pub(crate) struct Reader<'a> {
    bytes: &'a [u8],
    pos:   usize,
}

impl<'a> Reader<'a> {
    pub(crate) fn new(bytes: &'a [u8]) -> Self { Self { bytes, pos: 0 } }

    /// Borrow the next `n` bytes, or error if the blob is short.
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

    pub(crate) fn read_u8(&mut self) -> Result<u8, StateCodecError> {
        Ok(self.take(1)?[0])
    }

    pub(crate) fn read_bool(&mut self) -> Result<bool, StateCodecError> {
        Ok(self.read_u8()? != 0)
    }

    pub(crate) fn read_str(&mut self) -> Result<String, StateCodecError> {
        let len = u32::from_le_bytes(self.take(4)?.try_into().expect("4 bytes"))
            as usize;
        let raw = self.take(len)?;
        String::from_utf8(raw.to_vec())
            .map_err(|_| err("state blob string field is not valid UTF-8"))
    }

    pub(crate) fn read_id(&mut self) -> Result<Id, StateCodecError> {
        self.read_str()?
            .parse()
            .map_err(|_| err("state blob holds an unparseable id"))
    }

    pub(crate) fn read_opt_id(
        &mut self,
    ) -> Result<Option<Id>, StateCodecError> {
        match self.read_u8()? {
            0 => Ok(None),
            1 => Ok(Some(self.read_id()?)),
            other => {
                Err(err(&format!("invalid Option tag byte {other} (want 0/1)")))
            }
        }
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
    fn str_round_trips_including_empty_and_unicode() {
        for s in ["", "alice", "a 500-emoji post 🎉🎉", "under_score"] {
            let mut out = Vec::new();
            put_str(&mut out, s);
            let mut r = Reader::new(&out);
            assert_eq!(r.read_str().unwrap(), s);
            r.finish().unwrap();
        }
    }

    #[test]
    fn opt_id_round_trips_both_arms() {
        let id = Id::new();
        for opt in [None, Some(id)] {
            let mut out = Vec::new();
            put_opt_id(&mut out, opt);
            let mut r = Reader::new(&out);
            assert_eq!(r.read_opt_id().unwrap(), opt);
            r.finish().unwrap();
        }
    }

    #[test]
    fn truncated_blob_errors_not_panics() {
        let mut out = Vec::new();
        put_str(&mut out, "hello");
        // Drop the last byte: the length prefix now over-claims.
        let short = &out[..out.len() - 1];
        let mut r = Reader::new(short);
        assert!(r.read_str().is_err());
    }

    #[test]
    fn trailing_bytes_error() {
        let mut out = Vec::new();
        put_bool(&mut out, true);
        out.push(0xFF); // one byte too many for a lone bool
        let mut r = Reader::new(&out);
        assert!(r.read_bool().is_ok());
        assert!(r.finish().is_err());
    }

    #[test]
    fn invalid_option_tag_errors() {
        let bytes = [7u8]; // neither 0 nor 1
        let mut r = Reader::new(&bytes);
        assert!(r.read_opt_id().is_err());
    }
}
