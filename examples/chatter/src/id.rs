//! [`Id`]: a UUIDv7 newtype rendered as a fixed 26-character lowercase
//! Crockford base32 string.
//!
//! This is the same identifier shape `examples/social` uses (see that crate's
//! `id` module for the long-form rationale), reproduced here rather than
//! shared because an example crate should be readable start to finish without
//! chasing a dependency on *another example*. The three properties this crate
//! actually leans on:
//!
//! - **UUIDv7 is time-ordered.** The 48-bit millisecond timestamp occupies the
//!   leading bytes, so [`Uuid`]'s byte-wise [`Ord`] — and therefore [`Id`]'s —
//!   agrees with creation order.
//! - **The canonical string contains no `-` and no `_`.** Stream ids are
//!   `channel-<id>` / `user-<id>`, and
//!   [`StoredRecord::category_and_suffix`](mess_store::StoredRecord::category_and_suffix)
//!   splits at the *first* `-`. An id that cannot contain `-` makes that split
//!   trivially unambiguous.
//! - **[`Id::from_parts`] is deterministic.** The seed generator draws every id
//!   from its seeded PRNG through this constructor, never from the wall clock,
//!   which is what makes "same `--seed`, same corpus" true.

use std::fmt;
use std::str::FromStr;

use uuid::Uuid;

/// The 32-symbol Crockford alphabet, in strictly increasing ASCII order
/// (digits, then lowercase letters, skipping the ambiguous `i`, `l`, `o`, `u`).
/// That ordering is what makes fixed-length string comparison agree with
/// numeric comparison.
const ALPHABET: &[u8; 32] = b"0123456789abcdefghjkmnpqrstvwxyz";

/// `byte -> 5-bit value` lookup, `0xFF` for "not a valid digit", built at
/// compile time from [`ALPHABET`] (case-insensitively). Crockford's optional
/// `i`/`l -> 1`, `o -> 0` visual aliases are deliberately **not** implemented:
/// two different strings must never decode to the same [`Id`].
const DECODE: [u8; 256] = build_decode_table();

const fn ascii_upper(c: u8) -> u8 {
    if c.is_ascii_lowercase() { c - (b'a' - b'A') } else { c }
}

const fn build_decode_table() -> [u8; 256] {
    let mut table = [0xFFu8; 256];
    let mut i = 0;
    while i < ALPHABET.len() {
        let lower = ALPHABET[i];
        table[lower as usize] = i as u8;
        table[ascii_upper(lower) as usize] = i as u8;
        i += 1;
    }
    table
}

/// The canonical string length: `ceil(128 / 5) = 26` symbols.
pub const ENCODED_LEN: usize = 26;

/// Total bits covered by [`ENCODED_LEN`] symbols (`26 * 5 = 130`); the 2 bits
/// beyond the 128 payload bits are the leading symbol's zero padding.
const TOTAL_BITS: u32 = (ENCODED_LEN as u32) * 5;

/// Encode a 128-bit big-endian value as 26 lowercase Crockford base32
/// characters, most-significant symbol first.
fn encode(bytes: [u8; 16]) -> [u8; ENCODED_LEN] {
    let value = u128::from_be_bytes(bytes);
    let mut out = [0u8; ENCODED_LEN];
    let mut i = 0;
    while i < ENCODED_LEN {
        let shift = TOTAL_BITS - 5 * (i as u32 + 1);
        out[i] = ALPHABET[((value >> shift) & 0b1_1111) as usize];
        i += 1;
    }
    out
}

/// Failure decoding an [`Id`] from a string.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum IdParseError {
    /// The string was not exactly [`ENCODED_LEN`] characters.
    #[error("id must be exactly {ENCODED_LEN} characters, got {0}")]
    WrongLength(usize),
    /// A byte in the string was not a Crockford base32 digit.
    #[error("id contains an invalid character {0:?} at byte offset {1}")]
    InvalidChar(char, usize),
    /// The leading symbol was outside `0..=7`. 26 base32 symbols carry 130
    /// bits but an id has only 128, so the first symbol holds just 3 bits;
    /// accepting `8..=z` there would make two different strings decode to the
    /// same id.
    #[error("id leading character {0:?} is out of range (must be 0-7)")]
    LeadingCharOutOfRange(char),
}

/// Decode a 26-character Crockford base32 string (case-insensitive) back into
/// the original 16 big-endian bytes. Total: never panics.
fn decode(s: &str) -> Result<[u8; 16], IdParseError> {
    let bytes = s.as_bytes();
    if bytes.len() != ENCODED_LEN {
        return Err(IdParseError::WrongLength(bytes.len()));
    }
    let mut value: u128 = 0;
    for (i, &b) in bytes.iter().enumerate() {
        let digit = DECODE[b as usize];
        if digit == 0xFF {
            let ch = s[i..].chars().next().unwrap_or('\u{FFFD}');
            return Err(IdParseError::InvalidChar(ch, i));
        }
        if i == 0 && digit > 7 {
            let ch = s.chars().next().unwrap_or('\u{FFFD}');
            return Err(IdParseError::LeadingCharOutOfRange(ch));
        }
        value |= (digit as u128) << (TOTAL_BITS - 5 * (i as u32 + 1));
    }
    Ok(value.to_be_bytes())
}

/// A time-ordered identifier: a UUIDv7 rendered canonically as 26 lowercase
/// Crockford base32 characters. See the module docs.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Id(Uuid);

impl Id {
    /// Construct an id deterministically from an explicit Unix millisecond
    /// timestamp and 10 explicit random/counter bytes — the same shape
    /// [`Uuid::now_v7`] draws from the clock and a CSPRNG, but reproducible.
    /// This is the seam [`crate::seed`] uses.
    #[must_use]
    #[inline]
    pub fn from_parts(unix_ms: u64, random: [u8; 10]) -> Self {
        Self(
            uuid::Builder::from_unix_timestamp_millis(unix_ms, &random)
                .into_uuid(),
        )
    }

    /// Mint a new id from the current wall-clock time and fresh randomness.
    #[must_use]
    #[inline]
    pub fn new() -> Self { Self(Uuid::now_v7()) }

    /// The canonical 26-character encoding, allocation-free.
    #[must_use]
    fn encoded(self) -> [u8; ENCODED_LEN] { encode(*self.0.as_bytes()) }
}

impl Default for Id {
    #[inline]
    fn default() -> Self { Self::new() }
}

impl fmt::Display for Id {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let arr = self.encoded();
        // Every byte comes from `ALPHABET`, which is pure ASCII.
        f.write_str(std::str::from_utf8(&arr).expect("crockford is ASCII"))
    }
}

impl fmt::Debug for Id {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Id({self})")
    }
}

impl FromStr for Id {
    type Err = IdParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(Uuid::from_bytes(decode(s)?)))
    }
}

impl serde::Serialize for Id {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> serde::Deserialize<'de> for Id {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct IdVisitor;
        impl serde::de::Visitor<'_> for IdVisitor {
            type Value = Id;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "a {ENCODED_LEN}-character Crockford base32 id")
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                v.parse().map_err(serde::de::Error::custom)
            }
        }
        deserializer.deserialize_str(IdVisitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_from_str_roundtrip_is_exact() {
        for i in 0..500u64 {
            let id =
                Id::from_parts(1_704_067_200_000 + i, [(i % 251) as u8; 10]);
            let s = id.to_string();
            assert_eq!(s.len(), ENCODED_LEN);
            assert_eq!(s.parse::<Id>().unwrap(), id);
        }
    }

    #[test]
    fn canonical_string_has_no_hyphen_or_underscore() {
        // The property `channel-<id>` stream naming depends on: an id can never
        // contribute a `-` (or `_`) to a stream id.
        for i in 0..500u64 {
            let s = Id::from_parts(i, [(i % 255) as u8; 10]).to_string();
            assert!(!s.contains('-'), "{s} contains a hyphen");
            assert!(!s.contains('_'), "{s} contains an underscore");
        }
    }

    #[test]
    fn string_order_matches_ord() {
        let early = Id::from_parts(1_000_000_000_000, [0u8; 10]);
        let late = Id::from_parts(1_000_000_000_001, [0u8; 10]);
        assert!(early < late);
        assert!(early.to_string() < late.to_string());
    }

    #[test]
    fn from_parts_is_deterministic() {
        let a = Id::from_parts(1_700_000_000_000, [7u8; 10]);
        let b = Id::from_parts(1_700_000_000_000, [7u8; 10]);
        assert_eq!(a, b);
    }

    #[test]
    fn rejects_wrong_length_and_bad_characters() {
        assert!(matches!(
            "abc".parse::<Id>(),
            Err(IdParseError::WrongLength(3))
        ));
        let with_u = "u".repeat(ENCODED_LEN);
        assert!(matches!(
            with_u.parse::<Id>(),
            Err(IdParseError::InvalidChar('u', 0))
        ));
        // 26 symbols carry 130 bits; only 3 fit in the leading symbol.
        let overflow = format!("8{}", "0".repeat(ENCODED_LEN - 1));
        assert!(matches!(
            overflow.parse::<Id>(),
            Err(IdParseError::LeadingCharOutOfRange('8'))
        ));
    }
}
