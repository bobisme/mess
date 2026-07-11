//! [`Id`]: a UUIDv7 newtype rendered as a fixed 26-character lowercase
//! Crockford base32 string (bn-gt5; supersedes bn-3c6's bare `ident::Id`,
//! which had no [`Ord`]).
//!
//! # Why UUIDv7
//!
//! A v7 UUID packs a 48-bit millisecond Unix timestamp into its most
//! significant bits, followed by 74 bits of monotonic-within-a-millisecond
//! random/counter data (see [RFC 9562 §5.7]). Two consequences matter here:
//!
//! - **Time-meaningful ordering.** [`uuid::Uuid`] derives [`Ord`] over its raw
//!   16 bytes, most-significant-byte first — which, because the timestamp
//!   occupies the leading bytes, means `Ord` on the `Uuid` (and therefore on
//!   [`Id`], a plain newtype over it) agrees with creation order: an id minted
//!   later never sorts before one minted earlier.
//! - **No coordination required.** Unlike a database auto-increment, any number
//!   of processes can mint ids concurrently with no shared counter, the classic
//!   event-sourcing id story.
//!
//! # Why base32, and why *this* base32
//!
//! [`Id`]'s [`Display`]/[`FromStr`] round-trip through a hand-rolled
//! [Crockford base32] encoding of the underlying 128 bits, fixed at exactly
//! 26 characters (`⌈128 / 5⌉ = 26`, with 2 padding zero bits in the leading
//! symbol — see [`encode`] below). Crockford's alphabet excludes the four
//! visually-ambiguous letters `i`, `l`, `o`, `u`, so every canonical id is
//! also typo- and transcription-resistant.
//!
//! **Critical property: string order preserves `Ord` order.** Crockford's
//! 32-symbol alphabet is listed in strictly increasing ASCII order (`'0'` …
//! `'9'` then `'a'` … `'z'`, skipping `i`/`l`/`o`/`u`), and [`encode`] emits
//! the 128 bits **big-endian, most-significant symbol first**, at a *fixed*
//! length with no separators. Those three facts together mean: for any two
//! ids `a`, `b`, `a.to_string() < b.to_string()` (byte/lexicographic string
//! comparison) if and only if `a < b` (`Ord` comparison of the wrapped
//! `Uuid`). [`tests::string_order_matches_ord`] proves this over random
//! pairs, and because `Ord` agrees with v7 creation order (above), so does
//! string order: ids sort chronologically as *strings*, not just as values —
//! handy for anything that stores or displays them as text (log lines, a
//! `BTreeMap<String, _>`, a spreadsheet).
//!
//! This canonical string also has no `-` and no `_` at all (see
//! [`crate::PAIR_SEP`] for why that matters to this crate's relationship
//! stream naming).
//!
//! [RFC 9562 §5.7]: https://www.rfc-editor.org/rfc/rfc9562.html#section-5.7
//! [Crockford base32]: https://www.crockford.com/base32.html

use std::fmt;
use std::str::FromStr;

use uuid::Uuid;

// ===========================================================================
// The Crockford base32 alphabet + its decode table
// ===========================================================================

/// The 32-symbol Crockford alphabet, **in strictly increasing ASCII order**
/// (digits, then lowercase letters, skipping the ambiguous `i`, `l`, `o`,
/// `u`). That ordering is exactly what makes fixed-length string comparison
/// agree with numeric comparison — see the module docs.
const ALPHABET: &[u8; 32] = b"0123456789abcdefghjkmnpqrstvwxyz";

/// `to_ascii_uppercase` restricted to `'a'..='z'`, written by hand so
/// [`build_decode_table`] can stay `const fn` without leaning on a trait
/// method's `const`-stability.
const fn ascii_upper(c: u8) -> u8 {
    if c.is_ascii_lowercase() { c - (b'a' - b'A') } else { c }
}

/// `byte -> 5-bit value` lookup, `0xFF` for "not a valid digit". Built once
/// at compile time from [`ALPHABET`], case-insensitively (each symbol's
/// lowercase *and* uppercase form map to the same value).
///
/// **Aliasing choice, documented:** this decoder does **not** implement
/// Crockford's optional `i`/`l -> 1`, `o -> 0` visual aliases. Two different
/// input strings must never decode to the same [`Id`] — that would make
/// [`FromStr`] a many-to-one map, which is a sharper property than this
/// crate needs to give up for marginally friendlier typo tolerance. A string
/// containing `i`, `l`, `o`, or `u` (either case) is simply rejected as an
/// invalid digit, exactly like any other out-of-alphabet byte.
const DECODE: [u8; 256] = build_decode_table();

const fn build_decode_table() -> [u8; 256] {
    let mut table = [0xFFu8; 256];
    let mut i = 0;
    while i < ALPHABET.len() {
        let lower = ALPHABET[i];
        let upper = ascii_upper(lower);
        table[lower as usize] = i as u8;
        table[upper as usize] = i as u8;
        i += 1;
    }
    table
}

/// The canonical string length: `ceil(128 / 5) = 26` symbols.
pub const ENCODED_LEN: usize = 26;

/// Total bits covered by [`ENCODED_LEN`] symbols (`26 * 5 = 130`); the 2
/// bits beyond the 128 payload bits are the leading symbol's zero padding.
const TOTAL_BITS: u32 = (ENCODED_LEN as u32) * 5;

/// Encode `bytes` (a 128-bit value, big-endian — i.e. `bytes[0]` is the most
/// significant byte, matching [`Uuid::as_bytes`]) as 26 lowercase Crockford
/// base32 characters, most-significant symbol first.
///
/// The value is treated as sitting in the low 128 bits of a virtual 130-bit
/// field; the top 2 bits of that field (the unused high bits of the leading
/// symbol) are implicitly zero because `u128` has no bits there to begin
/// with — no explicit masking needed. This is what "pad the leading partial
/// symbol" means in practice: the first symbol only ever encodes values
/// `0..=7` (3 real bits), never the full `0..=31`.
#[must_use]
fn encode(bytes: [u8; 16]) -> [u8; ENCODED_LEN] {
    let value = u128::from_be_bytes(bytes);
    let mut out = [0u8; ENCODED_LEN];
    let mut i = 0;
    while i < ENCODED_LEN {
        // Bit position (from the LSB) of this symbol's low bit within the
        // 128-bit value. For i == 0 this is 125 (the top 3 real bits); for
        // i == 25 this is 0 (the bottom 5 bits).
        let shift = TOTAL_BITS - 5 * (i as u32 + 1);
        let digit = ((value >> shift) & 0b1_1111) as usize;
        out[i] = ALPHABET[digit];
        i += 1;
    }
    out
}

/// Failure decoding a [`Id`] from a string.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum IdParseError {
    /// The string was not exactly [`ENCODED_LEN`] characters.
    #[error("id must be exactly {ENCODED_LEN} characters, got {0}")]
    WrongLength(usize),
    /// A byte in the string was not a Crockford base32 digit (case-
    /// insensitively) — this includes `i`, `l`, `o`, `u` and any
    /// punctuation, since this codec accepts no aliases and no separators.
    #[error("id contains an invalid character {0:?} at byte offset {1}")]
    InvalidChar(char, usize),
    /// The leading symbol was outside `0..=7`. 26 base32 symbols carry 130
    /// bits but an id has only 128, so the first symbol holds just 3 bits;
    /// accepting `8..=z` there would silently discard the overflowing top
    /// bits and make two different strings decode to the same id.
    #[error("id leading character {0:?} is out of range (must be 0-7)")]
    LeadingCharOutOfRange(char),
}

/// Decode a 26-character Crockford base32 string (case-insensitive) back
/// into the original 16 big-endian bytes. Total: a non-26-length input or
/// any byte outside the alphabet is rejected with a typed
/// [`IdParseError`], never a panic.
fn decode(s: &str) -> Result<[u8; 16], IdParseError> {
    let bytes = s.as_bytes();
    if bytes.len() != ENCODED_LEN {
        return Err(IdParseError::WrongLength(bytes.len()));
    }
    let mut value: u128 = 0;
    for (i, &b) in bytes.iter().enumerate() {
        let digit = DECODE[b as usize];
        if digit == 0xFF {
            // `s` was already validated as exactly `ENCODED_LEN` bytes of
            // `str`, but the offending byte itself might be a non-ASCII
            // UTF-8 continuation byte; recover the real `char` for the
            // error message by re-decoding from the original `&str`.
            let ch = s[i..].chars().next().unwrap_or('\u{FFFD}');
            return Err(IdParseError::InvalidChar(ch, i));
        }
        if i == 0 && digit > 7 {
            // Only 3 of the leading symbol's 5 bits fit in 128; rejecting
            // the rest keeps decode injective (see LeadingCharOutOfRange).
            let ch = s.chars().next().unwrap_or('\u{FFFD}');
            return Err(IdParseError::LeadingCharOutOfRange(ch));
        }
        let shift = TOTAL_BITS - 5 * (i as u32 + 1);
        value |= (digit as u128) << shift;
    }
    Ok(value.to_be_bytes())
}

// ===========================================================================
// Id
// ===========================================================================

/// A time-ordered identifier: a UUIDv7 rendered canonically as 26 lowercase
/// Crockford base32 characters. See the module docs for the full rationale.
///
/// `Ord` is derived (forwarding to [`uuid::Uuid`]'s own `Ord` over its raw
/// bytes) and agrees with both v7 creation order and canonical-string order —
/// see the module docs' "critical property".
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Id(Uuid);

impl Id {
    /// Mint a new id from the current wall-clock time and fresh randomness.
    /// Every call in a process is guaranteed ordered by creation time (see
    /// [`Uuid::now_v7`]).
    #[must_use]
    #[inline]
    pub fn new() -> Self { Self(Uuid::now_v7()) }

    /// Construct an id deterministically from an explicit Unix millisecond
    /// timestamp and 10 explicit random/counter bytes — the same shape
    /// [`Uuid::now_v7`] draws from the clock and a CSPRNG, but fully
    /// reproducible.
    ///
    /// This is the seam [`crate::seed`] uses: a seeded PRNG stands in for
    /// both the timestamp and the random bytes, so the same
    /// [`SeedConfig::seed`](crate::seed::SeedConfig::seed) draws
    /// byte-identical ids run to run, which the demo-tier determinism test
    /// depends on.
    #[must_use]
    #[inline]
    pub fn from_parts(unix_ms: u64, random: [u8; 10]) -> Self {
        Self(
            uuid::Builder::from_unix_timestamp_millis(unix_ms, &random)
                .into_uuid(),
        )
    }

    /// The canonical 26-character encoding as a fixed-size byte array, with
    /// no allocation — what [`Display`](fmt::Display) writes.
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
        // Every byte in `arr` comes from `ALPHABET`, which is pure ASCII, so
        // this is always valid UTF-8.
        let s = std::str::from_utf8(&arr).expect("crockford output is ASCII");
        f.write_str(s)
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
        // Always the canonical string, regardless of
        // `Serializer::is_human_readable`: this crate's one wire codec
        // (rmp-serde, via `#[derive(Event)]`) round-trips it exactly, and a
        // single representation is simpler to reason about than a
        // format-dependent one.
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
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};

    use super::*;

    fn is_crockford_clean(s: &str) -> bool {
        s.len() == ENCODED_LEN
            && s.bytes().all(|b| {
                b.is_ascii_digit()
                    || (b.is_ascii_lowercase()
                        && b != b'i'
                        && b != b'l'
                        && b != b'o'
                        && b != b'u')
            })
    }

    // -- (a) Display/FromStr roundtrip, including a randomized loop --------

    #[test]
    fn new_id_displays_as_26_clean_lowercase_chars() {
        let id = Id::new();
        let s = id.to_string();
        assert_eq!(s.len(), ENCODED_LEN);
        assert!(is_crockford_clean(&s), "not clean crockford: {s:?}");
    }

    #[test]
    fn display_from_str_roundtrip_is_exact() {
        let id = Id::new();
        let s = id.to_string();
        let parsed: Id = s.parse().unwrap();
        assert_eq!(id, parsed);
        assert_eq!(parsed.to_string(), s);
    }

    #[test]
    fn random_roundtrip_property_1000_uuids() {
        let mut rng = StdRng::seed_from_u64(0x00C0_FFEE_1234_5678);
        for _ in 0..1000 {
            let raw: u128 = rng.random();
            let id = Id(Uuid::from_bytes(raw.to_be_bytes()));
            let s = id.to_string();
            assert_eq!(s.len(), ENCODED_LEN, "wrong length for {s:?}");
            assert!(is_crockford_clean(&s), "not clean crockford: {s:?}");
            let back: Id = s.parse().unwrap_or_else(|e| {
                panic!("failed to parse our own encoding {s:?}: {e}")
            });
            assert_eq!(id, back, "roundtrip mismatch for {s:?}");
        }
    }

    // -- (b) ordering property ----------------------------------------------

    #[test]
    fn string_order_matches_ord_over_random_pairs() {
        let mut rng = StdRng::seed_from_u64(42);
        for _ in 0..1000 {
            let a = Id(Uuid::from_bytes(rng.random::<u128>().to_be_bytes()));
            let b = Id(Uuid::from_bytes(rng.random::<u128>().to_be_bytes()));
            let (sa, sb) = (a.to_string(), b.to_string());
            assert_eq!(
                sa.cmp(&sb),
                a.cmp(&b),
                "string order {sa:?} vs {sb:?} disagreed with Ord order"
            );
        }
    }

    #[test]
    fn later_timestamp_sorts_after_earlier() {
        let early = Id::from_parts(1_000_000_000_000, [0u8; 10]);
        let late = Id::from_parts(1_000_000_000_001, [0u8; 10]);
        assert!(early < late, "{early} should sort before {late}");
        assert!(early.to_string() < late.to_string());

        // Same millisecond, different random bytes: still well-ordered by
        // *some* deterministic order (not asserting which), but must be
        // self-consistent between Ord and string order.
        let a = Id::from_parts(1_000, [1u8; 10]);
        let b = Id::from_parts(1_000, [2u8; 10]);
        assert_eq!(a.cmp(&b), a.to_string().cmp(&b.to_string()));
    }

    #[test]
    fn from_parts_is_deterministic() {
        let a = Id::from_parts(1_700_000_000_000, [7u8; 10]);
        let b = Id::from_parts(1_700_000_000_000, [7u8; 10]);
        assert_eq!(a, b);
        assert_eq!(a.to_string(), b.to_string());
    }

    // -- (c) case-insensitive parse ------------------------------------------

    #[test]
    fn parse_accepts_uppercase_form_of_a_canonical_string() {
        let id = Id::new();
        let canonical = id.to_string();
        let upper = canonical.to_ascii_uppercase();
        assert_ne!(canonical, upper, "sanity: fixture has letters to case");
        let parsed: Id = upper.parse().unwrap();
        assert_eq!(id, parsed);
    }

    // -- (d) rejection --------------------------------------------------------

    #[test]
    fn rejects_wrong_length() {
        let id = Id::new();
        let s = id.to_string();
        let short = &s[..25];
        let long = format!("{s}0");
        assert!(matches!(
            short.parse::<Id>(),
            Err(IdParseError::WrongLength(25))
        ));
        assert!(matches!(
            long.parse::<Id>(),
            Err(IdParseError::WrongLength(27))
        ));
    }

    #[test]
    fn rejects_invalid_characters() {
        // 'u' is excluded from the Crockford alphabet entirely (no alias).
        let with_u = "u".repeat(ENCODED_LEN);
        assert!(matches!(
            with_u.parse::<Id>(),
            Err(IdParseError::InvalidChar('u', 0))
        ));
        // Uppercase 'U' too.
        let with_upper_u = "U".repeat(ENCODED_LEN);
        assert!(with_upper_u.parse::<Id>().is_err());
        // Punctuation.
        let mut s = "0".repeat(ENCODED_LEN);
        s.replace_range(3..4, "-");
        assert!(matches!(
            s.parse::<Id>(),
            Err(IdParseError::InvalidChar('-', 3))
        ));
    }

    #[test]
    fn rejects_out_of_range_leading_symbol() {
        // 26 symbols carry 130 bits; only 3 bits fit in the leading symbol.
        // Without the range check, "8000…" and "g000…" would silently decode
        // to the same id as "0000…" (reviewer probe, bn-gt5).
        for lead in ["8", "9", "g", "z", "G", "Z"] {
            let s = format!("{lead}{}", "0".repeat(ENCODED_LEN - 1));
            assert!(
                matches!(
                    s.parse::<Id>(),
                    Err(IdParseError::LeadingCharOutOfRange(_))
                ),
                "leading {lead:?} must be rejected"
            );
        }
        // The whole in-range boundary still parses and roundtrips.
        let max = format!("7{}", "z".repeat(ENCODED_LEN - 1));
        let id: Id = max.parse().expect("max in-range id parses");
        assert_eq!(id.to_string(), max);
    }

    #[test]
    fn canonical_string_has_no_hyphen_or_underscore() {
        for _ in 0..200 {
            let id = Id::new();
            let s = id.to_string();
            assert!(!s.contains('-'));
            assert!(!s.contains('_'));
        }
    }
}
