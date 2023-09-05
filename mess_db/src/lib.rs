#![warn(clippy::missing_const_for_fn, clippy::must_use_candidate)]

use std::borrow::Cow;

pub mod error;
pub mod msg;
pub mod read;
pub mod rocks;
pub mod rusqlite;
pub mod svc;
pub mod write;

/// StreamPos uses a 63-bit uint for representing position and
/// a u64 for storing the value with a 1-bit flag. This allows
/// the system to tell immediately whether the stream is using
/// strict Serial ordering or Causal ordering via the use of
/// a hybrid logical clock.
///
/// The reason for this distinction is to prevent mixing of the
/// types within a single stream.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[non_exhaustive]
pub enum StreamPos {
    Serial(u64),
    Causal(u64),
}

impl StreamPos {
    #[must_use]
    pub const fn encode(self) -> u64 {
        match self {
            StreamPos::Serial(pos) => pos << 1,
            StreamPos::Causal(pos) => (pos << 1) | 1,
        }
    }

    #[must_use]
    pub const fn decode(stored_position: u64) -> Self {
        match stored_position & 0b1 {
            0 => Self::Serial(stored_position >> 1),
            1 => Self::Causal(stored_position >> 1),
            _ => unreachable!(),
        }
    }

    // Returns the 63-bit position (as u64).
    #[must_use]
    pub const fn position(self) -> u64 {
        match self {
            Self::Serial(pos) | Self::Causal(pos) => pos,
        }
    }

    #[must_use]
    pub const fn next(self) -> Self {
        match self {
            StreamPos::Serial(pos) => Self::Serial(pos + 1),
            StreamPos::Causal(pos) => Self::Causal(pos + 1),
        }
    }
}

// Compile-time test cases for StreamPos
const _: () = {
    use StreamPos::*;
    qed::const_assert!(Serial(0b111).encode() == 0b1110);
    qed::const_assert!(Causal(0b111).encode() == 0b1111);
    qed::const_assert_matches!(StreamPos::decode(0b1110), Serial(0b111));
    qed::const_assert_matches!(StreamPos::decode(0b1111), Causal(0b111));
    qed::const_assert!(Serial(0b111).position() == 0b111);
    qed::const_assert!(Causal(0b111).position() == 0b111);
};

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct Position {
    pub global: u64,
    pub stream: StreamPos,
}

impl Position {
    #[must_use]
    pub const fn new(global: u64, stream: StreamPos) -> Self {
        Self { global, stream }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "sqlx", derive(::sqlx::FromRow))]
pub struct Message<'a> {
    global_position: u64,
    stream_position: StreamPos,
    // time_ms: u64,
    stream_name: Cow<'a, str>,
    message_type: Cow<'a, str>,
    data: Cow<'a, [u8]>,
    metadata: Option<Cow<'a, [u8]>>,
}

#[cfg(feature = "rusqlite")]
impl TryFrom<&::rusqlite::Row<'_>> for Message<'_> {
    type Error = ::rusqlite::Error;

    fn try_from(row: &::rusqlite::Row<'_>) -> Result<Self, Self::Error> {
        Ok(Self {
            global_position: row.get(0)?,
            stream_position: StreamPos::decode(row.get(1)?),
            // time_ms: row.get(2)?,
            stream_name: Cow::Owned(row.get(3)?),
            message_type: Cow::Owned(row.get(4)?),
            data: Cow::Owned(row.get(5)?),
            metadata: row.get::<_, Option<Vec<u8>>>(6)?.map(Cow::Owned),
            // id: row.get(7)?,
        })
    }
}
