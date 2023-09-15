#![warn(clippy::missing_const_for_fn, clippy::must_use_candidate)]

use std::borrow::Cow;

pub mod error;
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
pub enum StreamPos {
    Sequential(u64),
    Relaxed(u64),
}

#[cfg(feature = "rusqlite")]
impl ::rusqlite::ToSql for StreamPos {
    fn to_sql(&self) -> ::rusqlite::Result<::rusqlite::types::ToSqlOutput<'_>> {
        Ok(::rusqlite::types::ToSqlOutput::Owned(
            ::rusqlite::types::Value::Integer(self.encode() as i64),
        ))
    }
}

impl StreamPos {
    #[must_use]
    pub const fn encode(self) -> u64 {
        match self {
            StreamPos::Sequential(pos) => pos << 1,
            StreamPos::Relaxed(pos) => (pos << 1) | 1,
        }
    }

    #[must_use]
    pub const fn decode(stored_position: u64) -> Self {
        match stored_position & 0b1 {
            0 => Self::Sequential(stored_position >> 1),
            1 => Self::Relaxed(stored_position >> 1),
            _ => unreachable!(),
        }
    }

    // Returns the 63-bit position (as u64).
    #[must_use]
    pub const fn position(self) -> u64 {
        match self {
            Self::Sequential(pos) | Self::Relaxed(pos) => pos,
        }
    }

    #[must_use]
    pub const fn next(self) -> Self {
        match self {
            StreamPos::Sequential(pos) => Self::Sequential(pos + 1),
            StreamPos::Relaxed(pos) => Self::Relaxed(pos + 1),
        }
    }
}

// Compile-time test cases for StreamPos
const _: () = {
    use StreamPos::*;
    qed::const_assert!(Sequential(0b111).encode() == 0b1110);
    qed::const_assert!(Relaxed(0b111).encode() == 0b1111);
    qed::const_assert_matches!(StreamPos::decode(0b1110), Sequential(0b111));
    qed::const_assert_matches!(StreamPos::decode(0b1111), Relaxed(0b111));
    qed::const_assert!(Sequential(0b111).position() == 0b111);
    qed::const_assert!(Relaxed(0b111).position() == 0b111);
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
    pub global_position: u64,
    pub stream_position: StreamPos,
    // time_ms: u64,
    pub stream_name: Cow<'a, str>,
    pub message_type: Cow<'a, str>,
    pub data: Cow<'a, [u8]>,
    pub metadata: Option<Cow<'a, [u8]>>,
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
