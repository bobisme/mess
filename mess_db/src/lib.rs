pub mod error;
pub mod msg;
pub mod read;
pub mod rocks;
pub mod rusqlite;
pub mod sqlx;
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
    Serial(u64),
    Causal(u64),
}

impl StreamPos {
    pub fn to_store(self) -> u64 {
        match self {
            StreamPos::Serial(pos) => pos << 1,
            StreamPos::Causal(pos) => (pos << 1) | 1,
        }
    }

    pub fn from_store(stored_position: u64) -> Self {
        match stored_position & 1 {
            0 => Self::Serial(stored_position >> 1),
            1 => Self::Causal(stored_position >> 1),
            _ => unreachable!(),
        }
    }

    // Returns the 63-bit position (as u64).
    pub fn position(self) -> u64 {
        match self {
            Self::Serial(pos) | Self::Causal(pos) => pos,
        }
    }

    pub fn next(self) -> Self {
        match self {
            StreamPos::Serial(pos) => Self::Serial(pos + 1),
            StreamPos::Causal(pos) => Self::Causal(pos + 1),
        }
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct Position {
    pub global: u64,
    pub stream: StreamPos,
}

impl Position {
    pub fn new(global: u64, stream: StreamPos) -> Self {
        Self { global, stream }
    }
}

#[derive(PartialEq, Eq, Debug)]
#[cfg_attr(feature = "sqlx", derive(::sqlx::FromRow))]
pub struct Message {
    global_position: u64,
    stream_position: StreamPos,
    // time_ms: u64,
    stream_name: String,
    message_type: String,
    data: Vec<u8>,
    metadata: Option<Vec<u8>>,
}

#[cfg(feature = "rusqlite")]
impl TryFrom<&::rusqlite::Row<'_>> for Message {
    type Error = ::rusqlite::Error;

    fn try_from(row: &::rusqlite::Row<'_>) -> Result<Self, Self::Error> {
        Ok(Self {
            global_position: row.get(0)?,
            stream_position: StreamPos::from_store(row.get(1)?),
            // time_ms: row.get(2)?,
            stream_name: row.get(3)?,
            message_type: row.get(4)?,
            data: row.get(5)?,
            metadata: row.get(6)?,
            // id: row.get(7)?,
        })
    }
}

#[cfg(test)]
mod test_stream_pos {
    use super::*;
    use assert2::assert;

    use StreamPos::{Causal, Serial};

    #[rstest::rstest]
    #[case(Serial(1))]
    #[case(Serial(1 << 48))]
    #[case(Causal(1))]
    #[case(Causal(1 << 48))]
    fn position_returns_the_actual_position(#[case] input: StreamPos) {
        let expected = match input {
            Serial(pos) | Causal(pos) => pos,
        };
        assert!(input.position() == expected);
    }

    #[rstest::rstest]
    #[case(Serial(1), 2)]
    #[case(Serial(1 << 48), 1 << 49)]
    #[case(Causal(1), 3)]
    #[case(Causal(1 << 48), (1 << 49) + 1)]
    fn to_store_shifts_and_sets_bit(
        #[case] input: StreamPos,
        #[case] expected: u64,
    ) {
        assert!(input.to_store() == expected)
    }

    #[rstest::rstest]
    #[case(2, Serial(1))]
    #[case(1 << 49, Serial(1 << 48))]
    #[case(3, Causal(1))]
    #[case((1 << 49) + 1, Causal(1 << 48))]
    fn from_store_shifts_and_reads_bit(
        #[case] input: u64,
        #[case] expected: StreamPos,
    ) {
        assert!(StreamPos::from_store(input) == expected)
    }
}
