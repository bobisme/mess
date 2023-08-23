pub mod error;
pub mod msg;
pub mod read;
pub mod rusqlite;
pub mod sqlx;
pub mod write;

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct Position {
    pub global: u64,
    pub stream: Option<u64>,
}

impl Position {
    pub fn new(global: u64, stream: Option<u64>) -> Self {
        Self { global, stream }
    }
}

#[derive(PartialEq, Eq, Debug)]
#[cfg_attr(feature = "sqlx", derive(::sqlx::FromRow))]
pub struct Message {
    global_position: i64,
    position: i64,
    time_ms: i64,
    stream_name: String,
    message_type: String,
    data: String,
    metadata: Option<String>,
    id: String,
}

impl TryFrom<&::rusqlite::Row<'_>> for Message {
    type Error = ::rusqlite::Error;

    fn try_from(row: &::rusqlite::Row<'_>) -> Result<Self, Self::Error> {
        Ok(Self {
            global_position: row.get(0)?,
            position: row.get(1)?,
            time_ms: row.get(2)?,
            stream_name: row.get(3)?,
            message_type: row.get(4)?,
            data: row.get(5)?,
            metadata: row.get(6)?,
            id: row.get(7)?,
        })
    }
}
