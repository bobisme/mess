use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("{0}")]
    Other(String),

    #[error(transparent)]
    JoinError(#[from] tokio::task::JoinError),

    #[error(transparent)]
    RecvError(#[from] tokio::sync::oneshot::error::RecvError),

    // #[error(transparent)]
    // External(#[from] Box<dyn std::error::Error>),
    #[error(transparent)]
    #[cfg(feature = "rusqlite")]
    RusqliteError(#[from] ::rusqlite::Error),

    #[error(transparent)]
    #[cfg(feature = "rocksdb")]
    RocksDbError(#[from] ::rocksdb::Error),

    #[error("could not parse key to int")]
    #[cfg(feature = "rocksdb")]
    ParseKeyError,

    #[error("database migration {0} failed: {1}")]
    MigrationFailed(i32, String),

    #[error(transparent)]
    JSONError(#[from] serde_json::Error),
    // #[error("failed to initialize database")]
    // InitFailure,
    // #[error("failed to set user_version")]
    // UserVersion,
    #[error(
        "wrong stream pos for {}: expected {:?}, got {:?}",
        stream,
        expected,
        got
    )]
    WrongStreamPosition {
        stream: String,
        expected: Option<u64>,
        got: Option<u64>,
    },
    // #[error("the data for key `{0}` is not available")]
    // Redaction(String),
    // #[error("invalid header (expected {expected:?}, found {found:?})")]
    // InvalidHeader { expected: String, found: String },
    // #[error("unknown data store error")]
    // Unknown,
    #[error("could not get prepared statement {}", key)]
    PreparedStmtError { key: usize },

    #[error("record serialization: {0}")]
    SerError(String),

    #[error("record deserialization: {0}")]
    DeserError(String),

    #[error("error reading record: {0}")]
    ReadError(String),

    #[error("error writing record: {0}")]
    WriteError(String),
}

impl Error {
    // pub fn external(err: Box<dyn std::error::Error>) -> Self {
    //     Self::External(err)
    // }
}

pub type MessResult<T> = Result<T, Error>;
