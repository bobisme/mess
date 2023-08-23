use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error(transparent)]
    External(#[from] Box<dyn std::error::Error>),
    #[error(transparent)]
    #[cfg(feature = "sqlx")]
    SqlxError(#[from] sqlx::Error),
    #[error(transparent)]
    #[cfg(feature = "rusqlite")]
    RusqliteError(#[from] rusqlite::Error),
    #[error("database migration {0} failed: {1}")]
    MigrationFailed(i32, Box<dyn std::error::Error>),
    #[error(transparent)]
    JSONError(#[from] serde_json::Error),
    // #[error("failed to initialize database")]
    // InitFailure,
    // #[error("failed to set user_version")]
    // UserVersion,
    #[error("wrong stream pos for {}", stream)]
    WrongStreamPosition { stream: String },
    // #[error("the data for key `{0}` is not available")]
    // Redaction(String),
    // #[error("invalid header (expected {expected:?}, found {found:?})")]
    // InvalidHeader { expected: String, found: String },
    // #[error("unknown data store error")]
    // Unknown,
    #[error("could not get prepared statement {}", key)]
    PreparedStmtError { key: usize },
}

impl Error {
    pub fn external(err: Box<dyn std::error::Error>) -> Self {
        Self::External(err)
    }
}

pub type MessResult<T> = Result<T, Error>;
