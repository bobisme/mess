use thiserror::Error;

pub trait DisplayErr: std::error::Error + std::fmt::Display {}

impl<T> DisplayErr for T where T: std::error::Error + std::fmt::Display {}

#[derive(Error, Debug)]
pub enum Error<'a> {
    #[error(transparent)]
    DBError(#[from] mess_db::error::Error),
    #[error("{0}")]
    External(Box<dyn DisplayErr + 'a>),
    #[error("{0}")]
    ExternalString(String),
}

impl<'a> Error<'a> {
    #[inline]
    pub fn external(err: impl DisplayErr + 'a) -> Self {
        Self::External(Box::new(err))
    }
    #[inline]
    pub fn external_to_string(err: impl std::fmt::Display) -> Self {
        Self::ExternalString(err.to_string())
    }
}

pub type Result<'a, T> = core::result::Result<T, Error<'a>>;
