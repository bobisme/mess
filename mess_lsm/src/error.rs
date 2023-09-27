#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error(transparent)]
    IoError(#[from] std::io::Error),
    #[error("{0}")]
    ZeroVecError(zerovec::ZeroVecError),
    #[error(transparent)]
    PostcardError(#[from] postcard::Error),
    #[error("the block is full")]
    BlockFull,
}

impl PartialEq for Error {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::IoError(l0), Self::IoError(r0)) => {
                l0.to_string() == r0.to_string()
            }
            (Self::ZeroVecError(l0), Self::ZeroVecError(r0)) => l0 == r0,
            (Self::PostcardError(l0), Self::PostcardError(r0)) => l0 == r0,
            _ => {
                core::mem::discriminant(self) == core::mem::discriminant(other)
            }
        }
    }
}

impl Eq for Error {}

impl From<zerovec::ZeroVecError> for Error {
    fn from(err: zerovec::ZeroVecError) -> Self {
        Self::ZeroVecError(err)
    }
}

pub type Result<T> = core::result::Result<T, Error>;
