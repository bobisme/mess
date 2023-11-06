#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error(transparent)]
    IoError(#[from] std::io::Error),
    #[error("{0}")]
    ZeroVecError(zerovec::ZeroVecError),
    #[error(transparent)]
    PostcardError(#[from] postcard::Error),
    // #[error(transparent)]
    // BincodeError(#[from] bincode::Error),
    #[error("invalid block header")]
    InvalidBlockHeader,
    #[error("invalid header")]
    InvalidHeader,
    #[error("invalid entry at index: {index:?}")]
    InvalidEntry { index: Option<usize> },
    #[error("the block is full")]
    BlockFull,
    #[error("the entry is larger than the buffer")]
    EntryLargerThanBuffer,
    #[error("list is full")]
    ListFull,
    #[error("failed to reserve {size} bytes at {index}")]
    ReserveFailed { size: usize, index: usize },
    #[error("tried to push when bipbuffer region is full")]
    RangeFull,
    #[error("tried to pop when bipbuffer region is empty")]
    RangeEmpty,
    #[error("tried to push when bipbuffer region is full")]
    RegionFull,
    #[error("tried to pop when bipbuffer region is empty")]
    RegionEmpty,
    #[error("could not create a new reader")]
    ReaderBlocked,
    #[error("could not create a new writer")]
    WriterBlocked,
    #[error("can't release writer; not from this BBPP")]
    NotOwnWriter,
    #[error("...inconceivable...")]
    Inconceivable,
}

impl PartialEq for Error {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::IoError(l0), Self::IoError(r0)) => {
                l0.to_string() == r0.to_string()
            }
            (Self::ZeroVecError(l0), Self::ZeroVecError(r0)) => l0 == r0,
            (Self::PostcardError(l0), Self::PostcardError(r0)) => l0 == r0,
            (
                Self::InvalidEntry { index: l },
                Self::InvalidEntry { index: r },
            ) => l == r,
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
