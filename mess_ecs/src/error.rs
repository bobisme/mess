use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error(transparent)]
    DBError(#[from] mess_db::error::Error),
}
