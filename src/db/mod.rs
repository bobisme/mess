pub mod read;
pub mod sqlite;
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
