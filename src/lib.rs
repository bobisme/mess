#[warn(clippy::pedantic, clippy::perf, clippy::nursery)]
pub mod db {
    pub use mess_db::*;
}
pub mod ecs {
    pub use mess_ecs::*;
}
