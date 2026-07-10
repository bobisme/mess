#[warn(clippy::pedantic, clippy::perf, clippy::nursery)]
pub mod db {
    pub use mess_db::*;
}
pub mod ecs {
    #[allow(deprecated)]
    pub use mess_ecs::error::*;
    #[allow(deprecated)]
    pub use mess_ecs::*;
}
