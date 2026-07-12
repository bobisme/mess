//! Tournament candidates. Each is one `ExactDirectory` implementation; see
//! the crate docs for the roster and the per-module docs for layout details.

pub mod h0_hashmap;
pub mod h1_sorted;
pub mod h2_bitrank;
pub mod h3_pef;
pub mod h4_ptrhash;
pub mod h5_kbin;

pub use h0_hashmap::{FoldHashDir, SipHashDir};
pub use h1_sorted::SortedDir;
pub use h2_bitrank::BitRankDir;
pub use h3_pef::PefDir;
pub use h4_ptrhash::PtrHashDir;
pub use h5_kbin::KBinDir;
