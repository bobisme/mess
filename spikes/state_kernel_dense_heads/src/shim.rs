//! Atomic shim: `std` atomics normally, `loom` atomics under `--cfg loom`.
//!
//! Same convention as `crates/mess-log`: loom is a dev-dep and the swap is
//! driven by a bare `--cfg loom` in RUSTFLAGS.

#[cfg(loom)]
pub use loom::sync::atomic::{AtomicPtr, AtomicU8, AtomicU64, AtomicUsize, fence};
#[cfg(not(loom))]
pub use std::sync::atomic::{AtomicPtr, AtomicU8, AtomicU64, AtomicUsize, fence};

pub use std::sync::atomic::Ordering;

/// One reader-retry pause. Under loom this must be a scheduling point so the
/// model's bounded search can make progress past the spin.
#[inline]
pub fn spin() {
    #[cfg(loom)]
    loom::thread::yield_now();
    #[cfg(not(loom))]
    core::hint::spin_loop();
}
