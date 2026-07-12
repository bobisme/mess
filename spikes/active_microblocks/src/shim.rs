//! Atomic + cell shim: `std` types normally, `loom` types under `--cfg loom`.
//!
//! Same convention as `spikes/state_kernel_dense_heads` / `crates/mess-log`:
//! the loom model in `tests/loom_model.rs` exercises the REAL publication
//! functions in [`crate::micro`], not a mirror — so every atomic and every
//! once-written plain field in the microblock protocol goes through this
//! shim. Under loom, `UnsafeCell` accesses are tracked and any read that
//! races a write (i.e. a reader touching an entry slot the `published`
//! release/acquire pair does not order) is reported as a model violation.

#[cfg(loom)]
pub use loom::sync::atomic::{AtomicPtr, AtomicU16, AtomicU32, AtomicU64};
#[cfg(not(loom))]
pub use std::sync::atomic::{AtomicPtr, AtomicU16, AtomicU32, AtomicU64};

pub use std::sync::atomic::Ordering;

/// A once-written plain cell (block headers, packed entries, overflow slots,
/// global checkpoints). Writes happen strictly before the release-store that
/// publishes them; reads happen strictly after the corresponding
/// acquire-load. Loom checks exactly that.
#[derive(Debug)]
pub struct OnceCell<T>(#[cfg(loom)] loom::cell::UnsafeCell<T>, #[cfg(not(loom))] std::cell::UnsafeCell<T>);

impl<T> OnceCell<T> {
    pub fn new(v: T) -> Self {
        #[cfg(loom)]
        return OnceCell(loom::cell::UnsafeCell::new(v));
        #[cfg(not(loom))]
        return OnceCell(std::cell::UnsafeCell::new(v));
    }

    /// Read the cell. Safety: caller must have observed (via acquire) the
    /// publication that ordered the write of this cell.
    #[inline(always)]
    pub unsafe fn read(&self) -> T
    where
        T: Copy,
    {
        #[cfg(loom)]
        return self.0.with(|p| unsafe { *p });
        #[cfg(not(loom))]
        return unsafe { *self.0.get() };
    }

    /// Write the cell. Safety: single writer, and the slot must not yet be
    /// published to any reader.
    #[inline(always)]
    pub unsafe fn write(&self, v: T) {
        #[cfg(loom)]
        self.0.with_mut(|p| unsafe { *p = v });
        #[cfg(not(loom))]
        unsafe {
            *self.0.get() = v;
        }
    }
}

// The single-writer/published-count protocol is what makes sharing sound;
// loom verifies it on the real functions.
unsafe impl<T: Send + Copy> Send for OnceCell<T> {}
unsafe impl<T: Send + Copy> Sync for OnceCell<T> {}
