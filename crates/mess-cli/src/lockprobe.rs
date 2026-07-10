//! Read-only probe of the store's D9 single-writer lock.
//!
//! Every read-only command must work against a live-locked directory: it does
//! not take the lock, it *reports* the holder. This wraps
//! [`mess_log::lock::StoreLock`] so `doctor`/`inspect`/`verify` can say "held
//! by pid N" instead of failing.

use std::path::Path;

use mess_log::lock::{LockError, StoreLock};

/// The observed lock state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LockState {
    /// No live writer holds the store: the probe acquired and immediately
    /// released the lock. Exclusive operations are safe.
    Free,
    /// A live writer process holds the lock. `pid` is a best-effort
    /// diagnostic (`None` if it could not be cheaply read).
    Held { pid: Option<u32> },
    /// The lock file could not be opened/queried at all (a filesystem
    /// problem, not contention).
    Unknown { reason: String },
}

impl LockState {
    #[must_use]
    pub fn is_held(&self) -> bool { matches!(self, LockState::Held { .. }) }
}

/// Probe the lock without holding it: acquire-then-drop when free, report the
/// holder when contended. Never mutates committed data.
#[must_use]
pub fn probe(dir: &Path) -> LockState {
    match StoreLock::acquire(dir) {
        Ok(lock) => {
            // Immediately release: we only wanted to know it was free.
            drop(lock);
            LockState::Free
        }
        Err(LockError::HeldByOther { holder_pid, .. }) => {
            LockState::Held { pid: holder_pid }
        }
        Err(e) => LockState::Unknown { reason: e.to_string() },
    }
}
