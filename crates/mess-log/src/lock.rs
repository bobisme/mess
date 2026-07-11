//! `StoreLock`: single-writer-process enforcement for a store directory
//! (D9, `notes/mess-research/12_convergence.md`):
//!
//! ```text
//! one writer process per store directory, enforced by an OS lock file
//! many threads within that process
//! server mode: the server process owns the dir; clients speak a protocol
//! no cleverness
//! ```
//!
//! This is deliberately the dumbest thing that works: a single `LOCK`
//! file inside the store directory, held with the OS's own advisory
//! file lock (`flock(2)` on Unix, `LockFileEx` on Windows) via
//! `std::fs::File::try_lock` (stable since Rust 1.89 — no `rustix`/`fs2`
//! dependency needed; this crate stays at zero non-dev dependencies
//! plus `thiserror` for the error type). RocksDB and friends do exactly
//! this; there is no cleverer scheme to reach for.
//!
//! # Semantics — read this before relying on any of it
//!
//! **What is locked is the *open file description*, not the file's
//! bytes or its path.** The advisory lock lives in kernel state
//! attached to the `open()` call that produced our file descriptor. It
//! is automatically and unconditionally released when every descriptor
//! referencing that open file description is closed — on a clean
//! `Drop`, on `std::process::exit`, on a panic unwind, **and on
//! `SIGKILL`**, because closing every fd of a dying process is a kernel
//! teardown step that runs with no userspace cooperation at all. There
//! is no way for a process to leak a held `flock` past its own death.
//!
//! **What is *not* automatically cleaned up is the `LOCK` file itself.**
//! Its on-disk bytes (a diagnostic PID, written best-effort — see
//! below) survive both a clean close and a crash identically. This
//! implementation never deletes or renames the `LOCK` file on release,
//! *on purpose*: if clean shutdown deleted it while a crash left it
//! behind, the file's mere existence would look like a signal, and the
//! next writer (or an operator) might be tempted to use "does LOCK
//! exist" as a liveness check. That is exactly the cleverness D9 rules
//! out. The file existing means nothing; only a currently-held OS lock
//! means something, and only `try_lock` on that exact path can observe
//! it.
//!
//! **Consequence: a stale `LOCK` file left after `SIGKILL` MUST NOT,
//! and does not, block a fresh acquire.** The next `StoreLock::acquire`
//! opens the same path (creating it if it somehow doesn't exist),
//! `try_lock`s it, finds no live holder, and succeeds — the leftover
//! bytes are simply overwritten with the new holder's PID. See
//! [`acquire_after_stale_lock_file_from_dead_holder`](tests) for the
//! in-process approximation of this, and the note below on what that
//! test can't cover.
//!
//! # What can't be tested in-process
//!
//! A real `SIGKILL` of a distinct OS process can't be exercised inside
//! `cargo test -p mess-log` — that would need a helper binary, a
//! `Command::spawn`, and a `child.kill()`, which is out of scope for
//! this crate's unit tests (and would be a flaky addition to a fast
//! test suite). Instead the stale-file test opens a **second file
//! handle to the same LOCK path within the same test process**, locks
//! it, and drops that raw handle *without* going through
//! [`StoreLock`]'s `Drop` (so no application cleanup code runs at
//! all — only the kernel's fd-close). That is a faithful approximation
//! of what a `SIGKILL` teardown does to the lock (kernel closes the fd,
//! nothing else runs); it does not, and cannot, prove that a genuinely
//! separate OS process behaves the same way. That property rests on
//! `flock`/`LockFileEx` semantics documented by the OS, not on this
//! crate.
//!
//! # PID reporting is best-effort
//!
//! When `try_lock` reports the file is already held, we separately
//! (and without needing the lock) read the file's bytes and try to
//! parse a PID the current holder wrote when *it* acquired the lock.
//! This is racy and advisory only: the PID could be stale (overwritten
//! moments later), unparsable (partially written), or simply absent
//! (I/O error reading it). None of that affects correctness — the
//! `HeldByOther` error fires purely off `try_lock`'s `WouldBlock`; the
//! PID is decoration for a human reading the error message, never a
//! decision input.

use std::fmt;
use std::fs::{File, OpenOptions, TryLockError};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

/// Name of the lock file created inside the store directory.
pub const LOCK_FILE_NAME: &str = "LOCK";

/// Typed failure modes for [`StoreLock::acquire`].
#[derive(Debug, thiserror::Error)]
pub enum LockError {
    /// Another process already holds the store lock. `holder_pid` is a
    /// best-effort diagnostic (see module docs) — `None` if it could
    /// not be cheaply determined.
    #[error(
        "store dir {dir} is already locked by another process{}",
        HolderPid(*holder_pid)
    )]
    HeldByOther { dir: PathBuf, holder_pid: Option<u32> },
    /// Could not open (or create) the `LOCK` file at all — a store-dir
    /// or filesystem problem, not a contention problem.
    #[error("failed to open lock file {path}: {source}")]
    Open { path: PathBuf, source: io::Error },
    /// `try_lock` failed for a reason other than contention (e.g. the
    /// platform/filesystem doesn't support advisory locks at all).
    #[error("failed to acquire OS lock on {path}: {source}")]
    Lock { path: PathBuf, source: io::Error },
}

/// Formats `" (pid N)"` or `""` — factored out so the `#[error(...)]`
/// format string above stays a plain string literal.
struct HolderPid(Option<u32>);

impl fmt::Display for HolderPid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            Some(pid) => write!(f, " (pid {pid})"),
            None => Ok(()),
        }
    }
}

/// A held OS advisory lock on a store directory's `LOCK` file (D9).
///
/// Enforces one writer process per store directory. The lock is held
/// for the lifetime of this value and released on [`Drop`] — see the
/// module docs for exactly what "released" does and doesn't mean.
#[derive(Debug)]
pub struct StoreLock {
    file: File,
    path: PathBuf,
}

impl StoreLock {
    /// Acquire the single-writer lock on `dir`.
    ///
    /// Creates `dir/LOCK` if it does not exist (creating `dir` itself
    /// is the caller's responsibility). Fails immediately —
    /// non-blocking — with [`LockError::HeldByOther`] if another live
    /// process already holds it.
    pub fn acquire(dir: impl AsRef<Path>) -> Result<Self, LockError> {
        let dir = dir.as_ref();
        let path = dir.join(LOCK_FILE_NAME);

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            // We must NOT truncate on open: truncating happens only
            // after we hold the OS lock (write_holder_pid), never as
            // a side effect of merely opening the path.
            .truncate(false)
            .open(&path)
            .map_err(|source| LockError::Open { path: path.clone(), source })?;

        match file.try_lock() {
            Ok(()) => {}
            Err(TryLockError::WouldBlock) => {
                return Err(LockError::HeldByOther {
                    dir:        dir.to_path_buf(),
                    holder_pid: read_holder_pid(&path),
                });
            }
            Err(TryLockError::Error(source)) => {
                return Err(LockError::Lock { path: path.clone(), source });
            }
        }

        // Best-effort diagnostic for the *next* process that loses the
        // race against us — see module docs. Never fails acquisition:
        // the OS lock above is the only thing correctness depends on.
        let _ = write_holder_pid(&mut file);

        Ok(StoreLock { file, path })
    }

    /// The path of the `LOCK` file this value holds.
    pub fn path(&self) -> &Path { &self.path }
}

impl Drop for StoreLock {
    fn drop(&mut self) {
        // Explicit for documentation purposes: dropping `self.file`
        // right after this would release the OS lock on its own (fd
        // close does it), same as it would under SIGKILL with no
        // userspace code running at all. Calling `unlock` here just
        // makes the clean-close path say what it means. Errors are
        // not actionable in a `Drop`.
        let _ = self.file.unlock();
    }
}

/// Truncate and overwrite the lock file with our PID, for the benefit
/// of a future process that loses the race and wants a diagnostic.
fn write_holder_pid(file: &mut File) -> io::Result<()> {
    file.set_len(0)?;
    file.seek(SeekFrom::Start(0))?;
    writeln!(file, "{}", std::process::id())?;
    file.flush()
}

/// Best-effort read of whatever PID the current holder (if any) wrote.
/// Never fails the caller — any error or unparsable content is `None`.
fn read_holder_pid(path: &Path) -> Option<u32> {
    let mut f = File::open(path).ok()?;
    let mut buf = String::new();
    f.read_to_string(&mut buf).ok()?;
    buf.lines().next()?.trim().parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A self-sweeping real-fs temp dir (bn-2jr), unique per test, tagged by
    /// call site. Dev-dependency only — this crate's production deps stay
    /// just `thiserror`.
    fn test_dir(name: &str) -> mess_testkit::SweepingTempDir {
        mess_testkit::sweeping_temp_dir(&format!("store-lock-{name}"))
    }

    // bn-25j: touches the real OS filesystem (`test_dir` + `StoreLock`'s
    // `std::fs::File`/`OpenOptions`); Miri's isolation blocks real `open`
    // (`unsupported operation: \`open\` not available when isolation is
    // enabled`), so this is excluded from the Miri lane.
    #[test]
    #[cfg_attr(miri, ignore)]
    fn first_open_succeeds_and_writes_lock_file() {
        let dir = test_dir("first-open");
        let lock = StoreLock::acquire(dir.path()).expect("first open");
        assert_eq!(lock.path(), dir.path().join(LOCK_FILE_NAME));
        assert!(lock.path().exists());
    }

    // bn-25j: real fs (see note above).
    #[test]
    #[cfg_attr(miri, ignore)]
    fn second_open_of_same_dir_fails_typed_and_names_dir_and_pid() {
        let dir = test_dir("second-open");
        let _first = StoreLock::acquire(dir.path()).expect("first open");

        let err = StoreLock::acquire(dir.path())
            .expect_err("second open of a live-locked dir must fail");
        match &err {
            LockError::HeldByOther { dir: named, holder_pid } => {
                assert_eq!(named, dir.path());
                // We hold the lock in *this* process, so the PID we
                // wrote on acquire is readable and is our own pid.
                assert_eq!(*holder_pid, Some(std::process::id()));
            }
            other => panic!("expected HeldByOther, got {other:?}"),
        }
        // The typed error's Display names the dir and the PID, per
        // the acceptance criterion ("clear typed error naming the dir
        // and (if cheaply available) the holding PID").
        let msg = err.to_string();
        assert!(msg.contains(&dir.path().display().to_string()));
        assert!(msg.contains(&std::process::id().to_string()));
    }

    // bn-25j: real fs (see note above).
    #[test]
    #[cfg_attr(miri, ignore)]
    fn clean_close_releases_the_lock_for_a_new_acquire() {
        let dir = test_dir("clean-close");
        let lock = StoreLock::acquire(dir.path()).expect("first open");
        drop(lock);

        // Drop released the OS lock; a fresh acquire must succeed,
        // reusing the same LOCK file (never deleted, see module
        // docs).
        let second = StoreLock::acquire(dir.path())
            .expect("reacquire after clean close");
        drop(second);
    }

    // bn-25j: real fs (see note above).
    #[test]
    #[cfg_attr(miri, ignore)]
    fn reacquire_after_drop_can_repeat_indefinitely() {
        // Not just once: acquire/release must be repeatable, since a
        // real store dir is opened and closed many times over its
        // life.
        let dir = test_dir("repeat");
        for _ in 0..5 {
            let lock = StoreLock::acquire(dir.path()).expect("acquire");
            drop(lock);
        }
    }

    // bn-25j: real fs (see note above).
    #[test]
    #[cfg_attr(miri, ignore)]
    fn stale_lock_file_from_dead_holder_does_not_block_reacquire() {
        // Approximates a SIGKILL'd holder: a second, independent file
        // handle to the SAME lock path is opened and locked in this
        // same test process (std::fs::File, deliberately NOT wrapped
        // in StoreLock, so no application Drop code runs at all when
        // it goes away) — then dropped by falling out of scope. That
        // leaves exactly what a killed process leaves: a LOCK file on
        // disk with another process's stale PID bytes in it, and NO
        // live OS lock, because the kernel released it the moment the
        // fd closed. A genuine cross-process SIGKILL cannot be
        // exercised in this in-process unit test — see the module
        // docs' "What can't be tested in-process" section.
        let dir = test_dir("stale-file");
        let lock_path = dir.path().join(LOCK_FILE_NAME);

        {
            let mut dead_holder = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(&lock_path)
                .expect("open raw handle");
            dead_holder.try_lock().expect("raw handle takes the OS lock");
            writeln!(dead_holder, "999999999").expect("stamp a fake stale pid");
            // `dead_holder` drops here: fd closes, OS lock releases.
            // No unlock() call, no StoreLock::drop — this is the part
            // that models "no userspace code ran".
        }

        assert!(
            lock_path.exists(),
            "the LOCK file itself must survive — only the OS lock is gone"
        );

        let acquired = StoreLock::acquire(dir.path());
        assert!(
            acquired.is_ok(),
            "a stale LOCK FILE (unlocked) must not block a new writer: {:?}",
            acquired.err()
        );
    }

    // bn-25j: real fs (see note above).
    #[test]
    #[cfg_attr(miri, ignore)]
    fn concurrent_live_holder_still_blocks_even_with_stale_bytes_present() {
        // Companion to the stale-file test: prove we are not
        // accidentally keying off file *content* or *existence* by
        // pre-seeding a stale-looking pid, then taking a live lock
        // over it, and confirming a second acquire still fails.
        let dir = test_dir("live-over-stale-bytes");
        let lock_path = dir.path().join(LOCK_FILE_NAME);
        std::fs::write(&lock_path, "123456\n")
            .expect("seed stale-looking bytes");

        let _live = StoreLock::acquire(dir.path()).expect("live acquire");
        let err = StoreLock::acquire(dir.path()).expect_err(
            "a live holder must still block, regardless of prior bytes",
        );
        assert!(matches!(err, LockError::HeldByOther { .. }));
    }

    #[test]
    fn error_display_without_pid_omits_the_parenthetical() {
        let err = LockError::HeldByOther {
            dir:        PathBuf::from("/tmp/example-store"),
            holder_pid: None,
        };
        let msg = err.to_string();
        assert!(msg.contains("/tmp/example-store"));
        assert!(!msg.contains("pid"));
    }
}
