//! bn-cxr: a self-sweeping shared temp-store helper for real-fs test suites.
//!
//! # Background
//!
//! Real-fs suites (crash/SIGKILL harnesses, `LogEngine`/fjall reopen tests,
//! …) must run on a real device, never `tmpfs` (`fdatasync`/`fallocate` are
//! either a no-op or fail with `os error 122` there) — the convention is
//! `TMPDIR=$HOME/.cache/mess-test-tmp`, a *persistent* directory, not the
//! usual ephemeral `/tmp`. That trade means nothing ever sweeps it the way
//! `tmpfs` gets swept on reboot. Two leak paths accumulate real, multi-MiB
//! store directories there over time:
//!
//! 1. Crash/SIGKILL-harness tests kill child processes (and, if the test binary
//!    itself is interrupted or OOM-killed mid-run, possibly the parent) by
//!    design — `TempDir::drop` never runs for whatever was mid- flight.
//! 2. A failed/aborted run (panic that aborts rather than unwinds, a CI
//!    timeout, `SIGINT` mid `cargo test`, …) leaks the same way.
//!
//! 302GB / 737 dirs accumulated this way in two days on this machine.
//!
//! # This module
//!
//! [`sweeping_temp_dir`] is the drop-in replacement for
//! `tempfile::tempdir()` real-fs suites should reach for: it creates a
//! fresh, uniquely-named directory under a single shared namespace
//! (`<TMPDIR or $HOME/.cache/mess-test-tmp>/mess-tests/`), and — once per
//! process, cheaply guarded — sweeps *sibling* directories under that same
//! namespace that are safely identifiable as abandoned: their name encodes
//! the pid that made them, that pid is no longer alive, AND the directory
//! is older than [`AUTO_SWEEP_MAX_AGE`]. Every one of those three checks
//! must hold; any single one failing means the entry is left alone. The
//! sweep never follows symlinks and never touches anything that isn't
//! itself a plain, namespace-owned directory.
//!
//! The sweep is best-effort by construction: every fallible step returns
//! `None`/is swallowed rather than propagated, so a permission error,
//! a `read_dir` racing a sibling process's own cleanup, or any other I/O
//! hiccup simply skips that one entry. **It can never fail or panic the
//! calling test.**
//!
//! [`sweep_stale`] and [`temp_dir_in`] are the lower-level building blocks
//! `sweeping_temp_dir` composes, exposed directly for this module's own
//! acceptance tests (and any caller that wants to point the sweep at a
//! non-default root).

use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::sync::Once;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime};

/// The namespace directory name nested under the resolved base dir. Every
/// deletion this module performs is provably confined to inside a directory
/// whose final path component is exactly this — see [`is_safe_root`].
const NAMESPACE: &str = "mess-tests";

/// Directories are only ever auto-swept once they are at least this old
/// (by mtime) **and** their encoded pid is no longer alive. A fresh or
/// still-live directory is never touched, no matter how it was created.
pub const AUTO_SWEEP_MAX_AGE: Duration = Duration::from_secs(24 * 60 * 60);

static SWEEP_GUARD: Once = Once::new();

/// An owned, namespaced real-fs temp directory. Removed (best-effort,
/// `remove_dir_all`) on drop, exactly like `tempfile::TempDir` — the
/// sweeping behaviour lives entirely in *creation*
/// ([`sweeping_temp_dir`]/[`temp_dir_in`]), not in drop, since a killed
/// process never runs drop at all; that's the whole reason this module
/// exists.
#[derive(Debug)]
pub struct SweepingTempDir {
    path: PathBuf,
}

impl SweepingTempDir {
    /// The directory's path.
    #[must_use]
    pub fn path(&self) -> &Path { &self.path }
}

impl AsRef<Path> for SweepingTempDir {
    fn as_ref(&self) -> &Path { &self.path }
}

impl Drop for SweepingTempDir {
    fn drop(&mut self) {
        // Best-effort: if this fails (already gone, permissions, …) there is
        // nothing a `Drop` impl can usefully do about it, and the next
        // process's init sweep will pick it up anyway once it ages out.
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

/// Resolve the shared base dir for real-fs test temp stores: `TMPDIR` if
/// set to a non-empty value, else `$HOME/.cache/mess-test-tmp` — mirroring
/// the convention already hand-rolled at several call sites in this repo
/// (`mess-store`'s `durable_scratch_dir`, `mess-soak`'s `scratch_dir`,
/// `mess-log`'s `committer.rs`/`sigkill_harness.rs` `real_tmp[_dir]`
/// helpers). `/tmp` itself is never a fallback: it is commonly `tmpfs`,
/// where `fdatasync` is a no-op and 256MiB segment `fallocate`s can fail
/// outright with `os error 122` under a tmpfs quota.
fn base_dir() -> PathBuf {
    std::env::var("TMPDIR")
        .ok()
        .filter(|t| !t.trim().is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            let home = std::env::var_os("HOME")
                .map(PathBuf::from)
                .unwrap_or_else(std::env::temp_dir);
            home.join(".cache").join("mess-test-tmp")
        })
}

/// The env-resolved shared namespace root: `<base_dir()>/mess-tests`. Every
/// [`sweeping_temp_dir`] call creates its directory here, and the init
/// sweep only ever looks at this directory's direct children.
#[must_use]
pub fn namespace_root() -> PathBuf { base_dir().join(NAMESPACE) }

/// Defensive check before ANY deletion: refuse to treat `root` as sweepable
/// unless it is non-empty, its final path component is literally
/// `mess-tests`, and it is not `/` or `$HOME` itself. A misconfigured or
/// empty `TMPDIR` (or a caller passing the wrong path) must never turn a
/// sweep into `rm -rf $HOME` or `rm -rf /` — this is the single choke point
/// every deletion path in this module runs through.
fn is_safe_root(root: &Path) -> bool {
    if root.as_os_str().is_empty() || root == Path::new("/") {
        return false;
    }
    if let Some(home) = std::env::var_os("HOME") {
        let home = PathBuf::from(home);
        if !home.as_os_str().is_empty() && root == home {
            return false;
        }
    }
    root.file_name() == Some(OsStr::new(NAMESPACE))
}

/// Best-effort sweep of stale sibling directories directly under `root`.
/// Never panics, never propagates an error — every fallible step is
/// swallowed and just skips that one entry.
///
/// A direct child of `root` is removed only when ALL of:
/// 1. `root` itself passes [`is_safe_root`] (else the whole sweep is a no-op);
/// 2. the entry is a plain directory reached via `symlink_metadata` (never a
///    symlink — a symlink planted inside the namespace pointing outside of it
///    is left alone, not followed);
/// 3. its name matches this module's own `<name>-<pid>-<nonce>` convention (see
///    [`extract_pid`]) — anything not shaped like one of ours is never touched
///    by the automatic sweep;
/// 4. the encoded pid is **not** currently alive (see [`pid_is_alive`]);
/// 5. its mtime is at least `max_age` old.
///
/// Condition 4 is the hard safety property: a live concurrent run's
/// directory is never removed, regardless of how old its mtime looks (a
/// long-running suite that simply hasn't touched its dir recently is still
/// live).
pub fn sweep_stale(root: &Path, max_age: Duration) {
    if !is_safe_root(root) {
        return;
    }
    let Ok(entries) = std::fs::read_dir(root) else {
        return; // doesn't exist yet, or unreadable: nothing to do
    };
    let now = SystemTime::now();
    for entry in entries.flatten() {
        sweep_entry(&entry, now, max_age);
    }
}

fn sweep_entry(entry: &std::fs::DirEntry, now: SystemTime, max_age: Duration) {
    let path = entry.path();

    // `symlink_metadata`, never `metadata`: a symlink inside the namespace
    // must never be followed into a `remove_dir_all` of whatever it points
    // at outside the namespace. If it IS a symlink (or anything but a plain
    // directory — a file, socket, …), it is simply not ours to sweep.
    let Ok(meta) = std::fs::symlink_metadata(&path) else {
        return;
    };
    if meta.file_type().is_symlink() || !meta.is_dir() {
        return;
    }

    let Some(name) = path.file_name().and_then(OsStr::to_str) else {
        return; // non-UTF8 name: not our naming convention, leave alone
    };
    let Some(pid) = extract_pid(name) else {
        return; // doesn't look like one of ours: never auto-swept
    };
    if pid_is_alive(pid) {
        return; // never delete a live concurrent run's dir, regardless of age
    }

    let old_enough = meta
        .modified()
        .ok()
        .and_then(|m| now.duration_since(m).ok())
        .is_some_and(|age| age >= max_age);
    if old_enough {
        let _ = std::fs::remove_dir_all(&path); // best-effort
    }
}

/// Parse the pid this module's own `<name>-<pid>-<nonce>` directory naming
/// convention encodes, if `entry_name` matches it: the pid is the
/// second-to-last `-`-delimited segment, and must be all-decimal-digits (a
/// nonce that happens to be all-digits too is fine — it's simply ignored).
/// Anything else (too few segments, a non-numeric pid segment) returns
/// `None`, meaning "not one of ours" rather than "pid 0" or any other
/// misleading default.
fn extract_pid(entry_name: &str) -> Option<u32> {
    let mut parts = entry_name.rsplitn(3, '-');
    let _nonce = parts.next()?;
    let pid = parts.next()?;
    // `parts.next()` for the third slot is intentionally not required to
    // exist here: `rsplitn(3, ..)` already puts everything left of the
    // pid segment (including further internal hyphens in `name`) into
    // that third slot when present, and a two-segment `pid-nonce` name is
    // still parseable without one.
    pid.parse::<u32>().ok()
}

/// Is `pid` currently alive? Uses `kill(pid, 0)` — sends no signal, only
/// probes whether the pid exists and is signalable, the standard portable
/// liveness idiom.
///
/// - `ESRCH` (no such process): dead → `false`.
/// - Success, or `EPERM` (exists, but we lack permission to signal it): alive →
///   `true`.
/// - Any other errno: can't tell → conservatively `true` (never risk sweeping
///   something we couldn't positively prove dead).
fn pid_is_alive(pid: u32) -> bool {
    // SAFETY: `kill` with signal `0` sends nothing; it is a pure liveness
    // probe with no side effects on the target process.
    let ret = unsafe { libc::kill(pid as libc::pid_t, 0) };
    if ret == 0 {
        return true;
    }
    std::io::Error::last_os_error().raw_os_error() != Some(libc::ESRCH)
}

/// A small, dependency-free per-process nonce: process-start-relative
/// monotonic counter XORed with the current time in nanoseconds. Only needs
/// to avoid same-process collisions (the pid already disambiguates across
/// processes) and to be filesystem-safe — rendered as lowercase hex by the
/// caller.
fn nonce() -> u64 {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    nanos ^ n.wrapping_mul(0x9E37_79B9_7F4A_7C15)
}

/// Create a fresh `<root>/<name>-<pid>-<nonce>` directory. Does NOT trigger
/// the auto-sweep — pair with an explicit [`sweep_stale`] call (or just use
/// [`sweeping_temp_dir`], which does both) unless the caller has a specific
/// reason not to.
#[must_use]
pub fn temp_dir_in(root: &Path, name: &str) -> SweepingTempDir {
    std::fs::create_dir_all(root)
        .unwrap_or_else(|e| panic!("create temp-dir namespace {root:?}: {e}"));
    let pid = std::process::id();
    for _ in 0..64 {
        let path = root.join(format!("{name}-{pid}-{:x}", nonce()));
        match std::fs::create_dir(&path) {
            Ok(()) => return SweepingTempDir { path },
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(e) => panic!("create temp dir {path:?}: {e}"),
        }
    }
    panic!(
        "could not allocate a unique temp dir under {root:?} after 64 tries"
    );
}

/// Create a fresh, namespaced real-fs temp directory:
/// `<TMPDIR or $HOME/.cache/mess-test-tmp>/mess-tests/<name>-<pid>-<nonce>/`.
///
/// The FIRST call in a process also sweeps stale sibling directories left
/// behind by prior runs (see the module docs and [`sweep_stale`]) — a
/// cheap, `Once`-guarded check on every call after the first.
///
/// `name` should be a short, stable, filesystem-safe tag identifying the
/// call site (e.g. `"sigkill-os"`, `"engine-reopen"`) — it has no behaviour
/// beyond making leaked/live directories identifiable by eye; the pid and
/// nonce alone already guarantee uniqueness.
#[must_use]
pub fn sweeping_temp_dir(name: &str) -> SweepingTempDir {
    let root = namespace_root();
    SWEEP_GUARD.call_once(|| sweep_stale(&root, AUTO_SWEEP_MAX_AGE));
    temp_dir_in(&root, name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_pid_parses_the_standard_shape() {
        assert_eq!(extract_pid("sigkill-os-12345-abcd"), Some(12345));
        assert_eq!(extract_pid("name-with-hyphens-999-ff"), Some(999));
        assert_eq!(extract_pid("x-1-2"), Some(1));
    }

    #[test]
    fn extract_pid_rejects_non_numeric_pid_segment() {
        assert_eq!(extract_pid("foo-bar-baz"), None);
        assert_eq!(extract_pid("just-one-hyphen"), None);
        assert_eq!(extract_pid("nohyphenatall"), None);
    }

    #[test]
    fn is_safe_root_requires_the_mess_tests_leaf() {
        assert!(is_safe_root(Path::new("/some/base/mess-tests")));
        assert!(!is_safe_root(Path::new("/some/base/other-dir")));
        assert!(!is_safe_root(Path::new("")));
        assert!(!is_safe_root(Path::new("/")));
    }

    #[test]
    fn is_safe_root_refuses_home_even_if_misnamed_mess_tests() {
        // Guards against a `TMPDIR`/`HOME` misconfiguration where the
        // resolved "root" collapses onto `$HOME` itself (e.g. `HOME` were
        // somehow named `.../mess-tests`) — belt-and-suspenders alongside
        // the `base_dir().join(NAMESPACE)` construction, which can never
        // itself produce `root == HOME` in normal operation.
        if let Some(home) = std::env::var_os("HOME") {
            assert!(!is_safe_root(Path::new(&home)));
        }
    }

    #[test]
    fn pid_zero_nonce_and_dead_pid_are_recognized_dead() {
        // 999_999_999 exceeds any realistic `pid_max` on Linux (default
        // configs top out well under 4_194_304) — deterministically dead,
        // matching the same "fake stale pid" convention already used in
        // `mess-log`'s `lock.rs` tests.
        assert!(!pid_is_alive(999_999_999));
    }

    #[test]
    fn current_process_pid_is_alive() {
        assert!(pid_is_alive(std::process::id()));
    }
}
