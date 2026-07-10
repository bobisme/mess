//! Sim-capable runtime abstraction (bn-z98): the [`Clock`], [`Fs`] /
//! [`FileHandle`], and [`Runtime`] traits that put time, filesystem I/O,
//! and task spawning behind swappable implementations from the FIRST
//! Phase 3 commit — the design constraint that deterministic simulation
//! testing (DST) rests on.
//!
//! # Why this exists before the segment writer
//!
//! DST only works if every source of nondeterminism the committer touches
//! is behind a trait the test can substitute. Retrofitting these seams
//! after the committer/sealer (`bn-ise`) exist is miserable, so this bone
//! deliberately blocks that work: the segment writer is written against
//! [`Fs`]/[`Clock`], never against `std::fs`/`std::time` directly.
//!
//! # Trait surface — exactly what a segment writer + committer call
//!
//! Studied against [`docs/spec/01-log-format.md`] (segment/batch bytes)
//! and [`docs/spec/03-durability.md`] (group commit): the committer opens
//! a segment file, issues one **coalesced positioned write** per group,
//! one **`fdatasync`** barrier, `pread`s during recovery (and the
//! trailer-from-EOF R2 fast path needs the file length), and `rename`s
//! for atomic publish. That is the whole surface. Deliberately no
//! `read_dir`/`truncate` — the writer does not call them, and `bn-z98`'s
//! brief is "cover exactly that, do not over-abstract." Two methods were
//! added by `bn-36y` for the ENOSPC discipline: [`FileHandle::allocate`]
//! (segment preallocation at roll — the single point disk-full may strike)
//! and [`Fs::remove`] (cleaning up a segment whose preallocation failed, so
//! a failed roll leaves no husk). Both carry default impls so the rest of
//! the seam is untouched.
//!
//! # Zero-cost: generics, not `dyn`
//!
//! The hot path ([`FileHandle::pwrite`] / [`FileHandle::fdatasync`]) is
//! reached through **generic** (static) dispatch, so it monomorphizes to
//! the same code a direct `std::fs` call would — no vtable on the
//! append/commit path. Enum dispatch was the alternative; generics win
//! because the committer is generic over one concrete runtime for the
//! life of the store (no need to mix impls at runtime), so there is
//! nothing to pay a vtable for. See `examples/hotpath.rs` for the numbers
//! backing the no-regression claim.
//!
//! [`docs/spec/01-log-format.md`]: ../../../../docs/spec/01-log-format.md
//! [`docs/spec/03-durability.md`]: ../../../../docs/spec/03-durability.md

use std::future::Future;
use std::io;
use std::path::Path;
use std::time::Duration;

mod block_on;
pub mod real;
pub mod sim;
mod sim_fs;

#[cfg(test)]
mod testsuite;

pub use real::RealRuntime;
pub use sim::{Rng, SimRuntime};
pub use sim_fs::{CrashPlan, EnospcSite, Fault, SectorPlan, SimFs, TailPlan};

/// A monotonic instant, measured in nanoseconds since a runtime-defined
/// origin (real: process-relative; sim: virtual-time zero). Comparable and
/// hashable so the sim executor can order timers deterministically.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Instant {
    nanos: u64,
}

impl Instant {
    /// The runtime origin (`now()` at t=0).
    pub const ORIGIN: Instant = Instant { nanos: 0 };

    /// Nanoseconds since the origin.
    pub fn as_nanos(self) -> u64 { self.nanos }

    /// `self + dur`, saturating at `u64::MAX` nanos.
    pub fn saturating_add(self, dur: Duration) -> Instant {
        let add = u64::try_from(dur.as_nanos()).unwrap_or(u64::MAX);
        Instant { nanos: self.nanos.saturating_add(add) }
    }

    /// `self - earlier`, saturating at zero.
    pub fn saturating_duration_since(self, earlier: Instant) -> Duration {
        Duration::from_nanos(self.nanos.saturating_sub(earlier.nanos))
    }
}

/// How a file is opened. Deliberately tiny: the writer creates a segment
/// (read+write, create) or reopens one for recovery (read).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OpenOpts {
    pub read:     bool,
    pub write:    bool,
    pub create:   bool,
    pub truncate: bool,
}

impl OpenOpts {
    /// Create-or-open for the active segment: readable and writable.
    pub fn create_rw() -> Self {
        OpenOpts {
            read:     true,
            write:    true,
            create:   true,
            truncate: false,
        }
    }

    /// Reopen an existing file read-only (recovery scan / sealed reads).
    pub fn read_only() -> Self {
        OpenOpts {
            read:     true,
            write:    false,
            create:   false,
            truncate: false,
        }
    }
}

/// A virtual or real clock. `now` is monotonic; `sleep_until` is the only
/// async primitive the committer needs (the group-commit `max_delay` cap,
/// §2.2 of the durability spec, is a deadline, not a fixed sleep).
pub trait Clock: Clone {
    /// The current monotonic instant.
    fn now(&self) -> Instant;

    /// Resolve once the clock reaches `deadline` (immediately if already
    /// past). On the sim clock this advances *virtual* time only when
    /// every task is blocked, so a test never actually waits.
    fn sleep_until(&self, deadline: Instant)
    -> impl Future<Output = ()> + Send;

    /// Convenience: sleep for `dur` from `now`.
    fn sleep(&self, dur: Duration) -> impl Future<Output = ()> + Send {
        self.sleep_until(self.now().saturating_add(dur))
    }
}

/// The filesystem seam. A handle type ([`FileHandle`]) carries the hot
/// path; the `Fs` value itself is the namespace (open/rename).
pub trait Fs: Clone {
    /// The open-file handle. Shared (`&self` methods) so the committer can
    /// hand the same segment to a background sealer, matching the
    /// `Arc<File>` shape in `spikes/perf_append`. `Send + Clone` so a
    /// handle can be moved into a spawned actor.
    type File: FileHandle + Send + Clone;

    /// Open (and optionally create) the file at `path`.
    fn open(&self, path: &Path, opts: OpenOpts) -> io::Result<Self::File>;

    /// Atomically rename `from` to `to` (segment publish / manifest swap).
    fn rename(&self, from: &Path, to: &Path) -> io::Result<()>;

    /// Remove the file at `path`. The committer/writer surface deliberately
    /// avoids `remove` on the hot path; this exists for exactly ONE cleanup
    /// (`bn-36y`): a segment whose preallocation ([`FileHandle::allocate`])
    /// failed with `ENOSPC` at roll time must not be left behind as a
    /// zero-length husk, so
    /// [`SegmentWriter::create`](crate::writer::SegmentWriter::create)
    /// removes the never-headered file it just opened. The default returns
    /// `Unsupported`; [`RealFs`] and the sim fs override it. Callers of the
    /// cleanup path treat any error as best-effort (the husk carries no
    /// committed bytes, so recovery ignores it regardless).
    fn remove(&self, _path: &Path) -> io::Result<()> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "remove not supported by this Fs",
        ))
    }
}

/// An open file. Positioned I/O (`pwrite`/`pread`) plus the durability
/// barrier (`fdatasync`) — exactly the committer/recovery surface. All
/// methods take `&self`: a segment handle is shared, never re-`&mut`.
///
/// `len` is a fallible I/O query (`io::Result<u64>`), not a container
/// length; there is no meaningful `is_empty` companion.
#[allow(clippy::len_without_is_empty)]
pub trait FileHandle {
    /// Write `buf` at byte offset `off`, extending the file if needed.
    /// Returns the number of bytes written. Not durable until
    /// [`fdatasync`](FileHandle::fdatasync).
    fn pwrite(&self, off: u64, buf: &[u8]) -> io::Result<usize>;

    /// Read into `buf` starting at byte offset `off`. Returns bytes read
    /// (a short read at EOF, like `pread(2)`).
    fn pread(&self, off: u64, buf: &mut [u8]) -> io::Result<usize>;

    /// The durability barrier (D7): after this returns, everything written
    /// before it is durable. On the sim disks this is the fault-model
    /// commit point.
    fn fdatasync(&self) -> io::Result<()>;

    /// Current file length in bytes (R2: trailer `pread`-from-EOF).
    fn len(&self) -> io::Result<u64>;

    /// Preallocate `len` bytes of backing store for this file WITHOUT changing
    /// its logical length — real impl: `fallocate(2)` with
    /// `FALLOC_FL_KEEP_SIZE`, which reserves blocks but leaves `st_size` (and
    /// therefore [`len`](FileHandle::len) and every recovery `pread`)
    /// untouched. The point (`bn-36y`, `docs/spec/03-durability.md` §2.6): a
    /// segment is preallocated in full at roll time so a subsequent
    /// positioned write inside `[0, len)` cannot fail with `ENOSPC`
    /// mid-commit. Disk-full is thereby forced to strike at ONE predictable,
    /// recoverable point — the `allocate` call at roll — rather than
    /// scattered across the commit path.
    ///
    /// Returns an `ENOSPC` [`io::Error`] (`raw_os_error() == Some(ENOSPC)`)
    /// when the reservation cannot be satisfied. The default is a no-op
    /// (`Ok(())`) for handles that do not model space exhaustion; [`RealFile`]
    /// and the sim fs override it (the sim as a fault-injection point).
    fn allocate(&self, _len: u64) -> io::Result<()> { Ok(()) }
}

/// A whole runtime: a [`Clock`], a way to [`spawn`](Runtime::spawn) actor
/// tasks, and a bound [`Fs`]. The committer, the sealer, and the DST
/// harness are all generic over `R: Runtime` and are handed either
/// [`RealRuntime`] or [`SimRuntime`].
pub trait Runtime: Clock + Clone + Send + Sync + 'static {
    /// The filesystem bound to this runtime. `Send + Sync + 'static` so it
    /// can be handed to spawned actors.
    type Fs: Fs + Send + Sync + 'static;

    /// This runtime's filesystem handle.
    fn fs(&self) -> Self::Fs;

    /// Spawn a concurrent task. On [`SimRuntime`] the returned handle is
    /// scheduled by the seeded deterministic executor; on [`RealRuntime`]
    /// it runs on its own OS thread.
    fn spawn<F>(&self, fut: F) -> impl Future<Output = F::Output> + Send
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static;

    /// Drive `fut` to completion, running any tasks it spawns. This is the
    /// harness entry point (`block_on` in tests / `main`).
    fn block_on<F: Future>(&self, fut: F) -> F::Output;
}
