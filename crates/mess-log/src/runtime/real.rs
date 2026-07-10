//! The production runtime: real monotonic time, `std::fs` positioned I/O
//! with a true `fdatasync`, and OS-thread task spawning. No async
//! framework dependency — the only `Pending` sources are join handles
//! (parked via [`block_on`](super::block_on::block_on)) and thread-blocking
//! sleeps, each isolated on the spawning task's own thread.
//!
//! The hot path ([`RealFile::pwrite`] / [`RealFile::fdatasync`]) is a
//! direct `pwrite(2)` / `fdatasync(2)` via `std::os::unix::fs::FileExt` —
//! reached through generics, so the committer pays no dispatch cost over
//! calling `std::fs` itself (see `examples/hotpath.rs`).

use std::fs::{File, OpenOptions};
use std::future::Future;
use std::io;
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::time::Instant as StdInstant;

use super::block_on::block_on;
use super::{Clock, FileHandle, Fs, Instant, OpenOpts, Runtime};

/// The production filesystem: thin wrappers over `std::fs`.
#[derive(Clone, Copy, Default)]
pub struct RealFs;

impl Fs for RealFs {
    type File = RealFile;

    fn open(&self, path: &Path, opts: OpenOpts) -> io::Result<RealFile> {
        // No append mode: positioned writes address an explicit offset.
        let file = OpenOptions::new()
            .read(opts.read)
            .write(opts.write)
            .create(opts.create)
            .truncate(opts.truncate)
            .open(path)?;
        Ok(RealFile { file: Arc::new(file) })
    }

    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        std::fs::rename(from, to)
    }

    fn remove(&self, path: &Path) -> io::Result<()> {
        std::fs::remove_file(path)
    }
}

/// An open production file. `Arc<File>` so a segment handle can be shared
/// with a background sealer, matching `spikes/perf_append`'s shape.
#[derive(Clone)]
pub struct RealFile {
    file: Arc<File>,
}

impl FileHandle for RealFile {
    fn pwrite(&self, off: u64, buf: &[u8]) -> io::Result<usize> {
        self.file.write_at(buf, off)
    }

    fn pread(&self, off: u64, buf: &mut [u8]) -> io::Result<usize> {
        self.file.read_at(buf, off)
    }

    fn fdatasync(&self) -> io::Result<()> { self.file.sync_data() }

    fn len(&self) -> io::Result<u64> { Ok(self.file.metadata()?.len()) }

    /// `fallocate(fd, FALLOC_FL_KEEP_SIZE, 0, len)` (`bn-36y`): reserve `len`
    /// bytes of blocks WITHOUT extending `st_size`, so a segment preallocated
    /// in full at roll time never `ENOSPC`s mid-commit, while
    /// [`len`](FileHandle::len) and recovery still see only written content.
    /// On a filesystem that cannot honor the request (e.g. genuinely out of
    /// space) this returns the underlying `ENOSPC` error.
    fn allocate(&self, len: u64) -> io::Result<()> {
        use std::os::fd::AsRawFd;
        if len == 0 {
            return Ok(());
        }
        let len = i64::try_from(len).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidInput, "allocate len overflow")
        })?;
        // SAFETY: `self.file` owns a valid, open fd for the duration of this
        // call; `fallocate` reads no user memory. Errors are read via errno.
        let ret = unsafe {
            libc::fallocate(
                self.file.as_raw_fd(),
                libc::FALLOC_FL_KEEP_SIZE,
                0,
                len,
            )
        };
        if ret == 0 { Ok(()) } else { Err(io::Error::last_os_error()) }
    }
}

/// The production runtime.
#[derive(Clone)]
pub struct RealRuntime {
    origin: StdInstant,
    fs:     RealFs,
}

impl Default for RealRuntime {
    fn default() -> Self { Self::new() }
}

impl RealRuntime {
    /// A runtime with `now()` measured from this moment.
    pub fn new() -> Self {
        RealRuntime { origin: StdInstant::now(), fs: RealFs }
    }
}

/// A real sleep: blocks the calling task's thread until the deadline. Each
/// spawned task owns its thread, so this never stalls another task.
struct RealSleep {
    origin:   StdInstant,
    deadline: Instant,
}

impl Future for RealSleep {
    type Output = ();

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<()> {
        let elapsed = self.origin.elapsed();
        let target = self.deadline.saturating_duration_since(Instant::ORIGIN);
        if let Some(remaining) = target.checked_sub(elapsed) {
            std::thread::sleep(remaining);
        }
        Poll::Ready(())
    }
}

impl Clock for RealRuntime {
    fn now(&self) -> Instant {
        Instant {
            nanos: u64::try_from(self.origin.elapsed().as_nanos())
                .unwrap_or(u64::MAX),
        }
    }

    fn sleep_until(
        &self,
        deadline: Instant,
    ) -> impl Future<Output = ()> + Send {
        RealSleep { origin: self.origin, deadline }
    }
}

/// The future returned by [`Runtime::spawn`] on the real runtime; resolves
/// when the task's OS thread finishes.
pub struct RealJoin<T> {
    slot:  Arc<Mutex<Option<T>>>,
    waker: Arc<Mutex<Option<Waker>>>,
}

impl<T> Future for RealJoin<T> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<T> {
        if let Some(v) = self.slot.lock().unwrap().take() {
            return Poll::Ready(v);
        }
        *self.waker.lock().unwrap() = Some(cx.waker().clone());
        if let Some(v) = self.slot.lock().unwrap().take() {
            return Poll::Ready(v);
        }
        Poll::Pending
    }
}

impl Runtime for RealRuntime {
    type Fs = RealFs;

    fn fs(&self) -> RealFs { self.fs }

    fn spawn<F>(&self, fut: F) -> impl Future<Output = F::Output> + Send
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let slot: Arc<Mutex<Option<F::Output>>> = Arc::new(Mutex::new(None));
        let waker: Arc<Mutex<Option<Waker>>> = Arc::new(Mutex::new(None));
        let thread_slot = slot.clone();
        let thread_waker = waker.clone();
        std::thread::spawn(move || {
            let out = block_on(fut);
            *thread_slot.lock().unwrap() = Some(out);
            if let Some(w) = thread_waker.lock().unwrap().take() {
                w.wake();
            }
        });
        RealJoin { slot, waker }
    }

    fn block_on<F: Future>(&self, fut: F) -> F::Output { block_on(fut) }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::*;
    use crate::runtime::testsuite;

    // A unique temp path per test invocation so parallel test threads and
    // repeated runs never collide.
    fn tmp(name: &str) -> PathBuf {
        static N: AtomicU64 = AtomicU64::new(0);
        let n = N.fetch_add(1, Ordering::Relaxed);
        let mut p = std::env::temp_dir();
        p.push(format!(
            "mess-log-runtime-{}-{}-{}",
            std::process::id(),
            n,
            name
        ));
        p
    }

    struct Cleanup(Vec<PathBuf>);
    impl Drop for Cleanup {
        fn drop(&mut self) {
            for p in &self.0 {
                let _ = std::fs::remove_file(p);
            }
        }
    }

    // bn-25j: real fs (`std::fs::File::open`/`create`); Miri's isolation
    // blocks real `open` (`unsupported operation: \`open\` not available
    // when isolation is enabled`), so this is excluded from the Miri lane.
    #[test]
    #[cfg_attr(miri, ignore)]
    fn real_fs_roundtrip() {
        let p = tmp("roundtrip");
        let _c = Cleanup(vec![p.clone()]);
        testsuite::fs_roundtrip(&RealRuntime::new(), &p);
    }

    // bn-25j: real fs (see note above).
    #[test]
    #[cfg_attr(miri, ignore)]
    fn real_fs_rename() {
        let from = tmp("rename-from");
        let to = tmp("rename-to");
        let _c = Cleanup(vec![from.clone(), to.clone()]);
        testsuite::fs_rename(&RealRuntime::new(), &from, &to);
    }

    #[test]
    fn real_clock_advances() { testsuite::clock_advances(&RealRuntime::new()); }

    // bn-25j: real fs (see note above).
    #[test]
    #[cfg_attr(miri, ignore)]
    fn real_three_actors_share_a_segment() {
        let p = tmp("three-actors");
        let _c = Cleanup(vec![p.clone()]);
        testsuite::three_actors_share_a_segment(&RealRuntime::new(), &p);
    }

    #[test]
    fn real_actors_all_complete() {
        let order = testsuite::actor_completion_order(&RealRuntime::new());
        let mut sorted = order.clone();
        sorted.sort_unstable();
        assert_eq!(sorted, vec![0, 1, 2], "every actor must run exactly once");
    }
}
