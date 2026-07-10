//! The simulation runtime: a **seeded, single-threaded, deterministic**
//! async executor with virtual time, plus the in-memory fault filesystem
//! ([`SimFs`](super::sim_fs::SimFs)).
//!
//! This is the "thin in-house layer" the bone chose over madsim/turmoil:
//! a single-node store needs only sim-time + sim-fs + deterministic
//! scheduling of a handful of actors, and framework adoption (madsim
//! simulates tokio wholesale; turmoil is network-focused) buys nothing
//! here. The whole executor is a few hundred lines and the segment
//! writer/committer never sees it — they are generic over
//! [`Runtime`](super::Runtime).
//!
//! # Determinism
//!
//! Every scheduling choice comes from the seeded [`Rng`]: when several
//! spawned actors are runnable, which one steps next is `rng`-chosen, so a
//! `(seed)` pair reproduces a byte-identical interleaving. Virtual time
//! only advances when *every* task is blocked on a timer, so tests never
//! wait on a wall clock and time-dependent logic (group-commit
//! `max_delay`) is exercised instantly and reproducibly.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};

use super::sim_fs::SimFs;
use super::{Clock, Fault, Instant, Runtime};

// ---------------------------------------------------------------------------
// Rng — splitmix64: tiny, dependency-free, good enough for scheduling/faults
// ---------------------------------------------------------------------------

/// A small deterministic PRNG (splitmix64). Seeded once per
/// [`SimRuntime`]; drives both scheduling interleavings and randomized
/// fault plans so a seed reproduces a whole run.
#[derive(Debug, Clone)]
pub struct Rng {
    state: u64,
}

impl Rng {
    /// Seed the generator.
    pub fn new(seed: u64) -> Self { Rng { state: seed } }

    /// Next 64-bit value.
    pub fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Uniform in `[0, n)`. `n` must be non-zero.
    pub fn below(&mut self, n: u64) -> u64 {
        debug_assert!(n > 0);
        self.next_u64() % n
    }

    /// A fair coin.
    pub fn bool(&mut self) -> bool { self.next_u64() & 1 == 1 }

    /// True with probability `p` (clamped to `[0, 1]`).
    pub fn chance(&mut self, p: f64) -> bool {
        (self.next_u64() as f64) / (u64::MAX as f64) < p
    }
}

// ---------------------------------------------------------------------------
// Executor core
// ---------------------------------------------------------------------------

struct TaskSlot {
    fut: Option<Pin<Box<dyn Future<Output = ()> + Send>>>,
}

struct Exec {
    tasks:  Vec<TaskSlot>,
    /// Pending virtual-time timers: `(deadline, waker to fire)`.
    timers: Vec<(Instant, Waker)>,
    now:    Instant,
    rng:    Rng,
    /// Spawned task ids known to be runnable.
    ready:  Vec<usize>,
}

struct SimCore {
    exec:  Mutex<Exec>,
    /// Task ids woken by their wakers since the last scheduling step. A
    /// separate lock so a [`TaskWaker`] can push without touching `exec`.
    woken: Arc<Mutex<Vec<usize>>>,
}

impl SimCore {
    /// Tear the executor down: drop every parked task future and pending
    /// timer, and clear the ready/woken bookkeeping.
    ///
    /// # Why this exists — breaking the executor↔task reference cycle
    ///
    /// A spawned task's future lives in `Exec.tasks`, reachable from the
    /// shared `Arc<SimCore>`. That future routinely captures a
    /// [`SimRuntime`] handle (the group-commit timer at
    /// `committer.rs` does `rt.spawn(async move {
    /// rt_sleep.sleep_until(..).await })`), and a `SimRuntime` handle owns
    /// a **strong** `Arc<SimCore>`. So the graph contains a cycle:
    /// `Arc<SimCore>` → `Exec.tasks` → future → captured `SimRuntime` →
    /// `Arc<SimCore>`.
    ///
    /// A fire-and-forget task that never completes (the group-commit timer
    /// when its convoy closes early: its [`SimJoin`] handle is dropped but
    /// the task stays parked in `tasks`/`timers` forever) keeps that cycle
    /// alive. `SimCore`'s strong count therefore never reaches zero even
    /// once every external handle is gone, so the whole executor — task
    /// futures, their captured buffers, and timer wakers — leaks at process
    /// exit. A `Drop` on `SimCore` can never fire (it is the thing kept
    /// alive by the cycle), so teardown must be driven from the one handle
    /// that is *not* captured by any task: the runtime **owner** (see
    /// [`SimRuntime`]'s `owner` field and its `Drop`).
    ///
    /// Clearing `tasks`/`timers` drops those futures, releasing their
    /// captured `SimRuntime` clones (and the `Arc<SimCore>` inside each),
    /// which breaks the cycle so the core frees normally.
    fn drain(&self) {
        // Move the futures/timers out from under the lock, then drop them
        // *after* releasing it: a task future's destructor is arbitrary
        // user code and could re-enter the executor (e.g. drop a nested
        // handle), so dropping while holding `exec` risks a re-entrant
        // deadlock.
        let (tasks, timers) = {
            let mut ex = self.exec.lock().unwrap();
            ex.ready.clear();
            (std::mem::take(&mut ex.tasks), std::mem::take(&mut ex.timers))
        };
        self.woken.lock().unwrap().clear();
        drop(tasks);
        drop(timers);
    }

    /// Register a task future, mark it runnable, return its id.
    fn push_task(
        &self,
        fut: Pin<Box<dyn Future<Output = ()> + Send>>,
    ) -> usize {
        let mut ex = self.exec.lock().unwrap();
        let id = ex.tasks.len();
        ex.tasks.push(TaskSlot { fut: Some(fut) });
        ex.ready.push(id);
        id
    }

    /// One scheduling step. Returns `false` only when nothing is runnable
    /// and no timers remain (a settled or deadlocked executor).
    fn step(self: &Arc<Self>) -> bool {
        // Fold newly-woken ids into the ready set (canonical order).
        let picked = {
            let mut ex = self.exec.lock().unwrap();
            let mut woken = std::mem::take(&mut *self.woken.lock().unwrap());
            woken.sort_unstable();
            woken.dedup();
            for id in woken {
                if !ex.ready.contains(&id) {
                    ex.ready.push(id);
                }
            }

            if ex.ready.is_empty() {
                // No runnable task: advance virtual time to the next timer.
                if ex.timers.is_empty() {
                    return false;
                }
                let min = ex.timers.iter().map(|(d, _)| *d).min().unwrap();
                if min > ex.now {
                    ex.now = min;
                }
                let now = ex.now;
                let mut fired = Vec::new();
                let mut kept = Vec::new();
                for (d, w) in std::mem::take(&mut ex.timers) {
                    if d <= now {
                        fired.push(w);
                    } else {
                        kept.push((d, w));
                    }
                }
                ex.timers = kept;
                drop(ex);
                for w in fired {
                    w.wake();
                }
                return true;
            }

            // Deterministically choose which runnable actor steps next.
            let n = ex.ready.len() as u64;
            let k = ex.rng.below(n) as usize;
            let id = ex.ready.swap_remove(k);
            let fut = ex.tasks[id].fut.take();
            fut.map(|f| (id, f))
        };

        let Some((id, mut fut)) = picked else {
            return true;
        };
        let waker =
            Waker::from(Arc::new(TaskWaker { id, woken: self.woken.clone() }));
        let mut cx = Context::from_waker(&waker);
        if fut.as_mut().poll(&mut cx).is_pending() {
            self.exec.lock().unwrap().tasks[id].fut = Some(fut);
        }
        true
    }
}

/// Waker for a spawned task: records the task id as runnable.
struct TaskWaker {
    id:    usize,
    woken: Arc<Mutex<Vec<usize>>>,
}

impl Wake for TaskWaker {
    fn wake(self: Arc<Self>) { self.woken.lock().unwrap().push(self.id); }

    fn wake_by_ref(self: &Arc<Self>) {
        self.woken.lock().unwrap().push(self.id);
    }
}

/// Waker for the `block_on` root future: sets a flag.
struct FlagWaker {
    ready: Arc<AtomicBool>,
}

impl Wake for FlagWaker {
    fn wake(self: Arc<Self>) { self.ready.store(true, Ordering::Release); }

    fn wake_by_ref(self: &Arc<Self>) {
        self.ready.store(true, Ordering::Release);
    }
}

// ---------------------------------------------------------------------------
// Sleep future (virtual time)
// ---------------------------------------------------------------------------

/// The future returned by [`Clock::sleep_until`] on the sim clock.
pub struct Sleep {
    core:     Arc<SimCore>,
    deadline: Instant,
}

impl Future for Sleep {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let mut ex = self.core.exec.lock().unwrap();
        if ex.now >= self.deadline {
            Poll::Ready(())
        } else {
            ex.timers.push((self.deadline, cx.waker().clone()));
            Poll::Pending
        }
    }
}

// ---------------------------------------------------------------------------
// Join handle
// ---------------------------------------------------------------------------

/// The future returned by [`Runtime::spawn`] on the sim runtime; resolves
/// with the task's output once the deterministic executor completes it.
pub struct SimJoin<T> {
    slot:  Arc<Mutex<Option<T>>>,
    waker: Arc<Mutex<Option<Waker>>>,
}

impl<T> Future for SimJoin<T> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<T> {
        if let Some(v) = self.slot.lock().unwrap().take() {
            return Poll::Ready(v);
        }
        *self.waker.lock().unwrap() = Some(cx.waker().clone());
        // Re-check to close the set-then-register race.
        if let Some(v) = self.slot.lock().unwrap().take() {
            return Poll::Ready(v);
        }
        Poll::Pending
    }
}

// ---------------------------------------------------------------------------
// SimRuntime
// ---------------------------------------------------------------------------

/// A deterministic simulation runtime: virtual clock, seeded executor, and
/// an in-memory fault filesystem. Cheap to clone (shared core + fs).
///
/// # Owner vs. handle (why this is not `#[derive(Clone)]`)
///
/// Modeled on tokio's `Runtime` (an owner that tears the executor down when
/// dropped) versus its `Handle` (freely cloned, captured into tasks). The
/// handle returned by
/// [`new`](SimRuntime::new)/[`with_fault`](SimRuntime::with_fault)
/// is the **owner** (`owner == true`); [`Clone`] always yields a non-owning
/// handle (`owner == false`). Spawned task futures only ever capture
/// non-owning clones (`rt.clone()`), so no task can keep the executor alive.
/// When the owner drops, its [`Drop`] calls [`SimCore::drain`] to release
/// every parked task and timer — the only place the executor↔task reference
/// cycle documented on [`SimCore::drain`] can be broken. The owner must
/// therefore outlive every handle it hands out (the natural RAII shape:
/// `let rt = SimRuntime::new(..)` at the top of a test, clones handed to
/// committers/tasks below it).
pub struct SimRuntime {
    core:  Arc<SimCore>,
    fs:    SimFs,
    /// `true` only for the handle from `new`/`with_fault`; `Clone` sets it
    /// `false`. Exactly one owner exists per executor, and it is never
    /// captured into a spawned task, so its `Drop` is a reliable teardown
    /// signal that firing on a task-held clone could never be.
    owner: bool,
}

impl SimRuntime {
    /// A runtime seeded with `seed`, whose files default to the
    /// [`Fault::SECTOR_512`] block-reordering model.
    pub fn new(seed: u64) -> Self { Self::with_fault(seed, Fault::SECTOR_512) }

    /// A runtime whose files default to `fault`.
    pub fn with_fault(seed: u64, fault: Fault) -> Self {
        SimRuntime {
            core:  Arc::new(SimCore {
                exec:  Mutex::new(Exec {
                    tasks:  Vec::new(),
                    timers: Vec::new(),
                    now:    Instant::ORIGIN,
                    rng:    Rng::new(seed),
                    ready:  Vec::new(),
                }),
                woken: Arc::new(Mutex::new(Vec::new())),
            }),
            fs:    SimFs::new(fault),
            owner: true,
        }
    }

    /// Draw from the runtime's scheduling RNG (e.g. to build a randomized
    /// fault plan that is part of the same deterministic stream).
    pub fn with_rng<R>(&self, f: impl FnOnce(&mut Rng) -> R) -> R {
        f(&mut self.core.exec.lock().unwrap().rng)
    }
}

impl Clone for SimRuntime {
    /// Clones share the executor and fs but are **never** owners: a cloned
    /// handle (including every one captured into a spawned task) must not be
    /// able to tear the executor down. See the type-level docs.
    fn clone(&self) -> Self {
        SimRuntime {
            core:  self.core.clone(),
            fs:    self.fs.clone(),
            owner: false,
        }
    }
}

impl Drop for SimRuntime {
    /// Tear the executor down when the sole **owner** handle drops. Handles
    /// created by [`Clone`] are non-owning and drop silently; only the owner
    /// from `new`/`with_fault` drains the executor, releasing every parked
    /// task and timer. This is the one teardown point that can break the
    /// executor↔task reference cycle documented on [`SimCore::drain`] — a
    /// `Drop` on `SimCore` itself could never run, since the cycle keeps its
    /// strong count above zero.
    fn drop(&mut self) {
        if self.owner {
            self.core.drain();
        }
    }
}

impl Clock for SimRuntime {
    fn now(&self) -> Instant { self.core.exec.lock().unwrap().now }

    fn sleep_until(
        &self,
        deadline: Instant,
    ) -> impl Future<Output = ()> + Send {
        Sleep { core: self.core.clone(), deadline }
    }
}

impl Runtime for SimRuntime {
    type Fs = SimFs;

    fn fs(&self) -> SimFs { self.fs.clone() }

    fn spawn<F>(&self, fut: F) -> impl Future<Output = F::Output> + Send
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let slot: Arc<Mutex<Option<F::Output>>> = Arc::new(Mutex::new(None));
        let waker: Arc<Mutex<Option<Waker>>> = Arc::new(Mutex::new(None));
        let child_slot = slot.clone();
        let child_waker = waker.clone();
        let child = async move {
            let out = fut.await;
            *child_slot.lock().unwrap() = Some(out);
            if let Some(w) = child_waker.lock().unwrap().take() {
                w.wake();
            }
        };
        self.core.push_task(Box::pin(child));
        SimJoin { slot, waker }
    }

    fn block_on<F: Future>(&self, fut: F) -> F::Output {
        let mut root = std::pin::pin!(fut);
        let ready = Arc::new(AtomicBool::new(true));
        let waker = Waker::from(Arc::new(FlagWaker { ready: ready.clone() }));
        let mut cx = Context::from_waker(&waker);
        loop {
            if ready.swap(false, Ordering::AcqRel)
                && let Poll::Ready(v) = root.as_mut().poll(&mut cx)
            {
                return v;
            }
            if !self.core.step() && !ready.load(Ordering::Acquire) {
                panic!(
                    "sim runtime deadlock: root pending with no runnable task \
                     or timer"
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;
    use crate::runtime::testsuite;
    use crate::runtime::{
        CrashPlan, FileHandle, Fs, OpenOpts, SectorPlan, TailPlan,
    };

    // -- the shared runtime-agnostic suite, on the sim runtime -------------

    #[test]
    fn sim_fs_roundtrip() {
        testsuite::fs_roundtrip(&SimRuntime::new(1), Path::new("/seg-0"));
    }

    #[test]
    fn sim_fs_rename() {
        testsuite::fs_rename(
            &SimRuntime::new(1),
            Path::new("/tmp-seg"),
            Path::new("/seg-1"),
        );
    }

    #[test]
    fn sim_clock_advances() {
        // No wall-clock wait: virtual time jumps to the timer instantly.
        testsuite::clock_advances(&SimRuntime::new(1));
    }

    #[test]
    fn sim_three_actors_share_a_segment() {
        testsuite::three_actors_share_a_segment(
            &SimRuntime::new(7),
            Path::new("/seg-shared"),
        );
    }

    #[test]
    fn sim_scheduling_is_deterministic() {
        // Same seed => byte-identical interleaving; the whole point of DST.
        let a = testsuite::actor_completion_order(&SimRuntime::new(42));
        let b = testsuite::actor_completion_order(&SimRuntime::new(42));
        assert_eq!(a, b, "a seed must reproduce the exact interleaving");
        let mut sorted = a.clone();
        sorted.sort_unstable();
        assert_eq!(sorted, vec![0, 1, 2], "every actor runs exactly once");
    }

    #[test]
    fn sim_different_seeds_can_reorder() {
        // Across many seeds the scheduler produces more than one order —
        // evidence it actually explores interleavings, not a fixed schedule.
        let orders: std::collections::HashSet<Vec<u64>> = (0..64)
            .map(|s| testsuite::actor_completion_order(&SimRuntime::new(s)))
            .collect();
        assert!(orders.len() > 1, "scheduler never varied the interleaving");
    }

    // -- fault injection THROUGH the Fs trait ------------------------------

    /// SectorDisk (torn_write) behind the trait: an un-synced tail whose
    /// sectors did NOT persist is gone after a crash, while the synced
    /// prefix survives. This is a torn-write truncation surfaced entirely
    /// through `Fs`/`FileHandle` — the harness never touches the model.
    #[test]
    fn sector_fault_torn_tail_truncated() {
        let rt = SimRuntime::with_fault(1, Fault::SECTOR_512);
        let fs = rt.fs();
        let path = Path::new("/seg");
        let f = fs.open(path, OpenOpts::create_rw()).unwrap();

        // Sector 0: a header we make durable.
        f.pwrite(0, b"DURABLE-HEADER").unwrap();
        f.fdatasync().unwrap();
        // Sectors 1..3: a body written but never synced (in flight).
        f.pwrite(512, &[0xAB; 700]).unwrap();

        // Crash persisting NOTHING pending: the body reverts to background.
        fs.crash(path, CrashPlan::Sector(SectorPlan::default())).unwrap();

        let g = fs.open(path, OpenOpts::read_only()).unwrap();
        let mut head = [0u8; 14];
        assert_eq!(g.pread(0, &mut head).unwrap(), 14);
        assert_eq!(&head, b"DURABLE-HEADER", "synced prefix must survive");
        let mut body = [0u8; 700];
        g.pread(512, &mut body).unwrap();
        assert!(body.iter().all(|&b| b == 0), "un-synced body must be gone");
    }

    /// The A4 reordering shape through the trait: a later sector persists
    /// while an earlier one does not (marker-before-frames). The hole is a
    /// background sector between two persisted ones.
    #[test]
    fn sector_fault_reordering_leaves_a_hole() {
        let rt = SimRuntime::with_fault(1, Fault::SECTOR_512);
        let fs = rt.fs();
        let path = Path::new("/seg");
        let f = fs.open(path, OpenOpts::create_rw()).unwrap();

        // Three sectors written in one in-flight batch.
        f.pwrite(0, &[0x11; 512]).unwrap(); // sector 0
        f.pwrite(512, &[0x22; 512]).unwrap(); // sector 1 (the frame body)
        f.pwrite(1024, &[0x33; 512]).unwrap(); // sector 2 (the marker)

        // Persist sectors 0 and 2 but NOT 1 — reordering hole.
        fs.crash(
            path,
            CrashPlan::Sector(SectorPlan {
                persist: vec![0, 2],
                tear:    None,
            }),
        )
        .unwrap();

        let g = fs.open(path, OpenOpts::read_only()).unwrap();
        let mut s = [0u8; 512];
        g.pread(0, &mut s).unwrap();
        assert!(s.iter().all(|&b| b == 0x11), "sector 0 persisted");
        g.pread(512, &mut s).unwrap();
        assert!(s.iter().all(|&b| b == 0), "sector 1 is the reordering hole");
        g.pread(1024, &mut s).unwrap();
        assert!(
            s.iter().all(|&b| b == 0x33),
            "sector 2 persisted before sector 1"
        );
    }

    /// A recycled segment file still holds a stale prior generation in an
    /// un-persisted sector (A9's precondition), seeded and read back through
    /// the trait.
    #[test]
    fn sector_fault_resurrects_stale_generation() {
        let rt = SimRuntime::with_fault(1, Fault::SECTOR_512);
        let fs = rt.fs();
        let path = Path::new("/recycled");
        // Background: a stale batch from a previous life of this file region.
        let mut stale = vec![0u8; 1024];
        stale[..10].copy_from_slice(b"OLD-SECRET");
        fs.seed(path, Fault::SECTOR_512, stale);

        let f = fs.open(path, OpenOpts::create_rw()).unwrap();
        f.pwrite(0, b"NEW-DATA").unwrap(); // in flight, never synced
        fs.crash(path, CrashPlan::Sector(SectorPlan::default())).unwrap();

        let g = fs.open(path, OpenOpts::read_only()).unwrap();
        let mut buf = [0u8; 10];
        g.pread(0, &mut buf).unwrap();
        assert_eq!(
            &buf, b"OLD-SECRET",
            "un-persisted write leaves stale bytes"
        );
    }

    /// FaultWriter (crash_log) behind the trait: torn-tail truncation — the
    /// un-synced tail is cut at an arbitrary length; the synced prefix
    /// survives; `len()` reflects the truncation.
    #[test]
    fn tail_fault_torn_tail_truncated() {
        let rt = SimRuntime::with_fault(1, Fault::Tail);
        let fs = rt.fs();
        let path = Path::new("/seg-tail");
        let f = fs.open(path, OpenOpts::create_rw()).unwrap();

        f.pwrite(0, b"ACKED").unwrap();
        f.fdatasync().unwrap();
        f.pwrite(5, b"UNACKED-TAIL").unwrap();
        assert_eq!(f.len().unwrap(), 17);

        // Keep only the synced watermark: the whole tail is lost.
        fs.crash(path, CrashPlan::Tail(TailPlan { keep: 5, scramble: vec![] }))
            .unwrap();

        let g = fs.open(path, OpenOpts::read_only()).unwrap();
        assert_eq!(g.len().unwrap(), 5, "file truncated to the sync watermark");
        let mut buf = [0u8; 5];
        assert_eq!(g.pread(0, &mut buf).unwrap(), 5);
        assert_eq!(&buf, b"ACKED");
        // Nothing past the watermark.
        let mut tail = [0u8; 4];
        assert_eq!(g.pread(5, &mut tail).unwrap(), 0);
    }

    /// A torn (scrambled) surviving tail: the prefix is kept but a byte in
    /// the unsynced region is corrupted — what a CRC check must catch.
    #[test]
    fn tail_fault_scrambles_surviving_tail() {
        let rt = SimRuntime::with_fault(1, Fault::Tail);
        let fs = rt.fs();
        let path = Path::new("/seg-scramble");
        let f = fs.open(path, OpenOpts::create_rw()).unwrap();
        f.pwrite(0, b"SYNC").unwrap();
        f.fdatasync().unwrap();
        f.pwrite(4, b"TORNBYTES").unwrap();

        fs.crash(
            path,
            CrashPlan::Tail(TailPlan { keep: 13, scramble: vec![6] }),
        )
        .unwrap();

        let g = fs.open(path, OpenOpts::read_only()).unwrap();
        let mut buf = [0u8; 13];
        g.pread(0, &mut buf).unwrap();
        assert_eq!(&buf[..4], b"SYNC", "synced prefix intact");
        assert_ne!(buf[6], b'R', "byte 6 in the unsynced region was scrambled");
    }

    /// The seeded random crash (the `crash_random` shape the 24k sweep uses)
    /// is reproducible: same seed + same writes => same plan and image.
    #[test]
    fn crash_random_is_reproducible() {
        fn run(seed: u64) -> (SectorPlan, Vec<u8>) {
            let rt = SimRuntime::with_fault(seed, Fault::SECTOR_512);
            let fs = rt.fs();
            let path = Path::new("/seg");
            let f = fs.open(path, OpenOpts::create_rw()).unwrap();
            f.pwrite(0, &[0x11; 512]).unwrap();
            f.fdatasync().unwrap();
            f.pwrite(512, &[0x22; 1200]).unwrap();
            let plan =
                rt.with_rng(|rng| fs.crash_random(path, rng, 0.5).unwrap());
            let g = fs.open(path, OpenOpts::read_only()).unwrap();
            let mut img = vec![0u8; g.len().unwrap() as usize];
            g.pread(0, &mut img).unwrap();
            (plan, img)
        }
        assert_eq!(run(123), run(123), "a seed must reproduce the crash image");
    }
}
