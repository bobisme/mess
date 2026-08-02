//! Alloc-counter test: **zero heap allocations per event on the hot path**
//! after warmup (the `spikes/perf_append` discipline — one contiguous reusable
//! batch buffer, one positioned write per batch).
//!
//! This integration test binary installs a counting [`GlobalAlloc`] wrapper
//! (the standard trick — a `#[global_allocator]` here affects only this test
//! binary).
//!
//! We warm up (create the writer, encode/append one batch so the encoder's
//! buffer grows to its steady size), snapshot the allocation count, then run N
//! encode+append iterations and assert the count did not move.
//!
//! # bn-33l3: why the counter is per-thread and not process-global
//!
//! This test used to count into one process-global `AtomicUsize`, on the
//! reasoning that the binary has exactly one `#[test]` so nothing else can be
//! allocating. That reasoning is wrong, and it made the test flake under a
//! loaded parallel suite.
//!
//! nextest does give us process isolation — it invokes this binary as
//! `alloc-<hash> --exact <test> --nocapture`, one test per process — so no
//! *other test* can pollute the count. But it does not pass `--test-threads=1`,
//! and libtest at concurrency > 1 runs the test body on a **spawned thread**
//! (`library/test/src/lib.rs:461`) while the harness `main` thread stays alive
//! in the same process. `/proc/self/task` confirms two live threads for the
//! whole run.
//!
//! That harness thread allocates *after* it has spawned us:
//! `running_tests.insert(..)` at `lib.rs:462` (first `HashMap` insert) and
//! `timeout_queue.push_back(..)` at `lib.rs:463` (first `VecDeque` push) both
//! come after the spawn, and land only ~20-100 us before this test body's
//! first instruction on an idle host. Let the scheduler deschedule the harness
//! thread in that gap — routine under a 64-way parallel suite with real memory
//! pressure — and those allocations land *inside* the measured window, where a
//! process-global counter books them against the hot path. Reproduced
//! directly: 1 spurious count in 300 runs under `stress-ng` memory/IO
//! pressure, with the offending backtrace ending at `lib.rs:462`.
//!
//! The same class has a second, unavoidable instance: at
//! `TEST_WARN_TIMEOUT_S` (60 s) the harness thread allocates three more times
//! to emit "has been running for over 60 seconds" (`lib.rs:409`/`416`/`472`).
//! No amount of quiescing can make a process-global counter sound here.
//!
//! So the count is scoped to the thread under measurement. The hot paths this
//! test exercises are synchronous and run entirely on the calling thread —
//! `SegmentWriter::append` is a plain call, and `DirectCommitter` is by
//! construction the owner-only direct seam — so a real per-event allocation
//! still lands on this thread and still trips the assertion. Handing per-event
//! work to another thread would itself allocate on *this* thread (the spawn,
//! or the boxed message), so that regression is still caught too. The teeth
//! canary in Part 0 below proves the counter is live rather than silently
//! returning zero, and
//! `a_foreign_threads_allocations_are_not_charged_to_this_thread` fails
//! immediately if the counter is ever reverted to process-global.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::sync::atomic::{AtomicUsize, Ordering};

use mess_log::committer::{
    AppendRequest, ChainInit, DirectAppendRequest, DirectCommitter, Durability,
    EventInput, Roller,
};
use mess_log::encode::{BatchEncoder, BatchInput, Subframe};
use mess_log::runtime::{RealRuntime, Runtime};
use mess_log::writer::{BatchSpec, SegmentParams, SegmentWriter};

/// Number of allocating calls (alloc / alloc_zeroed / grow-via-realloc) made
/// by **this thread** since process start. Deallocs and shrinks are not
/// counted — we care that the hot path requests no NEW memory.
///
/// `const`-initialised on purpose: a lazily initialised thread-local would
/// have to allocate on first touch, and touching it from inside the global
/// allocator would then recurse. `Cell<usize>` has no destructor, so no TLS
/// destructor is registered either.
thread_local! {
    static THREAD_ALLOCS: Cell<usize> = const { Cell::new(0) };
}

/// Process-wide count of the same events. **Never asserted on** — see the
/// module docs (bn-33l3): libtest's harness thread allocates concurrently with
/// this test body, so this number is not a property of the hot path. It is
/// reported alongside a failure purely so the next reader can tell a real hot
/// path regression (per-thread delta > 0) from harness noise.
static PROCESS_ALLOCS: AtomicUsize = AtomicUsize::new(0);

struct Counting;

impl Counting {
    #[inline]
    fn count(&self) {
        PROCESS_ALLOCS.fetch_add(1, Ordering::Relaxed);
        // `try_with` cannot fail for a const-initialised, destructor-free
        // thread-local, but if it ever did we would silently undercount, so
        // the `teeth` canary in the test asserts the counter is live.
        let _ = THREAD_ALLOCS.try_with(|c| c.set(c.get().wrapping_add(1)));
    }
}

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        self.count();
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        self.count();
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn realloc(
        &self,
        ptr: *mut u8,
        layout: Layout,
        new_size: usize,
    ) -> *mut u8 {
        // A grow is a fresh allocation for our purposes; a shrink is not.
        if new_size > layout.size() {
            self.count();
        }
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static GLOBAL: Counting = Counting;

/// Allocations made by the calling thread. This is the measured quantity.
fn allocs() -> usize { THREAD_ALLOCS.with(Cell::get) }

/// Allocations made by every thread in the process. Diagnostics only.
fn process_allocs() -> usize { PROCESS_ALLOCS.load(Ordering::Relaxed) }

// bn-25j: the writer half of this test opens a real segment file via
// `RealRuntime` (a self-sweeping real-fs temp dir + `std::fs`); Miri's
// isolation blocks real `open`, and a 20_000-iteration hot-path timing test
// is also far too slow interpreted under Miri regardless. Excluded from the
// Miri lane.
#[test]
#[cfg_attr(miri, ignore)]
fn zero_allocs_per_event_on_the_hot_path() {
    const N: u64 = 20_000;

    // --- Part 0: teeth. Prove the counter actually observes an allocation --
    // Every assertion below is `== 0`, so a counter that silently stopped
    // counting (a broken TLS, a `#[global_allocator]` that no longer applies,
    // an over-eager fix to bn-33l3's flake) would make this whole test pass
    // vacuously. One deliberate allocation on the measuring thread, which must
    // be seen.
    let before = allocs();
    let canary = std::hint::black_box(Vec::<u8>::with_capacity(4096));
    let canary_allocs = allocs() - before;
    drop(std::hint::black_box(canary));
    assert!(
        canary_allocs >= 1,
        "the counting allocator did not observe a deliberate allocation on \
         the measuring thread ({canary_allocs} counted) — every zero-alloc \
         assertion in this test would be vacuous"
    );

    // --- Part 1: the pure encoder is zero-alloc after warmup ---------------
    let payload = [0xABu8; 64];
    let sfs = [Subframe::plain(1, 0, 0, &payload)];
    let mut enc = BatchEncoder::with_capacity(4096);

    let mk_input = |batch_id: u64, pos: u64| BatchInput {
        segment_epoch: 1,
        batch_id,
        first_global_pos: pos,
        stream_id: 7,
        category_id: 0,
        first_stream_version: 0,
        crypto_chain: None,
        subframes: &sfs,
    };

    // Warm up: force any buffer growth to happen now.
    enc.encode(&mk_input(0, 0)).unwrap();

    let before = allocs();
    let before_process = process_allocs();
    let mut sink = 0u8;
    for i in 1..N {
        let bytes = enc.encode(&mk_input(i, i)).unwrap();
        sink ^= bytes[0] ^ bytes[bytes.len() - 1]; // keep the work observable
    }
    let encode_allocs = allocs() - before;
    let encode_elsewhere = (process_allocs() - before_process) - encode_allocs;
    assert_eq!(
        encode_allocs, 0,
        "encoder allocated {encode_allocs} times over {N} batches (want 0); \
         other threads allocated {encode_elsewhere} times in the same window \
         (harness noise, not asserted — bn-33l3)"
    );
    std::hint::black_box(sink);

    // --- Part 2: encode + append through the writer is zero-alloc ----------
    // RealRuntime's pwrite is write_at(2): no Rust heap allocation.
    let rt = RealRuntime::new();
    let dir = mess_testkit::sweeping_temp_dir("alloc-hotpath");
    let path = dir.path().join("hotpath.seg");

    let mut w =
        SegmentWriter::create(&rt.fs(), &path, SegmentParams::new(1, 0, 1, 0))
            .unwrap();
    let spec = BatchSpec {
        stream_id:            7,
        category_id:          0,
        first_stream_version: 0,
        crypto_chain:         None,
        subframes:            &sfs,
    };

    // Warm up the writer's encoder buffer + first pwrite.
    w.append(&spec).unwrap();

    let before = allocs();
    let before_process = process_allocs();
    for _ in 1..N {
        w.append(&spec).unwrap();
    }
    let append_allocs = allocs() - before;
    let append_elsewhere = (process_allocs() - before_process) - append_allocs;
    assert_eq!(
        append_allocs, 0,
        "encode+append allocated {append_allocs} times over {N} batches (want \
         0); other threads allocated {append_elsewhere} times in the same \
         window (harness noise, not asserted — bn-33l3)"
    );

    // --- Part 3: the owner-only direct seam is zero-alloc after warmup -----
    const DIRECT_N: usize = 128;
    let direct_path = dir.path().join("direct-hotpath.seg");
    let mut params = SegmentParams::new(2, 0, 2, 1);
    params.segment_size = 1024 * 1024;
    let direct_writer =
        SegmentWriter::create(&rt.fs(), &direct_path, params).unwrap();
    let roll_root = dir.path().to_path_buf();
    let (roll_tx, _roll_rx) = std::sync::mpsc::channel();
    let roller = Roller::new(
        move |id| roll_root.join(format!("direct-{id}.seg")),
        roll_tx,
    );
    let mut direct = DirectCommitter::with_roll_chained(
        &rt,
        direct_writer,
        Durability::Process,
        roller,
        ChainInit::off(),
    );
    let make_units = |version| {
        (0..DIRECT_N)
            .map(|stream| {
                vec![DirectAppendRequest::Inputs(AppendRequest {
                    stream_id:            stream as u64 + 1,
                    category_id:          0,
                    first_stream_version: version,
                    events:               vec![EventInput::plain(
                        1,
                        0,
                        0,
                        vec![0xCD; 24],
                    )],
                })]
            })
            .collect()
    };
    direct
        .commit_ordered_group(make_units(0), |_, _, outcome| {
            std::hint::black_box(outcome);
        })
        .unwrap();
    let measured = make_units(1);
    let before = allocs();
    let before_process = process_allocs();
    let mut completed = 0;
    direct
        .commit_ordered_group(measured, |unit, batch, outcome| {
            assert_eq!(unit, completed);
            assert_eq!(batch, 0);
            completed += 1;
            std::hint::black_box(outcome);
        })
        .unwrap();
    let direct_allocs = allocs() - before;
    let direct_elsewhere = (process_allocs() - before_process) - direct_allocs;
    assert_eq!(completed, DIRECT_N);
    assert_eq!(
        direct_allocs, 0,
        "warmed DirectCommitter allocated {direct_allocs} times for \
         {DIRECT_N} small appends; other threads allocated {direct_elsewhere} \
         times in the same window (harness noise, not asserted — bn-33l3)"
    );
}

/// bn-33l3 regression guard for the *harness*, not the hot path: an
/// allocation made by another thread while a measurement window is open must
/// not be charged to the thread under measurement.
///
/// This is the flake reproduced deterministically. In the wild the other
/// thread is libtest's own harness thread finishing its post-spawn bookkeeping
/// (`library/test/src/lib.rs:462`/`463`) inside our window; here it is an
/// explicit helper, so the invariant is checked on every run instead of once
/// in a few thousand. If someone reverts the counter to a process-global
/// `AtomicUsize`, this test fails immediately and loudly.
#[test]
#[cfg_attr(miri, ignore)]
fn a_foreign_threads_allocations_are_not_charged_to_this_thread() {
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;

    const CHURN: usize = 1_000;

    let stop = Arc::new(AtomicBool::new(false));
    let done = Arc::new(AtomicUsize::new(0));
    let (helper_stop, helper_done) = (Arc::clone(&stop), Arc::clone(&done));

    // Everything that allocates on THIS thread happens before the window.
    let helper = std::thread::spawn(move || {
        while !helper_stop.load(Ordering::Relaxed) {
            drop(std::hint::black_box(Vec::<u8>::with_capacity(1024)));
            helper_done.fetch_add(1, Ordering::Relaxed);
        }
    });

    let before = allocs();
    let before_process = process_allocs();
    // Allocation-free work on this thread while the helper churns.
    let mut sink = 0u64;
    while done.load(Ordering::Relaxed) < CHURN {
        sink = sink.wrapping_add(1);
        std::thread::yield_now();
    }
    let mine = allocs() - before;
    let everyone = process_allocs() - before_process;

    stop.store(true, Ordering::Relaxed);
    helper.join().expect("helper thread");
    std::hint::black_box(sink);

    assert_eq!(
        mine, 0,
        "a concurrent thread's {everyone} allocations were charged to this \
         thread ({mine} counted) — the allocation counter is process-global \
         again and the hot-path assertions will flake under load (bn-33l3)"
    );
    assert!(
        everyone >= CHURN,
        "the helper thread's allocations were not observed at all ({everyone} \
         counted over {CHURN} churns) — this guard is vacuous"
    );
}
