//! Alloc-counter test: **zero heap allocations per event on the hot path**
//! after warmup (the `spikes/perf_append` discipline — one contiguous reusable
//! batch buffer, one positioned write per batch).
//!
//! This integration test binary installs a counting [`GlobalAlloc`] wrapper
//! (the standard trick — a `#[global_allocator]` here affects only this test
//! binary). It has exactly ONE `#[test]` so the measured window runs with no
//! other test thread allocating concurrently.
//!
//! We warm up (create the writer, encode/append one batch so the encoder's
//! buffer grows to its steady size), snapshot the allocation count, then run N
//! encode+append iterations and assert the count did not move.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

use mess_log::encode::{BatchEncoder, BatchInput, Subframe};
use mess_log::runtime::{RealRuntime, Runtime};
use mess_log::writer::{BatchSpec, SegmentParams, SegmentWriter};

/// Number of allocating calls (alloc / alloc_zeroed / grow-via-realloc) since
/// process start. Deallocs and shrinks are not counted — we care that the hot
/// path requests no NEW memory.
static ALLOCS: AtomicUsize = AtomicUsize::new(0);

struct Counting;

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCS.fetch_add(1, Ordering::Relaxed);
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        ALLOCS.fetch_add(1, Ordering::Relaxed);
        unsafe { System.alloc_zeroed(layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        // A grow is a fresh allocation for our purposes; a shrink is not.
        if new_size > layout.size() {
            ALLOCS.fetch_add(1, Ordering::Relaxed);
        }
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static GLOBAL: Counting = Counting;

fn allocs() -> usize {
    ALLOCS.load(Ordering::Relaxed)
}

// bn-25j: the writer half of this test opens a real segment file via
// `RealRuntime` (`std::env::temp_dir()` + `std::fs`); Miri's isolation
// blocks real `open`, and a 20_000-iteration hot-path timing test is also
// far too slow interpreted under Miri regardless. Excluded from the Miri
// lane.
#[test]
#[cfg_attr(miri, ignore)]
fn zero_allocs_per_event_on_the_hot_path() {
    const N: u64 = 20_000;

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
    let mut sink = 0u8;
    for i in 1..N {
        let bytes = enc.encode(&mk_input(i, i)).unwrap();
        sink ^= bytes[0] ^ bytes[bytes.len() - 1]; // keep the work observable
    }
    let encode_allocs = allocs() - before;
    assert_eq!(encode_allocs, 0, "encoder allocated {encode_allocs} times over {N} batches (want 0)");
    std::hint::black_box(sink);

    // --- Part 2: encode + append through the writer is zero-alloc ----------
    // RealRuntime's pwrite is write_at(2): no Rust heap allocation.
    let rt = RealRuntime::new();
    let path = std::env::temp_dir().join(format!("mess-log-alloc-{}.seg", std::process::id()));
    let _cleanup = RmOnDrop(path.clone());

    let mut w = SegmentWriter::create(&rt.fs(), &path, SegmentParams::new(1, 0, 1, 0)).unwrap();
    let spec = BatchSpec { stream_id: 7, category_id: 0, first_stream_version: 0, crypto_chain: None, subframes: &sfs };

    // Warm up the writer's encoder buffer + first pwrite.
    w.append(&spec).unwrap();

    let before = allocs();
    for _ in 1..N {
        w.append(&spec).unwrap();
    }
    let append_allocs = allocs() - before;
    assert_eq!(append_allocs, 0, "encode+append allocated {append_allocs} times over {N} batches (want 0)");
}

struct RmOnDrop(std::path::PathBuf);
impl Drop for RmOnDrop {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}
