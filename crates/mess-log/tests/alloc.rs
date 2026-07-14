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

use mess_log::committer::{
    AppendRequest, ChainInit, DirectAppendRequest, DirectCommitter, Durability,
    EventInput, Roller,
};
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

    unsafe fn realloc(
        &self,
        ptr: *mut u8,
        layout: Layout,
        new_size: usize,
    ) -> *mut u8 {
        // A grow is a fresh allocation for our purposes; a shrink is not.
        if new_size > layout.size() {
            ALLOCS.fetch_add(1, Ordering::Relaxed);
        }
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static GLOBAL: Counting = Counting;

fn allocs() -> usize { ALLOCS.load(Ordering::Relaxed) }

// bn-25j: the writer half of this test opens a real segment file via
// `RealRuntime` (a self-sweeping real-fs temp dir + `std::fs`); Miri's
// isolation blocks real `open`, and a 20_000-iteration hot-path timing test
// is also far too slow interpreted under Miri regardless. Excluded from the
// Miri lane.
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
    assert_eq!(
        encode_allocs, 0,
        "encoder allocated {encode_allocs} times over {N} batches (want 0)"
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
    for _ in 1..N {
        w.append(&spec).unwrap();
    }
    let append_allocs = allocs() - before;
    assert_eq!(
        append_allocs, 0,
        "encode+append allocated {append_allocs} times over {N} batches (want \
         0)"
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
    assert_eq!(completed, DIRECT_N);
    assert_eq!(
        direct_allocs, 0,
        "warmed DirectCommitter allocated {direct_allocs} times for \
         {DIRECT_N} small appends"
    );
}
