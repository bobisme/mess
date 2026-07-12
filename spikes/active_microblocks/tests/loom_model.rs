//! Loom model of the REAL microblock publication protocol (src/micro.rs
//! compiled with loom atomics + tracked cells via src/shim.rs; ENTRIES = 2
//! under `--cfg loom` so a block boundary is crossed with a tiny state
//! space).
//!
//! Scenario (per the bone brief): the single writer fills a block's entries
//! and publishes counts, then a later writer action allocates the next
//! block, links it via `previous`, and publishes the new tail; two
//! concurrent readers traverse tail/previous. Loom's tracked `UnsafeCell`s
//! flag any entry/header read that the `published`/tail release-acquire
//! edges do not order (i.e. uninitialized or torn reads); the assertions
//! check monotone watermarks/heads and that every successful read returns
//! exactly the schedule's value.
//!
//! Run: RUSTFLAGS="--cfg loom" cargo test --release --test loom_model

#![cfg(loom)]

use active_microblocks::micro::MicroIndex;
use loom::sync::Arc;
use mess_index::{BatchEntry, EventPtr};

fn batch(sid: u64, v: u64, pos: u64, off: u64) -> BatchEntry {
    BatchEntry {
        stream_id:            sid,
        first_stream_version: v,
        frame_count:          1,
        first_global_pos:     pos,
        ptr:                  EventPtr { segment_id: 1, offset: off },
    }
}

// Truth table for the fixed schedule: version -> offset.
const OFF: [u64; 3] = [100, 200, 300];

#[test]
fn writer_fills_links_and_publishes_under_two_readers() {
    let mut builder = loom::model::Builder::new();
    builder.preemption_bound = Some(3);
    builder.check(|| {
        let idx = Arc::new(MicroIndex::new(1, 4));

        let w = {
            let idx = idx.clone();
            loom::thread::spawn(move || {
                // Group 1 fills block A (ENTRIES = 2 under loom): entry
                // writes + published-count release stores.
                idx.apply_committed(2, &[batch(7, 0, 0, OFF[0]), batch(7, 1, 1, OFF[1])]);
                // Group 2: block A is full — allocate block B privately,
                // link previous = A, publish the new tail (release store).
                idx.apply_committed(3, &[batch(7, 2, 2, OFF[2])]);
            })
        };

        // Reader 1: head then resolve-at-head. A committed head must
        // resolve to exactly the schedule's pointer (a torn/uninitialized
        // entry would return a wrong offset — and loom's tracked cells
        // would flag the unordered access first). Covers the monotone
        // published-count contract: head is derived from one published
        // load, resolve re-loads it and must see at least as much.
        let r1 = {
            let idx = idx.clone();
            loom::thread::spawn(move || {
                if let Some(h) = idx.stream_head(7) {
                    let p = idx.resolve_f1(7, h).expect("committed head must resolve");
                    assert_eq!(p.segment_id, 1);
                    assert_eq!(p.offset, OFF[h as usize], "torn entry at head {h}");
                }
            })
        };
        // Reader 2: tail/previous traversal from the far end (v=2 lives in
        // block B, v=0 at the bottom of block A) with the binary and skip
        // search variants.
        let r2 = {
            let idx = idx.clone();
            loom::thread::spawn(move || {
                if let Some(p) = idx.resolve_f2(7, 2) {
                    assert_eq!(p.offset, OFF[2]);
                }
                if let Some(p) = idx.resolve_f3(7, 0) {
                    assert_eq!(p.offset, OFF[0]);
                }
            })
        };

        w.join().unwrap();
        r1.join().unwrap();
        r2.join().unwrap();

        // Quiescent: everything visible and exact.
        assert_eq!(idx.stream_head(7), Some(2));
        for v in 0..3u64 {
            assert_eq!(idx.resolve_f1(7, v).unwrap().offset, OFF[v as usize]);
            assert_eq!(idx.resolve_f2(7, v).unwrap().offset, OFF[v as usize]);
            assert_eq!(idx.resolve_f3(7, v).unwrap().offset, OFF[v as usize]);
        }
        assert_eq!(idx.global_seek(1).unwrap(), (0, OFF[0]));
    });
}
