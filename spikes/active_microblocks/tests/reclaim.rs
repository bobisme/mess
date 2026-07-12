//! Generation-slab reclamation (design §8.5 / research/03 §3.4): readers
//! hold a generation lease (an `Arc` generation handle via `ArcSwap`), the
//! roll retires the generation, and the slab frees only after every lease
//! drains. The use-after-free invariant (the canary poisoned by `Drop`
//! before the slabs release) is checked on EVERY read.

#![cfg(not(loom))]

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

use active_microblocks::workload::{Streams, apply_groups, build_schedule};
use active_microblocks::{F1, Index};
use arc_swap::ArcSwap;

/// Wrapper whose Drop flags retirement the instant the generation's
/// destructor begins (before the slabs free).
struct Tracked {
    idx:     F1,
    dropped: Arc<AtomicBool>,
}

impl Drop for Tracked {
    fn drop(&mut self) {
        self.dropped.store(true, Ordering::Release);
    }
}

#[test]
fn slab_freed_only_after_leases_drain() {
    let sched = build_schedule(Streams::Zipf(512), 20_000, 2, 42);
    let dropped_a = Arc::new(AtomicBool::new(false));
    let gen_a = {
        let idx = F1::new(1, 8);
        apply_groups(&idx, &sched.batches, 64);
        Arc::new(Tracked { idx, dropped: dropped_a.clone() })
    };
    let expect_head = |sid: u64| match sched.heads[sid as usize] {
        u64::MAX => None,
        h => Some(h),
    };

    let current: Arc<ArcSwap<Tracked>> = Arc::new(ArcSwap::from(gen_a));
    let stop = Arc::new(AtomicBool::new(false));
    let reads = Arc::new(AtomicU64::new(0));
    let start = Arc::new(Barrier::new(5));

    // 4 readers: lock-free generation loads, canary checked on every read.
    let readers: Vec<_> = (0..4)
        .map(|t| {
            let current = current.clone();
            let stop = stop.clone();
            let reads = reads.clone();
            let start = start.clone();
            let heads = sched.heads.clone();
            std::thread::spawn(move || {
                start.wait();
                let mut i = t as u64;
                while !stop.load(Ordering::Relaxed) {
                    let g = current.load();
                    g.idx.check_canary();
                    let sid = i % 512;
                    if heads[sid as usize] != u64::MAX {
                        let h = g.idx.head(sid);
                        if let Some(h) = h {
                            assert!(g.idx.resolve(sid, h).is_some());
                        }
                    }
                    g.idx.check_canary();
                    reads.fetch_add(1, Ordering::Relaxed);
                    i += 1;
                }
            })
        })
        .collect();

    start.wait();
    while reads.load(Ordering::Relaxed) < 10_000 {
        std::hint::spin_loop();
    }

    // Take explicit long-lived leases on generation A, then retire it.
    let lease_1 = current.load_full();
    let lease_2 = current.load_full();
    let gen_b = {
        let idx = F1::new(2, 8);
        apply_groups(&idx, &sched.batches[..1000], 64);
        Arc::new(Tracked { idx, dropped: Arc::new(AtomicBool::new(false)) })
    };
    current.store(gen_b); // seal/roll: A retired from the swap point

    // Leases still held: A must NOT free, and reads THROUGH the lease must
    // still be correct (readers replaying the sealed segment mid-roll).
    std::thread::sleep(Duration::from_millis(100));
    assert!(!dropped_a.load(Ordering::Acquire), "slab freed under live leases");
    lease_1.idx.check_canary();
    for sid in 0..64u64 {
        assert_eq!(lease_1.idx.head(sid), expect_head(sid));
        if sched.heads[sid as usize] != u64::MAX {
            assert!(lease_1.idx.resolve(sid, sched.heads[sid as usize]).is_some());
        }
    }
    lease_1.idx.check_canary();

    drop(lease_1);
    std::thread::sleep(Duration::from_millis(50));
    assert!(!dropped_a.load(Ordering::Acquire), "freed with one lease still held");
    lease_2.idx.check_canary();
    drop(lease_2);

    // All leases drained: the slab must free promptly.
    let t0 = Instant::now();
    while !dropped_a.load(Ordering::Acquire) {
        assert!(t0.elapsed() < Duration::from_secs(5), "slab never freed after drain");
        std::thread::yield_now();
    }

    stop.store(true, Ordering::Relaxed);
    for r in readers {
        r.join().unwrap();
    }
    assert!(reads.load(Ordering::Relaxed) > 10_000);
}

/// The same drain discipline while readers keep reading across MANY rolls
/// (generation churn under load; canary on every read).
#[test]
fn repeated_rolls_under_readers() {
    let sched = build_schedule(Streams::Zipf(128), 5_000, 1, 7);
    let mk = |seg: u64, flag: &Arc<AtomicBool>| {
        let idx = F1::new(seg, 8);
        apply_groups(&idx, &sched.batches, 32);
        Arc::new(Tracked { idx, dropped: flag.clone() })
    };
    let flag0 = Arc::new(AtomicBool::new(false));
    let current = Arc::new(ArcSwap::from(mk(1, &flag0)));
    let stop = Arc::new(AtomicBool::new(false));

    let readers: Vec<_> = (0..4)
        .map(|t| {
            let current = current.clone();
            let stop = stop.clone();
            std::thread::spawn(move || {
                let mut i = t as u64;
                let mut hits = 0u64;
                while !stop.load(Ordering::Relaxed) {
                    let g = current.load();
                    g.idx.check_canary();
                    if g.idx.resolve(i % 128, 0).is_some() {
                        hits += 1;
                    }
                    g.idx.check_canary();
                    i += 1;
                }
                hits
            })
        })
        .collect();

    let mut flags = vec![flag0];
    for roll in 0..30 {
        std::thread::sleep(Duration::from_millis(10));
        let flag = Arc::new(AtomicBool::new(false));
        current.store(mk(roll + 2, &flag));
        flags.push(flag);
    }
    // Every retired generation eventually frees (no leaked slabs)...
    let t0 = Instant::now();
    while flags[..flags.len() - 1]
        .iter()
        .any(|f| !f.load(Ordering::Acquire))
    {
        assert!(t0.elapsed() < Duration::from_secs(5), "retired slab never freed");
        std::thread::yield_now();
    }
    // ...and the live one never does.
    assert!(!flags.last().unwrap().load(Ordering::Acquire));

    stop.store(true, Ordering::Relaxed);
    for r in readers {
        assert!(r.join().unwrap() > 0);
    }
}
