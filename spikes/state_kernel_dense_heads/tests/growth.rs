//! Page growth under concurrent readers (threaded, non-loom): the directory
//! doubles many times while 4 readers hammer point reads, every read checked
//! against the torn-pair invariant `global == version << 32 | id`.

#![cfg(not(loom))]

use state_kernel_dense_heads::direct::{
    CellKind, DblCell, DirectTable, SeqCell, U128Cell, Update, check_pair,
    encode_global,
};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

fn growth_under_readers<C: CellKind>(grow_to: u64) {
    let table = DirectTable::<C>::new(1); // 1 page dir => many doublings
    let published = AtomicU64::new(0);
    let stop = AtomicBool::new(false);
    let violations = AtomicU64::new(0);
    let reads = AtomicU64::new(0);

    std::thread::scope(|s| {
        for t in 0..4u64 {
            let (table, published, stop, violations, reads) =
                (&table, &published, &stop, &violations, &reads);
            s.spawn(move || {
                let mut rr = 0u32;
                let mut x = (t + 1) * 0x9E37_79B9_7F4A_7C15;
                let mut ops = 0u64;
                while !stop.load(Ordering::Relaxed) {
                    let hi = published.load(Ordering::Acquire);
                    if hi == 0 {
                        std::thread::yield_now();
                        continue;
                    }
                    x ^= x << 13;
                    x ^= x >> 7;
                    x ^= x << 17;
                    let id = x % hi;
                    let (v, g) = table.get(id, &mut rr);
                    if v == 0 || !check_pair(id, v, g) {
                        violations.fetch_add(1, Ordering::Relaxed);
                    }
                    ops += 1;
                }
                reads.fetch_add(ops, Ordering::Relaxed);
            });
        }

        let mut versions = vec![0u64; grow_to as usize];
        let mut buf: Vec<Update> = Vec::with_capacity(2_000);
        let mut next = 0u64;
        let mut x = 0xDEAD_BEEFu64;
        while next < grow_to {
            buf.clear();
            let end = (next + 1_000).min(grow_to);
            for id in next..end {
                versions[id as usize] = 1;
                buf.push((id, 1, encode_global(1, id)));
            }
            if next > 0 {
                for _ in 0..1_000 {
                    x ^= x << 13;
                    x ^= x >> 7;
                    x ^= x << 17;
                    let id = x % next;
                    let v = versions[id as usize] + 1;
                    versions[id as usize] = v;
                    buf.push((id, v, encode_global(v, id)));
                }
            }
            table.apply(&mut buf);
            published.store(end, Ordering::Release);
            next = end;
        }
        std::thread::sleep(std::time::Duration::from_millis(50));
        stop.store(true, Ordering::Relaxed);
    });

    assert!(reads.load(Ordering::Relaxed) > 0, "readers made no progress");
    assert_eq!(
        violations.load(Ordering::Relaxed),
        0,
        "{}: torn or missing pairs observed during page growth",
        C::NAME
    );
}

#[test]
fn growth_under_readers_seqlock() {
    growth_under_readers::<SeqCell>(1_000_000);
}

#[test]
fn growth_under_readers_u128() {
    growth_under_readers::<U128Cell>(1_000_000);
}

#[test]
fn growth_under_readers_dblbuf() {
    // NOTE: A3's torn window (reader stalled across two same-cell updates)
    // exists in theory; this test documents whether it fires in practice.
    growth_under_readers::<DblCell>(1_000_000);
}

#[test]
fn sequential_read_back() {
    let table = DirectTable::<SeqCell>::new(1);
    let n = 100_000u64;
    let mut buf: Vec<Update> =
        (0..n).map(|id| (id, 1, encode_global(1, id))).collect();
    table.apply(&mut buf);
    let mut rr = 0u32;
    for id in 0..n {
        assert_eq!(table.get(id, &mut rr), (1, encode_global(1, id)));
    }
    // Absent ids read as (0, 0).
    assert_eq!(table.get(n + 5_000_000, &mut rr), (0, 0));
    assert_eq!(rr, 0, "no retries without a writer");
}
