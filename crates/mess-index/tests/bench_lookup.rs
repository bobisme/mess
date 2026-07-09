//! Micro-measurement (not a criterion harness — keeps deps light) for the two
//! hot metadata reads the bone calls out: `stream_head` point lookup and the
//! `dedupe_lookup` absorb check. Prints ns/op it measured on the current host
//! and asserts a generous ceiling so it never flakes in CI. The module docs'
//! headline numbers come from running this.
//!
//! Real fs + fjall threads -> `#[cfg_attr(miri, ignore)]`.

use std::time::Instant;

use mess_index::meta::{CommitGroup, Head, MetaStore, StreamId};

const N: u64 = 10_000;
const ITERS: u64 = 200_000;

#[test]
#[cfg_attr(miri, ignore)]
fn bench_head_and_dedupe_lookup() {
    let dir = tempfile::tempdir().unwrap();
    let store = MetaStore::open(dir.path()).unwrap();

    // Populate N heads and N dedupe keys, batched in groups of 256.
    let mut p = 0u64;
    while p < N {
        let mut g = CommitGroup::new(p + 256 + 1);
        for _ in 0..256 {
            if p >= N {
                break;
            }
            let id = StreamId(p + 1);
            g.stream_heads.push((id, Head { version: p, global_position: p }));
            g.dedupe.push((id, format!("dk{p}").into_bytes(), p));
            p += 1;
        }
        store.apply_group(&g).unwrap();
    }

    // Warm the cache.
    for i in 0..N {
        let _ = store.stream_head(StreamId(i + 1)).unwrap();
        let _ = store.dedupe_lookup(StreamId(i + 1), format!("dk{i}").as_bytes()).unwrap();
    }

    // Measure head lookups.
    let t = Instant::now();
    let mut hits = 0u64;
    for i in 0..ITERS {
        let id = StreamId((i % N) + 1);
        if store.stream_head(id).unwrap().is_some() {
            hits += 1;
        }
    }
    let head_ns = t.elapsed().as_nanos() as f64 / ITERS as f64;
    assert_eq!(hits, ITERS);

    // Measure dedupe lookups.
    let t = Instant::now();
    let mut hits = 0u64;
    for i in 0..ITERS {
        let k = i % N;
        let id = StreamId(k + 1);
        if store.dedupe_lookup(id, format!("dk{k}").as_bytes()).unwrap().is_some() {
            hits += 1;
        }
    }
    let dedupe_ns = t.elapsed().as_nanos() as f64 / ITERS as f64;
    assert_eq!(hits, ITERS);

    eprintln!("bench: stream_head   ~{head_ns:.1} ns/op");
    eprintln!("bench: dedupe_lookup ~{dedupe_ns:.1} ns/op");

    // Generous ceiling (20 µs) — the point is the printed number, not a tight
    // gate; asserting a small bound would flake under CI load.
    assert!(head_ns < 20_000.0, "head lookup too slow: {head_ns} ns");
    assert!(dedupe_ns < 20_000.0, "dedupe lookup too slow: {dedupe_ns} ns");
}
