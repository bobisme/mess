//! Append-path fold-chain cost (bn-1d0, spec 05 §10): ns/event and ev/s for
//! the two BLAKE3 stages the chain adds (`frame_hash`, then `chain_step`)
//! versus a frame-hash-only baseline. Ported from
//! `mess-log/examples/fold_chain_bench.rs`. Full-size N=1,000,000 matches
//! `phase5.foldchain.*` in `docs/perf/envelope.md`.

use std::hint::black_box;
use std::time::Instant;

use mess_log::fold_chain::{chain_step, frame_hash, genesis};

use crate::{Metric, RunSize};

pub fn run(size: RunSize) -> Vec<Metric> {
    let n: u64 = match size {
        RunSize::Full => 1_000_000,
        RunSize::Smoke => 5_000,
    };
    let payload = vec![0xA5u8; 250];

    // Warmup.
    let mut h = genesis(1);
    for v in 0..(n / 100).max(1) {
        h = chain_step(&h, &frame_hash(v, &payload), v);
    }
    black_box(h);

    // frame_hash only (1x BLAKE3/event) — the baseline a frame-hashing store
    // pays.
    let start = Instant::now();
    let mut acc = [0u8; 32];
    for v in 0..n {
        let fh = frame_hash(v, &payload);
        acc[0] ^= fh[0];
    }
    let fh_only = start.elapsed();
    black_box(acc);

    // full chain (2x BLAKE3/event) — frame_hash + chain_step.
    let mut h = genesis(1);
    let start = Instant::now();
    for v in 0..n {
        h = chain_step(&h, &frame_hash(v, &payload), v);
    }
    let full = start.elapsed();
    black_box(h);

    let fh_ns = fh_only.as_nanos() as f64 / n as f64;
    let full_ns = full.as_nanos() as f64 / n as f64;

    vec![
        Metric::new(
            "phase5.foldchain.append_overhead_ns",
            full_ns - fh_ns,
            "ns/event",
            format!(
                "chain_step delta: the second BLAKE3 (over 72 fixed bytes); \
                 {n} events, 250B payloads, single core"
            ),
        ),
        Metric::new(
            "phase5.foldchain.full_chain.ev_per_s",
            1e9 / full_ns,
            "ev/s",
            format!(
                "full chain (2x BLAKE3/event: frame_hash + chain_step); \
                 {full_ns:.1} ns/ev; {n} events, single core"
            ),
        ),
    ]
}
