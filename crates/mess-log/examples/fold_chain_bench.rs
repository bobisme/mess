//! Append-path fold-chain cost bench (bn-1d0, spec 05 §10). Prints ns/event and
//! ev/s for the two BLAKE3 stages the chain adds (frame_hash, then chain_step)
//! versus a frame-hash-only baseline, over realistic ~250 B payloads.
//!
//! Run (release, for meaningful numbers):
//!   cargo run -p mess-log --release --example fold_chain_bench

use std::hint::black_box;
use std::time::Instant;

use mess_log::fold_chain::{chain_step, frame_hash, genesis};

fn main() {
    let n: u64 = 1_000_000;
    let payload = vec![0xA5u8; 250];

    // Warmup.
    let mut h = genesis(1);
    for v in 0..10_000u64 {
        h = chain_step(&h, &frame_hash(v, &payload), v);
    }
    black_box(h);

    // frame_hash only (1× BLAKE3/event) — the baseline a frame-hashing store
    // pays.
    let start = Instant::now();
    let mut acc = [0u8; 32];
    for v in 0..n {
        let fh = frame_hash(v, &payload);
        acc[0] ^= fh[0];
    }
    let fh_only = start.elapsed();
    black_box(acc);

    // full chain (2× BLAKE3/event) — frame_hash + chain_step.
    let mut h = genesis(1);
    let start = Instant::now();
    for v in 0..n {
        h = chain_step(&h, &frame_hash(v, &payload), v);
    }
    let full = start.elapsed();
    black_box(h);

    let fh_ns = fh_only.as_nanos() as f64 / n as f64;
    let full_ns = full.as_nanos() as f64 / n as f64;
    println!("events: {n}, payload: {} B", payload.len());
    println!("frame_hash only : {fh_ns:7.1} ns/ev  ({:.0} ev/s)", 1e9 / fh_ns);
    println!(
        "full chain      : {full_ns:7.1} ns/ev  ({:.0} ev/s)",
        1e9 / full_ns
    );
    println!(
        "chain_step delta: {:7.1} ns/ev  (the second BLAKE3 over 72 fixed \
         bytes)",
        full_ns - fh_ns
    );
}
