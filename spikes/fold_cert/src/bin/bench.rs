//! Measurements for REPORT.md. Run with:
//!   cargo run --release --bin bench
//!
//! 1M events, ~250 B payloads:
//!   - append path: no hashing / frame_hash only / frame_hash + chain
//!   - load_verified cost for tails of 0 / 10 / 100 / 10_000 events
//!   - full-chain verification of the 1M-event stream (MB/s)

use fold_cert::*;
use std::hint::black_box;
use std::time::Instant;

const N: u64 = 1_000_000;
const PAYLOAD_LEN: usize = 250;

fn make_payloads(n: u64) -> Vec<Vec<u8>> {
    // Deterministic, cheap "random" fill so hashing sees realistic bytes.
    let mut seed = 0x9e3779b97f4a7c15u64;
    (0..n)
        .map(|i| {
            let mut p = encode_event((i % 3 == 2) as u8, 10 + i, PAYLOAD_LEN);
            for chunk in p[9..].chunks_mut(8) {
                seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
                let b = seed.to_le_bytes();
                let l = chunk.len();
                chunk.copy_from_slice(&b[..l]);
            }
            p
        })
        .collect()
}

fn main() {
    eprintln!("generating {N} payloads of {PAYLOAD_LEN} B...");
    let payloads = make_payloads(N);
    let total_bytes = (N as usize * PAYLOAD_LEN) as f64;

    // -- append baselines ---------------------------------------------------
    // Baseline 0: raw append, no hashing at all.
    let t = Instant::now();
    let mut frames: Vec<Frame> = Vec::with_capacity(N as usize);
    for (i, p) in payloads.iter().enumerate() {
        frames.push(Frame {
            stream_version: i as u64,
            payload: p.clone(),
            frame_hash: [0; 32],
            prev_stream_hash: [0; 32],
        });
    }
    let dt_raw = t.elapsed();
    black_box(&frames);
    drop(frames);

    // Baseline 1: frame_hash only (1 BLAKE3/event) — a store that hashes
    // frames for integrity but keeps no fold chain.
    let t = Instant::now();
    let mut frames: Vec<Frame> = Vec::with_capacity(N as usize);
    for (i, p) in payloads.iter().enumerate() {
        let fh = frame_hash(p, i as u64);
        frames.push(Frame {
            stream_version: i as u64,
            payload: p.clone(),
            frame_hash: fh,
            prev_stream_hash: [0; 32],
        });
    }
    let dt_fh = t.elapsed();
    black_box(&frames);
    drop(frames);

    // Full chain: frame_hash + chain_next (2 BLAKE3/event).
    let t = Instant::now();
    let mut stream = Stream::new("bench");
    stream.frames.reserve(N as usize);
    for p in payloads.iter() {
        stream.append(p.clone());
    }
    let dt_chain = t.elapsed();

    let evps = |d: std::time::Duration| N as f64 / d.as_secs_f64();
    println!("== append path (1M events, {PAYLOAD_LEN} B payloads) ==");
    println!("raw append (no hashing):     {:>10.0} ev/s  ({:?})", evps(dt_raw), dt_raw);
    println!("frame_hash only (1x BLAKE3): {:>10.0} ev/s  ({:?})", evps(dt_fh), dt_fh);
    println!("full chain (2x BLAKE3):      {:>10.0} ev/s  ({:?})", evps(dt_chain), dt_chain);
    println!(
        "chain overhead vs raw: {:.1}%   vs frame_hash-only: {:.1}%   per-event chain cost: {:.0} ns",
        (dt_chain.as_secs_f64() / dt_raw.as_secs_f64() - 1.0) * 100.0,
        (dt_chain.as_secs_f64() / dt_fh.as_secs_f64() - 1.0) * 100.0,
        (dt_chain - dt_fh).as_nanos() as f64 / N as f64,
    );

    // -- load_verified for various tail lengths ------------------------------
    println!("\n== load_verified (snapshot + tail replay) ==");
    let mut store = SnapshotStore::new();
    for tail in [0u64, 10, 100, 10_000] {
        let snap_v = N - 1 - tail;
        store.take_snapshot::<Account>(&stream, snap_v);
        // warm + measure (median of 5)
        let mut times = Vec::new();
        for _ in 0..5 {
            let t = Instant::now();
            let out = load_verified::<Account>(&stream, &mut store).unwrap();
            times.push(t.elapsed());
            black_box(out.state.balance);
            assert_eq!(out.tail_len, tail);
        }
        times.sort();
        println!("tail = {tail:>6} events: {:?} (median of 5)", times[2]);
    }

    // -- full-chain verification ---------------------------------------------
    println!("\n== full-chain verification (1M events) ==");
    let t = Instant::now();
    let acct = full_replay_verified::<Account>(&stream).unwrap();
    let dt = t.elapsed();
    black_box(acct.balance);
    println!(
        "full verify+fold: {:?}  ({:.0} ev/s, {:.0} MB/s payload)",
        dt,
        N as f64 / dt.as_secs_f64(),
        total_bytes / dt.as_secs_f64() / 1e6
    );
}
