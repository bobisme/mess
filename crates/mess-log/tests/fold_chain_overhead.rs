//! Append-path chain-maintenance cost (bn-1d0, spec 05 §10). Measures the
//! per-event overhead of maintaining the crypto chain (two BLAKE3 invocations,
//! §6.3) and checks it lands in the measured envelope (~165 ns/event; the spec
//! reports +33% CPU over a store that already hashes frames, ~3% of the composed
//! append budget). The bound here is deliberately loose so the gate is not
//! flaky on a loaded machine — the point is to prove the order of magnitude and
//! that the chain is nearly free, not to micro-benchmark.

use std::hint::black_box;
use std::time::Instant;

use mess_log::fold_chain::{chain_step, frame_hash, genesis};

/// The chain's *incremental* append overhead (§10) is the SECOND BLAKE3 — the
/// `chain_step` over 72 fixed bytes — on top of a store that already hashes
/// frames (`frame_hash`). We measure both stages and report the delta, which is
/// the ~165 ns/event figure §10 pins (+33% CPU over frame-hash-only). Measuring
/// the full two-hash `advance` instead would fold in the payload hash a
/// frame-hashing store already pays, overstating the chain's own cost.
#[test]
#[cfg_attr(miri, ignore)] // timing loop; not meaningful under miri
fn chain_append_overhead_is_in_envelope() {
    let n: u64 = 300_000;
    let payload = vec![0xA5u8; 250]; // realistic ~250 B event (§10)

    // Warm up (code paths, CPU frequency).
    let mut h = genesis(1);
    for v in 0..5_000u64 {
        h = chain_step(&h, &frame_hash(v, &payload), v);
    }
    black_box(h);

    // Baseline: frame_hash only (the 1× BLAKE3/event a frame-hashing store pays).
    let start = Instant::now();
    let mut acc = 0u8;
    for v in 0..n {
        acc ^= frame_hash(v, &payload)[0];
    }
    let fh_only = start.elapsed().as_nanos() as f64 / n as f64;
    black_box(acc);

    // Full chain: frame_hash + chain_step (2× BLAKE3/event).
    let mut h = genesis(1);
    let start = Instant::now();
    for v in 0..n {
        h = chain_step(&h, &frame_hash(v, &payload), v);
    }
    let full = start.elapsed().as_nanos() as f64 / n as f64;
    black_box(h);

    let delta = full - fh_only;
    eprintln!(
        "chain append overhead: frame_hash {fh_only:.1} ns/ev, full {full:.1} ns/ev, \
         chain_step delta {delta:.1} ns/ev (spec §10 envelope ~165 ns/ev)"
    );

    // Generous ceiling: the measured target is ~165 ns; 2 µs leaves ~12× slack
    // for CI contention while still catching a pathological regression.
    assert!(
        delta < 2_000.0,
        "chain_step delta {delta:.1} ns/event is far outside the ~165 ns \
         envelope — a real regression"
    );
    // The chain_step input is fixed-width (72 B), so it MUST be cheaper than the
    // payload hash over a realistic 250 B event (a sanity check on §10's claim
    // that "the second BLAKE3 is far cheaper than the payload hash").
    assert!(
        delta < fh_only + 100.0,
        "chain_step ({delta:.1} ns) should be no costlier than the payload hash \
         ({fh_only:.1} ns) — §10"
    );
}
