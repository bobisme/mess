//! Fold-chain append-cost coverage (bn-1d0, bn-2nd, spec 05 §10).
//!
//! The normal test suite checks the construction deterministically. Wall-clock
//! admission is an explicitly ignored release-mode test because measuring two
//! sub-microsecond phases while the workspace suite is running produces host-
//! load failures rather than product-regression signal.
//!
//! Run the performance admission test only on a quiet host:
//!
//! ```text
//! cargo test -p mess-log --release --test fold_chain_overhead \
//!   chain_append_overhead_is_in_envelope -- --ignored --nocapture
//! ```

use std::hint::black_box;
use std::time::Instant;

use mess_log::fold_chain::{
    Hash, TAG_CHAIN, advance, chain_step, frame_hash, genesis,
};

const PAYLOAD_BYTES: usize = 250;
const CHAIN_FIELDS_BYTES: usize = size_of::<Hash>() * 2 + size_of::<u64>();
const CHAIN_INPUT_BYTES: usize = 1 + CHAIN_FIELDS_BYTES;

/// Lock the structural reason the incremental chain step stays cheap: after
/// the one-byte domain tag it hashes exactly two 32-byte hashes and one u64,
/// independent of the event payload size. This assertion is deterministic and
/// therefore remains useful under parallel workspace-test load.
#[test]
fn chain_step_matches_the_fixed_72_byte_construction() {
    assert_eq!(CHAIN_FIELDS_BYTES, 72);
    assert_eq!(CHAIN_INPUT_BYTES, 73);

    for (stream_id, version, payload) in [
        (1, 0, &[][..]),
        (7, 42, &[0xA5; PAYLOAD_BYTES][..]),
        (u64::MAX, u64::MAX, &[0x5A; 4_096][..]),
    ] {
        let prev = genesis(stream_id);
        let fh = frame_hash(version, payload);

        let mut input = [0_u8; CHAIN_INPUT_BYTES];
        input[0] = TAG_CHAIN;
        input[1..33].copy_from_slice(&prev);
        input[33..65].copy_from_slice(&fh);
        input[65..].copy_from_slice(&version.to_le_bytes());

        let expected = *blake3::hash(&input).as_bytes();
        assert_eq!(chain_step(&prev, &fh, version), expected);
        assert_eq!(advance(&prev, version, payload), expected);
    }
}

/// Quiet-host admission for the chain's *incremental* append overhead (§10):
/// the second BLAKE3 (`chain_step` over 72 fixed bytes) on top of a store that
/// already pays for `frame_hash`.
///
/// This is intentionally ignored in the normal suite. `Instant` measures
/// scheduler contention along with the code, and subtracting two separately
/// timed phases magnifies that noise. The explicit release command in the
/// module docs is the performance gate; the deterministic construction test
/// above is the normal-suite regression gate.
#[test]
#[ignore = "quiet-host performance gate; run explicitly in --release mode"]
fn chain_append_overhead_is_in_envelope() {
    assert!(
        !cfg!(debug_assertions),
        "the fold-chain performance gate must run with --release"
    );

    const EVENTS: u64 = 1_000_000;
    const REPS: usize = 5;
    let payload = vec![0xA5_u8; PAYLOAD_BYTES];

    // Warm up code paths and CPU frequency before collecting paired samples.
    let mut h = genesis(1);
    for version in 0..10_000_u64 {
        h = chain_step(&h, &frame_hash(version, &payload), version);
    }
    black_box(h);

    let mut samples = Vec::with_capacity(REPS);
    for _ in 0..REPS {
        let start = Instant::now();
        let mut acc = 0_u8;
        for version in 0..EVENTS {
            acc ^= frame_hash(version, &payload)[0];
        }
        let frame_hash_ns = start.elapsed().as_nanos() as f64 / EVENTS as f64;
        black_box(acc);

        let mut h = genesis(1);
        let start = Instant::now();
        for version in 0..EVENTS {
            h = chain_step(&h, &frame_hash(version, &payload), version);
        }
        let full_ns = start.elapsed().as_nanos() as f64 / EVENTS as f64;
        black_box(h);

        samples.push((frame_hash_ns, full_ns, full_ns - frame_hash_ns));
    }

    samples.sort_by(|a, b| a.2.total_cmp(&b.2));
    let (frame_hash_ns, full_ns, delta_ns) = samples[REPS / 2];
    eprintln!(
        "fold-chain quiet-host median ({REPS} reps): frame_hash \
         {frame_hash_ns:.1} ns/event, full {full_ns:.1} ns/event, chain_step \
         delta {delta_ns:.1} ns/event (spec §10 target ~165 ns/event)"
    );

    // Three times the 169.3 ns/event same-machine release baseline preserves
    // architecture/runtime headroom while detecting a material regression.
    assert!(
        delta_ns < 510.0,
        "chain_step median delta {delta_ns:.1} ns/event exceeds the 510 \
         ns/event quiet-host envelope"
    );
    assert!(
        delta_ns < frame_hash_ns,
        "fixed-width chain_step ({delta_ns:.1} ns) should remain cheaper than \
         hashing a realistic {PAYLOAD_BYTES}-byte event ({frame_hash_ns:.1} \
         ns)"
    );
}
