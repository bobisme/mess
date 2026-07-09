//! bn-3kn — the whole-store DST scenario suite: seed-reproducible
//! simulations composing the real [`Committer`](mess_log::committer::Committer),
//! subscriber-style readers, and the sealer stand-in
//! ([`SegmentWriter::seal`](mess_log::writer::SegmentWriter::seal)) across a
//! chain of segments, under [`SimRuntime`](mess_log::runtime::SimRuntime)'s
//! virtual time and fault filesystem. See `dst_support/mod.rs` for the full
//! design writeup (what each scenario does, which invariants are checked,
//! and what is deliberately out of scope).
//!
//! # Profiles
//!
//! - [`dst_fast_profile`] — ~200 scenarios, on every `cargo test`.
//! - [`dst_full_profile`] — 2,000+ scenarios, `#[ignore]`d, run nightly by
//!   `.github/workflows/dst.yml` (mirroring `crash-harness.yml`'s split).
//! - [`same_seed_is_byte_identical`] — the seed-reproducibility proof: two
//!   independent runs of the SAME seed produce byte-identical traces,
//!   including a CRC32C of every segment's final on-disk image.
//! - [`crash_lag_seal_race_composed`] — a DEDICATED scenario class that
//!   forces every fault/actor type named in the bone's acceptance criteria
//!   into ONE run: a `Sealed` segment (the sealer stand-in), laggy
//!   concurrent subscribers, and a fault landing on the seal barrier itself.

#[path = "dst_support/mod.rs"]
mod dst_support;

use dst_support::{plan_scenario, run_scenario, SegKind};

/// ~200 scenarios: cheap enough for every `cargo test -p mess-log`.
#[test]
#[cfg_attr(miri, ignore = "200 full scenarios (each a multi-segment sim run with recover_segment \
    + crc32c passes) are far too slow under Miri's interpreter; same_seed_is_byte_identical below \
    gives Miri a real, cheap pass over the whole scenario path instead")]
fn dst_fast_profile() {
    let mut committer_segs = 0u64;
    let mut sealed_segs = 0u64;
    let mut sealed_via_r2 = 0u64;
    for seed in 0..200u64 {
        let trace = run_scenario(seed);
        let plan = plan_scenario(seed);
        for (seg, ev) in plan.segments.iter().zip(&trace.events) {
            match seg.kind {
                SegKind::Committer => committer_segs += 1,
                SegKind::Sealed => sealed_segs += 1,
            }
            let dst_support::TraceEvent::SegmentDone { sealed_via_r2: r2, .. } = ev;
            if *r2 {
                sealed_via_r2 += 1;
            }
        }
    }
    println!(
        "dst fast profile: 200 scenarios, {committer_segs} Committer segments, \
         {sealed_segs} Sealed segments, {sealed_via_r2} trusted via the R2 fast path"
    );
    assert!(committer_segs > 0, "fast profile never exercised a Committer-kind segment");
    assert!(sealed_segs > 0, "fast profile never exercised a Sealed-kind segment");
    assert!(sealed_via_r2 > 0, "fast profile never exercised the R2 (sealed) fast path");
}

/// The full 2k+ profile. `#[ignore]`d by default; the nightly `dst` workflow
/// runs it with `--ignored`.
#[test]
#[ignore = "full 2k+-scenario profile: run via `cargo test -- --ignored` (nightly CI)"]
fn dst_full_profile() {
    let mut committer_segs = 0u64;
    let mut sealed_segs = 0u64;
    let mut sealed_via_r2 = 0u64;
    let count = 2_500u64;
    for seed in 0..count {
        let trace = run_scenario(seed);
        let plan = plan_scenario(seed);
        for (seg, ev) in plan.segments.iter().zip(&trace.events) {
            match seg.kind {
                SegKind::Committer => committer_segs += 1,
                SegKind::Sealed => sealed_segs += 1,
            }
            let dst_support::TraceEvent::SegmentDone { sealed_via_r2: r2, .. } = ev;
            if *r2 {
                sealed_via_r2 += 1;
            }
        }
    }
    println!(
        "dst full profile: {count} scenarios, {committer_segs} Committer segments, \
         {sealed_segs} Sealed segments, {sealed_via_r2} trusted via the R2 fast path"
    );
}

/// Seed-reproducibility, literally: run the SAME seed twice (fresh
/// `SimRuntime`, fresh everything) and assert the two [`Trace`]s are
/// EQUAL — including each segment's image CRC32C, so this is a
/// byte-identical-event-trace proof, not just "the summary counts match".
#[test]
fn same_seed_is_byte_identical() {
    // A shorter list under Miri: this is the one dst test Miri actually
    // executes for real (the profiles are ignored above), so it also
    // stands in for Miri's "at least one genuine pass over the whole
    // scenario path" coverage — 2 seeds is enough to prove reproducibility
    // without ~4x-ing the mess-log Miri lane's runtime.
    let seeds: &[u64] = if cfg!(miri) { &[0, 42] } else { &[0, 1, 7, 42, 12345, 0xC0FFEE] };
    for &seed in seeds {
        let a = run_scenario(seed);
        let b = run_scenario(seed);
        assert_eq!(a, b, "seed {seed}: two runs of the same seed diverged");
    }
}

/// Different seeds explore different scenario shapes (composition sanity: if
/// every seed produced the same plan, the "seed-driven" claim would be
/// hollow).
#[test]
fn different_seeds_explore_different_shapes() {
    let mut seen = std::collections::HashSet::new();
    for seed in 0..64u64 {
        let plan = plan_scenario(seed);
        seen.insert(plan.segments.len());
    }
    assert!(seen.len() > 1, "every seed produced the same segment count");
}

/// The dedicated composed scenario class the bone names explicitly: crash +
/// subscriber-lag + seal-race in ONE scenario. Built directly (not via the
/// general random planner) so every run genuinely exercises all three:
/// a `Sealed` segment (the sealer stand-in, racing readers through its own
/// seal barrier), 2 laggy concurrent readers, and a fault landing on the
/// seal's own barrier (`FaultPhase::Seal`).
#[test]
#[cfg_attr(miri, ignore = "150 full scenarios: too slow under Miri, same rationale as dst_fast_profile")]
fn crash_lag_seal_race_composed() {
    use dst_support::{FaultPhase, PlannedBatch, SegPlan};
    use mess_log::committer::Durability;
    use mess_log::runtime::EnospcSite;

    for seed in 0..150u64 {
        let mut rng = mess_log::runtime::Rng::new(seed ^ 0x5EA1_5EA1);
        let n_events = 1 + rng.below(4);
        let batches: Vec<PlannedBatch> = (0..3)
            .map(|b| PlannedBatch {
                stream_id: 1,
                first_stream_version: b * n_events,
                events: (0..n_events).map(|i| vec![(seed ^ i) as u8; 8]).collect(),
            })
            .collect();
        let plan = SegPlan {
            kind: dst_support::SegKind::Sealed,
            durability: Durability::group_default(), // unused by Sealed kind
            writers: 1,
            batches: vec![batches],
            n_readers: 2, // subscriber races
            reader_lag_max_us: 200, // deliberate lag
            reader_seed_base: seed,
            sync_every: 1 + (seed % 2) as usize,
            enospc: Some(dst_support::EnospcFault {
                site: if seed % 2 == 0 { EnospcSite::Fdatasync } else { EnospcSite::Pwrite },
                phase: FaultPhase::Seal, // the crash lands ON the seal barrier
            }),
            tear_prob: 0.4,
            writer_seed: seed ^ 0xABCD,
        };

        let trace = dst_support::run_single_segment_scenario(seed, plan);
        let dst_support::TraceEvent::SegmentDone {
            recovered_next_pos, acked_end, max_reader_seen, ..
        } = trace.events[0];
        assert!(recovered_next_pos >= acked_end, "seed {seed}: acked data lost across the seal-race crash");
        assert!(
            recovered_next_pos >= max_reader_seen,
            "seed {seed}: a reader saw past what recovery reconstructed"
        );
    }
}

