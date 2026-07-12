//! Semantic-invalidity injections (research/05 §8): every mutated history
//! must be REJECTED by the oracle fold AND by the effect pipeline
//! (build → compose → apply) — never silently folded into a wrong state.
//! The mutated logs are RESEALED, so their anchor chains are
//! self-consistent: rejection must come from the algebra's own validation.

use segment_effect::hist::{self, Injection};
use segment_effect::recover::{
    v1_oracle, v2_effects_sequential, v3_effects_parallel,
};

fn assert_rejected_everywhere(kind: Injection, tag: &str, seeds: u64) {
    let mut injected = 0u64;
    for seed in 0..seeds {
        let h = hist::generate(seed);
        let Some((bad, at)) = hist::inject(&h, seed ^ 0xdead, kind) else {
            continue;
        };
        injected += 1;
        let v1 = v1_oracle(&bad);
        assert!(
            v1.is_err(),
            "{tag} seed {seed}: oracle accepted mutation at {at}"
        );
        let v2 = v2_effects_sequential(&bad);
        assert!(
            v2.is_err(),
            "{tag} seed {seed}: effect pipeline (seq) accepted mutation at {at}"
        );
        let v3 = v3_effects_parallel(&bad, 2);
        assert!(
            v3.is_err(),
            "{tag} seed {seed}: effect pipeline (parallel) accepted mutation at {at}"
        );
    }
    assert!(
        injected >= seeds / 2,
        "{tag}: too few viable injection sites ({injected}/{seeds})"
    );
}

#[test]
fn head_gap_overlap_is_rejected() {
    assert_rejected_everywhere(Injection::HeadGapOverlap, "head-gap", 300);
}

#[test]
fn registry_conflict_is_rejected() {
    assert_rejected_everywhere(Injection::RegistryConflict, "registry", 300);
}

#[test]
fn allocator_regression_is_rejected() {
    assert_rejected_everywhere(Injection::AllocatorRegression, "alloc", 300);
}

#[test]
fn position_skew_is_rejected() {
    assert_rejected_everywhere(Injection::PositionSkew, "position", 300);
}
