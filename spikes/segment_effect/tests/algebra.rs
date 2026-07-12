//! Algebraic property tests: associativity of `⊗` on valid ordered
//! histories, identity, codec roundtrip, reordered-effect `⊥`, duplicate
//! application idempotence (research/02 §12), and the constructed
//! head-path `⊥` that anchors alone would not catch.

use segment_effect::effect::{ApplyErr, ComposeErr, SegmentEffect, compose};
use segment_effect::hist;
use segment_effect::kernel::KernelState;
use segment_effect::model::{Capsule, Log};
use segment_effect::recover::{OrderedApplier, build_all};

fn valid_effects(seed: u64) -> (segment_effect::model::Log, Vec<SegmentEffect>) {
    let h = hist::generate(seed);
    let effects = build_all(&h.log).expect("valid history builds");
    (h.log, effects)
}

#[test]
fn compose_is_associative_on_valid_histories() {
    let mut checked = 0u64;
    for seed in 0..200u64 {
        let (_, effects) = valid_effects(seed);
        if effects.len() < 3 {
            continue;
        }
        for i in 0..(effects.len() - 2).min(8) {
            let (a, b, c) = (&effects[i], &effects[i + 1], &effects[i + 2]);
            let left = compose(&compose(a, b).unwrap(), c).unwrap();
            let right = compose(a, &compose(b, c).unwrap()).unwrap();
            assert_eq!(left, right, "seed {seed} at {i}: (a⊗b)⊗c != a⊗(b⊗c)");
            checked += 1;
        }
    }
    assert!(checked > 100, "associativity barely exercised ({checked})");
}

#[test]
fn empty_effect_is_identity() {
    for seed in 0..50u64 {
        let (_, effects) = valid_effects(seed);
        let Some(e) = effects.first() else { continue };
        let mut left_id =
            SegmentEffect::empty(e.first, e.first_anchor, e.dedupe_span);
        left_id.first_segment = e.first_segment;
        left_id.last_segment = e.first_segment;
        left_id.epoch = e.epoch;
        let composed = compose(&left_id, e).unwrap();
        assert_eq!(&composed, e, "seed {seed}: empty ⊗ e != e");

        let mut right_id =
            SegmentEffect::empty(e.last, e.last_anchor, e.dedupe_span);
        right_id.first_segment = e.last_segment;
        right_id.last_segment = e.last_segment;
        right_id.epoch = e.epoch;
        let composed = compose(e, &right_id).unwrap();
        // Right identity is exact except dedupe expiry cannot resurrect:
        // same cursor, same floor -> exact.
        assert_eq!(&composed, e, "seed {seed}: e ⊗ empty != e");
    }
}

#[test]
fn codec_roundtrip_and_corruption_detection() {
    for seed in 0..100u64 {
        let (_, effects) = valid_effects(seed);
        for e in effects.iter().take(4) {
            let bytes = e.encode();
            let d = SegmentEffect::decode(&bytes).expect("roundtrip decode");
            assert_eq!(&d, e);
            assert_eq!(d.hash(), e.hash());
            // Any single flipped byte must fail decode (CRC).
            let mut i = seed as usize % bytes.len();
            for _ in 0..4 {
                let mut bad = bytes.clone();
                bad[i] ^= 0x10;
                assert!(
                    SegmentEffect::decode(&bad).is_err(),
                    "flip at {i} undetected"
                );
                i = (i + bytes.len() / 3 + 1) % bytes.len();
            }
            // Truncation must fail decode.
            assert!(SegmentEffect::decode(&bytes[..bytes.len() - 1]).is_err());
        }
    }
}

#[test]
fn reordered_effects_are_bottom() {
    let mut exercised = 0;
    for seed in 0..100u64 {
        let (log, effects) = valid_effects(seed);
        if effects.len() < 2 {
            continue;
        }
        let i = (seed as usize) % (effects.len() - 1);
        // Swapped composition is not adjacent -> ⊥.
        assert_eq!(
            compose(&effects[i + 1], &effects[i]).unwrap_err(),
            ComposeErr::NotAdjacent,
            "seed {seed}: swapped compose must be ⊥"
        );
        // Swapped application: cursor mismatch -> ⊥, state untouched.
        let mut st = KernelState::new(log.dedupe_span, log.epoch_span);
        let mut ap = OrderedApplier::default();
        for e in &effects[..i] {
            ap.apply(&mut st, e).unwrap();
        }
        let before = st.digest();
        let err = st.apply_effect(&effects[i + 1]).unwrap_err();
        assert!(
            matches!(err, ApplyErr::CursorMismatch { .. }),
            "seed {seed}: got {err:?}"
        );
        assert_eq!(st.digest(), before, "⊥ apply must not mutate state");
        exercised += 1;
    }
    assert!(exercised > 50);
}

#[test]
fn constructed_head_path_bottom_without_anchor_help() {
    // Two hand-built segments over one stream; tamper the second effect's
    // first_prior while keeping cursors/anchors consistent. Only the PATH
    // check can reject this.
    let capsules = vec![
        Capsule::UserBatch {
            stream_id: 7,
            first_version: 0,
            event_count: 2,
            first_global_pos: 0,
        },
        Capsule::UserBatch {
            stream_id: 7,
            first_version: 2,
            event_count: 3,
            first_global_pos: 2,
        },
    ];
    let log = Log::seal(capsules, &[1], 1_000);
    let effects = build_all(&log).unwrap();
    let (e1, e2) = (&effects[0], &effects[1]);
    let mut tampered = e2.clone();
    tampered.heads.get_mut(&7).unwrap().first_prior = 3;
    match compose(e1, &tampered) {
        Err(ComposeErr::HeadPath { stream_id: 7, end_of_first: 2, start_of_second: 3 }) => {}
        other => panic!("expected HeadPath ⊥, got {other:?}"),
    }
    let mut st = KernelState::new(log.dedupe_span, log.epoch_span);
    st.apply_effect(e1).unwrap();
    match st.apply_effect(&tampered) {
        Err(ApplyErr::HeadPath { stream_id: 7, state_head: 2, effect_first: 3 }) => {}
        other => panic!("expected HeadPath ⊥, got {other:?}"),
    }
}

#[test]
fn duplicate_application_is_idempotent_via_identity() {
    for seed in 0..100u64 {
        let (log, effects) = valid_effects(seed);
        if effects.is_empty() {
            continue;
        }
        let mut st = KernelState::new(log.dedupe_span, log.epoch_span);
        let mut ap = OrderedApplier::default();
        for e in &effects {
            assert!(ap.apply(&mut st, e).unwrap());
        }
        let done = st.digest();
        // Re-applying any already-applied effect: known identity -> no-op.
        let dup = &effects[(seed as usize) % effects.len()];
        assert!(!ap.apply(&mut st, dup).unwrap(), "must skip duplicate");
        assert_eq!(st.digest(), done, "duplicate skip must not change state");
        // Raw apply without identity tracking: ⊥, state unchanged.
        let err = st.apply_effect(dup).unwrap_err();
        assert!(matches!(err, ApplyErr::CursorMismatch { .. }));
        assert_eq!(st.digest(), done);
    }
}

#[test]
fn parallel_tree_reduce_matches_sequential_reduce() {
    use segment_effect::recover::{reduce_ordered_parallel, reduce_ordered_tree};
    for seed in 0..60u64 {
        let (_, effects) = valid_effects(seed);
        if effects.is_empty() {
            continue;
        }
        let seq = effects
            .iter()
            .skip(1)
            .try_fold(effects[0].clone(), |a, b| compose(&a, b))
            .unwrap();
        let tree = reduce_ordered_tree(&effects).unwrap();
        assert_eq!(seq, tree, "seed {seed}: tree != left fold");
        for threads in [2usize, 3, 8] {
            let par = reduce_ordered_parallel(&effects, threads).unwrap();
            assert_eq!(seq, par, "seed {seed}: parallel({threads}) != left fold");
        }
    }
}
