//! bn-3kn — harness self-test: prove the DST invariant checker actually
//! CATCHES a real bug, not just rubber-stamps every run it is given.
//!
//! `dst_support`'s central property, checked after every segment in every
//! scenario, is "reader-never-past-watermark, globally": whatever a live
//! reader was ever shown as committed must actually be recoverable after a
//! crash (`assert!(rec.next_pos >= outcome.max_reader_seen, ...)` in
//! `dst_support::run_one_segment`). That property is only as strong as the
//! REAL watermark/barrier ordering it depends on — D7/W1
//! (`docs/spec/03-durability.md` §3, `watermark.rs`'s module docs): the
//! watermark MUST advance strictly AFTER the covering `fdatasync` returns,
//! never before. This file plants EXACTLY that ordering bug (publish before
//! the barrier, not after) in a minimal, test-only harness — no production
//! code is touched — and shows the SAME assertion the scenario driver runs
//! fails deterministically, with a seed that reproduces the failure
//! byte-for-byte on replay.

use std::path::Path;

use mess_log::encode::Subframe;
use mess_log::format::SEGMENT_SIZE;
use mess_log::runtime::{CrashPlan, Fault, Runtime, SimRuntime, TailPlan};
use mess_log::scanner::recover_segment;
use mess_log::watermark::Watermark;
use mess_log::writer::{BatchSpec, SegmentParams, SegmentWriter};

/// Append one batch, then either the REAL ordering (sync, THEN publish) or
/// the BUGGY ordering (publish, THEN [crash strikes before sync ever runs]).
/// Returns `(watermark_value_a_reader_could_have_observed,
/// recovered_next_pos_after_a_crash_right_there)`.
fn run_ordering_case(seed: u64, buggy: bool) -> (u64, u64) {
    let rt = SimRuntime::with_fault(seed, Fault::Tail);
    let fs = rt.fs();
    let path = Path::new("/seg-bug");
    let mut writer = SegmentWriter::create(
        &fs,
        path,
        SegmentParams {
            segment_size: SEGMENT_SIZE,
            ..SegmentParams::new(0, 0, 1, 0)
        },
    )
    .unwrap();
    let wm = Watermark::new(0);

    let payload = b"payload".to_vec();
    let subs = [Subframe::plain(1, 1, 0, &payload)];
    let spec = BatchSpec {
        stream_id:            1,
        category_id:          1,
        first_stream_version: 0,
        crypto_chain:         None,
        subframes:            &subs,
    };
    writer.append(&spec).unwrap();

    let observed_watermark = if buggy {
        // THE BUG: publish the position as committed BEFORE the barrier
        // (`commit_group` in committer.rs does the opposite — sync, THEN
        // `watermark.advance` — precisely to prevent this). `sync()` is
        // deliberately never reached: the crash below models the process
        // dying in exactly the window this ordering opens up.
        wm.advance(writer.next_pos());
        wm.get()
    } else {
        // THE REAL ORDER (D7/W1): barrier first, publish only after it
        // returns.
        writer.sync().unwrap();
        wm.advance(writer.next_pos());
        wm.get()
    };

    // Crash keeping only what was ACTUALLY fsync'd: `TailPlan::keep` clamps
    // up to the real sync watermark regardless of what we ask for, so the
    // healthy order (already synced) always keeps its batch; only the buggy
    // order (never synced) can lose it.
    fs.crash(path, CrashPlan::Tail(TailPlan { keep: 0, scramble: vec![] }))
        .unwrap();
    let rec = recover_segment(&fs, path).unwrap();
    (observed_watermark, rec.next_pos)
}

/// The exact invariant `dst_support::run_one_segment` checks, extracted so
/// this file demonstrably runs the SAME assertion, not a look-alike.
fn assert_reader_never_past_watermark(
    observed_watermark: u64,
    recovered_next_pos: u64,
) {
    assert!(
        recovered_next_pos >= observed_watermark,
        "a reader observed position {observed_watermark} as committed, but \
         recovery only reconstructed up to {recovered_next_pos}"
    );
}

/// Sanity: the REAL ordering never trips the checker. (If this ever failed,
/// the self-test below would be meaningless — it would mean EVERY run
/// panics, buggy or not.)
#[test]
fn healthy_ordering_never_trips_the_checker() {
    for seed in 0..20u64 {
        let (w, r) = run_ordering_case(seed, false);
        assert_reader_never_past_watermark(w, r);
    }
}

/// The bone's acceptance criterion: a deliberately-injected ordering bug
/// (publish-before-watermark) IS caught by the harness's own invariant
/// checker, and the seed that catches it replays deterministically.
#[test]
fn buggy_ordering_is_caught_and_replays_deterministically() {
    const SEED: u64 = 4242;

    let run = || {
        std::panic::catch_unwind(|| {
            let (w, r) = run_ordering_case(SEED, true);
            assert_reader_never_past_watermark(w, r);
        })
    };

    // Silence the panic hook's stderr spam for this expected-panic probe.
    let prev_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(|_| {}));
    let first = run();
    let second = run();
    std::panic::set_hook(prev_hook);

    assert!(
        first.is_err(),
        "the harness must CATCH the publish-before-watermark bug"
    );
    assert!(
        second.is_err(),
        "seed {SEED} must replay the SAME caught failure deterministically"
    );

    // Byte-identical replay, not just "both panicked": the raw (watermark,
    // recovered) pair the checker was fed is identical across runs.
    let a = run_ordering_case(SEED, true);
    let b = run_ordering_case(SEED, true);
    assert_eq!(
        a, b,
        "seed {SEED}: the buggy run's own observations must reproduce exactly"
    );
    assert_eq!(
        a,
        (1, 0),
        "seed {SEED}: the bug loses exactly the one published-but-unsynced \
         position"
    );
}
