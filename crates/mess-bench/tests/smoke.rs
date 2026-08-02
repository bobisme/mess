//! The fast smoke variant wired into normal `cargo test -p mess-bench`
//! (bn-4pk acceptance: "smoke test in-suite"). Runs every workload at
//! [`mess_bench::RunSize::Smoke`] sizes — the SAME functions the gated
//! `mess-bench run` binary calls, just parameterized small — so a change
//! that breaks a workload's code path (a renamed API, a panic on an edge
//! case, a JSON shape drift) fails an ordinary test run instead of quietly
//! rotting until the next nightly full run notices. No floor enforcement:
//! smoke sizes are not meaningful throughput numbers (see the module docs
//! on `mess_bench` for why).
//!
//! This test still touches the real filesystem (the append/durable/engine/
//! recovery workloads need a segment file), so it uses a real-fs scratch
//! dir under [`default_scratch_root`] — never `std::env::temp_dir()` (which
//! is `/tmp`, tmpfs on this class of machine, exactly the trap
//! `assert_real_fs` exists to catch) — deliberately the SAME root
//! `mess-bench run` itself resolves, so this test also exercises that
//! resolution. The per-run subdirectory is created via `mess_testkit`'s
//! `temp_dir_in` (bn-2jr, dev-dependency only — `default_scratch_root`
//! itself in `src/lib.rs` stays free of it: that helper also backs the
//! shipped `mess-bench` binary's non-dev-dependency runtime path), so a
//! panic mid-run no longer leaks the scratch dir the way the old
//! unconditional-on-success `remove_dir_all` did, and concurrent smoke runs
//! on one host no longer collide on a shared fixed `smoke-test` name.

use mess_bench::{RunSize, assert_real_fs, default_scratch_root, run_all};

#[test]
fn smoke_harness_runs_every_workload_and_emits_expected_metrics() {
    let scratch =
        mess_testkit::temp_dir_in(&default_scratch_root(), "smoke-test");
    assert_real_fs(scratch.path()).expect(
        "smoke test scratch dir must be real-fs; set MESS_BENCH_DIR to an \
         ext4/xfs/btrfs path if $HOME/.cache is unexpectedly tmpfs on this \
         host",
    );

    let metrics = run_all(RunSize::Smoke, scratch.path(), 0);

    let expected = [
        "mess_log.buffered.ev_per_s",
        "mess_log.durable.ev_per_s",
        "sealed.global_scan.ev_per_s",
        "sealed.stream_replay.ev_per_s",
        "phase5.sealed.bytes_per_event",
        "phase5.sealed.columnar_replay.ev_per_s",
        "engine.buffered.ev_per_s",
        "engine.sealed_replay.ev_per_s",
        "phase5.foldchain.append_overhead_ns",
        "phase5.foldchain.full_chain.ev_per_s",
        "phase5.verify.load_verified.ev_per_s",
        "recovery.s_per_gib",
        "engine.live_tail.catchup_solo.ev_per_s",
        "engine.live_tail.catchup_loaded.ev_per_s",
        "engine.live_tail.catchup_scaling",
        "engine.live_tail.writer_tailed.ev_per_s",
        "engine.live_tail.writer_retention",
        "engine.live_tail.tail_reader.ev_per_s",
        "engine.live_tail.append_to_delivery_us",
        "engine.live_tail.stream_paging.ev_per_s",
    ];
    for key in expected {
        let found = metrics.iter().find(|m| m.metric == key);
        assert!(
            found.is_some(),
            "smoke run did not emit expected metric {key}"
        );
        let v = found.unwrap().value;
        assert!(
            v.is_finite() && v >= 0.0,
            "metric {key} produced a non-finite/negative value: {v}"
        );
    }
}

/// `run --only` must narrow the run to exactly the named workload and leave
/// the rest of the ledger empty (bn-1r9c). Guards the selector that makes it
/// possible to re-measure ONE workload on a shared host — if a `step!` name
/// and [`mess_bench::WORKLOAD_NAMES`] ever drift apart, `--only` silently
/// runs nothing, and a ledger with no rows in it passes `compare` trivially.
#[test]
fn only_selector_runs_exactly_the_named_workload() {
    let scratch =
        mess_testkit::temp_dir_in(&default_scratch_root(), "smoke-only");
    assert_real_fs(scratch.path()).expect("smoke scratch must be real-fs");

    let only = vec!["live_tail".to_string()];
    let metrics = mess_bench::run_selected(
        RunSize::Smoke,
        scratch.path(),
        0,
        Some(&only),
    );

    assert!(!metrics.is_empty(), "--only live_tail produced no metrics");
    for m in &metrics {
        assert!(
            m.metric.starts_with("engine.live_tail."),
            "--only live_tail also ran something else: {}",
            m.metric
        );
    }
}

/// Every selector name must be spelled the same way in `WORKLOAD_NAMES` and in
/// `run_selected`'s `step!` arms; an unmatched name runs zero workloads.
#[test]
fn every_declared_workload_name_selects_something() {
    let scratch =
        mess_testkit::temp_dir_in(&default_scratch_root(), "smoke-names");
    assert_real_fs(scratch.path()).expect("smoke scratch must be real-fs");

    for name in mess_bench::WORKLOAD_NAMES {
        let only = vec![(*name).to_string()];
        let metrics = mess_bench::run_selected(
            RunSize::Smoke,
            scratch.path(),
            0,
            Some(&only),
        );
        assert!(
            !metrics.is_empty(),
            "WORKLOAD_NAMES lists {name:?} but no step! arm matches it"
        );
    }
}

/// The comparison's two PASS lines must stay distinguishable (bn-1r9c review).
///
/// `compare` skips a floor whose metric is absent from the ledger, so "no
/// regressions" and "the full gate passed" are different claims. Before
/// `--only` existed the skip path was unreachable in the gate flow and the
/// single `PASS: all N ...` line was always true; now a ledger can cover six
/// of twenty floors, and that line is exactly what CI greps for. So this
/// pins BOTH strings through the real binary — the observable output, not an
/// internal helper — and asserts a narrowed ledger can never produce the
/// full-gate form.
#[test]
fn compare_pass_line_never_claims_a_full_gate_a_narrowed_ledger_did_not_run() {
    let dir =
        mess_testkit::temp_dir_in(&default_scratch_root(), "compare-strings");
    let floors_path = dir.path().join("floors.json");
    std::fs::write(
        &floors_path,
        r#"{"floors":[
          {"metric":"a.ev_per_s","direction":"min","floor":10.0,
           "tolerance":0.1,"source":"test"},
          {"metric":"b.ev_per_s","direction":"min","floor":10.0,
           "tolerance":0.1,"source":"test"}
        ]}"#,
    )
    .expect("write floors");

    let row = |m: &str| {
        format!(
            r#"{{"metric":"{m}","value":100.0,"unit":"ev/s",
                 "conditions":"test"}}"#
        )
    };
    let ledger = |rows: String| {
        format!(
            r#"{{"date":"2026-08-02","mode":"full","machine":{{
                 "cpu_model":"test","cpu_count":1,"kernel":"test",
                 "scratch_dir":"test","scratch_fs":"ext4"}},
                 "metrics":[{rows}]}}"#
        )
    };
    let full = dir.path().join("full.json");
    std::fs::write(
        &full,
        ledger(format!("{},{}", row("a.ev_per_s"), row("b.ev_per_s"))),
    )
    .expect("write full ledger");
    let narrowed = dir.path().join("narrowed.json");
    std::fs::write(&narrowed, ledger(row("a.ev_per_s")))
        .expect("write narrowed ledger");

    let compare = |l: &std::path::Path| {
        let out = std::process::Command::new(env!("CARGO_BIN_EXE_mess-bench"))
            .arg("compare")
            .arg("--ledger")
            .arg(l)
            .arg("--floors")
            .arg(&floors_path)
            .output()
            .expect("run mess-bench compare");
        assert!(
            out.status.success(),
            "compare should exit 0 when nothing regressed: {:?}",
            out.status
        );
        String::from_utf8_lossy(&out.stderr).into_owned()
    };

    let full_out = compare(&full);
    let expected_full = format!(
        "PASS: all 2 floor-gated metrics within tolerance of {}",
        floors_path.display()
    );
    assert!(
        full_out.contains(&expected_full),
        "a ledger covering every floor must print the historical full-gate \
         line {expected_full:?}, got: {full_out}"
    );

    let narrowed_out = compare(&narrowed);
    assert!(
        narrowed_out.contains(
            "PASS (narrowed): 1 of 2 floor-gated metrics checked; 1 absent \
             from this ledger — NOT a full gate."
        ),
        "a ledger missing a gated metric must print the narrowed line, got: \
         {narrowed_out}"
    );
    assert!(
        !narrowed_out.contains("PASS: all"),
        "a narrowed ledger must NEVER emit the full-gate line CI greps for: \
         {narrowed_out}"
    );
}

#[test]
fn floors_file_parses_and_every_floor_metric_name_is_non_empty() {
    let path =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("floors.json");
    let floors =
        mess_bench::load_floors(&path).expect("floors.json must parse");
    assert!(
        !floors.floors.is_empty(),
        "floors.json must seed at least one floor"
    );
    for f in &floors.floors {
        assert!(!f.metric.is_empty());
        assert!(
            f.tolerance >= 0.0,
            "{}: tolerance must be a non-negative magnitude",
            f.metric
        );
        assert!(f.floor.is_finite());
    }
}
