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
