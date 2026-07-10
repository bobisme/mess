//! CI smoke: run the real soak driver in-process for ~20s with crashes enabled
//! and every probe on. Green means: a real `LogEngine` on a real filesystem
//! survived multiple drop-and-reopen crash cycles with zero invariant
//! violations, and each probe actually executed against live engine state.
//!
//! Not a unit test of the probes' *firing* logic — that is
//! `probe::tests`/`shadow::tests`/etc., which feed each probe a doctored input.
//! This test proves the whole rig runs end-to-end and stays green on a correct
//! engine (the negative half of "the probes work": they do NOT false-positive
//! over a long, crash-punctuated, mixed-workload run).

use std::path::PathBuf;
use std::time::Duration;

use mess_soak::config::{Config, CrashMode};

/// A real-fs scratch dir: never `/tmp` (tmpfs here — the driver would refuse
/// it). Honors `TMPDIR` when it points at a real device, else `$HOME/.cache`.
fn scratch_dir(tag: &str) -> PathBuf {
    let base = std::env::var("TMPDIR")
        .ok()
        .filter(|t| !t.is_empty())
        .map(PathBuf::from)
        .filter(|p| !mess_soak::resource::is_tmpfs(p).unwrap_or(true))
        .unwrap_or_else(|| {
            PathBuf::from(std::env::var("HOME").unwrap_or_else(|_| ".".into())).join(".cache")
        });
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    base.join("mess-soak-test").join(format!("{tag}-{nonce}"))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn soak_smoke_survives_crashes_with_all_probes_on() {
    let dir = scratch_dir("smoke");
    let cfg = Config {
        duration: Duration::from_secs(20),
        streams: 32,
        writers: 4,
        subscribers: 3,
        // Deterministic crash trigger: every 4000 actions (~6s at the dev
        // box's action rate) → ~3 crash cycles inside 20s, >= the required 2 —
        // and the same store state at each crash point on any machine.
        crash_every_actions: 4000,
        crash_mode: CrashMode::DropReopen,
        seed: 0xC0FFEE,
        dir: dir.clone(),
        zipf_skew: 1.1,
        max_batch: 4,
        // Small segments so rolls + background seals churn during the run.
        segment_size: 128 * 1024,
        // Generous ceilings: the smoke asserts the run does NOT trip them.
        rss_ceiling_bytes: 2 * 1024 * 1024 * 1024,
        fd_ceiling: 1024,
        fsync_p99_ceiling: Duration::ZERO,
        metrics_every: Duration::from_secs(5),
        verbose: true,
        ..Config::default()
    };

    let report = match mess_soak::run(cfg).await {
        Ok(r) => r,
        Err(aborted) => panic!("soak aborted:\n{}", aborted.dump),
    };
    println!("smoke report: {report:#?}");

    // Crashes actually happened (acceptance: >= 2 crash cycles inside).
    assert!(report.crashes >= 2, "expected >= 2 crash cycles, got {}", report.crashes);
    // Work actually happened.
    assert!(report.appends > 0, "no appends");
    assert!(report.events >= report.appends, "events < appends");
    assert!(report.final_events > 0, "no durable events survived");
    // Every probe class actually executed against live engine state.
    assert!(report.index_checks > 0, "index==log probe never ran");
    assert!(report.density_checks > 0, "density probe never ran");
    assert!(report.head_checks > 0, "head probe never ran");
    assert!(report.subscription_reads > 0, "subscription probe never ran");
    assert!(report.subscribers_created > 0, "no subscribers ever joined");
    // Reopen was measured on every crash.
    assert!(report.max_reopen > Duration::ZERO, "reopen time never recorded");

    // Cleanup (leave nothing behind on green; an abort leaves the dir for
    // post-mortem, per the dump contract).
    let _ = std::fs::remove_dir_all(&dir);
}

/// A second, deterministic-ish short run with a different seed, no crashes —
/// proves the crash machinery is not load-bearing for a clean pass and that a
/// distinct seed also stays green.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn soak_smoke_crashless_clean_pass() {
    let dir = scratch_dir("clean");
    let cfg = Config {
        duration: Duration::from_secs(5),
        streams: 16,
        writers: 3,
        subscribers: 2,
        crash_every_actions: 0, // no crashes
        crash_mode: CrashMode::DropReopen,
        seed: 0x1234_5678,
        dir: dir.clone(),
        segment_size: 128 * 1024,
        metrics_every: Duration::from_secs(2),
        ..Config::default()
    };
    let report = match mess_soak::run(cfg).await {
        Ok(r) => r,
        Err(aborted) => panic!("crashless soak aborted:\n{}", aborted.dump),
    };
    assert_eq!(report.crashes, 0);
    assert!(report.appends > 0);
    assert!(report.final_events > 0);
    let _ = std::fs::remove_dir_all(&dir);
}
