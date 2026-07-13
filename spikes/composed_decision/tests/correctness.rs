//! Spike J correctness gates for the carried-forward `FlatEngine`.
//!
//! Spike B (bn-28g) proved the flat kernel correct against the engine as it
//! stood at `a3d40ff1`. Spike J re-runs the load-bearing subset against
//! TODAY's engine, because bn-34o / bn-2cj / bn-2ib(C) / bn-9mw(E) /
//! bn-3of(I) have all landed since. A performance rematch is only worth
//! reading if the thing being rematched still produces the same answers as
//! the engine it wants to replace.
//!
//! `cargo test --release`

use std::collections::HashMap;
use std::sync::Arc;

use composed_decision::{
    AppendError, FlatConfig, FlatEngine, RecordToAppend, Variant, Version,
};
use mess_log::committer::Durability;
use mess_store::backend::Backend;
use mess_store::{EngineOptions, LogEngine};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

fn scratch() -> mess_testkit::SweepingTempDir {
    let root = std::env::var_os("MESS_BENCH_DIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| {
            std::path::PathBuf::from(std::env::var("HOME").expect("HOME"))
                .join(".cache")
                .join("mess-bench")
        });
    std::fs::create_dir_all(&root).expect("create scratch base");
    mess_testkit::temp_dir_in(&root, "composed-j-test")
}

fn recs(n: usize) -> Vec<RecordToAppend> {
    (0..n)
        .map(|i| RecordToAppend {
            message_type: "ev.t".to_string(),
            data: vec![i as u8; 32],
        })
        .collect()
}

fn flat(dir: &std::path::Path, variant: Variant) -> Arc<FlatEngine> {
    Arc::new(
        FlatEngine::open(
            dir,
            FlatConfig {
                durability: Durability::Process,
                variant,
                ..Default::default()
            },
        )
        .expect("open flat"),
    )
}

/// THE gate: 800 randomized ops (correct / stale / future expected versions,
/// empty batches) driven through the CURRENT engine and the FlatEngine, in the
/// same order. Every accept/conflict outcome and every returned version /
/// global position must match exactly. If the engine's semantics drifted under
/// C/E/I, this is what catches it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn differential_oracle_against_todays_engine() {
    let sc = scratch();
    let engine = LogEngine::open_with(
        sc.path().join("log"),
        EngineOptions { durability: Durability::Process, ..Default::default() },
    )
    .expect("open LogEngine");
    let f = flat(&sc.path().join("flat"), Variant::B0Direct);

    let mut rng = StdRng::seed_from_u64(0xC0FFEE);
    let mut heads: HashMap<u64, Version> = HashMap::new();
    let mut accepted = 0u64;
    let mut conflicts = 0u64;

    for _ in 0..800 {
        let sid: u64 = rng.gen_range(0..8);
        let stream = format!("s{sid}");
        let truth = *heads.get(&sid).unwrap_or(&Version::NoStream);
        // Correct / stale / future / any-version expectations.
        let exp = match rng.gen_range(0..10) {
            0..=6 => truth,
            7 => Version::NoStream,
            _ => Version::At(9_999),
        };
        let n = if rng.gen_range(0..10) == 0 { 0 } else { rng.gen_range(1..5) };
        let r = recs(n);

        let a = engine.append_batch(&stream, exp, &r).await;
        let b = f.append_batch(&stream, exp, &r).await;

        match (&a, &b) {
            (Ok(x), Ok(y)) => {
                assert_eq!(
                    x.version, y.version,
                    "version diverged on {stream}"
                );
                assert_eq!(
                    x.last_global_position, y.last_global_position,
                    "global position diverged on {stream}"
                );
                heads.insert(sid, x.version);
                accepted += 1;
            }
            (
                Err(AppendError::Conflict { actual: ax, .. }),
                Err(AppendError::Conflict { actual: ay, .. }),
            ) => {
                assert_eq!(ax, ay, "conflict witness diverged on {stream}");
                conflicts += 1;
            }
            other => panic!("outcome diverged on {stream}: {other:?}"),
        }
    }
    assert!(accepted > 400 && conflicts > 50, "oracle exercised both paths");

    // Recovery equivalence: the STANDARD scanner must reproduce the flat
    // engine's shadow heads from the log alone (no Fjall, no meta store).
    let exit = Arc::into_inner(f).expect("sole handle").close();
    assert_eq!(exit.stats.position_mismatches, 0);
    assert_eq!(
        exit.next_global,
        engine.total_events() as u64,
        "flat watermark == engine total_events"
    );
}

/// Same-stream `Exact(v)` race: exactly one winner, everyone else conflicts
/// with the winner's head — the invariant the per-stream `AppendGate` used to
/// provide and the owner now provides for free.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_exact_version_race_has_exactly_one_winner() {
    let sc = scratch();
    let f = flat(&sc.path().join("flat"), Variant::B0Direct);
    for round in 0..50u64 {
        let mut joins = Vec::new();
        for _ in 0..8 {
            let f = Arc::clone(&f);
            joins.push(tokio::spawn(async move {
                f.append_batch(
                    "hot",
                    if round == 0 {
                        Version::NoStream
                    } else {
                        Version::At(round - 1)
                    },
                    &recs(1),
                )
                .await
            }));
        }
        let mut wins = 0;
        for j in joins {
            match j.await.expect("task") {
                Ok(_) => wins += 1,
                Err(AppendError::Conflict { .. }) => {}
                Err(e) => panic!("unexpected {e:?}"),
            }
        }
        assert_eq!(wins, 1, "round {round}: exactly one writer may win");
    }
    let exit = Arc::into_inner(f).expect("sole handle").close();
    assert_eq!(exit.stats.position_mismatches, 0);
    assert_eq!(exit.next_global, 50, "one event committed per round");
}

/// Cross-stream density: returned position ranges must tile [0, N) exactly —
/// no gaps, no overlap — which is what makes the single owner a legitimate
/// publisher of the global order.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn cross_stream_positions_tile_exactly() {
    let sc = scratch();
    let f = flat(&sc.path().join("flat"), Variant::B0Direct);
    let mut joins = Vec::new();
    for w in 0..8u64 {
        let f = Arc::clone(&f);
        joins.push(tokio::spawn(async move {
            let mut ends = Vec::new();
            let mut ver = Version::NoStream;
            for _ in 0..100 {
                let out = f
                    .append_batch(&format!("s{w}"), ver, &recs(5))
                    .await
                    .expect("append");
                ver = out.version;
                ends.push(out.last_global_position);
            }
            ends
        }));
    }
    let mut ends: Vec<u64> = Vec::new();
    for j in joins {
        ends.extend(j.await.expect("task"));
    }
    ends.sort_unstable();
    // 8 writers x 100 appends x 5 events = 4000 events; every append's last
    // position must be a distinct multiple-of-5 boundary covering [0, 4000).
    assert_eq!(ends.len(), 800);
    for (i, e) in ends.iter().enumerate() {
        assert_eq!(*e, (i as u64 + 1) * 5 - 1, "dense tiling broken at {i}");
    }
    let exit = Arc::into_inner(f).expect("sole handle").close();
    assert_eq!(exit.next_global, 4000);
    assert_eq!(exit.stats.position_mismatches, 0);
}
