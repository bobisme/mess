//! `chatter bench` must emit **parseable JSON with all five cells**, so an A/B
//! harness can consume its stdout without scraping prose.
//!
//! Run at a deliberately tiny scale: the point is the contract of the output,
//! not the numbers. (Numbers from a debug test build are meaningless anyway —
//! which is exactly why the report carries a `profile` field, asserted below.)

use chatter::bench::{self, BenchConfig, CELL_NAMES};
use chatter::seed::SeedConfig;
use mess_testkit::sweeping_temp_dir;
use serde_json::Value;

#[tokio::test]
async fn bench_emits_parseable_json_with_all_five_cells() {
    let t = sweeping_temp_dir("chatter-bench-json");
    let dir = t.path().join("bench-store");
    let cfg = BenchConfig {
        scroll_pages: 3,
        scroll_page_size: 16,
        tail_page_size: 64,
        ..BenchConfig::new(dir.clone(), SeedConfig::tiny(5))
    };

    let report = bench::run(&cfg).await.expect("bench runs");
    let json = report.to_json();

    // Parseable, not merely printable.
    let v: Value = serde_json::from_str(&json).expect("bench output is JSON");
    assert_eq!(v["tool"], "chatter-bench");
    assert_eq!(v["seed"], 5);
    // A debug test build must SAY it is a debug build; a release run says
    // release. Either way the consumer can tell.
    let profile = v["profile"].as_str().expect("profile is a string");
    assert!(
        profile == "debug" || profile == "release",
        "unexpected profile {profile:?}"
    );
    assert_eq!(
        profile,
        if cfg!(debug_assertions) { "debug" } else { "release" }
    );

    // Every cell, present, named, and timed.
    let cells = v["cells"].as_array().expect("cells is an array");
    assert_eq!(cells.len(), CELL_NAMES.len());
    for (cell, expected) in cells.iter().zip(CELL_NAMES.iter()) {
        assert_eq!(
            cell["name"].as_str(),
            Some(*expected),
            "cells must appear in the documented order"
        );
        let ms = cell["ms"].as_f64().expect("ms is a number");
        assert!(ms >= 0.0 && ms.is_finite(), "cell {expected} had ms={ms}");
        assert!(
            !cell["note"].as_str().unwrap_or_default().is_empty(),
            "cell {expected} must carry a human-readable note"
        );
        assert!(cell["detail"].is_object(), "cell {expected} needs a detail");
    }
    for name in CELL_NAMES {
        assert!(report.cell(name).is_some(), "missing cell {name}");
    }

    // The bench ran against a store that actually reached the sealed tier —
    // otherwise the cold_reopen and scroll_back cells would be measuring the
    // hot tier and quietly lying.
    let sealed = v["store"]["census"]["sealed_segments"]
        .as_u64()
        .expect("sealed_segments is a number");
    assert!(
        sealed > 1,
        "chatter bench must run against a multi-segment sealed store, got \
         {sealed}"
    );
    assert_eq!(
        v["store"]["seal_pack"], true,
        "the default corpus seals as SealPack"
    );

    // Cell-specific contracts a harness will rely on.
    let seed_cell = report.cell("seed").unwrap();
    assert_eq!(
        seed_cell.detail["events"].as_u64().unwrap(),
        (SeedConfig::tiny(5).messages
            + SeedConfig::tiny(5).reactions()
            + SeedConfig::tiny(5).users
            + SeedConfig::tiny(5).channels
            + SeedConfig::tiny(5).renames
            + SeedConfig::tiny(5).archives) as u64,
    );
    let scroll = report.cell("scroll_back").unwrap();
    assert!(scroll.detail["pages"].as_u64().unwrap() > 0);
    assert!(scroll.detail["messages"].as_u64().unwrap() > 0);
    let tail = report.cell("tail_catch_up").unwrap();
    assert!(tail.detail["records"].as_u64().unwrap() > 0);
    let resume = report.cell("checkpoint_resume").unwrap();
    assert!(
        resume.detail["resumed_from"].as_u64().unwrap() > 0,
        "the checkpoint_resume cell must perform a REAL resume"
    );
    assert!(resume.detail["checkpoint_bytes"].as_u64().unwrap() > 0);
    assert!(resume.detail["from0_ms"].as_f64().unwrap() >= 0.0);

    // The corpus block is the provenance a harness records next to timings.
    assert_eq!(
        v["corpus"]["messages"].as_u64().unwrap(),
        SeedConfig::tiny(5).messages as u64
    );
    assert!(v["corpus"]["deepest_channel"].as_u64().unwrap() > 0);
    assert!(dir.exists(), "the bench store is left on disk for inspection");
}

/// The bench always seeds a **fresh** corpus: a second run over the same dir
/// wipes it first, so the `seed` cell never times an append onto an existing
/// store (which would be a different, meaningless measurement).
#[tokio::test]
async fn a_second_bench_run_reseeds_rather_than_appending() {
    let t = sweeping_temp_dir("chatter-bench-reseed");
    let dir = t.path().join("bench-store");
    let cfg = BenchConfig {
        scroll_pages: 1,
        scroll_page_size: 8,
        tail_page_size: 64,
        ..BenchConfig::new(dir.clone(), SeedConfig::tiny(6))
    };
    let first = bench::run(&cfg).await.expect("first run");
    let second = bench::run(&cfg).await.expect("second run");
    assert_eq!(
        first.store.census.published_events,
        second.store.census.published_events,
        "a re-run must produce the same store, not a doubled one"
    );
    assert_eq!(first.corpus.messages, second.corpus.messages);
}
