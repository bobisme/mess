//! bn-2za — seal-time Reed-Solomon parity sidecar wiring.
//!
//! The background roll-sealer emits a `.par` sidecar next to each sealed
//! segment **only when `EngineOptions::parity` is enabled** (evidence-gated,
//! off by default). These tests pin both halves of that contract against a real
//! engine that rolls (and therefore seals) several tiny segments under load.

use std::path::Path;

use mess_index::sealed::parity::{ParityConfig, ParitySidecar};
use mess_store::backend::{Backend, RecordToAppend};
use mess_store::{EngineOptions, LogEngine, Version};

fn rec(data: &[u8]) -> RecordToAppend {
    RecordToAppend {
        message_type: "ev".to_string(),
        data:         data.to_vec(),
    }
}

/// Force many rolls (and thus many background seals) with a tiny active
/// segment.
fn rolling_opts() -> EngineOptions {
    EngineOptions { segment_size: 16 * 1024, ..EngineOptions::default() }
}

async fn fill(engine: &LogEngine, n: usize) {
    let mut expected = Version::NoStream;
    for i in 0..n {
        let out = engine
            .append_batch(
                "s",
                expected,
                &[rec(format!("event-{i:05}").as_bytes())],
            )
            .await
            .expect("append");
        expected = out.version;
    }
}

/// Wait (bounded) for at least one `.par` sidecar to appear under `sealed/`.
fn count_par(sealed: &Path) -> usize {
    std::fs::read_dir(sealed)
        .map(|rd| {
            rd.flatten()
                .filter(|e| {
                    e.path().extension().and_then(|x| x.to_str()) == Some("par")
                })
                .count()
        })
        .unwrap_or(0)
}

/// With parity enabled, the sealer writes a valid, matching `.par` for sealed
/// segments; disabled (the default) it writes none.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn parity_sidecar_written_only_when_enabled() {
    // --- enabled ---
    let on_dir = tempfile::tempdir().expect("tempdir");
    let on_store = on_dir.path().join("store");
    let mut opts = rolling_opts();
    opts.parity = ParityConfig { enabled: true, ..Default::default() };
    let engine = LogEngine::open_with(&on_store, opts).expect("open");
    fill(&engine, 400).await; // spans several 16 KiB segments ⇒ several seals

    let sealed = on_store.join("sealed");
    // Bounded wait for the background sealer to land at least one .par.
    let mut pars = 0;
    for _ in 0..200 {
        pars = count_par(&sealed);
        if pars > 0 {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(25));
    }
    assert!(
        pars > 0,
        "parity enabled ⇒ at least one .par sidecar must be written"
    );

    // Every .par must parse + CRC-validate and match a real sealed .log by id.
    for entry in std::fs::read_dir(&sealed).unwrap().flatten() {
        let path = entry.path();
        if path.extension().and_then(|x| x.to_str()) != Some("par") {
            continue;
        }
        let side =
            ParitySidecar::open(&path).expect("read par").expect("valid par");
        // The sidecar covers the finalized .log byte-for-byte.
        let log = on_store.join(format!("seg-{:08}.log", side.segment_id()));
        let bytes = std::fs::read(&log).expect("read sealed log");
        assert_eq!(
            bytes.len() as u64,
            side.source_len(),
            "par source_len must equal the sealed .log length"
        );
        // Undamaged: parity localizes zero damaged blocks.
        assert!(
            side.damaged_shards(&bytes).is_empty(),
            "a freshly sealed segment has no damaged blocks under its own \
             parity"
        );
    }
    drop(engine);

    // --- disabled (default) ---
    let off_dir = tempfile::tempdir().expect("tempdir");
    let off_store = off_dir.path().join("store");
    let engine =
        LogEngine::open_with(&off_store, rolling_opts()).expect("open");
    fill(&engine, 400).await;
    // Give the sealer the same opportunity to run.
    std::thread::sleep(std::time::Duration::from_millis(300));
    drop(engine);
    assert_eq!(
        count_par(&off_store.join("sealed")),
        0,
        "parity is off by default ⇒ no .par sidecars"
    );
}
