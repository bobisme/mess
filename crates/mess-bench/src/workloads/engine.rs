//! Engine end-to-end: buffered append and sealed replay through the
//! composed `mess-log` + `mess-index` engine (`mess_store::LogEngine`).
//! Ported from `mess-store/benches/engine_envelope.rs`'s gates 1 and 2.
//! Full-size params (400 x 5,000-event batches = 2,000,000 events) match
//! `engine.buffered.ev_per_s` / `engine.sealed_replay.ev_per_s` in
//! `docs/perf/envelope.md`.

use std::path::Path;
use std::time::Instant;

use mess_log::committer::Durability;
use mess_store::backend::{Backend, RecordToAppend};
use mess_store::{EngineOptions, EventStore, LogEngine, Version};

use crate::{Metric, RunSize};

fn rec(i: usize) -> RecordToAppend {
    RecordToAppend {
        message_type: "account.deposited".to_string(),
        data:         (i as u64).to_le_bytes().to_vec(),
    }
}

pub fn run(size: RunSize, scratch: &Path) -> Vec<Metric> {
    let (batches, per_batch): (usize, usize) = match size {
        RunSize::Full => (400, 5_000),
        RunSize::Smoke => (4, 50),
    };
    let total = batches * per_batch;
    let dir = scratch.join("engine");
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create scratch dir");

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();

    let engine = LogEngine::open_with(
        dir.join("append"),
        EngineOptions {
            durability: Durability::Process,
            segment_size: 2 * 1024 * 1024 * 1024,
            ..Default::default()
        },
    )
    .expect("open");

    let batch: Vec<RecordToAppend> = (0..per_batch).map(rec).collect();
    let started = Instant::now();
    rt.block_on(async {
        let mut expected = Version::NoStream;
        for _ in 0..batches {
            let out = engine
                .append_batch("bench-stream", expected, &batch)
                .await
                .expect("append");
            expected = out.version;
        }
    });
    let append_elapsed = started.elapsed();
    let append_ev_per_s = total as f64 / append_elapsed.as_secs_f64();

    // Seal the corpus: reads now route through the sealed ReplaySet cold path.
    engine.seal_active().expect("seal");
    let store = EventStore::new(engine).with_page_size(total + 1);

    let started = Instant::now();
    let records = rt.block_on(async {
        store
            .backend()
            .read_stream("bench-stream", Version::NoStream, total + 1)
            .await
            .expect("read sealed")
    });
    let replay_elapsed = started.elapsed();
    assert_eq!(records.len(), total, "sealed replay returned wrong count");
    let replay_ev_per_s = total as f64 / replay_elapsed.as_secs_f64();

    let _ = std::fs::remove_dir_all(&dir);

    vec![
        Metric::new(
            "engine.buffered.ev_per_s",
            append_ev_per_s,
            "ev/s",
            format!(
                "full composed engine append path (LogEngine::append_batch, \
                 real mess-log committer), Durability::Process; {batches} x \
                 {per_batch}-event batches = {total} events; ~24B payload; \
                 real-fs scratch; single run"
            ),
        ),
        Metric::new(
            "engine.sealed_replay.ev_per_s",
            replay_ev_per_s,
            "ev/s",
            format!(
                "EventStore load of a {total}-event sealed corpus: \
                 read_stream routes through the real mess-index ReplaySet \
                 cold path, then materialises StoredRecords; single stream; \
                 single run"
            ),
        ),
    ]
}
