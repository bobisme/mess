//! Concurrent readers against a live writer (`bn-1dlb`).
//!
//! Every other reference workload is a single-writer loop, so none of them
//! touches the one lock every reader shares: `LogEngine` keeps all engine
//! clones on one `Arc<Mutex<Book>>` (`mess-store/src/engine.rs:191`), and the
//! public read surface goes through it — `head` (4405), `stream_id_of` (3709),
//! `event_type_id_of` (3717), and record decode (3816). A concurrent read load
//! against an active writer was therefore completely unmeasured.
//!
//! The two reader metrics are deliberately separated rather than summed.
//! `head(name)` resolves the name to a stream id through `book.registry` and
//! THEN reads `book.heads`, both under the same lock; `stream_id_of` does only
//! the resolution. Reporting them apart is what distinguishes "the head map is
//! the bottleneck" from "name resolution is the bottleneck" — a distinction
//! the Phase 5 dense-head work turns on, and which an aggregate would hide.

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Instant;

use mess_log::committer::Durability;
use mess_store::backend::{Backend, RecordToAppend};
use mess_store::{EngineOptions, LogEngine, Version};

use crate::{Metric, RunSize};

fn rec(i: usize) -> RecordToAppend {
    RecordToAppend {
        message_type: "account.deposited".to_string(),
        data:         (i as u64).to_le_bytes().to_vec(),
    }
}

pub fn run(size: RunSize, scratch: &Path) -> Vec<Metric> {
    // `readers` is the variable under test: 0 isolates the writer so the
    // contended numbers below have an uncontended reference on the same host
    // and in the same run, which is the only comparison this box can make
    // reliably (see bn-1gn1 — run-to-run drift here is larger than most
    // effects worth measuring).
    let (batches, per_batch, readers, streams): (usize, usize, usize, usize) =
        match size {
            RunSize::Full => (400, 2_000, 8, 64),
            RunSize::Smoke => (4, 50, 2, 8),
        };
    let dir = scratch.join("reader-contention");
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create scratch dir");

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(8)
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

    // Register every stream up front so readers resolve real ids rather than
    // measuring the miss path, and so the writer is not minting registrations
    // during the measured window.
    let names: Vec<String> =
        (0..streams).map(|s| format!("reader-stream-{s}")).collect();
    rt.block_on(async {
        for name in &names {
            engine
                .append_batch(name, Version::NoStream, &[rec(0)])
                .await
                .expect("prime stream");
        }
    });

    let solo = write_throughput(&rt, &engine, &names, batches, per_batch, 0);
    let (contended, head_reads_per_s, resolve_per_s) =
        contended_pass(&rt, &engine, &names, batches, per_batch, readers);

    drop(engine);
    let _ = std::fs::remove_dir_all(&dir);

    // Retention of writer throughput under read load. 1.0 means readers cost
    // the writer nothing; well below 1.0 means the shared Book mutex is a real
    // bottleneck and Phase 5's premise holds.
    let retention = if solo > 0.0 { contended / solo } else { 0.0 };

    vec![
        Metric::new(
            "engine.reader_contention.writer_solo.ev_per_s",
            solo,
            "ev/s",
            format!(
                "writer-only reference: {batches} x {per_batch}-event batches \
                 round-robin over {streams} streams, Durability::Process, no \
                 concurrent readers; same process and host as the contended \
                 pass below"
            ),
        ),
        Metric::new(
            "engine.reader_contention.writer_loaded.ev_per_s",
            contended,
            "ev/s",
            format!(
                "same writer with {readers} concurrent reader tasks hammering \
                 Backend::head and stream_id_of across {streams} streams"
            ),
        ),
        Metric::new(
            "engine.reader_contention.writer_retention",
            retention,
            "ratio",
            format!(
                "writer_loaded / writer_solo; 1.0 = readers are free. Below \
                 1.0 quantifies contention on the shared Arc<Mutex<Book>> \
                 with {readers} readers"
            ),
        ),
        Metric::new(
            "engine.reader_contention.head_reads_per_s",
            head_reads_per_s,
            "op/s",
            format!(
                "aggregate Backend::head(name) rate across the {} head-only \
                 readers during the contended pass; resolves name->id via \
                 book.registry AND reads book.heads, both under the Book mutex",
                readers.div_ceil(2)
            ),
        ),
        Metric::new(
            "engine.reader_contention.resolve_per_s",
            resolve_per_s,
            "op/s",
            format!(
                "aggregate stream_id_of(name) rate across the {} resolve-only \
                 readers during the contended pass; name->id resolution ONLY. \
                 Compare against head_reads_per_s (same reader count) to \
                 split Book mutex cost between name resolution and the head \
                 map",
                readers / 2
            ),
        ),
    ]
}

/// Round-robin appends across `names`, returning events/sec.
fn write_throughput(
    rt: &tokio::runtime::Runtime,
    engine: &LogEngine,
    names: &[String],
    batches: usize,
    per_batch: usize,
    _readers: usize,
) -> f64 {
    let batch: Vec<RecordToAppend> = (0..per_batch).map(rec).collect();
    // Read the live head per stream rather than assuming a starting version:
    // the solo pass has already advanced these streams by the time the
    // contended pass runs, and a stale `expected` would conflict on every
    // append instead of measuring anything.
    let mut heads = rt.block_on(async {
        let mut heads = Vec::with_capacity(names.len());
        for name in names {
            heads.push(engine.head(name).await.expect("head"));
        }
        heads
    });
    let started = Instant::now();
    let written = rt.block_on(async {
        let mut written = 0usize;
        for b in 0..batches {
            let i = b % names.len();
            let out = engine
                .append_batch(&names[i], heads[i], &batch)
                .await
                .expect("append");
            heads[i] = out.version;
            written += per_batch;
        }
        written
    });
    written as f64 / started.elapsed().as_secs_f64()
}

/// The same write loop with `readers` reader tasks running alongside.
/// Returns (writer ev/s, head ops/s, resolve ops/s).
fn contended_pass(
    rt: &tokio::runtime::Runtime,
    engine: &LogEngine,
    names: &[String],
    batches: usize,
    per_batch: usize,
    readers: usize,
) -> (f64, f64, f64) {
    let stop = Arc::new(AtomicBool::new(false));
    let head_ops = Arc::new(AtomicU64::new(0));
    let resolve_ops = Arc::new(AtomicU64::new(0));

    let handles: Vec<_> = rt.block_on(async {
        (0..readers)
            .map(|r| {
                let engine = engine.clone();
                let names: Vec<String> = names.to_vec();
                let stop = Arc::clone(&stop);
                let head_ops = Arc::clone(&head_ops);
                let resolve_ops = Arc::clone(&resolve_ops);
                // Half the readers do head-only, half do resolve-only. Running
                // both in ONE loop makes the two rates equal by construction
                // (one of each per iteration) and measures nothing; as
                // separate populations their throughputs are independent, and
                // the ratio between them is what splits Book mutex cost
                // between name resolution and the head map.
                let head_reader = r % 2 == 0;
                tokio::spawn(async move {
                    // Stagger the starting stream per reader so they do not
                    // march in lockstep over the same id.
                    let mut i = r;
                    while !stop.load(Ordering::Relaxed) {
                        let name = &names[i % names.len()];
                        if head_reader {
                            let _ = engine.head(name).await.expect("head");
                            head_ops.fetch_add(1, Ordering::Relaxed);
                        } else {
                            let _ = engine.stream_id_of(name);
                            resolve_ops.fetch_add(1, Ordering::Relaxed);
                        }
                        i = i.wrapping_add(1);
                        // MANDATORY. `head` is `async` but performs no I/O, so
                        // this loop never returns to the scheduler on its own.
                        // Without an explicit yield the reader tasks pin every
                        // worker thread, tokio cannot preempt them, and the
                        // writer is starved forever rather than merely slowed
                        // — which measures a livelock instead of contention.
                        tokio::task::yield_now().await;
                    }
                })
            })
            .collect()
    });

    let started = Instant::now();
    let ev_per_s =
        write_throughput(rt, engine, names, batches, per_batch, readers);
    let window = started.elapsed().as_secs_f64();

    stop.store(true, Ordering::Relaxed);
    rt.block_on(async {
        for h in handles {
            let _ = h.await;
        }
    });

    let heads = head_ops.load(Ordering::Relaxed) as f64 / window;
    let resolves = resolve_ops.load(Ordering::Relaxed) as f64 / window;
    (ev_per_s, heads, resolves)
}
