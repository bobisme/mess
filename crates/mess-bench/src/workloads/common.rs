//! Shared helpers for the committer-driven append workloads
//! ([`super::buffered_append`], [`super::durable_append`]) — both are the
//! same `Committer` drive at a different [`mess_log::committer::Durability`],
//! ported from `mess-log/examples/durable_bench.rs`.

use std::path::{Path, PathBuf};
use std::time::Instant;

use mess_log::committer::{AppendRequest, Committer, Durability, EventInput};
use mess_log::runtime::{RealRuntime, Runtime};
use mess_log::writer::{SegmentParams, SegmentWriter};

/// Result of a best-of-`reps` committer drive.
pub(crate) struct CommitterResult {
    pub best_ev_per_s:         f64,
    pub total_events:          u64,
    pub mean_fsync_us_of_best: f64,
    pub fsyncs_of_best:        u64,
}

/// Drive `writers` concurrent appenders, each submitting `batches_per_writer`
/// batches of `batch` events, through a real [`Committer`] at `durability`,
/// best-of-`reps`. Matches `durable_bench.rs`'s methodology exactly (same
/// ~250B payload, same request shape) so the numbers are comparable to the
/// `docs/perf/envelope.md` rows recorded via that example.
pub(crate) fn drive_committer(
    scratch: &Path,
    tag: &str,
    durability: Durability,
    writers: u64,
    batch: usize,
    batches_per_writer: u64,
    reps: usize,
) -> CommitterResult {
    let total_events = writers * batches_per_writer * (batch as u64);
    let est_bytes = total_events.saturating_mul(320).saturating_add(1 << 20);
    let segment_size = est_bytes.max(16 * 1024 * 1024).next_power_of_two();
    std::fs::create_dir_all(scratch).expect("create scratch dir");
    let payload: Vec<u8> = (0..250u32).map(|i| (i & 0xFF) as u8).collect();

    let mut best_ev_s = 0.0f64;
    let mut best_secs = f64::INFINITY;
    let mut best_fsyncs = 0u64;
    let mut best_mean_fsync_us = 0.0f64;

    for rep in 0..reps {
        let rt = RealRuntime::new();
        let fs = rt.fs();
        let path: PathBuf = scratch
            .join(format!("bench-{tag}-{}-{rep}.seg", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let mut params = SegmentParams::new(1, 0, 1, 0);
        params.segment_size = segment_size;
        let writer =
            SegmentWriter::create(&fs, &path, params).expect("create segment");

        let (elapsed, fsyncs, mean_fsync_us) = rt.block_on(async {
            let c = Committer::spawn(&rt, writer, durability);
            let start = Instant::now();
            let mut joins = Vec::new();
            for w in 0..writers {
                let ap = c.appender();
                let payload = payload.clone();
                joins.push(rt.spawn(async move {
                    for b in 0..batches_per_writer {
                        let events: Vec<EventInput> = (0..batch)
                            .map(|_| {
                                EventInput::plain(1, 0, 0, payload.clone())
                            })
                            .collect();
                        let req = AppendRequest {
                            stream_id: w,
                            category_id: 0,
                            first_stream_version: b * (batch as u64),
                            events,
                        };
                        ap.append(req).await.expect("append acked");
                    }
                }));
            }
            for j in joins {
                j.await;
            }
            let elapsed = start.elapsed().as_secs_f64();
            let fsyncs = c.fsync_count();
            let mean_fsync_us = c.mean_fsync_nanos() as f64 / 1000.0;
            c.shutdown().await;
            (elapsed, fsyncs, mean_fsync_us)
        });

        let ev_s = total_events as f64 / elapsed;
        if elapsed < best_secs {
            best_secs = elapsed;
            best_ev_s = ev_s;
            best_fsyncs = fsyncs;
            best_mean_fsync_us = mean_fsync_us;
        }
        let _ = std::fs::remove_file(&path);
    }

    CommitterResult {
        best_ev_per_s: best_ev_s,
        total_events,
        mean_fsync_us_of_best: best_mean_fsync_us,
        fsyncs_of_best: best_fsyncs,
    }
}
