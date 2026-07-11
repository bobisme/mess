//! Phase 3 exit-gate throughput bench (bn-2m1): the real committer append
//! path, driven at the two durability extremes the gate names.
//!
//!   * `durable`  — [`Durability::group_default`] (D7 early-close group commit,
//!     `fdatasync` per group). Every ack is fully durable. Gate: >= 100k ev/s
//!     at 4 writers x 100-event batches on ext4.
//!   * `buffered` — [`Durability::Process`]: `write(2)` into page cache, no
//!     barrier. Gate: >= 1M ev/s (encode + pwrite ceiling).
//!
//! Best-of-N wall time (device state and scheduler jitter only ever make a run
//! *slower*, so the fastest rep is the truest read of the code path — the same
//! best-of methodology as `spikes/perf_group_commit`). Durable runs MUST use an
//! ext4 scratch dir: on tmpfs `fdatasync` is a no-op and the number is a lie.
//!
//! ```text
//! MESS_BENCH_DIR=$HOME/.cache/mess-bench \
//!   cargo run -p mess-log --release --example durable_bench -- \
//!   <durable|buffered> [writers] [batch] [batches_per_writer] [reps]
//! ```

use std::path::PathBuf;
use std::time::Instant;

use mess_log::committer::{AppendRequest, Committer, Durability, EventInput};
use mess_log::runtime::{RealRuntime, Runtime};
use mess_log::writer::{SegmentParams, SegmentWriter};

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let mode = args.get(1).map(String::as_str).unwrap_or("durable");
    let writers: u64 = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(4);
    let batch: usize = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(100);
    let batches_per_writer: u64 = args
        .get(4)
        .and_then(|s| s.parse().ok())
        .unwrap_or(if mode == "buffered" { 5_000 } else { 500 });
    let reps: usize = args.get(5).and_then(|s| s.parse().ok()).unwrap_or(5);

    let durability = match mode {
        "durable" => Durability::group_default(),
        "buffered" => Durability::Process,
        other => {
            eprintln!("unknown mode {other:?}; use 'durable' or 'buffered'");
            std::process::exit(2);
        }
    };

    let total_events = writers * batches_per_writer * (batch as u64);
    // ~250 B payload + framing; size the (preallocated) segment with headroom
    // so the whole run lands in one segment (the committer owns exactly
    // one).
    let est_bytes = total_events.saturating_mul(320).saturating_add(1 << 20);
    let segment_size = est_bytes.max(256 * 1024 * 1024).next_power_of_two();

    // `MESS_BENCH_DIR`, if set, still wins (a caller pinning a specific ext4
    // mount for the durable numbers) — routed through `temp_dir_in` so its
    // one nested run dir is still cleaned up on exit like every other site
    // (bn-2jr). Unset, this falls back to the same shared self-sweeping
    // namespace (`TMPDIR`/`$HOME/.cache/mess-test-tmp`) every other real-fs
    // suite in this repo uses, rather than a bare, never-swept
    // `std::env::temp_dir()/mess-bench`.
    let scratch = match std::env::var_os("MESS_BENCH_DIR").map(PathBuf::from) {
        Some(root) => {
            std::fs::create_dir_all(&root).expect("create scratch base dir");
            mess_testkit::temp_dir_in(&root, "durable-bench")
        }
        None => mess_testkit::sweeping_temp_dir("durable-bench"),
    };

    // ~250 B payload, the measured production event size.
    let payload: Vec<u8> = (0..250u32).map(|i| (i & 0xFF) as u8).collect();

    println!(
        "durable_bench mode={mode} writers={writers} batch={batch} \
         batches/writer={batches_per_writer} reps={reps} \
         total_events={total_events} segment_size={}MiB scratch={}",
        segment_size >> 20,
        scratch.path().display()
    );

    let mut best_ev_s = 0.0f64;
    let mut best_secs = f64::INFINITY;
    let mut all: Vec<f64> = Vec::with_capacity(reps);

    for rep in 0..reps {
        let rt = RealRuntime::new();
        let fs = rt.fs();
        let path = scratch
            .path()
            .join(format!("bench-{mode}-{}-{rep}.seg", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let mut params = SegmentParams::new(1, 0, 1, 0);
        params.segment_size = segment_size;
        let writer =
            SegmentWriter::create(&fs, &path, params).expect("create segment");

        let secs = rt.block_on(async {
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

        let (elapsed, fsyncs, mean_fsync_us) = secs;
        let ev_s = total_events as f64 / elapsed;
        all.push(ev_s);
        if elapsed < best_secs {
            best_secs = elapsed;
            best_ev_s = ev_s;
        }
        println!(
            "  rep {rep}: {elapsed:.4}s  {ev_s:.0} ev/s  fsyncs={fsyncs}  \
             ev/fsync={:.0}  mean_fsync={mean_fsync_us:.2}us",
            if fsyncs > 0 { total_events as f64 / fsyncs as f64 } else { 0.0 }
        );

        let _ = std::fs::remove_file(&path);
    }

    let reps_str =
        all.iter().map(|v| format!("{v:.0}")).collect::<Vec<_>>().join(" / ");
    println!(
        "RESULT mode={mode} writers={writers}x{batch} BEST {best_ev_s:.0} \
         ev/s (best {best_secs:.4}s)  all reps: [{reps_str}]"
    );
}
