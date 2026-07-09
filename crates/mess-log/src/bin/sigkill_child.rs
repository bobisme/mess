//! bn-2iq: the SIGKILL child writer. Spawned by `tests/sigkill_harness.rs`
//! (never invoked directly): appends batches under a chosen
//! [`Durability`] mode to a real segment file on a real device and
//! reports each committer ack as one line on stdout — a pipe the parent
//! reads concurrently, so what the parent has *actually received* by the
//! time it SIGKILLs this process is exactly the ack ledger the harness
//! reconciles against recovery (see the harness module doc for why the
//! ack must be reported strictly *after* the committer acks, not before).
//!
//! Usage (positional, all required):
//!   sigkill_child <segment-path> <process|os|group> <writers> <events-per-batch>
//!
//! Never exits on its own (loops appending forever, bounded only by
//! `SEGMENT_SIZE`, chosen large enough that no realistic kill-delay window
//! fills it) — the parent's SIGKILL is the only way this process ends.

use std::env;
use std::io::Write;
use std::path::PathBuf;
use std::sync::mpsc;

use mess_log::committer::{AppendOutcome, AppendRequest, Committer, Durability, EventInput};
use mess_log::runtime::{RealRuntime, Runtime};
use mess_log::writer::{SegmentParams, SegmentWriter};

/// Large enough that even the nightly profile's longest kill delay never
/// runs a writer into `SegmentFull` — rolling/sealing is Phase 4 and
/// deliberately out of scope for this harness (one segment, never rolled).
const SEGMENT_SIZE: u64 = 1024 * 1024 * 1024; // 1 GiB

fn parse_mode(s: &str) -> Durability {
    match s {
        "process" => Durability::Process,
        "os" => Durability::Os,
        "group" => Durability::group_default(),
        other => panic!("unknown durability mode {other:?} (want process|os|group)"),
    }
}

fn make_events(n: usize, writer: u64, batch: u64) -> Vec<EventInput> {
    (0..n)
        .map(|i| {
            let payload = format!("sigkill-harness w{writer} b{batch} e{i}").into_bytes();
            EventInput::plain(1, 1, 0, payload)
        })
        .collect()
}

fn main() {
    let args: Vec<String> = env::args().collect();
    assert_eq!(
        args.len(),
        5,
        "usage: sigkill_child <segment-path> <process|os|group> <writers> <events-per-batch>"
    );
    let path = PathBuf::from(&args[1]);
    let mode = parse_mode(&args[2]);
    let writers: u64 = args[3].parse().expect("writers: u64");
    let events_per: usize = args[4].parse().expect("events-per-batch: usize");

    let rt = RealRuntime::new();
    let fs = rt.fs();
    let writer = SegmentWriter::create(
        &fs,
        &path,
        SegmentParams { segment_size: SEGMENT_SIZE, ..SegmentParams::new(0, 0, 1, 0) },
    )
    .expect("create segment");

    // A single reporter thread owns stdout exclusively: one `write_all`
    // call per line (well under PIPE_BUF, so the write is atomic — SIGKILL
    // cannot tear an in-flight `write(2)`, only ever land strictly before
    // or strictly after one, per `spikes/recovery_scale`'s finding),
    // flushed immediately so the parent's concurrent reader can observe it
    // before any kill. Serializing every writer task's acks through one
    // channel avoids interleaved partial lines from concurrent stdout
    // writers.
    let (ack_tx, ack_rx) = mpsc::channel::<(u64, u64)>();
    let reporter = std::thread::spawn(move || {
        let mut out = std::io::stdout();
        while let Ok((first, last)) = ack_rx.recv() {
            let line = format!("{first} {last}\n");
            if out.write_all(line.as_bytes()).is_err() {
                break;
            }
            let _ = out.flush();
        }
    });

    rt.block_on(async {
        let committer = Committer::spawn(&rt, writer, mode);
        let mut joins = Vec::new();
        for w in 0..writers {
            let appender = committer.appender();
            let ack_tx = ack_tx.clone();
            joins.push(rt.spawn(async move {
                let mut version = 0u64;
                let mut batch = 0u64;
                loop {
                    let events = make_events(events_per, w, batch);
                    let req = AppendRequest {
                        stream_id: w,
                        category_id: 0,
                        first_stream_version: version,
                        events,
                    };
                    match appender.append(req).await {
                        Ok(AppendOutcome::Acked { first_position, last_position }) => {
                            // A send error means the reporter thread is
                            // gone (stdout write failed, e.g. parent
                            // closed its read end) — nothing more to do.
                            let _ = ack_tx.send((first_position, last_position));
                        }
                        Ok(AppendOutcome::Indeterminate) => {
                            // No ack earned (§7.2) — correctly not counted.
                        }
                        Err(_) => break, // SegmentFull or Closed: stop this writer.
                    }
                    version += events_per as u64;
                    batch += 1;
                }
            }));
        }
        for j in joins {
            j.await;
        }
        // Reached only if every writer exhausted the segment without being
        // killed (should not happen in the harness; a manual run without a
        // kill would get here eventually).
        committer.shutdown().await;
    });

    drop(ack_tx);
    let _ = reporter.join();
}
