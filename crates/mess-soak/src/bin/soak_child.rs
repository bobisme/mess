//! `soak-child` — the forked worker the `--crash-mode sigkill` parent kills.
//!
//! Opens the SAME store dir as the parent under `Durability::Os` (every acked
//! append is `fdatasync`ed before its ack is reported) and drives a real
//! concurrent workload: `writers` tasks, each owning a disjoint slice of the
//! stream space (partitioned by `stream_idx % writers`, so no two tasks race
//! one stream and every task always knows the exact expected version). Each
//! ack is reported as one atomic stdout line the parent reads concurrently —
//! so whatever the parent has received when it `SIGKILL`s this process is
//! exactly the ledger recovery is reconciled against.
//!
//! Reports the ack STRICTLY AFTER the committer ack (never before): the parent
//! must never believe an event is durable that a well-timed kill could prove
//! was not. Loops forever; the parent's `SIGKILL` is the only exit.
//!
//! Usage (positional, all required):
//!   soak-child <dir> <seed> <streams> <writers> <segment-size> <round>

use std::io::Write;
use std::path::PathBuf;
use std::sync::mpsc;

use mess_log::committer::Durability;
use mess_store::backend::{AppendError, Appended, Backend, RecordToAppend};
use mess_store::{EngineOptions, LogEngine, Version};

fn main() {
    let args: Vec<String> = std::env::args().collect();
    assert_eq!(
        args.len(),
        7,
        "usage: soak-child <dir> <seed> <streams> <writers> <segment-size> \
         <round>"
    );
    let dir = PathBuf::from(&args[1]);
    let seed: u64 = args[2].parse().expect("seed");
    let streams: u64 = args[3].parse().expect("streams");
    let writers: u64 = args[4].parse().expect("writers");
    let segment_size: u64 = args[5].parse().expect("segment-size");
    let round: u64 = args[6].parse().expect("round");

    let engine = LogEngine::open_with(
        &dir,
        EngineOptions {
            durability: Durability::Os,
            segment_size: segment_size.max(4096),
            ..EngineOptions::default()
        },
    )
    .expect("open engine in child");

    // One reporter owns stdout: one `write_all` per ack line, each well under
    // PIPE_BUF so a SIGKILL can only land strictly before or after a write,
    // never tearing one (the same discipline as mess-log's sigkill_child).
    let (tx, rx) = mpsc::channel::<(u64, u64, u64, u64)>();
    let reporter = std::thread::spawn(move || {
        let mut out = std::io::stdout();
        while let Ok((stream_idx, first_sp, first_global, count)) = rx.recv() {
            let line =
                format!("A {stream_idx} {first_sp} {first_global} {count}\n");
            if out.write_all(line.as_bytes()).is_err() {
                break;
            }
            let _ = out.flush();
        }
    });

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads((writers as usize).clamp(2, 8))
        .enable_all()
        .build()
        .expect("tokio runtime");

    rt.block_on(async move {
        let mut handles = Vec::new();
        for w in 0..writers {
            let engine = engine.clone();
            let tx = tx.clone();
            handles.push(tokio::spawn(async move {
                writer_loop(engine, tx, w, writers, streams, seed ^ round)
                    .await;
            }));
        }
        drop(tx);
        for h in handles {
            let _ = h.await;
        }
    });
    drop(reporter);
}

/// Append forever to this writer's partition of the stream space.
async fn writer_loop(
    engine: LogEngine,
    tx: mpsc::Sender<(u64, u64, u64, u64)>,
    writer: u64,
    writers: u64,
    streams: u64,
    seed: u64,
) {
    // This writer's streams: every idx with idx % writers == writer.
    let my_streams: Vec<u64> =
        (0..streams).filter(|i| i % writers == writer).collect();
    if my_streams.is_empty() {
        return;
    }
    // Local head per owned stream (exact expected version; no cross-task race).
    let mut heads: Vec<Version> = vec![Version::NoStream; my_streams.len()];
    let mut rr = 0usize;
    let mut nonce = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15);

    loop {
        let slot = rr % my_streams.len();
        rr = rr.wrapping_add(1);
        let stream_idx = my_streams[slot];
        let name = format!("stream-{stream_idx:05}");
        let expected = heads[slot];

        // 1..=4 events per batch, deterministic from the nonce.
        let count = 1 + (nonce % 4);
        let first_sp = expected.next_position();
        let mut recs = Vec::with_capacity(count as usize);
        for k in 0..count {
            nonce = nonce.wrapping_add(0x9E37_79B9_7F4A_7C15);
            let mut data = Vec::with_capacity(16);
            data.extend_from_slice(&nonce.to_le_bytes());
            data.extend_from_slice(&(first_sp + k).to_le_bytes());
            recs.push(RecordToAppend {
                message_type: "soak.event".into(),
                data,
            });
        }

        match engine.append_batch(&name, expected, &recs).await {
            Ok(Appended { version, last_global_position }) => {
                let first_global = last_global_position + 1 - count;
                heads[slot] = version;
                // Report the ack ONLY now — strictly after the committer acked
                // and fdatasync returned. A send error means the parent closed
                // the pipe (it is killing us); nothing left to do.
                if tx.send((stream_idx, first_sp, first_global, count)).is_err()
                {
                    return;
                }
            }
            Err(AppendError::Conflict { actual, .. }) => {
                // Should not happen (single writer per stream), but stay
                // correct: resync and retry rather than corrupt the ledger.
                heads[slot] = actual;
            }
            Err(AppendError::Backend(_)) => {
                // Segment full / engine error: yield and retry; the parent's
                // kill is the real terminator.
                tokio::task::yield_now().await;
            }
        }
    }
}
