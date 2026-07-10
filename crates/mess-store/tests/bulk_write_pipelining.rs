//! bn-162: the "bulk writes" guide's runnable, measured demonstration
//! (`docs/perf/bulk-writes.md`).
//!
//! The dogfood finding (bn-1mw, `examples/social`'s seeder): 1,488 sequential
//! [`EventStore::command`] round trips (load → decide → append) took ~8.5s on
//! a quiet machine. This file proves the guide's two claims about the fix —
//! bounded-concurrency commands to DISTINCT streams:
//!
//! 1. **Safety**: pipelined commands to distinct streams produce byte-for-byte
//!    identical store contents to the sequential baseline — same per-stream
//!    heads, same folded state. This is the load-bearing claim; bn-1s0's
//!    per-stream [`AppendGate`](mess_store::engine) is what makes it true (see
//!    `crates/mess-store/tests/engine_append_gate.rs` for the same claim one
//!    layer down, at the raw `append_batch` level).
//! 2. **Speed**: pipelining is meaningfully faster, because a durable
//!    (`Durability::Group`) engine's committer degrades to one `fdatasync` per
//!    command when there is only ever one writer in flight (`early-close`,
//!    `docs/spec/03-durability.md` §2.2) — sequential `command` calls are
//!    exactly that case. Concurrent distinct-stream commands give the
//!    committer's single gather point (§2.4-§2.5) multiple in-flight requests
//!    to coalesce into one barrier, "pipelining for free".
//!
//! Both streams of proof run against a real (non-tmpfs) durable engine —
//! `Durability::Group` with the spec's recommended defaults (§2.2:
//! `max_delay: 1ms`, `max_bytes: 8MiB`) — because `Durability::Process`
//! (the engine's own default, buffered/no-fsync) would hide exactly the
//! latency this test exists to demonstrate.
#![cfg(not(miri))]

use std::convert::Infallible;
use std::future::Future;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use mess_core::{Aggregate, CodecError, Decide, Event};
use mess_log::committer::Durability;
use mess_store::engine::EngineOptions;
use mess_store::{EventStore, LogEngine, Version};

// ===========================================================================
// A minimal domain: open a stream, then record one value on it. Two commands
// per stream (`Open` then `Record`) so every timed `Record` command runs
// against an already-interned stream name (bn-150's synchronous
// meta-store fsync for a brand-new name is a real, deliberate, but
// DIFFERENT durability cost from the one this test measures — see
// `engine_append_gate.rs`'s priming note); `Open` is issued for every
// stream, sequentially, before either timed phase.
// ===========================================================================

#[derive(Debug, Clone, PartialEq, Eq)]
enum ItemEvent {
    Opened,
    Recorded { value: u64 },
}

impl Event for ItemEvent {
    fn name(&self) -> &'static str {
        match self {
            ItemEvent::Opened => "item.opened",
            ItemEvent::Recorded { .. } => "item.recorded",
        }
    }

    fn encode(&self) -> Result<Vec<u8>, CodecError> {
        Ok(match self {
            ItemEvent::Opened => Vec::new(),
            ItemEvent::Recorded { value } => value.to_le_bytes().to_vec(),
        })
    }

    fn decode(name: &str, data: &[u8]) -> Result<Self, CodecError> {
        match name {
            "item.opened" => Ok(ItemEvent::Opened),
            "item.recorded" => {
                let bytes: [u8; 8] =
                    data.try_into().map_err(|_| CodecError::Decode {
                        event_name: name.to_string(),
                        source:     format!(
                            "expected 8 payload bytes, got {}",
                            data.len()
                        ),
                    })?;
                Ok(ItemEvent::Recorded { value: u64::from_le_bytes(bytes) })
            }
            other => Err(CodecError::UnknownEventName(other.to_string())),
        }
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
struct Item {
    opened: bool,
    value:  Option<u64>,
}

impl Aggregate for Item {
    type Event = ItemEvent;

    fn apply(&mut self, event: &ItemEvent) {
        match event {
            ItemEvent::Opened => self.opened = true,
            ItemEvent::Recorded { value } => self.value = Some(*value),
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct Open;

#[derive(Debug, Clone, Copy)]
struct Record {
    value: u64,
}

impl Decide<Open> for Item {
    type Rejection = Infallible;

    fn decide(&self, _cmd: Open) -> Result<Vec<ItemEvent>, Infallible> {
        Ok(vec![ItemEvent::Opened])
    }
}

impl Decide<Record> for Item {
    type Rejection = Infallible;

    fn decide(&self, cmd: Record) -> Result<Vec<ItemEvent>, Infallible> {
        Ok(vec![ItemEvent::Recorded { value: cmd.value }])
    }
}

fn stream_name(i: usize) -> String { format!("bulk-item-{i}") }

// ===========================================================================
// Harness
// ===========================================================================

/// A tempdir rooted under `$HOME/.cache/mess-test-tmp`, never `/tmp` — `/tmp`
/// is a quota-limited tmpfs on this host (256MiB `fallocate` fails with os
/// error 122) and `fdatasync` is a no-op on tmpfs, which would make this
/// test's whole premise (sequential commands are durability-barrier-bound)
/// dishonestly fast. Mirrors `engine_append_gate.rs`'s helper of the same
/// name.
fn durable_scratch_dir(prefix: &str) -> tempfile::TempDir {
    let base: PathBuf = std::env::var_os("HOME")
        .map(PathBuf::from)
        .expect("HOME must be set")
        .join(".cache/mess-test-tmp");
    std::fs::create_dir_all(&base).expect("create scratch base dir");
    tempfile::Builder::new()
        .prefix(prefix)
        .tempdir_in(&base)
        .expect("tempdir_in scratch base")
}

/// A real durable engine: `Durability::Group` at the spec's recommended
/// defaults (`docs/spec/03-durability.md` §2.2). This is the mode that
/// exhibits early-close's "degrades to sync-per-batch when there is only one
/// writer in flight" behaviour the guide is about — the engine's own default
/// (`Durability::Process`) is buffered/no-fsync and would hide it.
fn open_durable_engine(dir: &std::path::Path) -> LogEngine {
    LogEngine::open_with(
        dir,
        EngineOptions {
            durability: Durability::Group {
                max_delay: Duration::from_millis(1),
                max_bytes: 8 * 1024 * 1024,
            },
            ..EngineOptions::default()
        },
    )
    .expect("open durable engine")
}

/// Run `n` `Open` commands, one per distinct stream, sequentially — always
/// outside either timed phase below, so brand-new-stream-name interning cost
/// (bn-150) never leaks into the measurement.
async fn prime(store: &EventStore<LogEngine>, n: usize) {
    for i in 0..n {
        store
            .command::<Item, _>(&stream_name(i), Open)
            .await
            .expect("prime: open stream");
    }
}

/// The blessed bounded-concurrency pattern from `docs/perf/bulk-writes.md`:
/// at most `k` command futures in flight at any time, refilling as each
/// completes. Built on [`tokio::task::JoinSet`] (no `futures` crate dep is
/// needed anywhere in this workspace) — the equivalent of a bounded
/// `join_all`/`FuturesUnordered` window.
async fn run_bounded<I, F>(k: usize, mut futures: I)
where
    I: Iterator<Item = F>,
    F: Future<Output = ()> + Send + 'static,
{
    let mut set = tokio::task::JoinSet::new();
    for f in futures.by_ref().take(k.max(1)) {
        set.spawn(f);
    }
    while let Some(res) = set.join_next().await {
        res.expect("pipelined command task panicked");
        if let Some(f) = futures.next() {
            set.spawn(f);
        }
    }
}

/// The shared demonstration: build two independent durable stores, prime
/// both with `n` distinct streams, then write the SAME second event to every
/// stream — once sequentially, once pipelined at concurrency `k`. Returns
/// `(sequential_elapsed, pipelined_elapsed)` after asserting the two stores'
/// contents are identical.
async fn run_demo(n: usize, k: usize) -> (Duration, Duration) {
    let seq_dir = durable_scratch_dir("mess-bulk-write-seq-");
    let pipe_dir = durable_scratch_dir("mess-bulk-write-pipe-");
    let store_seq = EventStore::new(open_durable_engine(seq_dir.path()));
    let store_pipe = EventStore::new(open_durable_engine(pipe_dir.path()));

    // Prime: every stream exists at version 0 (Opened) before either timed
    // phase, on both stores.
    prime(&store_seq, n).await;
    prime(&store_pipe, n).await;

    // Phase 1 — SEQUENTIAL baseline: n distinct-stream `command` round trips,
    // one fully awaited before the next starts. This is exactly the seeder's
    // original shape (bn-1mw): every command is the only writer in flight, so
    // early-close gives it a full, un-amortized durability barrier.
    let seq_start = Instant::now();
    for i in 0..n {
        store_seq
            .command::<Item, _>(&stream_name(i), Record { value: i as u64 })
            .await
            .expect("sequential record");
    }
    let seq_elapsed = seq_start.elapsed();

    // Phase 2 — PIPELINED: the same n commands, to n OTHER distinct
    // (already-primed) streams on the other store, run at bounded
    // concurrency k via the blessed pattern.
    let pipe_start = Instant::now();
    let futures = (0..n).map(|i| {
        let store = store_pipe.clone();
        let stream = stream_name(i);
        async move {
            store
                .command::<Item, _>(&stream, Record { value: i as u64 })
                .await
                .expect("pipelined record");
        }
    });
    run_bounded(k, futures).await;
    let pipe_elapsed = pipe_start.elapsed();

    // --- Safety: prove identical contents, not just print a speed number. ---
    for i in 0..n {
        let a = store_seq
            .load::<Item>(&stream_name(i))
            .await
            .expect("load from sequential store");
        let b = store_pipe
            .load::<Item>(&stream_name(i))
            .await
            .expect("load from pipelined store");
        assert_eq!(
            a.version, b.version,
            "stream {i}: head diverged between sequential and pipelined runs"
        );
        assert_eq!(
            a.version,
            Version::At(1),
            "stream {i}: expected exactly 2 events (Opened + Recorded)"
        );
        assert_eq!(
            a.state, b.state,
            "stream {i}: folded state diverged between sequential and \
             pipelined runs"
        );
        assert_eq!(
            a.state.value,
            Some(i as u64),
            "stream {i}: recorded value does not match what was written"
        );
    }

    (seq_elapsed, pipe_elapsed)
}

// ===========================================================================
// Tests
// ===========================================================================

/// Fast default variant (runs in `cargo test -p mess-store`, no `--ignored`):
/// small enough to comfortably clear well under the ~30s budget, but a real
/// durable (fsync-bound) run, so the speedup it measures is the genuine
/// group-commit-coalescing effect, not noise.
#[tokio::test]
async fn pipelined_distinct_stream_commands_match_sequential_and_are_faster() {
    const N: usize = 64;
    const K: usize = 16;

    let (seq_elapsed, pipe_elapsed) = run_demo(N, K).await;
    let ratio =
        seq_elapsed.as_secs_f64() / pipe_elapsed.as_secs_f64().max(1e-9);

    eprintln!(
        "pipelined_distinct_stream_commands_match_sequential_and_are_faster: \
         N={N} K={K} sequential={seq_elapsed:?} pipelined={pipe_elapsed:?} \
         (speedup {ratio:.2}x)"
    );

    assert!(
        ratio > 1.3,
        "expected bounded-concurrency commands to DISTINCT streams to \
         meaningfully outrun sequential command round-trips — bn-1s0's \
         per-stream AppendGate plus the committer's single gather point \
         (docs/spec/03-durability.md §2.4-§2.5) should let concurrently \
         in-flight distinct-stream appends coalesce into far fewer fdatasync \
         calls, the same effect engine_append_gate.rs proves one layer down; \
         got sequential={seq_elapsed:?} pipelined={pipe_elapsed:?} \
         ratio={ratio:.2}x"
    );
}

/// Heavier, `#[ignore]`d variant at the seeder's actual scale (bn-1mw: 1,488
/// sequential commands) — not part of the default test run, but this is the
/// harness `docs/perf/bulk-writes.md` cites its measured numbers from. Run
/// with:
/// `CLANG_PATH=/usr/bin/clang cargo test -p mess-store --release --test \
/// bulk_write_pipelining -- --ignored --nocapture`
#[tokio::test]
#[ignore]
async fn pipelined_bulk_write_bench_at_seeder_scale() {
    const N: usize = 1488;
    const K: usize = 32;

    let (seq_elapsed, pipe_elapsed) = run_demo(N, K).await;
    let ratio =
        seq_elapsed.as_secs_f64() / pipe_elapsed.as_secs_f64().max(1e-9);

    eprintln!(
        "pipelined_bulk_write_bench_at_seeder_scale: N={N} K={K} \
         sequential={seq_elapsed:?} pipelined={pipe_elapsed:?} (speedup \
         {ratio:.2}x)"
    );

    assert!(
        ratio > 1.3,
        "expected a large speedup at seeder scale; got \
         sequential={seq_elapsed:?} pipelined={pipe_elapsed:?} \
         ratio={ratio:.2}x"
    );
}
