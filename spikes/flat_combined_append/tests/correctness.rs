//! Spike B (bn-28g) correctness gates for `FlatEngine` (B0 and B1):
//!
//! 1. same-stream concurrent `Exact(v)` races → exactly one success, one
//!    version conflict;
//! 2. cross-stream concurrency → dense global positions (exact tiling), and
//!    watermark == total committed events;
//! 3. dropped caller futures mid-flight → no position gaps, committed
//!    appends still visible after the watermark advances, engine not wedged;
//! 4. differential oracle: an identical randomized workload through
//!    `LogEngine` and `FlatEngine` yields identical accept/conflict outcomes
//!    and identical per-stream head sequences;
//! 5. reopen: after FlatEngine writes + crash-free shutdown, a standard
//!    recovery scan of the segment reproduces exactly the shadow heads;
//! 6. the byte-bounded ring demonstrably blocks producers (queue memory
//!    bounded gate).

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;

use flat_combined_append::{
    AppendError, FlatConfig, FlatEngine, RecordToAppend, Variant, Version,
};
use mess_log::committer::Durability;
use mess_log::runtime::Runtime;
use mess_log::scanner::recover_segment;
use mess_store::backend::Backend;
use mess_store::{EngineOptions, LogEngine};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

fn rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap()
}

fn recs(n: usize, tag: u8) -> Vec<RecordToAppend> {
    (0..n)
        .map(|i| RecordToAppend {
            message_type: "ev.t".to_string(),
            data:         vec![tag, i as u8, 7, 7, 7, 7, 7, 7],
        })
        .collect()
}

fn open_flat(dir: &std::path::Path, variant: Variant) -> FlatEngine {
    FlatEngine::open(
        dir,
        FlatConfig { variant, ..Default::default() },
    )
    .expect("open FlatEngine")
}

// ---------------------------------------------------------------------------
// 1. same-stream Exact(v) races
// ---------------------------------------------------------------------------

fn exact_version_race(variant: Variant) {
    let scratch = mess_testkit::sweeping_temp_dir("flat-race");
    let engine = Arc::new(open_flat(scratch.path(), variant));
    let rt = rt();
    rt.block_on(async {
        for round in 0..100 {
            let stream = format!("race-{round}");
            let a = {
                let e = Arc::clone(&engine);
                let s = stream.clone();
                tokio::spawn(async move {
                    e.append_batch(&s, Version::NoStream, &recs(3, 1)).await
                })
            };
            let b = {
                let e = Arc::clone(&engine);
                let s = stream.clone();
                tokio::spawn(async move {
                    e.append_batch(&s, Version::NoStream, &recs(3, 2)).await
                })
            };
            let ra = a.await.unwrap();
            let rb = b.await.unwrap();
            let oks = [&ra, &rb].iter().filter(|r| r.is_ok()).count();
            assert_eq!(oks, 1, "round {round}: exactly one Exact(v) winner");
            let loser = if ra.is_ok() { rb } else { ra };
            match loser {
                Err(AppendError::Conflict { expected, actual }) => {
                    assert_eq!(expected, Version::NoStream);
                    assert_eq!(actual, Version::At(2), "loser saw winner's head");
                }
                other => panic!("loser must be a version conflict, got {other:?}"),
            }
        }
    });
    let exit = Arc::into_inner(engine).unwrap().close();
    assert_eq!(exit.stats.conflicts, 100);
    assert_eq!(exit.stats.accepted_batches, 100);
    assert_eq!(exit.stats.position_mismatches, 0);
}

#[test]
fn exact_version_race_one_winner_b0() { exact_version_race(Variant::B0); }

#[test]
fn exact_version_race_one_winner_b1() { exact_version_race(Variant::B1); }

// ---------------------------------------------------------------------------
// 2. cross-stream dense global positions + watermark
// ---------------------------------------------------------------------------

fn dense_positions(variant: Variant) {
    const WRITERS: u64 = 8;
    const APPENDS: u64 = 100;
    const EVENTS: usize = 5;
    let scratch = mess_testkit::sweeping_temp_dir("flat-dense");
    let engine = Arc::new(open_flat(scratch.path(), variant));
    let rt = rt();
    let ranges: Vec<(u64, u64)> = rt.block_on(async {
        let mut joins = Vec::new();
        for w in 0..WRITERS {
            let e = Arc::clone(&engine);
            joins.push(tokio::spawn(async move {
                let stream = format!("s{w}");
                let mut expected = Version::NoStream;
                let mut out = Vec::new();
                for _ in 0..APPENDS {
                    let r = e
                        .append_batch(&stream, expected, &recs(EVENTS, w as u8))
                        .await
                        .expect("append");
                    expected = r.version;
                    let last = r.last_global_position;
                    out.push((last + 1 - EVENTS as u64, last));
                }
                out
            }));
        }
        let mut all = Vec::new();
        for j in joins {
            all.extend(j.await.unwrap());
        }
        all
    });
    let total = WRITERS * APPENDS * EVENTS as u64;
    assert_eq!(
        engine.watermark(),
        total,
        "watermark == total committed events"
    );
    // Exact tiling: sort ranges, assert consecutive coverage of [0, total).
    let mut sorted = ranges.clone();
    sorted.sort_unstable();
    let mut next = 0u64;
    for (first, last) in sorted {
        assert_eq!(first, next, "dense global positions, no gaps/overlap");
        next = last + 1;
    }
    assert_eq!(next, total);
    let exit = Arc::into_inner(engine).unwrap().close();
    assert_eq!(exit.next_global, total);
    assert_eq!(exit.stats.position_mismatches, 0);
}

#[test]
fn cross_stream_dense_positions_b0() { dense_positions(Variant::B0); }

#[test]
fn cross_stream_dense_positions_b1() { dense_positions(Variant::B1); }

// ---------------------------------------------------------------------------
// 3. dropped futures mid-flight
// ---------------------------------------------------------------------------

fn dropped_futures(variant: Variant) {
    const TASKS: u64 = 200;
    const EVENTS: usize = 5;
    let scratch = mess_testkit::sweeping_temp_dir("flat-drop");
    // Group durability: a real (multi-ms on ext4) barrier widens the window
    // in which a caller can vanish while its intent is in flight.
    let engine = Arc::new(
        FlatEngine::open(
            scratch.path(),
            FlatConfig {
                variant,
                durability: Durability::group_default(),
                ..Default::default()
            },
        )
        .expect("open"),
    );
    let rt = rt();
    rt.block_on(async {
        let mut handles = Vec::new();
        for t in 0..TASKS {
            let e = Arc::clone(&engine);
            handles.push(tokio::spawn(async move {
                let stream = format!("d{t}");
                e.append_batch(&stream, Version::NoStream, &recs(EVENTS, 3))
                    .await
            }));
        }
        // Abort every other caller "mid-flight": some intents are not yet
        // sent, some are queued, some are already submitted to the durable
        // committer. The owner must run every accepted intent to a terminal
        // state regardless.
        for (i, h) in handles.iter().enumerate() {
            if i % 2 == 0 {
                h.abort();
            }
        }
        for (i, h) in handles.into_iter().enumerate() {
            let joined = h.await;
            if i % 2 == 1 {
                joined.expect("surviving caller").expect("append acked");
            }
        }
        // The engine must not be wedged: a sentinel append still commits.
        engine
            .append_batch("sentinel", Version::NoStream, &recs(1, 9))
            .await
            .expect("sentinel append");
    });
    let watermark = engine.watermark();
    let exit = Arc::into_inner(engine).unwrap().close();
    assert_eq!(exit.stats.position_mismatches, 0);
    assert_eq!(
        exit.next_global, watermark,
        "everything accepted was published (owner drained before close)"
    );

    // Recover with the standard scanner: the committed prefix must tile
    // exactly to the shadow next_global — no gaps from dropped callers —
    // and every committed batch is whole (head == 4 for its 5 events).
    let mrt = mess_log::runtime::RealRuntime::new();
    let recovery = recover_segment(
        &mrt.fs(),
        &scratch.path().join("flat-seg-1.log"),
    )
    .expect("recover");
    assert_eq!(recovery.next_pos, exit.next_global, "no position gaps");
    for (&sid, &head) in &recovery.stream_heads {
        let name = exit
            .stream_ids
            .iter()
            .find(|&(_, &id)| id == sid)
            .map(|(n, _)| n.clone())
            .unwrap();
        if name == "sentinel" {
            assert_eq!(head, 0);
        } else {
            assert_eq!(head, EVENTS as u64 - 1, "committed batch {name} is whole");
        }
    }
    // Dropped-but-committed appends are visible: recovered heads match the
    // shadow heads exactly (a batch either fully committed or never wrote).
    let shadow: BTreeMap<u64, u64> =
        exit.heads.iter().map(|(&k, &v)| (k, v)).collect();
    assert_eq!(recovery.stream_heads, shadow);
}

#[test]
fn dropped_futures_no_gaps_b0() { dropped_futures(Variant::B0); }

#[test]
fn dropped_futures_no_gaps_b1() { dropped_futures(Variant::B1); }

// ---------------------------------------------------------------------------
// 4. differential oracle vs the current engine
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq)]
enum Outcome {
    Ok { version: Version, last_global: u64 },
    Conflict { expected: Version, actual: Version },
}

fn oracle(variant: Variant) {
    const STREAMS: usize = 8;
    const OPS: usize = 600;
    let scratch = mess_testkit::sweeping_temp_dir("flat-oracle");
    let flat = open_flat(&scratch.path().join("flat"), variant);
    let log = LogEngine::open_with(
        scratch.path().join("log"),
        EngineOptions {
            durability: Durability::Process,
            ..Default::default()
        },
    )
    .expect("open LogEngine");

    // One deterministic op sequence, applied sequentially to both engines.
    let mut rng = StdRng::seed_from_u64(0x2867);
    let rt = rt();
    let (flat_out, log_out, op_streams) = rt.block_on(async {
        let mut heads: Vec<Version> = vec![Version::NoStream; STREAMS];
        let mut flat_out: Vec<Outcome> = Vec::with_capacity(OPS);
        let mut log_out: Vec<Outcome> = Vec::with_capacity(OPS);
        let mut op_streams: Vec<usize> = Vec::with_capacity(OPS);
        for op in 0..OPS {
            let s = rng.gen_range(0..STREAMS);
            op_streams.push(s);
            let stream = format!("o{s}");
            let roll: f64 = rng.r#gen();
            let expected = if roll < 0.6 {
                heads[s] // correct
            } else if roll < 0.8 {
                // stale/wrong: NoStream or one behind
                match heads[s] {
                    Version::NoStream => Version::At(3),
                    Version::At(0) => Version::NoStream,
                    Version::At(n) => Version::At(n - 1),
                }
            } else if roll < 0.9 {
                // future
                Version::At(heads[s].next_position() + 4)
            } else {
                heads[s] // correct, but empty batch below
            };
            let n = if roll >= 0.9 { 0 } else { rng.gen_range(1..=6) };
            let records = recs(n, op as u8);

            let f = flat.append_batch(&stream, expected, &records).await;
            let l = log.append_batch(&stream, expected, &records).await;
            let fo = match f {
                Ok(a) => Outcome::Ok {
                    version:     a.version,
                    last_global: a.last_global_position,
                },
                Err(AppendError::Conflict { expected, actual }) => {
                    Outcome::Conflict { expected, actual }
                }
                Err(e) => panic!("flat backend error: {e:?}"),
            };
            let lo = match l {
                Ok(a) => Outcome::Ok {
                    version:     a.version,
                    last_global: a.last_global_position,
                },
                Err(mess_store::AppendError::Conflict { expected, actual }) => {
                    Outcome::Conflict { expected, actual }
                }
                Err(e) => panic!("log backend error: {e:?}"),
            };
            assert_eq!(fo, lo, "op {op}: engines disagreed");
            if let Outcome::Ok { version, .. } = fo
                && n > 0
            {
                heads[s] = version;
            }
            flat_out.push(fo);
            log_out.push(lo);
        }
        (flat_out, log_out, op_streams)
    });
    assert_eq!(flat_out, log_out);

    // Per-stream head sequences match (already implied by per-op equality;
    // asserted explicitly per the spike brief).
    let mut flat_heads: HashMap<usize, Vec<Version>> = HashMap::new();
    for (i, o) in flat_out.iter().enumerate() {
        if let Outcome::Ok { version, .. } = o {
            flat_heads.entry(op_streams[i]).or_default().push(*version);
        }
    }
    let mut log_heads: HashMap<usize, Vec<Version>> = HashMap::new();
    for (i, o) in log_out.iter().enumerate() {
        if let Outcome::Ok { version, .. } = o {
            log_heads.entry(op_streams[i]).or_default().push(*version);
        }
    }
    assert_eq!(flat_heads, log_heads);

    let exit = flat.close();
    assert_eq!(exit.stats.position_mismatches, 0);
    drop(log);
}

#[test]
fn differential_oracle_b0() { oracle(Variant::B0); }

#[test]
fn differential_oracle_b1() { oracle(Variant::B1); }

// ---------------------------------------------------------------------------
// 5. reopen: standard recovery scan == shadow state
// ---------------------------------------------------------------------------

fn reopen(variant: Variant) {
    let scratch = mess_testkit::sweeping_temp_dir("flat-reopen");
    let engine = Arc::new(open_flat(scratch.path(), variant));
    let rt = rt();
    rt.block_on(async {
        let mut joins = Vec::new();
        for w in 0..6u64 {
            let e = Arc::clone(&engine);
            joins.push(tokio::spawn(async move {
                let stream = format!("r{w}");
                let mut expected = Version::NoStream;
                for i in 0..40u64 {
                    let r = e
                        .append_batch(
                            &stream,
                            expected,
                            &recs(1 + (i as usize % 4), w as u8),
                        )
                        .await
                        .expect("append");
                    expected = r.version;
                }
                // One deliberate conflict + one empty batch per stream:
                // both must write NOTHING.
                let c = e
                    .append_batch(&stream, Version::NoStream, &recs(2, 0xEE))
                    .await;
                assert!(matches!(c, Err(AppendError::Conflict { .. })));
                e.append_batch(&stream, expected, &[]).await.expect("empty ok");
            }));
        }
        for j in joins {
            j.await.unwrap();
        }
    });
    let exit = Arc::into_inner(engine).unwrap().close();
    assert_eq!(exit.stats.position_mismatches, 0);
    assert_eq!(exit.stats.conflicts, 6);
    assert_eq!(exit.stats.empties, 6);

    let mrt = mess_log::runtime::RealRuntime::new();
    let recovery = recover_segment(
        &mrt.fs(),
        &scratch.path().join("flat-seg-1.log"),
    )
    .expect("recover");
    let shadow: BTreeMap<u64, u64> =
        exit.heads.iter().map(|(&k, &v)| (k, v)).collect();
    assert_eq!(recovery.stream_heads, shadow, "recovered heads == shadow");
    assert_eq!(recovery.next_pos, exit.next_global);
    assert_eq!(
        recovery.accepted.len() as u64,
        exit.stats.accepted_batches,
        "conflicts and empty batches wrote nothing"
    );
}

#[test]
fn reopen_matches_shadow_b0() { reopen(Variant::B0); }

#[test]
fn reopen_matches_shadow_b1() { reopen(Variant::B1); }

// ---------------------------------------------------------------------------
// 6. byte-bounded ring blocks producers
// ---------------------------------------------------------------------------

#[test]
fn byte_bounded_ring_blocks_producers() {
    let scratch = mess_testkit::sweeping_temp_dir("flat-bound");
    // Ring: 4 KiB of byte budget. Each intent below costs ~1.8 KiB, so two
    // fit and the third MUST wait until one reaches a terminal state. The
    // owner is stalled 80 ms per group (test-only knob) so the window is
    // deterministic, not a race.
    let engine = Arc::new(
        FlatEngine::open(
            scratch.path(),
            FlatConfig {
                ring_bytes: 4096,
                group_stall: Some(Duration::from_millis(80)),
                ..Default::default()
            },
        )
        .expect("open"),
    );
    let rt = rt();
    rt.block_on(async {
        let big = vec![RecordToAppend {
            message_type: "ev.t".to_string(),
            data:         vec![0u8; 1600],
        }];
        let t1 = {
            let e = Arc::clone(&engine);
            let b = big.clone();
            tokio::spawn(async move {
                e.append_batch("q1", Version::NoStream, &b).await
            })
        };
        let t2 = {
            let e = Arc::clone(&engine);
            let b = big.clone();
            tokio::spawn(async move {
                e.append_batch("q2", Version::NoStream, &b).await
            })
        };
        // Give t1/t2 time to be admitted (their permits are held until
        // their intents complete, ~80–160 ms away behind the stall).
        tokio::time::sleep(Duration::from_millis(20)).await;

        let f3 = engine.append_batch("q3", Version::NoStream, &big);
        tokio::pin!(f3);
        // The third producer must be BLOCKED awaiting ring space now.
        tokio::select! {
            _ = &mut f3 => panic!("third producer was admitted past the byte bound"),
            _ = tokio::time::sleep(Duration::from_millis(30)) => {}
        }
        // …and must complete once the owner drains the ring.
        f3.await.expect("third append eventually admitted + acked");
        t1.await.unwrap().expect("t1");
        t2.await.unwrap().expect("t2");
    });
    let exit = Arc::into_inner(engine).unwrap().close();
    assert_eq!(exit.stats.accepted_batches, 3);
    assert_eq!(exit.next_global, 3);
}

// ---------------------------------------------------------------------------
// B0Direct (owner owns the writer, design.md §6.3) — same gates
// ---------------------------------------------------------------------------

#[test]
fn exact_version_race_one_winner_b0d() { exact_version_race(Variant::B0Direct); }

#[test]
fn cross_stream_dense_positions_b0d() { dense_positions(Variant::B0Direct); }

#[test]
fn dropped_futures_no_gaps_b0d() { dropped_futures(Variant::B0Direct); }

#[test]
fn differential_oracle_b0d() { oracle(Variant::B0Direct); }

#[test]
fn reopen_matches_shadow_b0d() { reopen(Variant::B0Direct); }
