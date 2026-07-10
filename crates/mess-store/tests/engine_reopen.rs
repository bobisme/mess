//! bn-20b — the reviewer's crash-recovery probe, as a regression test.
//!
//! A GENUINE fresh [`LogEngine::open`] over a populated directory (drop the
//! engine, release the D9 `StoreLock`, open the same dir again — NO shared
//! `Arc`) must return the EXACT pre-crash data through every read path, then
//! keep appending densely. Before bn-20b, `recover` rebuilt only the active
//! index and never repopulated the record book, so a fresh open returned
//! silent-empty reads and the next append tripped the dense-position invariant
//! (a `Book::record` panic in release). This test pins the fix: payloads are
//! rehydrated from the durable log via `mess-log`'s frame-decode API, and names
//! from the persisted interner tables — across MULTIPLE reopen cycles, which
//! also exercises the resume-in-place segment writer.

use mess_store::backend::{Backend, RecordToAppend};
use mess_store::{LogEngine, Version};

fn rec(message_type: &str, data: &[u8]) -> RecordToAppend {
    RecordToAppend { message_type: message_type.to_string(), data: data.to_vec() }
}

/// Assert the three read paths agree with the expected `(stream, type, data,
/// stream_pos, global_pos)` tuples for the whole global order.
async fn assert_global(engine: &LogEngine, expected: &[(&str, &str, Vec<u8>, u64, u64)]) {
    let g = engine.read_global(None, 1000).await.unwrap();
    assert_eq!(g.len(), expected.len(), "global event count");
    for (got, (stream, ty, data, sp, gp)) in g.iter().zip(expected) {
        assert_eq!(&got.stream_id, stream, "global gp={gp} stream");
        assert_eq!(&got.message_type, ty, "global gp={gp} type");
        assert_eq!(&got.data, data, "global gp={gp} data");
        assert_eq!(got.stream_position, *sp, "global gp={gp} stream_position");
        assert_eq!(got.global_position, *gp, "global gp={gp} global_position");
    }
}

#[tokio::test]
async fn genuine_reopen_returns_exact_pre_crash_data_then_continues() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store_path = dir.path().join("store");

    // ---- Phase 1: write a few events across two streams, then "crash". ----
    {
        let engine = LogEngine::open(&store_path).expect("open fresh");
        engine
            .append_batch(
                "acct-1",
                Version::NoStream,
                &[rec("account.opened", b"alice"), rec("account.deposited", &10i64.to_le_bytes())],
            )
            .await
            .expect("append acct-1 b0");
        engine
            .append_batch("acct-2", Version::NoStream, &[rec("account.opened", b"bob")])
            .await
            .expect("append acct-2 b0");
        engine
            .append_batch("acct-1", Version::At(1), &[rec("account.withdrawn", &3i64.to_le_bytes())])
            .await
            .expect("append acct-1 b1");
        // Drop the engine: releases the StoreLock and the in-process book.
    }

    let pre_crash: Vec<(&str, &str, Vec<u8>, u64, u64)> = vec![
        ("acct-1", "account.opened", b"alice".to_vec(), 0, 0),
        ("acct-1", "account.deposited", 10i64.to_le_bytes().to_vec(), 1, 1),
        ("acct-2", "account.opened", b"bob".to_vec(), 0, 2),
        ("acct-1", "account.withdrawn", 3i64.to_le_bytes().to_vec(), 2, 3),
    ];

    // ---- Phase 2: GENUINE fresh open over the populated dir. ----
    let engine = LogEngine::open(&store_path).expect("reopen 1");

    // head: exactly the pre-crash heads, not NoStream.
    assert_eq!(engine.head("acct-1").await.unwrap(), Version::At(2), "acct-1 head survives");
    assert_eq!(engine.head("acct-2").await.unwrap(), Version::At(0), "acct-2 head survives");
    assert_eq!(engine.head("never-seen").await.unwrap(), Version::NoStream);

    // read_stream: exact payloads + names, not silent-empty.
    let s1 = engine.read_stream("acct-1", Version::NoStream, 100).await.unwrap();
    assert_eq!(s1.len(), 3);
    assert_eq!(s1[0].message_type, "account.opened");
    assert_eq!(s1[0].data, b"alice");
    assert_eq!(s1[0].stream_id, "acct-1");
    assert_eq!(s1[2].message_type, "account.withdrawn");
    assert_eq!(s1[2].data, 3i64.to_le_bytes());
    assert_eq!(s1[2].global_position, 3);

    // read_global: the whole durable order, exact.
    assert_global(&engine, &pre_crash).await;

    // ---- Phase 3: keep appending post-reopen (dense-position invariant). ----
    engine
        .append_batch("acct-2", Version::At(0), &[rec("account.deposited", &50i64.to_le_bytes())])
        .await
        .expect("append acct-2 after reopen");
    engine
        .append_batch("acct-1", Version::At(2), &[rec("account.deposited", &7i64.to_le_bytes())])
        .await
        .expect("append acct-1 after reopen");

    let mut all = pre_crash.clone();
    all.push(("acct-2", "account.deposited", 50i64.to_le_bytes().to_vec(), 1, 4));
    all.push(("acct-1", "account.deposited", 7i64.to_le_bytes().to_vec(), 3, 5));

    assert_eq!(engine.head("acct-2").await.unwrap(), Version::At(1));
    assert_eq!(engine.head("acct-1").await.unwrap(), Version::At(3));
    assert_global(&engine, &all).await;

    // ---- Phase 4: a SECOND reopen — proves resume-in-place is durable across
    // cycles (the post-reopen appends became part of the one contiguous log). ----
    drop(engine);
    let engine = LogEngine::open(&store_path).expect("reopen 2");
    assert_eq!(engine.head("acct-1").await.unwrap(), Version::At(3));
    assert_eq!(engine.head("acct-2").await.unwrap(), Version::At(1));
    assert_global(&engine, &all).await;

    // And it still appends densely after the second reopen.
    engine
        .append_batch("acct-2", Version::At(1), &[rec("account.withdrawn", &5i64.to_le_bytes())])
        .await
        .expect("append after second reopen");
    all.push(("acct-2", "account.withdrawn", 5i64.to_le_bytes().to_vec(), 2, 6));
    assert_global(&engine, &all).await;
    let s2 = engine.read_stream("acct-2", Version::At(0), 100).await.unwrap();
    // After version 0: deposited(50) then withdrawn(5).
    assert_eq!(s2.len(), 2);
    assert_eq!(s2[0].data, 50i64.to_le_bytes());
    assert_eq!(s2[1].data, 5i64.to_le_bytes());
    assert_eq!(s2[1].global_position, 6);
}

/// bn-1vu — the sealed tier must survive a restart.
///
/// A stream sealed into the cold tier before a "crash" (drop the engine,
/// release the `StoreLock`, reopen the same dir with NO shared `Arc`) must, on
/// reopen, be served from the reloaded sealed sidecars — not silently fall back
/// to hot replay because the `SealedStore` started empty. Before bn-1vu, `open`
/// always started with an empty `SealedStore`, so `sealed_segment_count()` was
/// 0 after any restart. This pins the reload: the durable `.pidx` (plus its
/// `.pcol`/`.filter` siblings) is reopened and installed, `read_stream` routes
/// through the cold tier, and the payloads come back byte-exact.
#[tokio::test]
async fn reopen_loads_sealed_sidecars_and_serves_from_sealed_tier() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store_path = dir.path().join("store");

    {
        let engine = LogEngine::open(&store_path).expect("open fresh");
        engine
            .append_batch(
                "acct-1",
                Version::NoStream,
                &[rec("account.opened", b"alice"), rec("account.deposited", &10i64.to_le_bytes())],
            )
            .await
            .expect("append b0");
        engine
            .append_batch("acct-1", Version::At(1), &[rec("account.withdrawn", &3i64.to_le_bytes())])
            .await
            .expect("append b1");
        // Seal the active segment into the cold tier, writing durable sidecars.
        engine.seal_active().expect("seal_active");
        assert!(engine.sealed_segment_count() > 0, "sealed at runtime");
        // Drop: release the StoreLock and every in-process handle.
    }

    // GENUINE fresh reopen: the sealed tier must be repopulated from the
    // durable `.pidx` on disk, not start empty.
    let engine = LogEngine::open(&store_path).expect("reopen");
    assert!(
        engine.sealed_segment_count() > 0,
        "sealed tier must be reloaded from durable sidecars on reopen"
    );

    // Reads route through the sealed tier (its stream is in the sealed store)
    // and return the exact pre-crash data.
    let s = engine.read_stream("acct-1", Version::NoStream, 100).await.unwrap();
    assert_eq!(s.len(), 3, "all sealed events readable after restart");
    assert_eq!(s[0].message_type, "account.opened");
    assert_eq!(s[0].data, b"alice");
    assert_eq!(s[0].stream_id, "acct-1");
    assert_eq!(s[2].message_type, "account.withdrawn");
    assert_eq!(s[2].data, 3i64.to_le_bytes());
    assert_eq!(s[2].global_position, 2);
    assert_eq!(engine.head("acct-1").await.unwrap(), Version::At(2), "head survives");

    // read_global still returns the whole durable order.
    let expected: Vec<(&str, &str, Vec<u8>, u64, u64)> = vec![
        ("acct-1", "account.opened", b"alice".to_vec(), 0, 0),
        ("acct-1", "account.deposited", 10i64.to_le_bytes().to_vec(), 1, 1),
        ("acct-1", "account.withdrawn", 3i64.to_le_bytes().to_vec(), 2, 2),
    ];
    assert_global(&engine, &expected).await;
}

/// bn-1vu — a crash mid-seal must leave a recoverable state.
///
/// The sidecar writer is crash-atomic (temp file → fsync → rename), so a crash
/// during a seal leaves either the previous state or a complete `.pidx`, never
/// a torn one under its real name. This test simulates the two crash shapes the
/// reopen path must tolerate — a torn `*.pidx.tmp` husk left before the rename,
/// and a truncated/garbage `.pidx` that fails its CRC — and asserts the engine
/// still reopens, installs neither, and serves every event from the durable log
/// (the hot tier), losing nothing.
#[tokio::test]
async fn crash_mid_seal_leaves_recoverable_state_served_from_log() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store_path = dir.path().join("store");

    {
        let engine = LogEngine::open(&store_path).expect("open fresh");
        engine
            .append_batch("acct-1", Version::NoStream, &[rec("account.opened", b"alice")])
            .await
            .expect("append b0");
        engine
            .append_batch("acct-1", Version::At(0), &[rec("account.deposited", &10i64.to_le_bytes())])
            .await
            .expect("append b1");
        // Do NOT complete a seal. Fabricate the two partial-seal crash shapes
        // the atomic writer can leave behind:
        let sealed = store_path.join("sealed");
        std::fs::create_dir_all(&sealed).unwrap();
        // 1) a torn temp husk written before the atomic rename (no real .pidx).
        std::fs::write(
            sealed.join("seg-00000000000000000001.pidx.tmp"),
            b"torn-partial-sidecar-never-renamed",
        )
        .unwrap();
        // 2) a garbage/truncated .pidx that fails its content CRC.
        std::fs::write(
            sealed.join("seg-00000000000000000002.pidx"),
            b"not a real sidecar - fails magic/CRC",
        )
        .unwrap();
    }

    // Reopen must succeed, install neither partial artifact, and serve reads
    // from the durable log.
    let engine = LogEngine::open(&store_path).expect("reopen after crash mid-seal");
    assert_eq!(
        engine.sealed_segment_count(),
        0,
        "no torn/corrupt sidecar may be installed into the sealed tier"
    );
    let s = engine.read_stream("acct-1", Version::NoStream, 100).await.unwrap();
    assert_eq!(s.len(), 2, "every event still served from the log");
    assert_eq!(s[0].data, b"alice");
    assert_eq!(s[1].data, 10i64.to_le_bytes());
    assert_eq!(engine.head("acct-1").await.unwrap(), Version::At(1));
}

/// bn-20b measurement: engine open-with-rehydration wall time over a populated
/// single-segment corpus. Writes a realistic corpus, drops the engine, and
/// times a fresh `LogEngine::open` (which scans the durable segment, decodes
/// every frame's payload, and rebuilds the record book + active index + the
/// interner from the meta name tables). `#[ignore]`d — run explicitly to
/// refresh the `docs/perf/envelope.md` row. Prints events, wall time, ev/s.
#[tokio::test]
#[ignore = "measurement: run with --release --ignored --nocapture to refresh the envelope"]
async fn measure_open_with_rehydration_wall_time() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store_path = dir.path().join("store");

    const STREAMS: usize = 200;
    const BATCHES_PER_STREAM: u64 = 100;
    const EVENTS_PER_BATCH: usize = 50;
    let total_events = STREAMS as u64 * BATCHES_PER_STREAM * EVENTS_PER_BATCH as u64;

    {
        let engine = LogEngine::open(&store_path).expect("open fresh");
        for s in 0..STREAMS {
            let stream = format!("stream-{s}");
            let mut expected = Version::NoStream;
            for b in 0..BATCHES_PER_STREAM {
                let recs: Vec<RecordToAppend> = (0..EVENTS_PER_BATCH)
                    .map(|i| rec("evt.t", &(b * 100 + i as u64).to_le_bytes()))
                    .collect();
                let out = engine.append_batch(&stream, expected, &recs).await.unwrap();
                expected = out.version;
            }
        }
    }

    let t0 = std::time::Instant::now();
    let engine = LogEngine::open(&store_path).expect("reopen");
    let elapsed = t0.elapsed();
    assert_eq!(engine.total_events(), total_events as usize, "rehydrated every event");

    let ev_per_s = total_events as f64 / elapsed.as_secs_f64();
    println!(
        "engine open-with-rehydration: {total_events} events in {:.4} s ({:.0} ev/s)",
        elapsed.as_secs_f64(),
        ev_per_s
    );
}
