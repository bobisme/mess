//! Integration tests for the fjall-backed metadata tables (bn-1ku).
//!
//! Every test touches the real filesystem and fjall's background threads, so
//! all are `#[cfg_attr(miri, ignore)]`.

use mess_index::meta::{
    CommitGroup, Head, MetaStore, MetaTable, SnapshotHead, StreamId,
};

/// A synthetic committed log record — the minimal shape recovery replays.
#[derive(Clone)]
struct LogRecord {
    global_position: u64,
    stream:          StreamId,
    stream_version:  u64,
    dedupe_key:      Option<Vec<u8>>,
}

/// A deterministic log: `n` records round-robined across `streams` streams,
/// each carrying a distinct dedupe key.
fn synth_log(n: u64, streams: u64) -> Vec<LogRecord> {
    let mut versions = vec![0u64; streams as usize];
    (0..n)
        .map(|p| {
            let s = p % streams;
            let v = versions[s as usize];
            versions[s as usize] += 1;
            LogRecord {
                global_position: p,
                stream:          StreamId(s + 1),
                stream_version:  v,
                dedupe_key:      Some(format!("k{p}").into_bytes()),
            }
        })
        .collect()
}

/// Apply one record as its own commit group (end_position = gp + 1).
fn apply_record(store: &MetaStore, r: &LogRecord) {
    let mut g = CommitGroup::new(r.global_position + 1);
    g.stream_heads.push((
        r.stream,
        Head {
            version:         r.stream_version,
            global_position: r.global_position,
        },
    ));
    if let Some(k) = &r.dedupe_key {
        g.dedupe.push((r.stream, k.clone(), r.global_position));
    }
    store.apply_group(&g).expect("apply group");
}

#[test]
#[cfg_attr(miri, ignore)]
fn roundtrip_all_tables() {
    let dir = tempfile::tempdir().unwrap();
    let store = MetaStore::open(dir.path()).unwrap();

    // stream head
    let mut g = CommitGroup::new(43);
    g.stream_heads
        .push((StreamId(7), Head { version: 4, global_position: 42 }));
    g.snapshot_heads.push((
        StreamId(7),
        SnapshotHead {
            covered_version: 4,
            global_position: 42,
            snapshot_ref:    b"blob-ptr".to_vec(),
        },
    ));
    g.dedupe.push((StreamId(7), b"cmd-1".to_vec(), 42));
    store.apply_group(&g).unwrap();

    assert_eq!(
        store.stream_head(StreamId(7)).unwrap(),
        Some(Head { version: 4, global_position: 42 })
    );
    assert_eq!(store.stream_head(StreamId(9)).unwrap(), None);

    let snap = store.snapshot_head(StreamId(7)).unwrap().unwrap();
    assert_eq!(snap.covered_version, 4);
    assert_eq!(snap.global_position, 42);
    assert_eq!(snap.snapshot_ref, b"blob-ptr");

    assert_eq!(store.dedupe_lookup(StreamId(7), b"cmd-1").unwrap(), Some(42));
    assert_eq!(store.dedupe_lookup(StreamId(7), b"cmd-none").unwrap(), None);

    // checkpoint
    store.set_checkpoint("orders-projection", 100).unwrap();
    assert_eq!(store.checkpoint("orders-projection").unwrap(), Some(100));
    assert_eq!(store.checkpoint("unknown").unwrap(), None);

    // high-water advanced to the group end
    assert_eq!(store.high_water(MetaTable::StreamHeads).unwrap(), 43);
    assert_eq!(store.high_water(MetaTable::Dedupe).unwrap(), 43);
}

/// Acceptance: `rm -rf` the metadata dir -> full rebuild from log yields
/// byte-equal table contents.
#[test]
#[cfg_attr(miri, ignore)]
fn rebuild_is_byte_equal() {
    let log = synth_log(500, 7);

    let dir_a = tempfile::tempdir().unwrap();
    let store_a = MetaStore::open(dir_a.path()).unwrap();
    for r in &log {
        apply_record(&store_a, r);
    }
    let heads_a = store_a.dump(MetaTable::StreamHeads).unwrap();
    let snaps_a = store_a.dump(MetaTable::SnapshotHeads).unwrap();
    let dedupe_a = store_a.dump(MetaTable::Dedupe).unwrap();
    drop(store_a);

    // Fresh directory (the "rm -rf" — a brand-new dir is the same starting
    // point as a deleted one), rebuilt from the identical log.
    let dir_b = tempfile::tempdir().unwrap();
    let store_b = MetaStore::open(dir_b.path()).unwrap();
    for r in &log {
        apply_record(&store_b, r);
    }
    let heads_b = store_b.dump(MetaTable::StreamHeads).unwrap();
    let snaps_b = store_b.dump(MetaTable::SnapshotHeads).unwrap();
    let dedupe_b = store_b.dump(MetaTable::Dedupe).unwrap();

    assert_eq!(heads_a, heads_b, "stream_heads not byte-equal after rebuild");
    assert_eq!(snaps_a, snaps_b, "snapshot_heads not byte-equal after rebuild");
    assert_eq!(
        dedupe_a, dedupe_b,
        "dedupe window not byte-equal after rebuild"
    );
}

/// Acceptance: on reopen the tables lag the log; lag is detected and replaying
/// the gap closes it.
#[test]
#[cfg_attr(miri, ignore)]
fn crash_lag_detected_and_replayed() {
    let dir = tempfile::tempdir().unwrap();
    let log = synth_log(200, 3);
    let log_end = 200u64; // watermark: all 200 positions durable in the log

    // Pre-crash: the tables reached only position 120 (the buffered tail was
    // lost). Persist so the 120-prefix survives the reopen, then drop.
    {
        let store = MetaStore::open(dir.path()).unwrap();
        for r in log.iter().take(120) {
            apply_record(&store, r);
        }
        store.persist().unwrap();
        assert_eq!(store.high_water(MetaTable::StreamHeads).unwrap(), 120);
    }

    // Reopen: the store lags the log.
    let store = MetaStore::open(dir.path()).unwrap();
    assert_eq!(store.high_water(MetaTable::StreamHeads).unwrap(), 120);
    let gap = store.lag(MetaTable::StreamHeads, log_end).unwrap();
    assert_eq!(gap, Some((120, 200)), "expected a detected gap [120,200)");

    // Recovery replays exactly the gap [from, to).
    let (from, to) = gap.unwrap();
    for r in log
        .iter()
        .filter(|r| r.global_position >= from && r.global_position < to)
    {
        apply_record(&store, r);
    }

    // Gap closed, and the recovered tables match a from-scratch full build.
    assert_eq!(store.lag(MetaTable::StreamHeads, log_end).unwrap(), None);
    assert_eq!(store.high_water(MetaTable::StreamHeads).unwrap(), 200);

    let recovered = store.dump(MetaTable::StreamHeads).unwrap();
    let fresh_dir = tempfile::tempdir().unwrap();
    let fresh = MetaStore::open(fresh_dir.path()).unwrap();
    for r in &log {
        apply_record(&fresh, r);
    }
    assert_eq!(recovered, fresh.dump(MetaTable::StreamHeads).unwrap());
}

/// Dedupe: A6 absorb returns the original position; the window is bounded and
/// ages the oldest entries out; seq bounds survive a reopen.
#[test]
#[cfg_attr(miri, ignore)]
fn dedupe_absorb_and_aging() {
    let dir = tempfile::tempdir().unwrap();
    let store = MetaStore::open_with_capacity(dir.path(), 4).unwrap();
    let s = StreamId(1);

    // Ten distinct keys, positions 0..10.
    for p in 0u64..10 {
        let mut g = CommitGroup::new(p + 1);
        g.dedupe.push((s, format!("k{p}").into_bytes(), p));
        store.apply_group(&g).unwrap();
    }

    // Bounded: only the last `capacity` (4) survive.
    assert_eq!(store.dump(MetaTable::Dedupe).unwrap().len(), 4);
    for p in 0u64..6 {
        assert_eq!(
            store.dedupe_lookup(s, format!("k{p}").as_bytes()).unwrap(),
            None,
            "k{p} should have aged out"
        );
    }
    for p in 6u64..10 {
        assert_eq!(
            store.dedupe_lookup(s, format!("k{p}").as_bytes()).unwrap(),
            Some(p),
            "k{p} should be in-window and return its original position"
        );
    }

    // A6 absorb: a writer sees the recent key and returns the original
    // position instead of committing a second time.
    let dup = store.dedupe_lookup(s, b"k9").unwrap();
    assert_eq!(
        dup,
        Some(9),
        "duplicate command absorbs to the original position"
    );

    // Seq bounds survive a reopen: aging still bounds the window after crash.
    drop(store);
    let store = MetaStore::open_with_capacity(dir.path(), 4).unwrap();
    for p in 10u64..13 {
        let mut g = CommitGroup::new(p + 1);
        g.dedupe.push((s, format!("k{p}").into_bytes(), p));
        store.apply_group(&g).unwrap();
    }
    assert_eq!(
        store.dump(MetaTable::Dedupe).unwrap().len(),
        4,
        "still bounded after reopen"
    );
    assert_eq!(store.dedupe_lookup(s, b"k12").unwrap(), Some(12));
    assert_eq!(
        store.dedupe_lookup(s, b"k8").unwrap(),
        None,
        "k8 aged out after reopen inserts"
    );
}

/// Checkpoints have their own lag: a projection's checkpoint value is its
/// high-water, and the gap to the log end is what it must reprocess.
#[test]
#[cfg_attr(miri, ignore)]
fn checkpoint_lag() {
    let dir = tempfile::tempdir().unwrap();
    let store = MetaStore::open(dir.path()).unwrap();

    assert_eq!(store.checkpoint_lag("proj", 50).unwrap(), Some((0, 50)));
    store.set_checkpoint("proj", 30).unwrap();
    assert_eq!(store.checkpoint_lag("proj", 50).unwrap(), Some((30, 50)));
    store.set_checkpoint("proj", 50).unwrap();
    assert_eq!(store.checkpoint_lag("proj", 50).unwrap(), None);
}
