//! Negative tests: every tampering / mismatch scenario must be DETECTED.

use fold_cert::*;

fn build_stream(id: &str, n: u64) -> Stream {
    let mut s = Stream::new(id);
    for i in 0..n {
        let tag = (i % 3 == 2) as u8;
        s.append(encode_event(tag, 10 + i, 32));
    }
    s
}

// 1. Corrupted snapshot blob -> state_hash mismatch.
#[test]
fn corrupted_snapshot_blob_detected() {
    let stream = build_stream("neg-blob", 30);
    let mut store = SnapshotStore::new();
    store.take_snapshot::<Account>(&stream, 19);
    store.get_mut("neg-blob").unwrap().1[3] ^= 0xff; // flip a byte in the blob

    let err = load_verified::<Account>(&stream, &mut store).unwrap_err();
    assert_eq!(err, VerifyError::StateHashMismatch);
}

// 2. Snapshot claiming the wrong version -> prefix hash mismatch.
#[test]
fn snapshot_claiming_wrong_version_detected() {
    let stream = build_stream("neg-ver", 30);
    let mut store = SnapshotStore::new();
    store.take_snapshot::<Account>(&stream, 19);
    // Lie about the version: state/hash are for v=19 but ref claims v=25.
    store.get_mut("neg-ver").unwrap().0.stream_version = 25;

    let err = load_verified::<Account>(&stream, &mut store).unwrap_err();
    // Path A recomputes h[25] from frame 25 and it won't equal the stored
    // h[19].
    assert_eq!(err, VerifyError::PrefixHashMismatch { path: VerifyPath::FromFrameV });
}

// 2b. Wrong version detected by Path B when frame v is unavailable to Path A.
#[test]
fn wrong_version_detected_by_path_b() {
    let stream = build_stream("neg-ver-b", 30);
    let mut store = SnapshotStore::new();
    let mut r = store.take_snapshot::<Account>(&stream, 19).clone();
    r.stream_version = 25;
    // Consult Path B directly: frame 26's prev_stream_hash is h[25] != h[19].
    assert_eq!(
        stream.frame(26).unwrap().prev_stream_hash == r.event_prefix_hash,
        false,
        "Path B must reject h[19] presented as h[25]"
    );
}

// 3a. Tampered historical event in the TAIL -> detected during tail replay.
#[test]
fn tampered_tail_event_detected_during_tail_replay() {
    let mut stream = build_stream("neg-tamper-tail", 50);
    let mut store = SnapshotStore::new();
    store.take_snapshot::<Account>(&stream, 19);

    // Tamper payload of frame 35 (in the tail 20..=49). Attacker rewrites the
    // amount but cannot recompute the downstream chain without detection.
    stream.frames[35].payload[1] ^= 0xff;

    let err = load_verified::<Account>(&stream, &mut store).unwrap_err();
    // Recomputed frame_hash no longer matches the stored one -> localized to
    // the exact tampered frame.
    assert_eq!(err, VerifyError::ChainBreakFrameHash { at_version: 35 });
}

// 3b. Tamperer ALSO fixes up the stored frame_hash -> the chain still breaks
// at the next frame's prev_stream_hash (or at the head anchor if last).
#[test]
fn tampered_tail_event_with_fixed_frame_hash_detected_at_next_link() {
    let mut stream = build_stream("neg-tamper-fixup", 50);
    let mut store = SnapshotStore::new();
    store.take_snapshot::<Account>(&stream, 19);

    stream.frames[35].payload[1] ^= 0xff;
    stream.frames[35].frame_hash = frame_hash(&stream.frames[35].payload, 35);

    let err = load_verified::<Account>(&stream, &mut store).unwrap_err();
    assert_eq!(err, VerifyError::ChainBreakPrev { at_version: 36 });
}

// 3c. Tampered event in the last frame, frame_hash fixed up -> only the head
// anchor catches it. This is why the trusted head record is load-bearing.
#[test]
fn tampered_last_event_caught_only_by_head_anchor() {
    let mut stream = build_stream("neg-tamper-last", 50);
    let mut store = SnapshotStore::new();
    store.take_snapshot::<Account>(&stream, 19);

    stream.frames[49].payload[1] ^= 0xff;
    stream.frames[49].frame_hash = frame_hash(&stream.frames[49].payload, 49);

    let err = load_verified::<Account>(&stream, &mut store).unwrap_err();
    assert!(matches!(err, VerifyError::HeadMismatch { .. }));
}

// 3d. Tampered event BEFORE the snapshot point -> invisible to snapshot load
// (that is the point of snapshots: the prefix is not re-read), but MUST be
// detected by full-replay verification.
#[test]
fn tampered_prefix_event_detected_by_full_replay() {
    let mut stream = build_stream("neg-tamper-prefix", 50);
    let mut store = SnapshotStore::new();
    store.take_snapshot::<Account>(&stream, 19);

    stream.frames[5].payload[1] ^= 0xff;

    // Snapshot load does NOT touch frame 5 -> passes. Document, don't hide.
    let out = load_verified::<Account>(&stream, &mut store).unwrap();
    assert!(!out.rebuilt_by_replay);

    // Full verified replay from genesis catches it at the tamper point.
    let err = full_replay_verified::<Account>(&stream).unwrap_err();
    assert_eq!(err, VerifyError::ChainBreakFrameHash { at_version: 5 });
}

// 4. fold_version mismatch -> snapshot invalidated + rebuilt by replay.
#[test]
fn fold_version_mismatch_invalidates_and_rebuilds() {
    let stream = build_stream("neg-foldver", 40);
    let mut store = SnapshotStore::new();
    store.take_snapshot::<Account>(&stream, 19);
    // Simulate a snapshot written by older fold logic.
    store.get_mut("neg-foldver").unwrap().0.fold_version = Account::FOLD_VERSION - 1;

    let out = load_verified::<Account>(&stream, &mut store).unwrap();
    assert!(out.rebuilt_by_replay, "must rebuild by replay, not trust the stale fold");
    assert_eq!(out.tail_len, 40);
    assert!(store.get("neg-foldver").is_none(), "stale snapshot must be invalidated");
    assert_eq!(out.state, full_replay_verified::<Account>(&stream).unwrap());
}

// 5a. Truncated tail (frames chopped off the end, head record intact).
#[test]
fn truncated_tail_detected() {
    let mut stream = build_stream("neg-trunc", 50);
    let mut store = SnapshotStore::new();
    store.take_snapshot::<Account>(&stream, 19);

    stream.frames.truncate(45); // lose the last 5 frames; head_hash unchanged

    let err = load_verified::<Account>(&stream, &mut store).unwrap_err();
    assert!(matches!(err, VerifyError::HeadMismatch { .. }));
}

// 5b. Truncation BELOW the snapshot version.
#[test]
fn truncation_below_snapshot_version_detected() {
    let mut stream = build_stream("neg-trunc2", 50);
    let mut store = SnapshotStore::new();
    store.take_snapshot::<Account>(&stream, 19);

    stream.frames.truncate(10);

    let err = load_verified::<Account>(&stream, &mut store).unwrap_err();
    assert_eq!(
        err,
        VerifyError::SnapshotBeyondHead { snapshot_version: 19, head: Some(9) }
    );
}

// 5c. Reordered tail (two adjacent frames swapped).
#[test]
fn reordered_tail_detected() {
    let mut stream = build_stream("neg-reorder", 50);
    let mut store = SnapshotStore::new();
    store.take_snapshot::<Account>(&stream, 19);

    stream.frames.swap(30, 31);

    let err = load_verified::<Account>(&stream, &mut store).unwrap_err();
    assert_eq!(err, VerifyError::VersionOutOfOrder { expected: 30, got: 31 });
}

// 6. Cross-stream snapshot confusion. Two streams with IDENTICAL payloads;
// present stream A's snapshot when loading stream B.
#[test]
fn cross_stream_snapshot_rejected_when_genesis_binds_stream_id() {
    let a = build_stream("acct-A", 30);
    let b = build_stream("acct-B", 30); // identical payload sequence

    let mut store_a = SnapshotStore::new();
    let r_a = store_a.take_snapshot::<Account>(&a, 19).clone();

    // Attacker re-labels A's snapshot as B's and plants it in B's store.
    let mut store_b = SnapshotStore::new();
    let blob = store_a.get("acct-A").unwrap().1.clone();
    let mut forged = r_a.clone();
    forged.stream_id = "acct-B".to_string();
    // (plant it)
    store_b_insert(&mut store_b, forged, blob);

    // Even though payloads are identical, h chains differ because
    // h[-1] = BLAKE3("mess-stream" || stream_id) differs.
    let err = load_verified::<Account>(&b, &mut store_b).unwrap_err();
    assert_eq!(err, VerifyError::PrefixHashMismatch { path: VerifyPath::FromFrameV });
}

// 6b. COUNTERFACTUAL: without stream_id in the genesis, the same attack
// SUCCEEDS — proving the stream_id binding is load-bearing, not decorative.
#[test]
fn cross_stream_snapshot_accepted_without_stream_id_in_genesis() {
    let unbound = blob_hash(b"mess-stream"); // same genesis for ALL streams
    let mut a = Stream::with_genesis("acct-A", unbound);
    let mut b = Stream::with_genesis("acct-B", unbound);
    for i in 0..30u64 {
        let p = encode_event((i % 3 == 2) as u8, 10 + i, 32);
        a.append(p.clone());
        b.append(p);
    }

    let mut store = SnapshotStore::new();
    // take_snapshot recomputes from the REAL genesis; build the ref manually
    // against the unbound genesis instead.
    let mut h = unbound;
    let mut state = Account::init();
    for i in 0..=19u64 {
        let f = a.frame(i).unwrap();
        h = chain_next(&h, &frame_hash(&f.payload, i), i);
        state.apply(&f.payload);
    }
    let blob = state.to_bytes();
    let forged = SnapshotRef {
        stream_id: "acct-B".to_string(), // relabeled!
        stream_version: 19,
        event_prefix_hash: h,
        state_hash: blob_hash(&blob),
        fold_version: Account::FOLD_VERSION,
    };
    store_b_insert(&mut store, forged, blob);

    // The forged cross-stream snapshot passes every check: attack succeeds.
    let out = load_verified::<Account>(&b, &mut store).unwrap();
    assert!(!out.rebuilt_by_replay);
    // (Here payloads were identical so the state happens to be right; with
    // differing payloads it would be silently WRONG state. The point is the
    // certificate failed to bind the stream identity.)
}

// 7. Frame v+1's prev_stream_hash rewritten to match a forged snapshot: Path
// B alone is fooled; the tail replay + head anchor still catch it.
#[test]
fn forged_frame_header_fooling_path_b_caught_by_tail_replay() {
    let mut stream = build_stream("neg-forge-b", 50);
    let mut store = SnapshotStore::new();
    store.take_snapshot::<Account>(&stream, 19);

    // Forge: bogus prefix hash in both the snapshot and frame 20's header.
    let bogus = [0xAB; 32];
    store.get_mut("neg-forge-b").unwrap().0.event_prefix_hash = bogus;
    stream.frames[20].prev_stream_hash = bogus;

    let err = load_verified::<Account>(&stream, &mut store).unwrap_err();
    // Path A (frame 19, recomputed) rejects first in our implementation.
    assert_eq!(err, VerifyError::PrefixHashMismatch { path: VerifyPath::FromFrameV });

    // If frame 19 were compacted (Path A unavailable), Path B accepts the
    // forgery — but the chain then breaks at frame 21 during tail replay:
    let r = store.get("neg-forge-b").unwrap().0.clone();
    let mut state = Account::from_bytes(&store.get("neg-forge-b").unwrap().1).unwrap();
    let err2 =
        replay_verified(&stream, r.stream_version + 1, r.event_prefix_hash, &mut state)
            .unwrap_err();
    assert_eq!(err2, VerifyError::ChainBreakPrev { at_version: 21 });
}

fn store_b_insert(store: &mut SnapshotStore, r: SnapshotRef, blob: Vec<u8>) {
    // SnapshotStore has no public forge API; emulate via take+overwrite.
    // (test helper) — insert by constructing through the map directly.
    store.insert_raw(r, blob);
}
