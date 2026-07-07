//! Positive-path tests: both certification paths, empty vs non-empty tail,
//! no-snapshot fallback, and the version-0 edge cases.

use fold_cert::*;

fn build_stream(id: &str, n: u64) -> Stream {
    let mut s = Stream::new(id);
    for i in 0..n {
        // alternate deposits/withdrawals, deterministic amounts
        let tag = (i % 3 == 2) as u8; // every 3rd is a withdrawal
        s.append(encode_event(tag, 10 + i, 32));
    }
    s
}

#[test]
fn load_with_empty_tail_uses_only_path_a() {
    let stream = build_stream("acct-1", 20);
    let mut store = SnapshotStore::new();
    store.take_snapshot::<Account>(&stream, 19); // snapshot AT head

    let out = load_verified::<Account>(&stream, &mut store).unwrap();
    assert_eq!(out.tail_len, 0);
    assert!(!out.rebuilt_by_replay);
    // Tail empty -> frame v+1 does not exist -> only FromFrameV can certify.
    assert_eq!(out.paths_used, vec![VerifyPath::FromFrameV]);
    assert_eq!(out.state, full_replay_verified::<Account>(&stream).unwrap());
}

#[test]
fn load_with_tail_uses_both_paths() {
    let stream = build_stream("acct-2", 50);
    let mut store = SnapshotStore::new();
    store.take_snapshot::<Account>(&stream, 29);

    let out = load_verified::<Account>(&stream, &mut store).unwrap();
    assert_eq!(out.tail_len, 20);
    assert_eq!(out.paths_used, vec![VerifyPath::FromFrameV, VerifyPath::FromFrameVPlus1]);
    assert_eq!(out.state, full_replay_verified::<Account>(&stream).unwrap());
}

#[test]
fn path_b_alone_suffices_when_frame_v_compacted() {
    // Simulate compaction of the prefix: hollow out payloads of frames 0..=v.
    // certify_prefix must fall back to Path B (frame v+1's header).
    let stream = build_stream("acct-3", 50);
    let mut store = SnapshotStore::new();
    let r = store.take_snapshot::<Account>(&stream, 29);

    let mut compacted = stream.clone();
    // Drop frames 0..=29 entirely; keep indices aligned by replacing with a
    // sentinel-free approach: we just test certify_prefix on a stream whose
    // frame(v) accessor returns None. Easiest faithful simulation: truncate
    // from the front is not representable in the Vec-indexed toy store, so
    // instead blank the payload so Path A would FAIL if it were consulted.
    compacted.frames[29].payload = vec![];

    // certify_prefix consults Path A first here and fails (payload gone) —
    // which demonstrates a real design constraint: Path A requires frame v's
    // payload to still be readable. See SPEC GAP 9.
    let err = certify_prefix(&compacted, &r).unwrap_err();
    assert_eq!(err, VerifyError::PrefixHashMismatch { path: VerifyPath::FromFrameV });

    // A store that KNOWS frame v is compacted skips Path A. Model that by
    // certifying against a stream where frame v is genuinely absent: build a
    // fresh check using only frame v+1.
    let f_next = stream.frame(30).unwrap();
    assert_eq!(f_next.prev_stream_hash, r.event_prefix_hash, "Path B certifies h[v] directly");
}

#[test]
fn no_snapshot_falls_back_to_full_verified_replay() {
    let stream = build_stream("acct-4", 40);
    let mut store = SnapshotStore::new();
    let out = load_verified::<Account>(&stream, &mut store).unwrap();
    assert!(out.rebuilt_by_replay);
    assert_eq!(out.tail_len, 40);
    assert_eq!(out.state.tx_count, 40);
}

#[test]
fn snapshot_at_version_0_works_via_frame_0() {
    // Version 0 = snapshot after the FIRST event. Path A reads frame 0, whose
    // prev_stream_hash is the genesis hash h[-1]. There is no way to express
    // an EMPTY-prefix snapshot in this scheme (SPEC GAP 2).
    let stream = build_stream("acct-5", 3);
    let mut store = SnapshotStore::new();
    let r = store.take_snapshot::<Account>(&stream, 0);
    assert_eq!(stream.frame(0).unwrap().prev_stream_hash, genesis_hash("acct-5"));

    let paths = certify_prefix(&stream, &r).unwrap();
    assert_eq!(paths, vec![VerifyPath::FromFrameV, VerifyPath::FromFrameVPlus1]);

    let out = load_verified::<Account>(&stream, &mut store).unwrap();
    assert_eq!(out.tail_len, 2);
    assert_eq!(out.state, full_replay_verified::<Account>(&stream).unwrap());
}

#[test]
fn empty_stream_head_is_genesis() {
    let stream = Stream::new("acct-6");
    assert_eq!(stream.head_hash, genesis_hash("acct-6"));
    assert_eq!(stream.head_version(), None);
    // Full replay of an empty stream verifies trivially against genesis.
    let out = full_replay_verified::<Account>(&stream).unwrap();
    assert_eq!(out, Account::init());
}
