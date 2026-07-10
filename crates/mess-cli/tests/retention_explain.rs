//! Acceptance: `mess retention explain` matches the bn-2ug decision function
//! on a constructed corpus — both for the trivially-deletable case and for a
//! blocked case (a live snapshot whose certification frames live in the sealed
//! segment).

mod common;

use mess_cli::metaread;
use mess_cli::retention;
use mess_index::meta::{CommitGroup, MetaStore, SnapshotHead, StreamId};
use mess_index::sealed::retention::{RetentionDecision, decide_segment};
use mess_index::sealed::segment::SealedSegmentIndex;

/// The 14-byte snapshot-ref v1 image (tag||fold_version||flags||ptr), matching
/// `mess_store::fjall_snapshot::encode_ref` so `metaread` decodes it.
fn ref_v1(fold_version: u32, covers_empty_prefix: bool) -> Vec<u8> {
    let mut v = vec![0x01u8];
    v.extend_from_slice(&fold_version.to_le_bytes());
    v.push(u8::from(covers_empty_prefix));
    v.extend_from_slice(&0u64.to_le_bytes());
    v
}

/// Write a snapshot head for `stream_id` at `version` into the (closed) meta
/// store, so the store presents a live snapshot for the retention decision.
fn inject_snapshot(dir: &std::path::Path, stream_id: u64, version: u64) {
    let meta =
        MetaStore::open(mess_cli::store::meta_dir(dir)).expect("open meta");
    let mut group = CommitGroup::new(version + 2);
    group.snapshot_heads.push((
        StreamId(stream_id),
        SnapshotHead {
            covered_version: version,
            global_position: version,
            snapshot_ref:    ref_v1(1, false),
        },
    ));
    meta.apply_group(&group).expect("apply snapshot head");
    drop(meta);
}

fn stream_id_of(dir: &std::path::Path, name: &str) -> u64 {
    metaread::read(dir)
        .expect("meta")
        .stream_names
        .into_iter()
        .find(|(_, n)| n == name)
        .map(|(id, _)| id)
        .expect("stream present")
}

/// No live snapshots → every sealed segment is deletable, and the CLI verdict
/// equals `decide_segment(idx, &[], &[])`.
#[test]
fn deletable_matches_decision_function() {
    let d = tempfile::tempdir().expect("tempdir");
    common::build_corpus(d.path(), 5);

    let report = retention::run(d.path());
    let row = report
        .collection
        .iter()
        .find(|r| r["segment_id"] == common::SEG_ID)
        .expect("segment row");
    assert_eq!(row["verdict"], "deletable");

    // Independently confirm via the decision function the CLI wraps.
    let idx = SealedSegmentIndex::open(&common::pidx(d.path()))
        .expect("open sidecar");
    assert_eq!(decide_segment(&idx, &[], &[]), RetentionDecision::Deletable);
    assert_eq!(report.exit_code(), 0);
}

/// A live snapshot at v=2 whose certification frames (v and v+1) live in the
/// sealed segment blocks it — and the CLI verdict + blockers match
/// `decide_segment` fed the same live set.
#[test]
fn blocked_matches_decision_function() {
    let d = tempfile::tempdir().expect("tempdir");
    common::build_corpus(d.path(), 5); // versions 0..=4 in segment 1

    let sid = stream_id_of(d.path(), "acct-1");
    inject_snapshot(d.path(), sid, 2);

    // The CLI decision, read straight from the store's live snapshot set.
    let report = retention::run(d.path());
    let row = report
        .collection
        .iter()
        .find(|r| r["segment_id"] == common::SEG_ID)
        .expect("segment row");
    assert_eq!(row["verdict"], "blocked", "report: {:#?}", report.findings);

    // The same decision, computed directly against the sealed index with the
    // live set the store now carries.
    let idx = SealedSegmentIndex::open(&common::pidx(d.path()))
        .expect("open sidecar");
    let live: Vec<_> = metaread::read(d.path())
        .unwrap()
        .snapshots
        .iter()
        .filter(|s| !s.covers_empty_prefix)
        .map(|s| mess_index::sealed::retention::LiveSnapshotRef {
            stream_id: s.stream_id,
            version:   s.version,
        })
        .collect();
    let decision = decide_segment(&idx, &live, &[]);
    assert!(
        decision.is_blocked(),
        "decision function must block: {decision:?}"
    );

    // Blockers name stream `sid`, version 2, frames v and v+1.
    let blockers = row["blockers"].as_array().expect("blockers array");
    assert!(
        blockers.iter().any(|b| b["version"] == 2 && b["frame"] == "v"),
        "expected a frame-v blocker, got {blockers:?}"
    );
    assert!(
        blockers.iter().any(|b| b["version"] == 2 && b["frame"] == "v+1"),
        "expected a frame-v+1 blocker, got {blockers:?}"
    );
}
