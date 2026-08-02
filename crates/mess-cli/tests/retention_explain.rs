//! Acceptance: `mess retention explain` matches the bn-2ug decision function
//! on a constructed corpus — both for the trivially-deletable case and for a
//! blocked case (a live snapshot whose certification frames live in the sealed
//! segment).
//!
//! # bn-fj34: the blocked fixture is a real app snapshot now
//!
//! This suite used to inject its live snapshot straight into a legacy
//! `<dir>/meta` `snapshot_heads` row, whose keys were the engine's interned
//! **dense** stream ids — the same space the sealed sidecar uses, so the join
//! worked by construction. That store is deleted, and the surviving source (the
//! pack sidecar an app actually writes) keys its heads by **interim FNV** hash
//! instead, which matches a dense id only by accident.
//!
//! So the fixture is now a genuine [`PackSnapshotBackend`] save — the exact
//! artifact production writes — and it is what proves the id-space join
//! `retention::run` performs (pack record's self-describing stream NAME ->
//! `$registry` -> dense id) actually lands. Without that join this test reports
//! `deletable` for a segment holding a live snapshot's certification frames:
//! vacuous in the dangerous direction.

mod common;

use mess_cli::retention;
use mess_index::sealed::retention::{RetentionDecision, decide_segment};
use mess_index::sealed::segment::SealedSegmentIndex;
use mess_store::{
    LogEngine, PackSnapshotBackend, SnapshotCompatibility, SnapshotCoverage,
    SnapshotRef, SnapshotStore, SnapshotTrust, StableSnapshotId,
    StoredSnapshot, interim_stream_id,
};

/// Publish a real app snapshot of `acct-1` covering exactly `version` into the
/// store's pack sidecar — the same `<dir>/.snapshots.packs` location `metaread`
/// reads and an app writes.
///
/// The head is written at the backend seam (as `tests/golden.rs` does) rather
/// than through `EventStore::save_snapshot`, because the fixture needs a head
/// at a *chosen* version — v2 of a 0..=4 stream — so the certification frames
/// land inside the sealed segment. The state blob is irrelevant to retention,
/// which decides on covered version alone.
fn inject_snapshot(dir: &std::path::Path, version: u64) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    rt.block_on(async {
        let engine = LogEngine::open(dir).expect("open engine");
        let snaps = PackSnapshotBackend::open(
            engine,
            mess_cli::store::snapshot_pack_dir(dir),
        )
        .expect("open snapshot sidecar");
        snaps
            .save_snapshot(
                "acct-1",
                StoredSnapshot {
                    snapshot_ref: SnapshotRef {
                        compatibility: SnapshotCompatibility {
                            aggregate_schema_id: StableSnapshotId::new(
                                "mess-cli.test.acct",
                            ),
                            fold_version:        1,
                            codec_id:            StableSnapshotId::new(
                                "mess-cli.test.acct",
                            ),
                            codec_version:       1,
                        },
                        coverage:      SnapshotCoverage::Through(version),
                        trust:         SnapshotTrust::UnverifiedCache,
                        stream_id:     interim_stream_id("acct-1"),
                    },
                    state_blob:   version.to_le_bytes().to_vec(),
                },
            )
            .await
            .expect("save snapshot");
    });
    // Engine + sidecar writer locks released here.
}

/// bn-2di: names come from the log's $registry, which is also what
/// `retention::run` joins the snapshot's stream name against.
fn stream_id_of(dir: &std::path::Path, name: &str) -> u64 {
    let state = mess_cli::registryfold::fold(dir).expect("fold $registry");
    mess_cli::registryfold::stream_names(&state)
        .into_iter()
        .find(|(_, n)| n == name)
        .map(|(id, _)| id)
        .expect("stream present")
}

/// No live snapshots → every sealed segment is deletable, and the CLI verdict
/// equals `decide_segment(idx, &[], &[])`.
#[test]
fn deletable_matches_decision_function() {
    let d = mess_testkit::sweeping_temp_dir("cli-retention-d");
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

/// bn-1w4h: the same two facts over a **pack-sealed** store (bn-3of). The
/// decision is a statement about the segment, not about the file format its
/// sealed index happens to use, so a `.seal`-sealed segment must be blocked by
/// exactly the same live snapshot — and before this bone it was not evaluated
/// at all, which reported the store as having no sealed segments while a
/// retention executor would have been free to delete one.
#[test]
fn blocked_matches_decision_function_on_a_pack_sealed_store() {
    let d = mess_testkit::sweeping_temp_dir("cli-retention-pack");
    common::build_sealed_pack_store(d.path(), 5); // versions 0..=4, .seal
    assert!(common::seal(d.path()).exists(), "the fixture is pack-sealed");
    assert!(!common::pidx(d.path()).exists(), "and carries no .pidx");

    inject_snapshot(d.path(), 2);

    let sid = stream_id_of(d.path(), "acct-1");
    let report = retention::run(d.path());
    let row = report
        .collection
        .iter()
        .find(|r| r["segment_id"] == common::SEG_ID)
        .unwrap_or_else(|| panic!("a verdict row: {:#?}", report.collection));
    assert_eq!(row["verdict"], "blocked", "report: {:#?}", report.findings);
    assert_eq!(row["artifact"], "seal-pack");

    // The same decision, computed directly against the PACK the CLI opened.
    let idx = SealedSegmentIndex::open_pack(&common::seal(d.path()))
        .expect("open pack");
    let live = [mess_index::sealed::retention::LiveSnapshotRef {
        stream_id: sid,
        version:   2,
    }];
    assert!(decide_segment(&idx, &live, &[]).is_blocked());

    let blockers = row["blockers"].as_array().expect("blockers array");
    assert!(blockers.iter().any(|b| b["version"] == 2 && b["frame"] == "v"));
    assert!(blockers.iter().any(|b| b["version"] == 2 && b["frame"] == "v+1"));
}

/// A live snapshot at v=2 whose certification frames (v and v+1) live in the
/// sealed segment blocks it — and the CLI verdict + blockers match
/// `decide_segment` fed the same live set.
#[test]
fn blocked_matches_decision_function() {
    let d = mess_testkit::sweeping_temp_dir("cli-retention-d-1");
    common::build_corpus(d.path(), 5); // versions 0..=4 in segment 1

    inject_snapshot(d.path(), 2);

    // The engine's interned DENSE id — the space the sealed sidecar is keyed
    // by, and the one the snapshot's interim-FNV id is emphatically not in.
    let sid = stream_id_of(d.path(), "acct-1");
    assert_ne!(
        sid,
        interim_stream_id("acct-1"),
        "the fixture is only meaningful while the two id spaces differ; if \
         they ever coincide this test stops proving the join happens"
    );

    // The CLI decision, read straight from the store's live snapshot set.
    let report = retention::run(d.path());
    let row = report
        .collection
        .iter()
        .find(|r| r["segment_id"] == common::SEG_ID)
        .expect("segment row");
    assert_eq!(row["verdict"], "blocked", "report: {:#?}", report.findings);

    // `retention::run` must have resolved the pack record's stream NAME to the
    // dense id through the log's `$registry` — the reported live set is in the
    // decision function's id space, not the snapshot subsystem's.
    let reported = report.extra["live_snapshots"].as_array().expect("live");
    assert_eq!(reported.len(), 1, "one live snapshot: {reported:?}");
    assert_eq!(
        reported[0]["stream_id"], sid,
        "live snapshot must be reported in the sealed index's dense id space"
    );

    // The same decision, computed directly against the sealed index with the
    // live set the store now carries.
    let idx = SealedSegmentIndex::open(&common::pidx(d.path()))
        .expect("open sidecar");
    let live = [mess_index::sealed::retention::LiveSnapshotRef {
        stream_id: sid,
        version:   2,
    }];
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
