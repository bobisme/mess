//! bn-1w4h acceptance: the CLI knows the **whole** current sealed namespace —
//! bn-3of's consolidated `.seal` packs and bn-30u's `.refuted` quarantine
//! markers — on every surface that enumerates `sealed/`.
//!
//! Before this bone `mess` knew only loose `.pidx` sidecars, so against a
//! `seal_pack` store (still default-off; bn-ccx1 flips it):
//!
//! - `doctor` reported `sidecar-missing` for every HEALTHY segment,
//! - `retention explain` reported "no sealed segments to evaluate" over a store
//!   full of them — an explainer fronting a destructive operation, silently
//!   vacuous,
//! - `backup` left the `.seal` out of the cut, so a restored store carried a
//!   footer naming a pack that was not there and failed its own verify gate
//!   (bn-11g binds the footer to the exact pack, so the cut must be
//!   identity-complete),
//! - `verify` never opened a pack at all unless a footer named one, and
//! - a quarantine marker was invisible everywhere.
//!
//! Every test below is read-only with respect to the store except where it
//! deliberately plants a fixture; the quarantine tests additionally assert that
//! the CLI does not touch the evidence.

mod common;

use std::path::Path;

use mess_cli::report::{Finding, Report, Severity};
use mess_cli::{backup, doctor, inspect, restore, retention, store, verify};
use serde_json::Value;

fn tmp(name: &str) -> mess_testkit::SweepingTempDir {
    mess_testkit::sweeping_temp_dir(name)
}

fn finding<'a>(report: &'a Report, kind: &str) -> Option<&'a Finding> {
    report.findings.iter().find(|f| f.kind == kind)
}

fn count_kind(report: &Report, kind: &str) -> usize {
    report.findings.iter().filter(|f| f.kind == kind).count()
}

/// Every finding at `Warn` or worse — the set a healthy store must leave empty.
fn problems(report: &Report) -> Vec<String> {
    report
        .findings
        .iter()
        .filter(|f| f.severity >= Severity::Warn)
        .map(|f| format!("{} {}: {}", f.severity.as_str(), f.kind, f.message))
        .collect()
}

fn run_doctor(dir: &Path) -> Report {
    doctor::run(dir, &doctor::DoctorOptions::default())
}

fn run_verify(dir: &Path) -> Report {
    verify::run(dir, &verify::VerifyOptions::default())
}

// ---------------------------------------------------------------------------
// doctor
// ---------------------------------------------------------------------------

/// **The bone's headline symptom.** A healthy pack-sealed store produces ZERO
/// problems: the `.seal` pack is the segment's sealed index, so `doctor`
/// reports it verified instead of warning that a `.pidx` it never wanted is
/// missing.
#[test]
fn doctor_reports_a_pack_sealed_store_as_healthy() {
    let d = tmp("cli-seal-ns-doctor-ok");
    common::build_sealed_pack_store(d.path(), 6);
    assert!(common::seal(d.path()).exists(), "the fixture is pack-sealed");
    assert!(!common::pidx(d.path()).exists(), "and carries no .pidx");

    let report = run_doctor(d.path());
    assert!(
        problems(&report).is_empty(),
        "a healthy pack-sealed store must produce no warnings or errors: {:#?}",
        problems(&report)
    );
    let ok = finding(&report, "seal-pack-ok").expect("the pack is verified");
    assert_eq!(ok.severity, Severity::Ok);
    assert_eq!(ok.fields["segment_id"], Value::from(common::SEG_ID));
    assert!(
        finding(&report, "sidecar-missing").is_none(),
        "a pack-sealed segment is not missing a sidecar"
    );
    assert_eq!(report.exit_code(), 0);
}

/// The loose-sealed store still behaves exactly as before — the pack support is
/// an addition to the classification, not a replacement of it.
#[test]
fn doctor_still_reports_a_pidx_sealed_store_as_healthy() {
    let d = tmp("cli-seal-ns-doctor-pidx");
    common::build_corpus(d.path(), 6);
    common::seal_log_trailer(d.path());

    let report = run_doctor(d.path());
    assert!(problems(&report).is_empty(), "{:#?}", problems(&report));
    assert!(finding(&report, "sidecar-ok").is_some(), "the .pidx is verified");
    assert!(finding(&report, "seal-pack-ok").is_none(), "no pack here");
}

/// A segment sealed with **neither** artifact is the only case that is still
/// `sidecar-missing`, and the message no longer names `.pidx` alone.
#[test]
fn doctor_flags_a_sealed_segment_with_no_sealed_index() {
    let d = tmp("cli-seal-ns-doctor-none");
    common::build_sealed_pack_store(d.path(), 6);
    std::fs::remove_file(common::seal(d.path())).expect("remove pack");

    let report = run_doctor(d.path());
    let f = finding(&report, "sidecar-missing").expect("reported");
    assert_eq!(f.severity, Severity::Warn);
    assert!(
        f.message.contains(".seal") && f.message.contains(".pidx"),
        "the message must name both shapes: {}",
        f.message
    );
}

/// A corrupt pack is an `Error`, named as the pack it is — before bn-1w4h
/// `doctor` opened only `.pidx`es, so a pack-sealed store had no integrity
/// check at all.
#[test]
fn doctor_flags_a_corrupt_pack() {
    let d = tmp("cli-seal-ns-doctor-corrupt");
    common::build_sealed_pack_store(d.path(), 6);
    common::flip_byte(&common::seal(d.path()), 96);

    let report = run_doctor(d.path());
    let f = finding(&report, "seal-pack-corrupt").expect("reported");
    assert_eq!(f.severity, Severity::Error);
    assert_ne!(report.exit_code(), 0);
}

/// Mid-migration coexistence (bn-3of dual read): a `.seal` and a `.pidx` for
/// one segment. The pack serves; the sidecar is reported inert, not as a second
/// sealed index and not as a problem.
#[test]
fn doctor_reports_a_shadowed_pidx_as_inert() {
    let d = tmp("cli-seal-ns-doctor-shadow");
    common::build_sealed_pack_store(d.path(), 6);
    // Plant a legacy sidecar next to the pack (what a store sealed before the
    // format switch and re-sealed after it looks like).
    let side = tmp("cli-seal-ns-doctor-shadow-side");
    common::build_corpus(side.path(), 6);
    std::fs::copy(common::pidx(side.path()), common::pidx(d.path()))
        .expect("plant .pidx");

    let report = run_doctor(d.path());
    let f = finding(&report, "pidx-shadowed-by-pack").expect("reported");
    assert_eq!(f.severity, Severity::Info);
    assert!(problems(&report).is_empty(), "{:#?}", problems(&report));
    assert!(finding(&report, "seal-pack-ok").is_some(), "the pack serves");
}

// ---------------------------------------------------------------------------
// quarantine (bn-30u)
// ---------------------------------------------------------------------------

/// Quarantine the corpus segment's pack through the engine's own
/// [`mess_store::sealed_candidate::quarantine`] — the exact rename a refuting
/// open performs.
fn quarantine_the_pack(dir: &Path) -> std::path::PathBuf {
    let primary = common::seal(dir);
    mess_store::sealed_candidate::quarantine(&primary).expect("quarantine");
    let marker = std::path::PathBuf::from(format!(
        "{}{}",
        primary.display(),
        mess_store::QUARANTINE_SUFFIX
    ));
    assert!(marker.exists(), "the marker is on disk");
    assert!(!primary.exists(), "the candidate left the namespace");
    marker
}

/// A quarantined candidate is reported as quarantined — with its segment, its
/// original artifact kind, and its pending-re-seal state — and is NOT reported
/// as an unknown or orphan file.
#[test]
fn doctor_reports_a_quarantined_candidate_with_its_state() {
    let d = tmp("cli-seal-ns-quarantine");
    common::build_sealed_pack_store(d.path(), 6);
    let marker = quarantine_the_pack(d.path());

    let report = run_doctor(d.path());
    let f = finding(&report, "quarantined-candidate").expect("reported");
    assert_eq!(f.severity, Severity::Warn);
    assert_eq!(f.fields["segment_id"], Value::from(common::SEG_ID));
    assert_eq!(f.fields["artifact"], Value::from("seal"));
    assert_eq!(
        f.fields["state"],
        Value::from("pending-reseal"),
        "the segment has no sealed index, so it is owed a fresh seal"
    );
    assert_eq!(
        f.fields["parses"],
        Value::from(true),
        "these bytes are a perfectly good pack; the refutation was not \
         `unparsable`"
    );
    assert!(
        f.message.contains("do not delete"),
        "the finding must say the evidence is load-bearing: {}",
        f.message
    );
    assert_eq!(
        report.extra["quarantined"],
        Value::from(1),
        "the count is in the envelope"
    );

    // Read-only: the evidence survives every diagnosis surface untouched.
    let before = std::fs::read(&marker).expect("read marker");
    let _ = run_verify(d.path());
    let _ = inspect::run(d.path(), &inspect::InspectOptions::default());
    let _ = retention::run(d.path());
    assert_eq!(
        std::fs::read(&marker).expect("marker still there"),
        before,
        "no CLI surface may delete, repair, or rewrite a quarantine marker"
    );
}

/// A marker whose segment has since been re-sealed is inert evidence, not a
/// pending re-seal — the state field says which.
#[test]
fn doctor_distinguishes_a_resealed_segment_from_a_pending_one() {
    let d = tmp("cli-seal-ns-quarantine-resealed");
    common::build_sealed_pack_store(d.path(), 6);
    let pack = std::fs::read(common::seal(d.path())).expect("read pack");
    quarantine_the_pack(d.path());
    // The next open re-seals; simulate the outcome by restoring the pack.
    std::fs::write(common::seal(d.path()), &pack).expect("re-seal");

    let report = run_doctor(d.path());
    let f = finding(&report, "quarantined-candidate").expect("reported");
    assert_eq!(f.fields["state"], Value::from("resealed"));
    assert!(finding(&report, "seal-pack-ok").is_some());
}

/// A healthy store says so explicitly rather than staying silent about the
/// quarantine namespace.
#[test]
fn doctor_reports_no_quarantined_artifacts_on_a_clean_store() {
    let d = tmp("cli-seal-ns-quarantine-clean");
    common::build_sealed_pack_store(d.path(), 6);

    let report = run_doctor(d.path());
    let f = finding(&report, "no-quarantined-artifacts").expect("reported");
    assert_eq!(f.severity, Severity::Ok);
    assert_eq!(report.extra["quarantined"], Value::from(0));
}

/// `verify` names the quarantined candidate too (a thrown-away seal is exactly
/// the evidence this command exists to surface) — at `Info`, because the raw
/// log is authority and nothing committed is at risk.
#[test]
fn verify_reports_a_quarantined_candidate_without_failing() {
    let d = tmp("cli-seal-ns-quarantine-verify");
    common::build_pack_corpus(d.path(), 6);
    common::seal_pack_footer_unnamed(d.path()); // no identity to miss
    quarantine_the_pack(d.path());

    let report = run_verify(d.path());
    let f = finding(&report, "quarantined-candidate").expect("reported");
    assert_eq!(f.severity, Severity::Info);
    assert_eq!(f.fields["fallback"], Value::from("raw-segment-scan"));
    assert!(
        finding(&report, "orphan-sidecar").is_none()
            && finding(&report, "orphan-seal-pack").is_none(),
        "a quarantine marker is not an orphan artifact"
    );
    assert_eq!(report.exit_code(), 0, "quarantine is not a verify failure");
}

/// `inspect` describes the quarantine namespace in its envelope, and the key is
/// always present (an empty array on a healthy store) so the JSON schema does
/// not change shape with the store's health.
#[test]
fn inspect_lists_quarantined_artifacts() {
    let d = tmp("cli-seal-ns-quarantine-inspect");
    common::build_sealed_pack_store(d.path(), 6);
    let clean = inspect::run(d.path(), &inspect::InspectOptions::default());
    assert_eq!(clean.extra["quarantined"], Value::Array(vec![]));

    quarantine_the_pack(d.path());
    let report = inspect::run(d.path(), &inspect::InspectOptions::default());
    let rows = report.extra["quarantined"].as_array().expect("array");
    assert_eq!(rows.len(), 1, "{rows:?}");
    assert_eq!(rows[0]["segment_id"], Value::from(common::SEG_ID));
    assert_eq!(rows[0]["artifact"], Value::from("seal"));
    assert_eq!(rows[0]["primary"], Value::from(true));
}

// ---------------------------------------------------------------------------
// verify
// ---------------------------------------------------------------------------

/// A pack is verified on its own merits — trailer hash plus every section
/// checksum — even when the footer names nothing (the D-FMT-10 legacy policy).
/// That check simply did not exist before bn-1w4h.
#[test]
fn verify_validates_a_pack_the_footer_does_not_name() {
    let d = tmp("cli-seal-ns-verify-unnamed");
    common::build_pack_corpus(d.path(), 6);
    common::seal_pack_footer_unnamed(d.path());

    let report = run_verify(d.path());
    let f = finding(&report, "seal-pack-verified").expect("reported");
    assert_eq!(f.severity, Severity::Ok);
    assert_eq!(f.fields["segment_id"], Value::from(common::SEG_ID));
    // The legacy-policy advisory still fires; it is a Warn, not a failure.
    assert!(finding(&report, "seal-pack-unnamed").is_some());
    assert_eq!(report.exit_code(), 0);
}

/// A corrupt pack no one named is `seal-pack-corrupt` — and the bn-11g
/// identity findings stay silent, because there is no binding to break.
#[test]
fn verify_flags_a_corrupt_unnamed_pack() {
    let d = tmp("cli-seal-ns-verify-corrupt");
    common::build_pack_corpus(d.path(), 6);
    common::seal_pack_footer_unnamed(d.path());
    common::flip_byte(&common::seal(d.path()), 96);

    let report = run_verify(d.path());
    let f = finding(&report, "seal-pack-corrupt").expect("reported");
    assert_eq!(f.severity, Severity::Error);
    assert_eq!(f.fields["fallback"], Value::from("raw-segment-scan"));
    assert!(
        finding(&report, "seal-pack-unreadable").is_none(),
        "an unnamed pack cannot be reported through the identity path"
    );
    assert_ne!(report.exit_code(), 0);
}

/// bn-11g's identity findings still come out of the restructured single-open
/// path: a substituted pack is named expected-vs-observed, and the pack's own
/// integrity check does NOT double-report it.
#[test]
fn verify_still_reports_the_identity_mismatch_exactly_once() {
    let d = tmp("cli-seal-ns-verify-identity");
    common::build_pack_corpus(d.path(), 6);
    let real = common::pack_identity(d.path());
    common::seal_pack_footer(d.path());
    // A different pack for the same segment: same coverage, other bytes.
    let other = tmp("cli-seal-ns-verify-identity-other");
    common::build_pack_corpus(other.path(), 7);
    std::fs::copy(common::seal(other.path()), common::seal(d.path()))
        .expect("substitute");
    let observed = common::pack_identity(d.path());
    assert_ne!(observed, real, "the substitute is a different pack");

    let report = run_verify(d.path());
    let f = finding(&report, "seal-pack-identity-mismatch").expect("reported");
    assert_eq!(f.severity, Severity::Error);
    assert_eq!(
        count_kind(&report, "seal-pack-identity-mismatch"),
        1,
        "one broken binding, one finding"
    );
    assert!(
        finding(&report, "seal-pack-corrupt").is_none(),
        "a readable-but-wrong pack is a binding failure, not a corrupt file"
    );
    assert_ne!(report.exit_code(), 0);
}

/// A `.seal` whose `.log` is gone is the pack-shaped orphan the `.pidx` sweep
/// has always reported.
#[test]
fn verify_reports_an_orphan_pack() {
    let d = tmp("cli-seal-ns-verify-orphan");
    common::build_sealed_pack_store(d.path(), 6);
    std::fs::remove_file(store::log_path(d.path(), common::SEG_ID))
        .expect("remove log");

    let report = run_verify(d.path());
    let f = finding(&report, "orphan-seal-pack").expect("reported");
    assert_eq!(f.severity, Severity::Warn);
    assert!(
        finding(&report, "seal-pack-verified").is_some(),
        "an orphan pack is still opened and checked"
    );
}

// ---------------------------------------------------------------------------
// retention
// ---------------------------------------------------------------------------

/// A pack-sealed segment gets a retention verdict, from the same
/// `decide_segment` a `.pidx` segment does. Before bn-1w4h `retention explain`
/// skipped it entirely and answered "no sealed segments to evaluate" — an
/// explainer that fronts deletion, silently vacuous over a whole store.
#[test]
fn retention_decides_over_pack_sealed_segments() {
    let d = tmp("cli-seal-ns-retention");
    common::build_sealed_pack_store(d.path(), 6);

    let report = retention::run(d.path());
    let row = report
        .collection
        .iter()
        .find(|r| r["segment_id"] == common::SEG_ID)
        .unwrap_or_else(|| panic!("a verdict row: {:#?}", report.collection));
    assert_eq!(row["verdict"], "deletable");
    assert_eq!(row["artifact"], "seal-pack", "the shape is named in the row");
    assert!(
        finding(&report, "no-sealed-segments").is_none(),
        "the store is full of sealed segments"
    );

    // The verdict equals the decision function's, computed straight off the
    // pack the CLI opened.
    let idx = mess_index::sealed::SealedSegmentIndex::open_pack(&common::seal(
        d.path(),
    ))
    .expect("open pack");
    assert_eq!(
        mess_index::sealed::retention::decide_segment(&idx, &[], &[]),
        mess_index::sealed::retention::RetentionDecision::Deletable
    );
}

/// A segment whose only candidate is quarantined has no sealed index and so no
/// verdict — said out loud, because an absent row must never read as
/// "deletable" to whatever executes the deletion.
#[test]
fn retention_says_a_quarantined_segment_has_no_verdict() {
    let d = tmp("cli-seal-ns-retention-quarantine");
    common::build_sealed_pack_store(d.path(), 6);
    quarantine_the_pack(d.path());

    let report = retention::run(d.path());
    let f = finding(&report, "quarantined-no-verdict").expect("reported");
    assert_eq!(f.severity, Severity::Info);
    assert_eq!(f.fields["segment_id"], Value::from(common::SEG_ID));
    assert!(
        report.collection.is_empty(),
        "no sealed index means no verdict row: {:#?}",
        report.collection
    );
}

// ---------------------------------------------------------------------------
// backup / restore
// ---------------------------------------------------------------------------

/// **The cut must be identity-complete.** bn-11g binds the segment footer to
/// the exact pack that sealed it, and the footer travels inside the `.log`
/// bytes the cut copies whole — so leaving the `.seal` behind restores a store
/// whose footer names a pack that does not exist. The pack rides the cut, and
/// the restored store passes its own `verify --full` gate.
#[test]
fn backup_carries_the_seal_pack_and_the_restore_is_clean() {
    let src = tmp("cli-seal-ns-backup-src");
    let dest = tmp("cli-seal-ns-backup-dest");
    let restored = tmp("cli-seal-ns-backup-restored");
    common::build_sealed_pack_store(src.path(), 6);

    let report =
        backup::run(src.path(), dest.path(), &backup::BackupOptions::default());
    assert_eq!(report.exit_code(), 0, "{:#?}", report.findings);
    let json = report.to_json();
    let files = json["files"].as_array().expect("file rows");
    let pack_rel = format!("sealed/seg-{:020}.seal", common::SEG_ID);
    assert!(
        files.iter().any(|f| f["path"] == pack_rel.as_str()),
        "the .seal pack must be in the cut: {files:#?}"
    );
    assert!(
        dest.path().join(&pack_rel).exists(),
        "and on disk at the destination"
    );

    let rr = restore::run(
        dest.path(),
        restored.path(),
        &restore::RestoreOptions::default(),
    );
    assert_eq!(
        rr.exit_code(),
        0,
        "a restored pack-sealed store must verify: {:#?}",
        rr.findings
    );
    assert_eq!(rr.to_json()["verified"], Value::Bool(true));

    // And the restored pack is the very one the footer names.
    assert_eq!(
        common::pack_identity(restored.path()),
        common::pack_identity(src.path()),
        "the restored pack is byte-identical, so the binding still holds"
    );
    let vr = run_verify(restored.path());
    assert!(
        finding(&vr, "seal-pack-identity-verified").is_some(),
        "{:#?}",
        vr.findings
    );
}

/// A quarantine marker is evidence about the SOURCE store, plus that store's
/// durable re-seal trigger. It must not ride the cut into a destination that
/// never had the anomaly.
#[test]
fn backup_leaves_quarantine_markers_out_of_the_cut() {
    let src = tmp("cli-seal-ns-backup-quarantine-src");
    let dest = tmp("cli-seal-ns-backup-quarantine-dest");
    common::build_pack_corpus(src.path(), 6);
    common::seal_pack_footer_unnamed(src.path());
    quarantine_the_pack(src.path());

    let report =
        backup::run(src.path(), dest.path(), &backup::BackupOptions::default());
    let json = report.to_json();
    let files = json["files"].as_array().expect("file rows");
    assert!(
        files.iter().all(|f| !f["path"].as_str().unwrap().contains("refuted")),
        "no quarantine marker in the cut: {files:#?}"
    );
}

// ---------------------------------------------------------------------------
// the production shape: an engine-rolled, background-sealed pack store
// ---------------------------------------------------------------------------

/// The fixtures above hand-write the footer so the identity binding is a knob.
/// This one takes the whole production path instead — small segments, real
/// rolls, the background sealer writing the `.seal` and finalizing the footer
/// that names it (bn-11g) — and proves `doctor`/`verify` are clean on what a
/// `seal_pack` store actually looks like after running.
#[test]
fn a_rolled_engine_written_pack_store_is_clean() {
    use mess_store::backend::{Backend, RecordToAppend};
    use mess_store::{EngineOptions, LogEngine, Version};

    let d = tmp("cli-seal-ns-rolled");
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("runtime");
    let engine = LogEngine::open_with(
        d.path(),
        EngineOptions {
            segment_size: 16 * 1024,
            seal_pack: true,
            ..EngineOptions::default()
        },
    )
    .expect("open engine");
    rt.block_on(async {
        for i in 0..400u64 {
            let expected =
                if i == 0 { Version::NoStream } else { Version::At(i - 1) };
            engine
                .append_batch(
                    "acct-1",
                    expected,
                    &[RecordToAppend {
                        message_type: "account.happened".into(),
                        data:         vec![b'x'; 96],
                    }],
                )
                .await
                .expect("append");
        }
    });
    let deadline =
        std::time::Instant::now() + std::time::Duration::from_secs(30);
    while engine.sealed_segment_count() < 1 {
        assert!(std::time::Instant::now() < deadline, "sealer never ran");
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    drop(engine);

    let packs = store::discover_seal_packs(d.path());
    assert!(!packs.is_empty(), "the engine wrote real .seal packs");

    let report = run_doctor(d.path());
    let errors: Vec<_> = report
        .findings
        .iter()
        .filter(|f| f.severity >= Severity::Error)
        .collect();
    assert!(errors.is_empty(), "{errors:#?}");
    assert_eq!(
        count_kind(&report, "seal-pack-ok"),
        packs.len(),
        "every pack on disk is verified: {:#?}",
        report.findings
    );
    // The only segments that may be reported without a sealed index are ones
    // that genuinely have no pack (a roll whose seal had not landed yet).
    for f in report.findings.iter().filter(|f| f.kind == "sidecar-missing") {
        let id = f.fields["segment_id"].as_u64().expect("segment_id");
        assert!(
            !store::seal_path(d.path(), id).exists(),
            "segment {id} has a .seal and must not be reported missing one"
        );
    }

    // And the engine's own footer names its pack, which `verify` confirms
    // against the bytes on disk.
    let vr = run_verify(d.path());
    assert!(
        count_kind(&vr, "seal-pack-identity-verified") >= 1,
        "the production sealer binds the footer to the pack: {:#?}",
        vr.findings
    );
    assert_eq!(vr.exit_code(), 0, "{:#?}", vr.findings);
}
