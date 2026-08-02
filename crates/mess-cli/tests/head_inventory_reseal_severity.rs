//! bn-3m62: the two inventory questions bn-3qh0 left open, as acceptance tests.
//!
//! **A — the live head in the artifact inventory.** A pack-default store writes
//! no sealed artifact for the head at all (pinned engine-side in
//! `mess-store/tests/pack_default_head_artifacts.rs`), so there is no head
//! sidecar to suppress or to classify. What there *was* is a classification
//! gap: `authority`'s three-way `Serving` had no room for "not sealed yet", so
//! the head landed in `log-scan` — whose operator text says the background
//! sealer is owed a seal, which is precisely what the engine never does for a
//! head. Two costs, both fixed here: a healthy pack store could not report
//! `Ok`, and the always-present head hid the case that matters (a **sealed**
//! segment with no index) inside the same counter.
//!
//! **B — `seal-pack-missing` severity.** Two different states shared one Error.
//! With the segment's quarantine slot occupied the re-seal is durably owed and
//! the next open converges it (bn-30u / bn-3qh0); with no slot, nothing is
//! going to happen without an operator. They now report as
//! `seal-pack-reseal-pending` (Warn) and `seal-pack-missing` (Error), and
//! `doctor` splits `sidecar-missing` on the same bit with the same words — the
//! two tools must never describe one store differently, which they did before
//! this bone.
//!
//! The Error arm keeps its severity because `mess restore` gates on
//! `verify.worst() < Error` and spec 07 §1/§1.2 make `seal-pack-missing` the
//! reason a backup cut must be identity-complete. `a_torn_cut_still_errors`
//! pins that the split cannot weaken that gate.

#![cfg(not(miri))]

use std::path::Path;

use mess_cli::doctor::{self, DoctorOptions};
use mess_cli::report::{Finding, Report, Severity};
use mess_cli::store;
use mess_cli::verify::{self, VerifyOptions};
use mess_log::committer::Durability;
use mess_store::backend::{Backend, RecordToAppend};
use mess_store::{EngineOptions, LogEngine, Version};
use serde_json::Value;

mod common;

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

fn finding<'a>(report: &'a Report, kind: &str) -> &'a Finding {
    report.findings.iter().find(|f| f.kind == kind).unwrap_or_else(|| {
        let all: Vec<&str> =
            report.findings.iter().map(|f| f.kind.as_str()).collect();
        panic!("no `{kind}` finding; got {all:?}")
    })
}

fn no_finding(report: &Report, kind: &str) {
    assert!(
        report.findings.iter().all(|f| f.kind != kind),
        "unexpected `{kind}` finding"
    );
}

fn field<'a>(f: &'a Finding, name: &str) -> &'a Value {
    f.fields
        .get(name)
        .unwrap_or_else(|| panic!("finding {} has no `{name}` field", f.kind))
}

fn verify_report(dir: &Path) -> Report {
    verify::run(dir, &VerifyOptions::default())
}

fn doctor_report(dir: &Path) -> Report {
    doctor::run(dir, &DoctorOptions::default())
}

fn authority_summary(report: &Report) -> Value {
    let json: Value = serde_json::from_str(&mess_cli::format::render(
        report,
        mess_cli::format::Format::Json,
    ))
    .expect("doctor json");
    json["authority"]["summary"].clone()
}

fn authority_row(report: &Report, segment_id: u64) -> Value {
    let json: Value = serde_json::from_str(&mess_cli::format::render(
        report,
        mess_cli::format::Format::Json,
    ))
    .expect("doctor json");
    json["authority"]["segments"]
        .as_array()
        .expect("segments array")
        .iter()
        .find(|r| r["segment_id"] == segment_id)
        .unwrap_or_else(|| panic!("no authority row for segment {segment_id}"))
        .clone()
}

/// A real pack-default store that has rolled: several footer-bound `.seal`
/// segments plus a live, unsealed head. Returns the head's segment id.
fn build_rolled_pack_store(dir: &Path) -> u64 {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("runtime");
    rt.block_on(async {
        let opts = EngineOptions {
            durability: Durability::Process,
            segment_size: 32 * 1024,
            seal_pack: true,
            ..EngineOptions::default()
        };
        let engine = LogEngine::open_with(dir, opts).expect("open");
        let mut heads = [Version::NoStream; 4];
        for i in 0..400u64 {
            let s = (i % 4) as usize;
            let out = engine
                .append_batch(
                    &format!("acct-{s}"),
                    heads[s],
                    &[RecordToAppend {
                        message_type: format!("t{}", i % 3),
                        data:         vec![(i % 251) as u8; 300],
                    }],
                )
                .await
                .expect("append");
            heads[s] = out.version;
        }
        std::thread::sleep(std::time::Duration::from_millis(400));
    });
    let ids: Vec<u64> =
        store::discover_segments(dir).iter().map(|s| s.segment_id).collect();
    assert!(ids.len() >= 3, "the corpus must really roll: {ids:?}");
    *ids.last().expect("non-empty")
}

/// The quarantine slot a withdrawal or a bn-30u refutation occupies.
fn marker_path(dir: &Path, seg: u64) -> std::path::PathBuf {
    dir.join("sealed").join(format!("seg-{seg:020}.seal.refuted"))
}

// ---------------------------------------------------------------------------
// A — the live head in the artifact inventory
// ---------------------------------------------------------------------------

/// The head is `serving: unsealed`, not `log-scan`, and its row says so
/// explicitly with `sealed: false`.
///
/// The distinction is not cosmetic: `log-scan`'s fallback text promises the
/// operator that the background sealer is owed a seal for this segment, and for
/// a live head that is false by design — the engine excludes the head from the
/// re-seal queue unconditionally, because sealing writes the segment footer and
/// the head is still being appended to.
#[test]
fn the_live_head_is_classified_unsealed_not_log_scan() {
    let d = mess_testkit::sweeping_temp_dir("bn3m62-head-class");
    let head = build_rolled_pack_store(d.path());

    let report = doctor_report(d.path());
    let row = authority_row(&report, head);

    assert_eq!(row["sealed"], false, "the head carries no footer trailer");
    assert_eq!(row["serving"], "unsealed", "head row: {row:#}");
    assert_eq!(
        row["sealed_artifact"], "none",
        "and it really has no artifact — the fact the classification explains"
    );
    let fallback = row["fallback"].as_str().expect("fallback text");
    assert!(
        fallback.contains("not sealed yet")
            && fallback.contains("no re-seal is owed"),
        "the head's fallback must not promise an owed re-seal: {fallback}"
    );

    // Every rolled segment is served by its pack, and the head is the only
    // unsealed row.
    let ids: Vec<u64> = store::discover_segments(d.path())
        .iter()
        .map(|s| s.segment_id)
        .collect();
    for id in ids.iter().copied().filter(|&i| i != head) {
        let r = authority_row(&report, id);
        assert_eq!(r["sealed"], true, "segment {id} is footer-sealed");
        assert_eq!(r["serving"], "seal-pack", "segment {id}: {r:#}");
    }
}

/// A healthy pack-default store reports the authority finding at `Ok` again,
/// with `served_by_log_scan: 0` and the head counted separately.
///
/// Before this bone every such store — i.e. every store with a live head, i.e.
/// every store — reported `Info: 1 segment(s) have no sealed index`. A signal
/// that is always on is not a signal, and it is the one an operator needs when
/// a *sealed* segment loses its accelerator.
#[test]
fn a_healthy_pack_store_reports_authority_ok_with_the_head_counted_apart() {
    let d = mess_testkit::sweeping_temp_dir("bn3m62-head-ok");
    build_rolled_pack_store(d.path());

    let report = doctor_report(d.path());
    let f = finding(&report, "authority-accelerators");
    assert_eq!(
        f.severity,
        Severity::Ok,
        "a healthy pack store must report Ok: {}",
        f.message
    );
    assert_eq!(field(f, "served_by_log_scan"), 0);
    assert_eq!(field(f, "unsealed_segments"), 1);

    let summary = authority_summary(&report);
    assert_eq!(summary["served_by_log_scan"], 0);
    assert_eq!(summary["unsealed_segments"], 1);
    assert!(
        summary["served_by_seal_pack"].as_u64().expect("count") >= 2,
        "the rolled segments are pack-served: {summary:#}"
    );
    assert!(
        f.message.contains("not sealed yet"),
        "the Ok message still accounts for the head: {}",
        f.message
    );
}

/// The counter still fires — at `Info`, as `log-scan` — for the state it exists
/// for: a **sealed** segment whose accelerator is gone. This is the assertion
/// that proves the head was hiding a real signal rather than that the signal
/// was removed.
#[test]
fn a_sealed_segment_that_lost_its_pack_is_still_log_scan() {
    let d = mess_testkit::sweeping_temp_dir("bn3m62-lost-logscan");
    common::build_sealed_pack_store(d.path(), 8);
    std::fs::remove_file(common::seal(d.path())).expect("delete the pack");

    let report = doctor_report(d.path());
    let row = authority_row(&report, common::SEG_ID);
    assert_eq!(row["sealed"], true, "the segment IS footer-sealed");
    assert_eq!(row["serving"], "log-scan", "row: {row:#}");

    let f = finding(&report, "authority-accelerators");
    assert_eq!(f.severity, Severity::Info, "message: {}", f.message);
    assert_eq!(field(f, "served_by_log_scan"), 1);
    assert!(
        f.message.contains("SEALED"),
        "the message names which segments it counts: {}",
        f.message
    );
}

// ---------------------------------------------------------------------------
// B — owed re-seal vs genuinely lost
// ---------------------------------------------------------------------------

/// A withdrawn-awaiting-reseal segment (bn-3qh0's quarantine flow) is a `Warn`
/// that names its own convergence, and `verify` still exits clean.
#[test]
fn an_owed_reseal_is_a_warn_that_converges_at_the_next_open() {
    let d = mess_testkit::sweeping_temp_dir("bn3m62-owed");
    common::build_sealed_pack_store(d.path(), 8);
    mess_store::withdraw_sealed_index(
        &d.path().join("sealed"),
        common::SEG_ID,
        "bn-3m62 test",
    )
    .expect("withdraw");
    assert!(!common::seal(d.path()).exists(), "the pack is withdrawn");
    assert!(
        marker_path(d.path(), common::SEG_ID).exists(),
        "the quarantine slot is occupied"
    );

    let report = verify_report(d.path());
    let f = finding(&report, "seal-pack-reseal-pending");
    assert_eq!(f.severity, Severity::Warn, "message: {}", f.message);
    assert_eq!(field(f, "state"), "pending-reseal");
    assert_eq!(field(f, "converges"), "next-open");
    assert_eq!(field(f, "segment_id"), common::SEG_ID);
    assert_eq!(field(f, "fallback"), "raw-segment-scan");
    assert!(
        f.message.contains("next engine open")
            && f.message.contains("No operator action is required"),
        "the message must say it converges by itself: {}",
        f.message
    );

    // The Error arm must NOT also fire — one state, one finding.
    no_finding(&report, "seal-pack-missing");

    assert!(
        report.worst() < Severity::Error,
        "a converging re-seal is not a verify failure: worst = {:?}",
        report.worst()
    );
    assert_eq!(report.exit_code(), 0, "and it exits clean");
}

/// A pack that is simply **gone**, with nothing on disk asking for it back,
/// stays an `Error` — and now says why an operator has to act.
///
/// bn-3qh0's `never_reseals_a_deleted_pack_on_its_own` is the proof behind the
/// wording: three consecutive reopens refute nothing, queue nothing, and the
/// pack never returns.
#[test]
fn a_genuinely_lost_pack_is_still_an_error_and_names_the_remedy() {
    let d = mess_testkit::sweeping_temp_dir("bn3m62-lost");
    common::build_sealed_pack_store(d.path(), 8);
    std::fs::remove_file(common::seal(d.path())).expect("delete the pack");
    assert!(
        !marker_path(d.path(), common::SEG_ID).exists(),
        "precondition: nothing requests a re-seal"
    );

    let report = verify_report(d.path());
    let f = finding(&report, "seal-pack-missing");
    assert_eq!(f.severity, Severity::Error, "message: {}", f.message);
    assert_eq!(field(f, "state"), "lost");
    assert_eq!(field(f, "remedy"), "mess rebuild-index");
    assert_eq!(field(f, "fallback"), "raw-segment-scan");
    assert!(
        f.message.contains("will NOT restore it on its own"),
        "the message must not promise a repair that will not come: {}",
        f.message
    );

    no_finding(&report, "seal-pack-reseal-pending");
    assert_eq!(
        report.exit_code(),
        mess_cli::report::EXIT_FINDINGS,
        "a lost pack fails verify"
    );
}

/// A `.pidx.refuted` sibling counts as the segment's re-seal request too.
///
/// The engine's trigger is ANY `*.refuted` whose name parses to the segment id,
/// not specifically a `.seal.refuted` — bn-3qh0's pack path withdraws a
/// shadowed sidecar family alongside the pack, and a CLI that keyed on the
/// primary extension alone would report "lost" for a store the next open
/// converges. The two notions of "owed a re-seal" must be one notion.
#[test]
fn any_quarantine_slot_for_the_segment_counts_as_the_reseal_request() {
    let d = mess_testkit::sweeping_temp_dir("bn3m62-sibling-slot");
    common::build_sealed_pack_store(d.path(), 8);
    std::fs::remove_file(common::seal(d.path())).expect("delete the pack");
    // A quarantine slot for the same segment, under a different primary kind.
    std::fs::write(
        d.path()
            .join("sealed")
            .join(format!("seg-{:020}.pidx.refuted", common::SEG_ID)),
        b"refuted bytes",
    )
    .expect("write marker");

    let report = verify_report(d.path());
    let f = finding(&report, "seal-pack-reseal-pending");
    assert_eq!(f.severity, Severity::Warn);
    no_finding(&report, "seal-pack-missing");
}

/// The backup contract, pinned: a cut that copies the `.log` but not the
/// `.seal` restores a store that still fails `verify` at `Error`.
///
/// This is why the lost arm keeps its severity. `mess restore` gates on
/// `verify.worst() < Error`, and spec 07 §1 makes `seal-pack-missing` the
/// reason a cut must be identity-complete rather than merely data-complete. The
/// split cannot weaken the gate by construction, because `*.refuted` markers
/// are deliberately excluded from a cut — so a restored store never has a
/// quarantine slot and always lands in the Error arm.
#[test]
fn a_torn_cut_still_errors() {
    let src = mess_testkit::sweeping_temp_dir("bn3m62-cut-src");
    common::build_sealed_pack_store(src.path(), 8);
    // Belt and braces: give the SOURCE a quarantine slot, so the test would
    // fail loudly if a cut ever started carrying one.
    mess_store::withdraw_sealed_index(
        &src.path().join("sealed"),
        common::SEG_ID,
        "bn-3m62 test",
    )
    .expect("withdraw");

    // The torn cut: the `.log` (footer and all) and nothing under `sealed/`.
    let dst = mess_testkit::sweeping_temp_dir("bn3m62-cut-dst");
    std::fs::copy(
        store::log_path(src.path(), common::SEG_ID),
        store::log_path(dst.path(), common::SEG_ID),
    )
    .expect("copy the log");

    let report = verify_report(dst.path());
    let f = finding(&report, "seal-pack-missing");
    assert_eq!(
        f.severity,
        Severity::Error,
        "a data-complete but identity-incomplete cut must fail restore's \
         verify gate: {}",
        f.message
    );
    assert_eq!(field(f, "state"), "lost");
    assert!(
        report.worst() >= Severity::Error,
        "restore gates on worst() < Error"
    );
}

// ---------------------------------------------------------------------------
// doctor / verify consistency about one store
// ---------------------------------------------------------------------------

/// The same withdrawn store, described by both tools: both call it
/// `pending-reseal`, both say it converges at the next open, and neither treats
/// it as a failure.
#[test]
fn doctor_and_verify_agree_about_an_owed_reseal() {
    let d = mess_testkit::sweeping_temp_dir("bn3m62-agree-owed");
    common::build_sealed_pack_store(d.path(), 8);
    mess_store::withdraw_sealed_index(
        &d.path().join("sealed"),
        common::SEG_ID,
        "bn-3m62 test",
    )
    .expect("withdraw");

    let v = verify_report(d.path());
    let d_rep = doctor_report(d.path());

    let vf = finding(&v, "seal-pack-reseal-pending");
    let df = finding(&d_rep, "sidecar-missing");
    let dq = finding(&d_rep, "quarantined-candidate");

    assert_eq!(field(vf, "state"), "pending-reseal");
    assert_eq!(field(df, "state"), "pending-reseal");
    assert_eq!(field(dq, "state"), "pending-reseal");
    assert_eq!(field(vf, "converges"), "next-open");
    assert_eq!(field(df, "converges"), "next-open");

    assert_eq!(vf.severity, Severity::Warn);
    assert_eq!(df.severity, Severity::Warn);
    assert!(
        df.message.contains("quarantine slot")
            && df.message.contains("next engine open"),
        "doctor names the same mechanism verify does: {}",
        df.message
    );
    assert!(
        v.worst() < Severity::Error && d_rep.worst() < Severity::Error,
        "neither tool calls a converging state a failure"
    );
}

/// The same lost-pack store, described by both tools: both call it `lost` and
/// both name `mess rebuild-index`. They differ only where their jobs differ —
/// `verify` adds the non-zero exit that guards the backup contract.
///
/// Before this bone `doctor` told the operator this segment "is owed a
/// re-seal", which is exactly what it is not.
#[test]
fn doctor_and_verify_agree_about_a_lost_pack() {
    let d = mess_testkit::sweeping_temp_dir("bn3m62-agree-lost");
    common::build_sealed_pack_store(d.path(), 8);
    std::fs::remove_file(common::seal(d.path())).expect("delete the pack");

    let v = verify_report(d.path());
    let d_rep = doctor_report(d.path());

    let vf = finding(&v, "seal-pack-missing");
    let df = finding(&d_rep, "sidecar-missing");

    assert_eq!(field(vf, "state"), "lost");
    assert_eq!(field(df, "state"), "lost");
    assert_eq!(field(vf, "remedy"), "mess rebuild-index");
    assert_eq!(field(df, "remedy"), "mess rebuild-index");
    assert!(
        df.message.contains("will not rebuild the index on its own"),
        "doctor must not promise a re-seal nothing requested: {}",
        df.message
    );
    assert!(
        !df.message.contains("is owed a re-seal"),
        "the pre-bn-3m62 wording is gone: {}",
        df.message
    );
}

/// A healthy pack-sealed store is silent in both tools: no missing-pack finding
/// of either kind, and both reports clean.
#[test]
fn a_healthy_pack_store_reports_neither_finding() {
    let d = mess_testkit::sweeping_temp_dir("bn3m62-healthy");
    common::build_sealed_pack_store(d.path(), 8);

    let v = verify_report(d.path());
    no_finding(&v, "seal-pack-missing");
    no_finding(&v, "seal-pack-reseal-pending");
    assert_eq!(
        finding(&v, "seal-pack-identity-verified").severity,
        Severity::Ok
    );
    assert_eq!(v.exit_code(), 0);

    let d_rep = doctor_report(d.path());
    no_finding(&d_rep, "sidecar-missing");
    assert!(d_rep.worst() < Severity::Error);
}
