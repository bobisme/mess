//! Acceptance for `bn-w5my`: what `mess verify` says about a `.reg` registry
//! delta (`bn-26pp`).
//!
//! The delta is discardable acceleration — engine open point-reads the same
//! `$registry` batches out of the log whenever it cannot use one — so the
//! contract this file pins is the discardable law in three parts:
//!
//! 1. **Absence is silence.** No `.reg` is never a finding, whichever way a
//!    segment came by that state: a loose seal of a segment that registered
//!    nothing, an operator who deleted the file, or a pack-sealed segment,
//!    which by design (`bn-3h64`) carries its delta as a `REGISTRY_DELTA`
//!    section inside the `.seal` and has no sibling at all.
//! 2. **Corruption is reported and survivable.** A `.reg` that exists and is
//!    damaged or refused is an `Error` finding — the same severity a damaged
//!    `.pcol` gets — and the rest of the store still verifies clean around it.
//! 3. **The cross-check is the engine's own.** A delta that parses is admitted
//!    only if the segment's serving pointer directory vouches for its batch
//!    layout (`SealedSegmentIndex::accepts_registry_delta`), against a `.pidx`
//!    and against a `.seal` pack alike.

mod common;

use std::path::Path;

use mess_cli::report::{Finding, Report, Severity};
use mess_cli::store;
use mess_cli::verify::{self, VerifyOptions};

fn tmp(name: &str) -> mess_testkit::SweepingTempDir {
    mess_testkit::sweeping_temp_dir(name)
}

fn verify_full(dir: &Path) -> Report {
    verify::run(dir, &VerifyOptions { full: true, repair: false })
}

/// The registry-delta pass's findings (its `check` is `registry-delta`).
fn reg_findings(report: &Report) -> Vec<&Finding> {
    report.findings.iter().filter(|f| f.check == "registry-delta").collect()
}

fn kinds(report: &Report) -> Vec<&str> {
    report.findings.iter().map(|f| f.kind.as_str()).collect()
}

/// The `.reg` of the single-segment corpus.
fn reg(dir: &Path) -> std::path::PathBuf {
    store::reg_path(dir, common::SEG_ID)
}

/// A healthy loose-sealed corpus: the `.reg` its seal wrote passes its own CRC
/// **and** the layout cross-check against the `.pidx` that was written with it.
#[test]
fn healthy_reg_verifies_against_the_pointer_directory() {
    let d = tmp("cli-reg-verify-healthy");
    common::build_corpus(d.path(), 5);
    common::seal_log_trailer(d.path());
    assert!(reg(d.path()).exists(), "a loose seal writes the .reg");

    let report = verify_full(d.path());
    assert_eq!(
        report.exit_code(),
        0,
        "healthy store verifies clean: {:#?}",
        report.findings
    );

    let found = reg_findings(&report);
    assert_eq!(
        found.len(),
        1,
        "exactly one registry-delta finding: {found:#?}"
    );
    assert_eq!(found[0].severity, Severity::Ok);
    assert_eq!(found[0].kind, "reg-verified");
    assert_eq!(
        found[0].fields.get("cross_checked").and_then(|v| v.as_bool()),
        Some(true),
        "the pointer directory vouched for it: {:#?}",
        found[0]
    );
    assert_eq!(
        found[0].fields.get("segment_id").and_then(|v| v.as_u64()),
        Some(common::SEG_ID)
    );
}

/// A single flipped byte inside the `.reg` — its end-to-end `crc32c` catches
/// it. The finding is an `Error` (a damaged `.pcol` is no different), and it is
/// the ONLY error: the log, the trailer cross-check, the `.pidx` and the
/// `.pcol` all still verify, because a registry delta was never load-bearing
/// for any of them.
#[test]
fn corrupt_reg_is_reported_and_the_rest_of_the_store_still_verifies() {
    let d = tmp("cli-reg-verify-corrupt");
    common::build_corpus(d.path(), 5);
    common::seal_log_trailer(d.path());

    let path = reg(d.path());
    let len = std::fs::metadata(&path).expect("stat .reg").len();
    common::flip_byte(&path, len / 2);

    let report = verify_full(d.path());
    assert_ne!(report.exit_code(), 0, "a damaged .reg is reported");

    let errors: Vec<&Finding> = report
        .findings
        .iter()
        .filter(|f| f.severity == Severity::Error)
        .collect();
    assert_eq!(
        errors.len(),
        1,
        "the .reg is the only damage: {:#?}",
        report.findings
    );
    assert_eq!(errors[0].kind, "reg-corrupt");
    assert_eq!(errors[0].check, "registry-delta");
    assert_eq!(
        errors[0].fields.get("fallback").and_then(|v| v.as_str()),
        Some("registry-point-read"),
        "the finding names what a reader does instead"
    );

    // The store around it is intact — same passes as a clean corpus.
    for want in ["sealed-segment-verified", "pidx-verified", "pcol-reassembled"]
    {
        assert!(
            kinds(&report).contains(&want),
            "{want} still passes: {:#?}",
            report.findings
        );
    }
}

/// Wholesale garbage in place of the file (the engine_reopen sidecar
/// corruptor): magic, not CRC, is what rejects it — still `reg-corrupt`.
#[test]
fn garbage_reg_is_reported() {
    let d = tmp("cli-reg-verify-garbage");
    common::build_corpus(d.path(), 4);
    common::seal_log_trailer(d.path());

    common::overwrite(&reg(d.path()), b"not a registry delta - bad magic");

    let report = verify_full(d.path());
    assert_ne!(report.exit_code(), 0);
    assert!(
        reg_findings(&report)
            .iter()
            .any(|f| f.kind == "reg-corrupt" && f.severity == Severity::Error),
        "expected reg-corrupt: {:#?}",
        report.findings
    );
}

/// A truncated `.reg` — a crash between `write` and `rename` could never leave
/// one (the sidecar is renamed into place whole), but a damaged filesystem can.
/// Bounded parsing rejects it.
#[test]
fn truncated_reg_is_reported() {
    let d = tmp("cli-reg-verify-truncated");
    common::build_corpus(d.path(), 4);
    common::seal_log_trailer(d.path());

    let path = reg(d.path());
    let len = std::fs::metadata(&path).expect("stat .reg").len();
    common::truncate(&path, len / 2);

    let report = verify_full(d.path());
    assert_ne!(report.exit_code(), 0);
    assert!(
        reg_findings(&report).iter().any(|f| f.kind == "reg-corrupt"),
        "expected reg-corrupt: {:#?}",
        report.findings
    );
}

/// Something occupying the `.reg` name that cannot be read as a file at all (a
/// directory, here — the portable way to force the error without depending on
/// permissions) is an I/O report, distinct from a corrupt image.
#[test]
fn unreadable_reg_is_reported() {
    let d = tmp("cli-reg-verify-io");
    common::build_corpus(d.path(), 4);
    common::seal_log_trailer(d.path());

    let path = reg(d.path());
    std::fs::remove_file(&path).expect("remove the .reg");
    std::fs::create_dir(&path).expect("occupy the name");

    let report = verify_full(d.path());
    assert_ne!(report.exit_code(), 0);
    let found = reg_findings(&report);
    assert_eq!(found.len(), 1, "{found:#?}");
    assert_eq!(found[0].kind, "reg-io");
    assert_eq!(found[0].severity, Severity::Error);
}

/// When the segment's own pointer sidecar is the damaged thing, the `.reg` is
/// not blamed for it: the layout step is skipped (there is no trustworthy
/// directory to compare against), the delta reports only what it can prove
/// about itself, and the one error in the report is the `.pidx`'s.
#[test]
fn a_damaged_pointer_sidecar_is_not_charged_to_the_delta() {
    let d = tmp("cli-reg-verify-bad-index");
    common::build_corpus(d.path(), 5);
    common::seal_log_trailer(d.path());
    common::overwrite(&common::pidx(d.path()), b"not a real sidecar");

    let report = verify_full(d.path());
    let found = reg_findings(&report);
    assert_eq!(found.len(), 1, "{found:#?}");
    assert_eq!(found[0].kind, "reg-verified");
    assert_eq!(found[0].severity, Severity::Ok);
    assert_eq!(
        found[0].fields.get("cross_checked").and_then(|v| v.as_bool()),
        Some(false),
        "no directory to cross-check against: {:#?}",
        found[0]
    );
    let errors: Vec<&str> = report
        .findings
        .iter()
        .filter(|f| f.severity == Severity::Error)
        .map(|f| f.kind.as_str())
        .collect();
    assert_eq!(errors, vec!["pidx-corrupt"], "{:#?}", report.findings);
}

/// A structurally perfect delta from **another store's** segment: it passes its
/// own CRC and every internal invariant, and the pointer directory refuses it
/// anyway because its `event_count` describes a different span. This is the
/// check that makes the delta unable to invent, drop, or move a batch.
#[test]
fn foreign_reg_is_reported_as_a_layout_mismatch() {
    let donor = tmp("cli-reg-verify-donor");
    let d = tmp("cli-reg-verify-foreign");
    // Same segment id and base_pos, different number of events.
    common::build_corpus(donor.path(), 9);
    common::build_corpus(d.path(), 4);
    common::seal_log_trailer(d.path());

    std::fs::copy(reg(donor.path()), reg(d.path())).expect("plant the donor");

    let report = verify_full(d.path());
    assert_ne!(report.exit_code(), 0, "a refused delta is reported");

    let found = reg_findings(&report);
    assert_eq!(found.len(), 1, "{found:#?}");
    assert_eq!(found[0].severity, Severity::Error);
    assert_eq!(found[0].kind, "reg-layout-mismatch");
    assert_eq!(
        found[0].fields.get("fallback").and_then(|v| v.as_str()),
        Some("registry-point-read")
    );
    // The store itself is untouched — only the accelerator is refused.
    assert!(
        kinds(&report).contains(&"sealed-segment-verified"),
        "the segment still matches its trailer: {:#?}",
        report.findings
    );
}

/// Absence is silence: a loose-sealed store whose `.reg` was deleted verifies
/// exactly as clean as one that never had a delta at all. Nothing reports it,
/// at any severity.
#[test]
fn absent_reg_is_silent() {
    let d = tmp("cli-reg-verify-absent");
    common::build_corpus(d.path(), 5);
    common::seal_log_trailer(d.path());
    std::fs::remove_file(reg(d.path())).expect("delete the .reg");

    let report = verify_full(d.path());
    assert_eq!(
        report.exit_code(),
        0,
        "a missing accelerator is not a problem: {:#?}",
        report.findings
    );
    assert!(
        reg_findings(&report).is_empty(),
        "absence is silence, not even an Info: {:#?}",
        report.findings
    );
}

/// A pack-sealed segment (bn-3h64) has NO sibling `.reg` by design — its delta
/// is a `REGISTRY_DELTA` section inside the `.seal`, covered by the whole-image
/// trailer hash `verify_seal_pack` recomputes. `verify` must not read that
/// absence as damage.
#[test]
fn pack_sealed_segment_without_a_reg_is_silent() {
    let d = tmp("cli-reg-verify-pack");
    common::build_sealed_pack_store(d.path(), 5);
    assert!(!reg(d.path()).exists(), "bn-3h64: a pack gets no sibling .reg");

    let report = verify_full(d.path());
    assert_eq!(
        report.exit_code(),
        0,
        "pack store verifies clean: {:#?}",
        report.findings
    );
    assert!(
        reg_findings(&report).is_empty(),
        "no .reg to report on: {:#?}",
        report.findings
    );
    // The pack itself WAS verified — this is silence about the sibling, not a
    // hole in the coverage of the delta's actual container.
    assert!(
        kinds(&report).contains(&"seal-pack-verified"),
        "the pack (delta section included) was verified: {:#?}",
        report.findings
    );
}

/// A `.reg` that *does* sit next to a pack-sealed segment is cross-checked
/// against the **pack's** pointer directory, not ignored — engine open reaches
/// for the sibling file whenever the pack has no delta section of its own, so
/// verify judges it the same way. Same appends on both sides, so the pack
/// vouches for the loose store's delta.
#[test]
fn a_reg_beside_a_pack_is_cross_checked_against_the_pack() {
    let donor = tmp("cli-reg-verify-pack-donor");
    let d = tmp("cli-reg-verify-pack-sibling");
    common::build_corpus(donor.path(), 5);
    common::build_sealed_pack_store(d.path(), 5);

    std::fs::copy(reg(donor.path()), reg(d.path())).expect("plant the delta");

    let report = verify_full(d.path());
    let found = reg_findings(&report);
    assert_eq!(found.len(), 1, "{found:#?}");
    assert_eq!(
        found[0].kind, "reg-verified",
        "the pack's own directory vouches for it: {:#?}",
        found[0]
    );
    assert_eq!(
        found[0].fields.get("cross_checked").and_then(|v| v.as_bool()),
        Some(true)
    );
    assert_eq!(report.exit_code(), 0, "{:#?}", report.findings);
}
