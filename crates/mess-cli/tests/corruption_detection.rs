//! Acceptance: `mess verify` MUST detect every corruption class the existing
//! harnesses inject. Each case builds a real sealed corpus, applies a
//! byte-level corruptor reused from the mess-log crash/torn harnesses
//! (flip/truncate) and the engine_reopen sidecar tests (overwrite/flip), then
//! asserts a non-zero exit and a typed finding of the expected kind.

mod common;

use mess_cli::report::{Report, Severity};
use mess_cli::verify::{self, VerifyOptions};
use mess_log::format::{HEADER_LEN, SEGMENT_HEADER_LEN};

/// Whether the report has an `Error` finding whose `kind` matches, or whose
/// `stop` field equals `kind` (the scan-stop class attached to a sealed-body
/// shortfall).
fn has_error(report: &Report, kind: &str) -> bool {
    report.findings.iter().any(|f| {
        f.severity == Severity::Error
            && (f.kind == kind
                || f.fields.get("stop").and_then(|v| v.as_str()) == Some(kind))
    })
}

fn tmp() -> mess_testkit::SweepingTempDir {
    mess_testkit::sweeping_temp_dir("cli-corruption-detection")
}

fn verify_full(dir: &std::path::Path) -> Report {
    verify::run(dir, &VerifyOptions { full: true, repair: false })
}

/// A clean, uncorrupted corpus verifies clean: exit 0, no error findings.
#[test]
fn clean_corpus_verifies_clean() {
    let d = tmp();
    common::build_corpus(d.path(), 5);
    common::seal_log_trailer(d.path());

    let report = verify_full(d.path());
    assert_eq!(
        report.exit_code(),
        0,
        "clean corpus must exit 0: {:#?}",
        report.findings
    );
    assert!(
        !report.findings.iter().any(|f| f.severity == Severity::Error),
        "clean corpus must have no error findings: {:#?}",
        report.findings
    );
}

/// Class 1: a flipped payload byte in the log body (crash-harness bit flip) —
/// the A4 batch CRC rejects the batch, the sealed body falls short of the
/// trailer, and verify reports a `batch-bad-crc` stop.
#[test]
fn detects_log_batch_crc_corruption() {
    let d = tmp();
    common::build_corpus(d.path(), 5);
    common::seal_log_trailer(d.path());

    // Flip a byte inside the FIRST batch's payload region (past both the
    // segment header and the batch header, so it is a pure payload flip).
    let off = (SEGMENT_HEADER_LEN + HEADER_LEN + 2) as u64;
    common::flip_byte(
        &mess_cli::store::log_path(d.path(), common::SEG_ID),
        off,
    );

    let report = verify_full(d.path());
    assert_ne!(report.exit_code(), 0, "log CRC corruption must exit non-zero");
    assert!(
        has_error(&report, "batch-bad-crc"),
        "expected a batch-bad-crc finding, got: {:#?}",
        report.findings
    );
}

/// Class 2: a flipped byte in the segment header (within its `header_crc`
/// coverage) — the header does not validate, so the segment holds no trusted
/// committed structure.
#[test]
fn detects_segment_header_corruption() {
    let d = tmp();
    common::build_corpus(d.path(), 4);
    common::seal_log_trailer(d.path());

    common::flip_byte(&mess_cli::store::log_path(d.path(), common::SEG_ID), 5);

    let report = verify_full(d.path());
    assert_ne!(report.exit_code(), 0, "header corruption must exit non-zero");
    assert!(
        has_error(&report, "segment-header-corrupt"),
        "expected segment-header-corrupt, got: {:#?}",
        report.findings
    );
}

/// Class 3: a wholesale garbage `.pidx` (the engine_reopen "not a real sidecar"
/// corruptor) — magic/CRC fail on open.
#[test]
fn detects_pidx_garbage() {
    let d = tmp();
    common::build_corpus(d.path(), 3);

    common::overwrite(
        &common::pidx(d.path()),
        b"not a real sidecar - fails magic/CRC",
    );

    let report = verify_full(d.path());
    assert_ne!(report.exit_code(), 0, "garbage .pidx must exit non-zero");
    assert!(
        has_error(&report, "pidx-corrupt"),
        "expected pidx-corrupt, got: {:#?}",
        report.findings
    );
}

/// Class 4: a single flipped byte in the `.pidx` content — the sidecar's
/// content CRC catches it.
#[test]
fn detects_pidx_crc_flip() {
    let d = tmp();
    common::build_corpus(d.path(), 3);

    // Flip a byte in the middle of the sidecar (a pointer/dir byte, covered by
    // the content CRC).
    let path = common::pidx(d.path());
    let len = std::fs::metadata(&path).unwrap().len();
    common::flip_byte(&path, len / 2);

    let report = verify_full(d.path());
    assert_ne!(report.exit_code(), 0, "flipped .pidx must exit non-zero");
    assert!(
        has_error(&report, "pidx-corrupt"),
        "expected pidx-corrupt, got: {:#?}",
        report.findings
    );
}

/// Class 5: a flipped byte in the `.pcol` payload sidecar — its content CRC
/// (or, under --full, block reassembly) catches it.
#[test]
fn detects_pcol_corruption() {
    let d = tmp();
    common::build_corpus(d.path(), 4);

    let path = common::pcol(d.path());
    let len = std::fs::metadata(&path).unwrap().len();
    common::flip_byte(&path, len / 2);

    let report = verify_full(d.path());
    assert_ne!(report.exit_code(), 0, "flipped .pcol must exit non-zero");
    assert!(
        report.findings.iter().any(|f| {
            f.severity == Severity::Error
                && (f.kind == "pcol-corrupt"
                    || f.kind == "pcol-reassembly-failed")
        }),
        "expected pcol-corrupt/pcol-reassembly-failed, got: {:#?}",
        report.findings
    );
}
