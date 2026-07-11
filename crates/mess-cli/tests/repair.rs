//! Acceptance (bn-2za): `mess verify --repair` reconstructs deliberately
//! corrupted blocks of a sealed segment from its Reed-Solomon `.par` parity
//! sidecar, and refuses safely when it cannot.
//!
//! Four shapes, per the bone:
//! 1. Corrupt N blocks within tolerance ⇒ repair restores byte-exact (and a
//!    follow-up `verify --full` is green).
//! 2. Corruption beyond tolerance ⇒ typed refusal, originals untouched.
//! 3. The parity sidecar itself corrupted ⇒ detected, no repair attempted.
//! 4. Parity generation is deterministic (same segment ⇒ same `.par`).

mod common;

use mess_cli::report::{Report, Severity};
use mess_cli::verify::{self, VerifyOptions};

fn tmp() -> mess_testkit::SweepingTempDir {
    mess_testkit::sweeping_temp_dir("cli-repair")
}

fn repair_run(dir: &std::path::Path) -> Report {
    verify::run(dir, &VerifyOptions { full: true, repair: true })
}

fn has_error(report: &Report, kind: &str) -> bool {
    report
        .findings
        .iter()
        .any(|f| f.severity == Severity::Error && f.kind == kind)
}

fn find_ok<'a>(
    report: &'a Report,
    kind: &str,
) -> Option<&'a mess_cli::report::Finding> {
    report
        .findings
        .iter()
        .find(|f| f.severity == Severity::Ok && f.kind == kind)
}

/// Byte offset of shard `i` for the small test config (64-byte shards).
fn shard_off(i: u64) -> u64 { i * 64 }

/// Build a sealed corpus with a parity sidecar; return the temp dir and the
/// pristine sealed `.log` bytes for byte-exactness assertions.
fn sealed_corpus_with_parity(
    n_batches: u64,
) -> (mess_testkit::SweepingTempDir, Vec<u8>) {
    let d = tmp();
    common::build_corpus(d.path(), n_batches);
    common::seal_log_trailer(d.path());
    common::write_parity(d.path(), common::parity_test_cfg());
    let pristine =
        std::fs::read(mess_cli::store::log_path(d.path(), common::SEG_ID))
            .unwrap();
    (d, pristine)
}

/// Shape 1: two damaged blocks in one group (M=2, the tolerance edge) are
/// reconstructed byte-exact, and the follow-up full verify is clean.
#[test]
fn repairs_within_tolerance_and_reverifies_green() {
    let (d, pristine) = sealed_corpus_with_parity(5);
    let log = mess_cli::store::log_path(d.path(), common::SEG_ID);

    // Damage shards 1 and 2 (both in group 0 = shards 0..3), within M=2.
    common::flip_byte(&log, shard_off(1) + 5);
    common::flip_byte(&log, shard_off(2) + 9);
    assert_ne!(std::fs::read(&log).unwrap(), pristine, "corruption must land");

    let report = repair_run(d.path());
    let repaired = find_ok(&report, "repaired").unwrap_or_else(|| {
        panic!("a `repaired` finding, got: {:#?}", report.findings)
    });
    let blocks =
        repaired.fields.get("repaired_blocks").expect("repaired_blocks field");
    assert_eq!(
        blocks,
        &serde_json::json!([1, 2]),
        "must name exactly blocks 1 and 2"
    );

    // Byte-exact restoration.
    assert_eq!(
        std::fs::read(&log).unwrap(),
        pristine,
        "repaired .log must be byte-exact"
    );

    // The damaged original is preserved, and the exit is clean.
    let damaged: Vec<_> = std::fs::read_dir(d.path())
        .unwrap()
        .flatten()
        .filter(|e| e.file_name().to_string_lossy().contains(".damaged-"))
        .collect();
    assert_eq!(
        damaged.len(),
        1,
        "exactly one .damaged-<ts> backup must remain"
    );

    // A fresh full verify (no repair) over the now-repaired store is green:
    // the repair restored the committed bytes exactly.
    let after =
        verify::run(d.path(), &VerifyOptions { full: true, repair: false });
    assert_eq!(
        after.exit_code(),
        0,
        "post-repair verify must be green: {:#?}",
        after.findings
    );
    assert!(!after.findings.iter().any(|f| f.severity == Severity::Error));
}

/// Shape 2: three damaged blocks in one group exceed the M=2 budget — verify
/// refuses with a typed finding and leaves the segment untouched.
#[test]
fn beyond_tolerance_refuses_and_leaves_originals() {
    let (d, _pristine) = sealed_corpus_with_parity(6);
    let log = mess_cli::store::log_path(d.path(), common::SEG_ID);

    // Damage shards 1, 2, 3 — all in group 0, so 3 > M=2.
    common::flip_byte(&log, shard_off(1) + 3);
    common::flip_byte(&log, shard_off(2) + 3);
    common::flip_byte(&log, shard_off(3) + 3);
    let damaged_bytes = std::fs::read(&log).unwrap();

    let report = repair_run(d.path());
    assert!(
        has_error(&report, "repair-beyond-tolerance"),
        "expected repair-beyond-tolerance, got: {:#?}",
        report.findings
    );
    assert_ne!(report.exit_code(), 0, "beyond-tolerance must exit non-zero");

    // Originals untouched: the .log is exactly the damaged bytes, no backup
    // made.
    assert_eq!(
        std::fs::read(&log).unwrap(),
        damaged_bytes,
        "originals must be untouched"
    );
    let backups = std::fs::read_dir(d.path())
        .unwrap()
        .flatten()
        .filter(|e| e.file_name().to_string_lossy().contains(".damaged-"))
        .count();
    assert_eq!(backups, 0, "no repair ⇒ no .damaged backup");
}

/// Shape 3: the parity sidecar is itself corrupted (a flip inside its
/// content-CRC coverage). Repair detects it and does not touch the segment.
#[test]
fn corrupt_parity_sidecar_detected_no_repair() {
    let (d, _pristine) = sealed_corpus_with_parity(5);
    let log = mess_cli::store::log_path(d.path(), common::SEG_ID);

    // Also damage the segment so a repair *would* otherwise be attempted.
    common::flip_byte(&log, shard_off(1) + 4);
    let damaged_bytes = std::fs::read(&log).unwrap();

    // Corrupt the parity sidecar in its parity blob (covered by the self CRC).
    let par = common::par(d.path());
    let plen = std::fs::metadata(&par).unwrap().len();
    common::flip_byte(&par, plen - 8); // inside content, before the trailing CRC

    let report = repair_run(d.path());
    assert!(
        has_error(&report, "par-corrupt"),
        "expected par-corrupt, got: {:#?}",
        report.findings
    );
    assert_ne!(report.exit_code(), 0);
    // The damaged segment is left exactly as-is (no repair attempted).
    assert_eq!(
        std::fs::read(&log).unwrap(),
        damaged_bytes,
        "segment must be untouched"
    );
}

/// Shape 4: parity generation is deterministic — regenerating over the same
/// sealed bytes yields a byte-identical `.par`.
#[test]
fn parity_generation_is_deterministic() {
    let d = tmp();
    common::build_corpus(d.path(), 5);
    common::seal_log_trailer(d.path());

    common::write_parity(d.path(), common::parity_test_cfg());
    let first = std::fs::read(common::par(d.path())).unwrap();
    // Regenerate over the identical (untouched) sealed bytes.
    common::write_parity(d.path(), common::parity_test_cfg());
    let second = std::fs::read(common::par(d.path())).unwrap();
    assert_eq!(first, second, "same segment must produce byte-identical .par");
}
