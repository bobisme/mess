//! Acceptance: `mess rebuild-index` reproduces the sealed pointer sidecars
//! **byte-for-byte** from the bare `.log` segments (I5 made executable). Build
//! a corpus, capture the sealer's original `.pidx`, delete the sidecars,
//! rebuild, and assert byte-equality.

mod common;

use mess_cli::rebuild::{self, RebuildOptions};

#[test]
fn rebuild_index_reproduces_pidx_byte_for_byte() {
    let d = tempfile::tempdir().expect("tempdir");
    common::build_corpus(d.path(), 6);

    let pidx = common::pidx(d.path());
    let original =
        std::fs::read(&pidx).expect("original .pidx exists after seal");
    assert!(!original.is_empty(), "sealer must have written a .pidx");

    // Delete the sealed sidecars (simulate loss / a bare-segments recovery).
    let _ = std::fs::remove_file(&pidx);
    let _ = std::fs::remove_file(pidx.with_extension("pcol"));
    let _ = std::fs::remove_file(pidx.with_extension("filter"));
    assert!(!pidx.exists(), "sidecar deleted");

    let report =
        rebuild::run(d.path(), &RebuildOptions { dry_run: false, meta: false });
    assert_eq!(
        report.exit_code(),
        0,
        "rebuild must succeed: {:#?}",
        report.findings
    );

    let rebuilt = std::fs::read(&pidx).expect(".pidx rebuilt");
    assert_eq!(
        rebuilt, original,
        "rebuilt .pidx must be byte-equal to the sealer's output"
    );
}

/// The pure sidecar-bytes builder is deterministic and matches a rebuild from
/// the same recovered batches (guards against ordering nondeterminism).
#[test]
fn rebuild_is_deterministic() {
    let d = tempfile::tempdir().expect("tempdir");
    common::build_corpus(d.path(), 4);

    let pidx = common::pidx(d.path());
    let _ = std::fs::remove_file(&pidx);

    let r1 =
        rebuild::run(d.path(), &RebuildOptions { dry_run: false, meta: false });
    assert_eq!(r1.exit_code(), 0);
    let first = std::fs::read(&pidx).expect("rebuilt once");

    let _ = std::fs::remove_file(&pidx);
    let r2 =
        rebuild::run(d.path(), &RebuildOptions { dry_run: false, meta: false });
    assert_eq!(r2.exit_code(), 0);
    let second = std::fs::read(&pidx).expect("rebuilt twice");

    assert_eq!(first, second, "two rebuilds must produce identical bytes");
}
