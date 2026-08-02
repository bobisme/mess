//! Acceptance for `bn-2ln` online backup + restore (doc 07):
//!
//! 1. Backup taken while a writer is actively appending restores to a store
//!    that passes `verify --full`, with events after the cut cleanly excluded.
//! 2. A crash artifact (torn tail beyond the watermark) never enters the cut;
//!    the restore is clean (cut semantics hold).
//! 3. Incremental copies only the new sealed segment.
//! 4. A torn backup (missing `BACKUP_MANIFEST`) makes restore refuse.
//! 5. A retention lease blocks a segment's deletion during a backup and
//!    releases it after.
//! 6. `bn-w5my`: the `.reg` registry delta rides the cut, so the restored store
//!    opens on the accelerated `$registry` path instead of point-reading.

mod common;

use std::path::Path;

use mess_cli::backup::{self, BACKUP_MANIFEST, BackupOptions};
use mess_cli::report::Report;
use mess_cli::restore::{self, RestoreOptions};
use mess_cli::{lease, retention, store};
use mess_store::backend::{Backend, RecordToAppend};
use mess_store::engine::{EngineOptions, LogEngine};
use mess_store::version::Version;
use serde_json::Value;

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime")
}

fn rec(data: &[u8]) -> RecordToAppend {
    RecordToAppend {
        message_type: "account.happened".into(),
        data:         data.to_vec(),
    }
}

fn small_engine(dir: &Path) -> LogEngine {
    LogEngine::open_with(
        dir,
        EngineOptions { segment_size: 1 << 20, ..EngineOptions::default() },
    )
    .expect("open engine")
}

/// A finding of the given `kind` is present.
fn has_kind(report: &Report, kind: &str) -> bool {
    report.findings.iter().any(|f| f.kind == kind)
}

fn field<'a>(json: &'a Value, key: &str) -> &'a Value { &json[key] }

/// Shape 1: backup while a writer is live (lock held, still appending), then
/// the writer keeps appending BEYOND the cut. The restored store passes
/// `verify --full`, its recovered watermark equals the cut watermark, and it
/// contains exactly the events committed before the cut — the later ones are
/// cleanly beyond it.
#[test]
fn backup_during_active_writing_restores_to_the_cut() {
    let src = mess_testkit::sweeping_temp_dir("cli-backup-restore-src");
    let dest = mess_testkit::sweeping_temp_dir("cli-backup-restore-dest");
    let restored =
        mess_testkit::sweeping_temp_dir("cli-backup-restore-restored");

    let rt = runtime();
    let backup_json = rt.block_on(async {
        let engine = small_engine(src.path());
        for i in 0..10u64 {
            let expected =
                if i == 0 { Version::NoStream } else { Version::At(i - 1) };
            engine
                .append_batch(
                    "acct-1",
                    expected,
                    &[rec(format!("e{i}").as_bytes())],
                )
                .await
                .expect("append");
        }
        // The writer is LIVE (engine open, lock held) while the backup runs.
        let report =
            backup::run(src.path(), dest.path(), &BackupOptions::default());
        assert_eq!(
            report.exit_code(),
            0,
            "backup should succeed: {:?}",
            report.findings
        );
        let json = report.to_json();
        // The writer keeps appending AFTER the cut — these must not appear in
        // the backup.
        for i in 10..15u64 {
            engine
                .append_batch(
                    "acct-1",
                    Version::At(i - 1),
                    &[rec(format!("e{i}").as_bytes())],
                )
                .await
                .expect("append past cut");
        }
        assert_eq!(
            engine.total_events(),
            15 + common::REGISTRY_EVENTS,
            "source now has 15 user events (plus the $registry records for \
             its one stream name and one type name)"
        );
        drop(engine);
        json
    });

    let watermark =
        field(&backup_json, "watermark").as_u64().expect("watermark");
    assert_eq!(
        watermark,
        10 + common::REGISTRY_EVENTS as u64,
        "cut watermark is the 10 events acked before the cut (+ $registry)"
    );
    assert!(dest.path().join(BACKUP_MANIFEST).exists(), "manifest written");

    // Restore into a fresh dir: copy back + full recovery + verify --full.
    let rr =
        restore::run(dest.path(), restored.path(), &RestoreOptions::default());
    assert_eq!(rr.exit_code(), 0, "restore should be clean: {:?}", rr.findings);
    let rj = rr.to_json();
    assert_eq!(
        field(&rj, "verified"),
        &Value::Bool(true),
        "verify --full clean"
    );
    assert_eq!(
        field(&rj, "recovered_watermark").as_u64(),
        Some(10 + common::REGISTRY_EVENTS as u64)
    );
    assert!(has_kind(&rr, "restore-complete"));

    // The restored store recovers exactly the 10 pre-cut events; the 5 events
    // the writer appended after the cut are cleanly excluded.
    runtime().block_on(async {
        let engine = small_engine(restored.path());
        assert_eq!(
            engine.total_events(),
            10 + common::REGISTRY_EVENTS,
            "restored store holds only the pre-cut events"
        );
        assert_eq!(
            engine.metrics().durable_watermark,
            10 + common::REGISTRY_EVENTS as u64
        );
    });
}

/// Shape 2 (crash semantics): a torn tail on the active segment — the artifact
/// a writer that crashed mid-append leaves — is beyond the durable watermark
/// and never enters the cut. The restore is clean regardless.
#[test]
fn torn_tail_from_a_crashed_writer_is_excluded_from_the_cut() {
    let src = mess_testkit::sweeping_temp_dir("cli-backup-restore-src-1");
    let dest = mess_testkit::sweeping_temp_dir("cli-backup-restore-dest-1");
    let restored =
        mess_testkit::sweeping_temp_dir("cli-backup-restore-restored-1");

    // Build a clean corpus, then append garbage past the committed prefix to
    // simulate a crash mid-write.
    common::build_corpus(src.path(), 8);
    let log = store::log_path(src.path(), common::SEG_ID);
    let scan =
        mess_cli::scan::scan_segment(common::SEG_ID, &log).expect("scan");
    let safe = scan.recovery.safe_offset;
    // Overwrite bytes just past the watermark with a torn/garbage batch header.
    {
        use std::io::{Seek, SeekFrom, Write};
        let mut f = std::fs::OpenOptions::new()
            .write(true)
            .open(&log)
            .expect("open log");
        f.seek(SeekFrom::Start(safe)).expect("seek");
        f.write_all(&[0xAB; 64]).expect("write torn tail");
        f.sync_all().expect("sync");
    }

    let report =
        backup::run(src.path(), dest.path(), &BackupOptions::default());
    assert_eq!(
        report.exit_code(),
        0,
        "backup ignores the torn tail: {:?}",
        report.findings
    );
    let watermark = report.to_json()["watermark"].as_u64().expect("watermark");
    assert_eq!(
        watermark,
        8 + common::REGISTRY_EVENTS as u64,
        "cut watermark is the 8 committed events (+ $registry), tail excluded"
    );

    let rr =
        restore::run(dest.path(), restored.path(), &RestoreOptions::default());
    assert_eq!(
        rr.exit_code(),
        0,
        "restore clean despite the crash artifact: {:?}",
        rr.findings
    );
    assert_eq!(
        rr.to_json()["recovered_watermark"].as_u64(),
        Some(8 + common::REGISTRY_EVENTS as u64)
    );
}

/// Shape 3: incremental backup copies only the segment that is new at the
/// destination; segments already present (matching size + CRC) are skipped.
#[test]
fn incremental_copies_only_new_segments() {
    let src = mess_testkit::sweeping_temp_dir("cli-backup-restore-src-2");
    let dest = mess_testkit::sweeping_temp_dir("cli-backup-restore-dest-2");

    // A sealed corpus (seg-1 gets a durable trailer so it is a content-stable
    // sealed segment, plus its .pidx/.pcol/.filter sidecars).
    common::build_corpus(src.path(), 5);
    common::seal_log_trailer(src.path());

    // How many segment files (role sealed|sidecar) were actually copied.
    let copied_segment_files = |r: &Report| -> usize {
        r.collection
            .iter()
            .filter(|row| {
                let role = row["role"].as_str().unwrap_or("");
                (role == "sealed" || role == "sidecar")
                    && row["action"] == "copied"
            })
            .count()
    };

    // Full backup copies seg-1 and its sidecars.
    let full = backup::run(
        src.path(),
        dest.path(),
        &BackupOptions { incremental: false },
    );
    assert_eq!(full.exit_code(), 0);
    assert!(copied_segment_files(&full) >= 1, "full copies the sealed segment");

    // Re-running incrementally with nothing changed copies no segment files
    // (every one is content-stable and already present with matching size +
    // CRC). Meta may still be refreshed; segments must not be.
    let noop = backup::run(
        src.path(),
        dest.path(),
        &BackupOptions { incremental: true },
    );
    assert_eq!(noop.exit_code(), 0);
    assert_eq!(
        copied_segment_files(&noop),
        0,
        "no segment re-copied when nothing changed"
    );
    assert!(
        noop.to_json()["skipped_files"].as_u64().unwrap() >= 1,
        "prior segment files skipped"
    );

    // Introduce a second sealed segment at the source (a byte-copy of seg-1's
    // immutable files under id 2 — self-describing sealed content).
    add_sealed_segment_copy(src.path(), common::SEG_ID, 2);

    let incr = backup::run(
        src.path(),
        dest.path(),
        &BackupOptions { incremental: true },
    );
    assert_eq!(incr.exit_code(), 0);
    assert!(copied_segment_files(&incr) >= 1, "seg-2 files copied");
    // Every *segment* file that was actually copied belongs to the new segment
    // (id 2); every seg-1 segment file was skipped.
    for row in &incr.collection {
        let role = row["role"].as_str().unwrap_or("");
        if (role == "sealed" || role == "sidecar") && row["action"] == "copied"
        {
            let path = row["path"].as_str().unwrap();
            assert!(
                path.contains("0002"),
                "only seg-2 segment files copied: {path}"
            );
        }
    }
}

/// Shape 4: a torn backup — the manifest (written last) is absent — makes
/// restore refuse, because its absence proves the copy never finished.
#[test]
fn missing_manifest_makes_restore_refuse() {
    let src = mess_testkit::sweeping_temp_dir("cli-backup-restore-src-3");
    let dest = mess_testkit::sweeping_temp_dir("cli-backup-restore-dest-3");
    let restored =
        mess_testkit::sweeping_temp_dir("cli-backup-restore-restored-2");

    common::build_corpus(src.path(), 4);
    let report =
        backup::run(src.path(), dest.path(), &BackupOptions::default());
    assert_eq!(report.exit_code(), 0);

    // Simulate a backup that crashed before the manifest landed.
    std::fs::remove_file(dest.path().join(BACKUP_MANIFEST))
        .expect("remove manifest");

    let rr =
        restore::run(dest.path(), restored.path(), &RestoreOptions::default());
    assert_ne!(rr.exit_code(), 0, "restore must refuse a torn backup");
    assert!(
        has_kind(&rr, "torn-backup"),
        "surfaces torn-backup: {:?}",
        rr.findings
    );
}

/// Restore refuses to overwrite a non-empty target directory.
#[test]
fn restore_refuses_non_empty_target() {
    let src = mess_testkit::sweeping_temp_dir("cli-backup-restore-src-4");
    let dest = mess_testkit::sweeping_temp_dir("cli-backup-restore-dest-4");
    let target = mess_testkit::sweeping_temp_dir("cli-backup-restore-target");

    common::build_corpus(src.path(), 3);
    backup::run(src.path(), dest.path(), &BackupOptions::default());
    std::fs::write(target.path().join("existing"), b"do not clobber")
        .expect("seed target");

    let rr =
        restore::run(dest.path(), target.path(), &RestoreOptions::default());
    assert_ne!(rr.exit_code(), 0);
    assert!(has_kind(&rr, "target-not-empty"));
}

/// Shape 5: a backup lease pins a sealed segment against retention while the
/// backup is running, and releases it when the lease is dropped.
#[test]
fn retention_lease_blocks_deletion_during_backup_and_releases_after() {
    let src = mess_testkit::sweeping_temp_dir("cli-backup-restore-src-5");
    common::build_corpus(src.path(), 5); // seg-1 has sidecars (retention-visible)

    // No snapshots, no lease -> seg-1 is deletable.
    let before = retention::run(src.path());
    let seg_row = |r: &Report| -> Value {
        r.collection
            .iter()
            .find(|row| row["segment_id"] == common::SEG_ID)
            .cloned()
            .expect("seg row")
    };
    assert_eq!(seg_row(&before)["verdict"], "deletable");

    // Hold a lease covering seg-1 (as `mess backup` does for its cut).
    let guard =
        lease::acquire(src.path(), "b-test", 1, 1, 5, lease::DEFAULT_TTL_SECS)
            .expect("acquire lease");
    let during = retention::run(src.path());
    assert_eq!(
        seg_row(&during)["verdict"],
        "blocked",
        "lease pins the segment"
    );
    assert!(
        has_kind(&during, "lease-hold"),
        "lease-hold surfaced: {:?}",
        during.findings
    );
    let lease_blockers = &seg_row(&during)["lease_blockers"];
    assert_eq!(lease_blockers.as_array().map(|a| a.len()), Some(1));

    // Release the lease -> seg-1 is deletable again.
    guard.release().expect("release lease");
    let after = retention::run(src.path());
    assert_eq!(
        seg_row(&after)["verdict"],
        "deletable",
        "released lease unblocks"
    );
    assert!(!has_kind(&after, "lease-hold"));
}

/// Shape 6 (bn-w5my): the `.reg` registry delta (bn-26pp) rides the cut with
/// the rest of the `.pidx` family, and the restored store is therefore *fast*,
/// not merely correct.
///
/// The proof is the engine's own counter (bn-11ba): opening the restored store
/// reports `registry_delta_admitted == 1` and no fallback, i.e. recovery folded
/// `$registry` out of the copied delta. Deleting the delta from a second
/// restore of the SAME backup flips exactly that pair — `admitted == 0`,
/// `fallback == 1` — with an identical event count, which is the discardable
/// law stated as a measurement: leaving the file out costs speed and nothing
/// else.
#[test]
fn registry_delta_rides_the_cut_and_the_restore_admits_it() {
    let src = mess_testkit::sweeping_temp_dir("cli-backup-reg-src");
    let dest = mess_testkit::sweeping_temp_dir("cli-backup-reg-dest");
    let fast = mess_testkit::sweeping_temp_dir("cli-backup-reg-fast");
    let slow = mess_testkit::sweeping_temp_dir("cli-backup-reg-slow");

    // A store whose log actually ROLLS: the engine only takes the
    // sidecar-trusted path (the one that reads a delta) for a sealed,
    // non-head segment, so a single-segment corpus could never exercise it.
    // Only seg-1 carries a `.reg` — registrations happen once per name, ever.
    build_rolling_sidecar_store(src.path());
    let reg = store::reg_path(src.path(), 1);
    assert!(reg.exists(), "the rolled+sealed seg-1 wrote a .reg");

    let report =
        backup::run(src.path(), dest.path(), &BackupOptions::default());
    assert_eq!(report.exit_code(), 0, "backup clean: {:?}", report.findings);

    // It is in the cut, under the same `sidecar` role as `.pcol`/`.filter`.
    let reg_rows: Vec<&Value> = report
        .collection
        .iter()
        .filter(|row| row["path"].as_str().is_some_and(|p| p.ends_with(".reg")))
        .collect();
    assert_eq!(
        reg_rows.len(),
        1,
        "one .reg in the cut: {:?}",
        report.collection
    );
    assert_eq!(reg_rows[0]["role"], "sidecar");
    assert_eq!(reg_rows[0]["action"], "copied");
    // ...and named in the manifest the restore verifies against.
    let manifest: backup::BackupManifest = serde_json::from_slice(
        &std::fs::read(dest.path().join(BACKUP_MANIFEST)).expect("manifest"),
    )
    .expect("manifest parses");
    assert!(
        manifest.files.iter().any(|f| f.path.ends_with(".reg")
            && f.role == "sidecar"
            && f.len > 0),
        "manifest lists the .reg: {:?}",
        manifest.files
    );

    let rr = restore::run(dest.path(), fast.path(), &RestoreOptions::default());
    assert_eq!(rr.exit_code(), 0, "restore clean: {:?}", rr.findings);
    assert!(store::reg_path(fast.path(), 1).exists(), ".reg restored");

    // The restored store opens on the accelerated path.
    let (fast_events, fast_admitted, fast_fallback) =
        open_and_count(fast.path());
    assert_eq!(
        fast_admitted, 1,
        "restored store folded $registry from the delta"
    );
    assert_eq!(fast_fallback, 0, "no segment fell back to point reads");

    // The same backup, restored again with the delta removed: identical events,
    // the slow path. The delta is acceleration, never authority (D1).
    let rr2 =
        restore::run(dest.path(), slow.path(), &RestoreOptions::default());
    assert_eq!(rr2.exit_code(), 0, "second restore clean: {:?}", rr2.findings);
    std::fs::remove_file(store::reg_path(slow.path(), 1))
        .expect("drop the .reg");
    let (slow_events, slow_admitted, slow_fallback) =
        open_and_count(slow.path());
    assert_eq!(slow_admitted, 0);
    assert_eq!(slow_fallback, 1, "that segment point-read its registrations");
    assert_eq!(
        fast_events, slow_events,
        "same store either way — the delta only changes how fast open is"
    );
}

/// Open a store and report `(total_events, deltas admitted, deltas fallen
/// back)` from the engine's own fallback counters (bn-11ba).
fn open_and_count(dir: &Path) -> (usize, u64, u64) {
    let engine =
        LogEngine::open_with(dir, rolling_sidecar_opts()).expect("open store");
    let o = engine.observability();
    (
        engine.total_events(),
        o.fallbacks.registry_delta_admitted,
        o.fallbacks.registry_delta_fallback,
    )
}

/// Loose-sidecar (`seal_pack: false`) options with a segment small enough that
/// the corpus below rolls several times.
fn rolling_sidecar_opts() -> EngineOptions {
    EngineOptions {
        segment_size: 4096,
        seal_pack: false,
        ..EngineOptions::default()
    }
}

/// Append until the log has rolled and the background sealer has installed at
/// least two sealed segments, so seg-1 is a sealed **non-head** segment — the
/// only shape whose `.reg` engine open ever reads.
fn build_rolling_sidecar_store(dir: &Path) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("runtime");
    let engine =
        LogEngine::open_with(dir, rolling_sidecar_opts()).expect("open engine");
    rt.block_on(async {
        for i in 0..120u64 {
            let expected =
                if i == 0 { Version::NoStream } else { Version::At(i - 1) };
            engine
                .append_batch("acct-1", expected, &[rec(&[b'x'; 64])])
                .await
                .expect("append");
        }
    });
    let deadline =
        std::time::Instant::now() + std::time::Duration::from_secs(30);
    while engine.sealed_segment_count() < 2 {
        assert!(
            std::time::Instant::now() < deadline,
            "sealer never installed 2 segments (got {})",
            engine.sealed_segment_count()
        );
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    drop(engine); // lock released
}

/// Copy a sealed segment's immutable files (`.log` + sidecars) to a new
/// segment id, producing a second self-describing sealed segment at the source
/// for the incremental test.
fn add_sealed_segment_copy(dir: &Path, from_id: u64, to_id: u64) {
    let from_log = store::log_path(dir, from_id);
    let to_log = store::log_path(dir, to_id);
    std::fs::copy(&from_log, &to_log).expect("copy .log");

    let from_pidx = store::pidx_path(dir, from_id);
    let to_pidx = store::pidx_path(dir, to_id);
    for ext in ["pidx", "pcol", "filter"] {
        let f = from_pidx.with_extension(ext);
        if f.exists() {
            std::fs::copy(&f, to_pidx.with_extension(ext))
                .expect("copy sidecar");
        }
    }
}
