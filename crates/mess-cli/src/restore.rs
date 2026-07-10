//! `mess restore <src> --to <dir>` — reconstruct a store from a backup and
//! prove it recovers (doc 07 §4).
//!
//! Restore refuses a non-empty target and an absent/torn backup (a missing
//! `BACKUP_MANIFEST` — written last by `mess backup` — proves the copy never
//! finished). It verifies every manifest file by size + CRC, copies them back,
//! then runs the same `verify --full` machinery a live store uses and reports
//! the recovered watermark, which MUST equal the manifest's.

use std::io::{self, Write};
use std::path::Path;

use mess_log::crc::crc32c_two;
use serde_json::json;

use crate::backup::{BACKUP_MANIFEST, BackupManifest, compute_cut};
use crate::report::{Finding, Report, Severity};
use crate::verify::{self, VerifyOptions};

/// Options for [`run`].
#[derive(Debug, Default, Clone)]
pub struct RestoreOptions {}

/// Whether `dir` is safe to restore into: it must not exist, or exist and be
/// empty. A non-empty directory is refused (never overwrite a store).
fn target_is_empty(dir: &Path) -> bool {
    match std::fs::read_dir(dir) {
        Ok(mut entries) => entries.next().is_none(),
        Err(e) if e.kind() == io::ErrorKind::NotFound => true,
        Err(_) => false,
    }
}

fn write_atomic(dest: &Path, rel: &str, bytes: &[u8]) -> io::Result<()> {
    let final_path = dest.join(rel);
    if let Some(parent) = final_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let tmp = final_path.with_extension("mrs.tmp");
    {
        let mut f = std::fs::File::create(&tmp)?;
        f.write_all(bytes)?;
        f.sync_all()?;
    }
    std::fs::rename(&tmp, &final_path)?;
    Ok(())
}

/// Restore the backup at `src` into `dir` (doc 07 §4).
pub fn run(src: &Path, dir: &Path, _opts: &RestoreOptions) -> Report {
    let mut report = Report::new("restore", "files");
    report.set("src", json!(src.display().to_string()));
    report.set("dir", json!(dir.display().to_string()));

    // 1. Refuse a non-empty target (never overwrite a store).
    if !target_is_empty(dir) {
        report.push_finding(Finding::new(
            Severity::Error,
            "restore",
            "target-not-empty",
            format!("refusing to restore into non-empty directory {}", dir.display()),
        ));
        return report;
    }

    // 2. Refuse an absent/torn backup: no manifest ⇒ the copy never finished.
    let manifest_path = src.join(BACKUP_MANIFEST);
    let manifest_bytes = match std::fs::read(&manifest_path) {
        Ok(b) => b,
        Err(e) => {
            report.push_finding(Finding::new(
                Severity::Error,
                "restore",
                "torn-backup",
                format!(
                    "no readable {BACKUP_MANIFEST} at {} ({e}); backup is incomplete or corrupt",
                    src.display()
                ),
            ));
            return report;
        }
    };
    let manifest: BackupManifest = match serde_json::from_slice(&manifest_bytes) {
        Ok(m) => m,
        Err(e) => {
            report.push_finding(Finding::new(
                Severity::Error,
                "restore",
                "torn-backup",
                format!("{BACKUP_MANIFEST} at {} failed to parse: {e}", src.display()),
            ));
            return report;
        }
    };
    report.set("manifest_watermark", json!(manifest.watermark));

    // 3. Verify every listed file exists at src with matching size + CRC.
    for entry in &manifest.files {
        let path = src.join(&entry.path);
        let bytes = match std::fs::read(&path) {
            Ok(b) => b,
            Err(e) => {
                report.push_finding(
                    Finding::new(
                        Severity::Error,
                        "restore",
                        "backup-file-missing",
                        format!("backup file {} missing/unreadable: {e}", entry.path),
                    )
                    .with("path", entry.path.clone()),
                );
                return report;
            }
        };
        if bytes.len() as u64 != entry.len || crc32c_two(&bytes, &[]) != entry.crc32c {
            report.push_finding(
                Finding::new(
                    Severity::Error,
                    "restore",
                    "backup-file-mismatch",
                    format!(
                        "backup file {} failed size/CRC check (len {} vs {}, corrupt copy)",
                        entry.path,
                        bytes.len(),
                        entry.len
                    ),
                )
                .with("path", entry.path.clone()),
            );
            return report;
        }
    }

    // 4. Copy every file into the target (temp + rename).
    let mut restored = 0u64;
    let mut restored_bytes = 0u64;
    for entry in &manifest.files {
        let bytes = match std::fs::read(src.join(&entry.path)) {
            Ok(b) => b,
            Err(e) => {
                report.push_finding(Finding::new(
                    Severity::Error,
                    "restore",
                    "restore-read-failed",
                    format!("failed to re-read {}: {e}", entry.path),
                ));
                return report;
            }
        };
        if let Err(e) = write_atomic(dir, &entry.path, &bytes) {
            report.push_finding(Finding::new(
                Severity::Error,
                "restore",
                "restore-write-failed",
                format!("failed to write {} into target: {e}", entry.path),
            ));
            return report;
        }
        restored += 1;
        restored_bytes += bytes.len() as u64;
        report.push_row(json!({ "path": entry.path, "role": entry.role, "len": entry.len }));
    }
    report.set("restored_files", json!(restored));
    report.set("restored_bytes", json!(restored_bytes));

    // 5. Full recovery + verify --full over the restored store.
    let verify_report = verify::run(dir, &VerifyOptions { full: true, ..Default::default() });
    let verify_clean = verify_report.worst() < Severity::Error;
    for f in &verify_report.findings {
        // Fold verify's findings into the restore report so a corruption
        // surfaces (and forces a non-zero exit) here.
        if f.severity >= Severity::Warn {
            report.push_finding(f.clone());
        }
    }
    report.set("verified", json!(verify_clean));

    // 6. Recovered watermark: recompute the cut of the restored store and
    //    confirm it equals the manifest's.
    let recovered = compute_cut(dir);
    report.set("recovered_watermark", json!(recovered.watermark));
    if recovered.watermark != manifest.watermark {
        report.push_finding(
            Finding::new(
                Severity::Error,
                "restore",
                "watermark-mismatch",
                format!(
                    "recovered watermark {} != manifest watermark {}",
                    recovered.watermark, manifest.watermark
                ),
            )
            .with("recovered", recovered.watermark)
            .with("manifest", manifest.watermark),
        );
        return report;
    }

    if verify_clean {
        report.push_finding(
            Finding::new(
                Severity::Ok,
                "restore",
                "restore-complete",
                format!(
                    "restored {restored} file(s); verify --full clean; recovered watermark {}",
                    recovered.watermark
                ),
            )
            .with("restored_files", restored)
            .with("recovered_watermark", recovered.watermark),
        );
    }
    report
}
