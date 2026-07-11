//! Backup **retention leases** (doc 07 §5): the durable, self-expiring marker
//! a running `mess backup` registers so retention (v1 whole-segment deletion)
//! cannot delete a sealed segment out from under the copy.
//!
//! A lease is a JSON file under `<dir>/leases/<backup_id>.lease`. It pins the
//! inclusive segment-id range of the backup's cut. Its liveness is a **TTL**:
//! a lease is active only while `now < expires_unix`, and `mess backup` renews
//! it while copying. A **crashed** backup therefore cannot leak the pin — its
//! lease simply expires and the retention reader ignores (and may unlink) it.
//! This is the same "a stale file does not block" discipline as the D9 writer
//! lock ([`mess_log::lock`]).
//!
//! This module owns the on-disk format and the TTL/liveness policy; the pure
//! "does this lease pin segment N?" predicate lives with the retention
//! decision function in [`mess_index::sealed::retention::BackupLease`], so the
//! decision layer is lease-aware and unit-testable without touching the
//! filesystem or the clock.

use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use mess_index::sealed::retention::BackupLease;
use serde::{Deserialize, Serialize};

/// Default lease TTL: a backup that stops renewing for this long is treated as
/// dead and its pin is released (doc 07 §5.2).
pub const DEFAULT_TTL_SECS: u64 = 60;

/// The on-disk lease record (JSON, stable field names).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LeaseFile {
    /// Unique id of the backup holding this lease (its lease-file stem).
    pub backup_id:              String,
    /// The pid of the `mess backup` process (diagnostic; TTL is
    /// authoritative).
    pub pid:                    u32,
    /// Wall-clock creation time (unix seconds).
    pub created_unix:           u64,
    /// Wall-clock expiry (unix seconds); the lease is inactive once `now >=`
    /// this. Renewed while the backup copies.
    pub expires_unix:           u64,
    /// Lowest segment id the cut protects (inclusive).
    pub protect_min_segment_id: u64,
    /// Highest segment id the cut protects (inclusive).
    pub protect_max_segment_id: u64,
    /// The cut's durable watermark (diagnostic).
    pub watermark:              u64,
}

impl LeaseFile {
    /// Whether this lease is still active at `now` (unix seconds).
    #[must_use]
    pub fn is_active(&self, now: u64) -> bool { now < self.expires_unix }

    /// The pure retention predicate this lease contributes (doc 07 §5.1).
    #[must_use]
    pub fn to_backup_lease(&self) -> BackupLease {
        BackupLease {
            backup_id:              self.backup_id.clone(),
            protect_min_segment_id: self.protect_min_segment_id,
            protect_max_segment_id: self.protect_max_segment_id,
        }
    }
}

/// Current wall-clock time in unix seconds (0 before the epoch, which never
/// happens in practice).
#[must_use]
pub fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// The `<dir>/leases` directory holding every backup lease.
#[must_use]
pub fn leases_dir(dir: &Path) -> PathBuf { dir.join("leases") }

fn lease_path(dir: &Path, backup_id: &str) -> PathBuf {
    leases_dir(dir).join(format!("{backup_id}.lease"))
}

/// Atomically write (or renew) a lease file: temp + fsync + rename, so a
/// reader never sees a half-written lease.
fn write_lease(dir: &Path, lease: &LeaseFile) -> io::Result<()> {
    let ldir = leases_dir(dir);
    std::fs::create_dir_all(&ldir)?;
    let final_path = lease_path(dir, &lease.backup_id);
    let tmp = final_path.with_extension("lease.tmp");
    let bytes = serde_json::to_vec_pretty(lease)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    {
        let mut f = std::fs::File::create(&tmp)?;
        f.write_all(&bytes)?;
        f.sync_all()?;
    }
    std::fs::rename(&tmp, &final_path)?;
    // fsync the directory so the rename is durable.
    if let Ok(d) = std::fs::File::open(&ldir) {
        let _ = d.sync_all();
    }
    Ok(())
}

/// A held backup lease. Renew it while copying; on drop (or explicit
/// [`release`](LeaseGuard::release)) the lease file is removed so retention is
/// unblocked immediately. A crash skips the removal, but the TTL still expires
/// the pin (doc 07 §5.2).
#[derive(Debug)]
pub struct LeaseGuard {
    dir:      PathBuf,
    lease:    LeaseFile,
    ttl_secs: u64,
    released: bool,
}

impl LeaseGuard {
    /// The backup id of this lease.
    #[must_use]
    pub fn backup_id(&self) -> &str { &self.lease.backup_id }

    /// Refresh the expiry to `now + ttl` and rewrite the lease file.
    pub fn renew(&mut self) -> io::Result<()> {
        self.lease.expires_unix = now_unix() + self.ttl_secs;
        write_lease(&self.dir, &self.lease)
    }

    /// Remove the lease file, unblocking retention immediately.
    pub fn release(mut self) -> io::Result<()> {
        self.released = true;
        remove_lease(&self.dir, &self.lease.backup_id)
    }
}

impl Drop for LeaseGuard {
    fn drop(&mut self) {
        if !self.released {
            let _ = remove_lease(&self.dir, &self.lease.backup_id);
        }
    }
}

fn remove_lease(dir: &Path, backup_id: &str) -> io::Result<()> {
    let path = lease_path(dir, backup_id);
    match std::fs::remove_file(&path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e),
    }
}

/// Register a backup lease protecting `[min_seg, max_seg]` for `ttl_secs`,
/// returning a guard that removes it on drop. Written and fsync'd before the
/// caller copies any segment (doc 07 §2 step 1).
pub fn acquire(
    dir: &Path,
    backup_id: impl Into<String>,
    min_seg: u64,
    max_seg: u64,
    watermark: u64,
    ttl_secs: u64,
) -> io::Result<LeaseGuard> {
    let now = now_unix();
    let lease = LeaseFile {
        backup_id: backup_id.into(),
        pid: std::process::id(),
        created_unix: now,
        expires_unix: now + ttl_secs,
        protect_min_segment_id: min_seg,
        protect_max_segment_id: max_seg,
        watermark,
    };
    write_lease(dir, &lease)?;
    Ok(LeaseGuard { dir: dir.to_path_buf(), lease, ttl_secs, released: false })
}

/// Read every lease file under `<dir>/leases`, returning the raw records. A
/// file that does not parse is skipped (advisory-safe — a corrupt lease never
/// blocks, matching the D1 posture). Best-effort: a missing directory yields
/// an empty list.
#[must_use]
pub fn read_all(dir: &Path) -> Vec<LeaseFile> {
    let mut out = Vec::new();
    let Ok(entries) = std::fs::read_dir(leases_dir(dir)) else {
        return out;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some("lease") {
            continue;
        }
        if let Ok(bytes) = std::fs::read(&path)
            && let Ok(lease) = serde_json::from_slice::<LeaseFile>(&bytes)
        {
            out.push(lease);
        }
    }
    out.sort_by(|a, b| a.backup_id.cmp(&b.backup_id));
    out
}

/// The **active** backup leases at `now`, as the pure retention predicate the
/// decision function consumes. Expired leases are filtered out (and their
/// stale files unlinked, best-effort) so a crashed backup's pin self-heals at
/// TTL (doc 07 §5.2).
#[must_use]
pub fn active_leases(dir: &Path, now: u64) -> Vec<BackupLease> {
    let mut out = Vec::new();
    for lease in read_all(dir) {
        if lease.is_active(now) {
            out.push(lease.to_backup_lease());
        } else {
            // Opportunistic cleanup of an expired (e.g. crashed-backup) lease.
            let _ = remove_lease(dir, &lease.backup_id);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn active_lease_pins_then_releases_on_drop() {
        let d = mess_testkit::sweeping_temp_dir("cli-lease-src-d");
        {
            let _guard = acquire(d.path(), "b1", 1, 5, 42, DEFAULT_TTL_SECS)
                .expect("acquire");
            let active = active_leases(d.path(), now_unix());
            assert_eq!(active.len(), 1, "lease is active while held");
            assert!(active[0].pins(3));
            assert!(active[0].pins(1));
            assert!(active[0].pins(5));
            assert!(!active[0].pins(6));
        }
        // Guard dropped -> lease file removed -> no active leases.
        assert!(
            active_leases(d.path(), now_unix()).is_empty(),
            "lease released on drop"
        );
    }

    #[test]
    fn expired_lease_is_inactive_and_cleaned_up() {
        let d = mess_testkit::sweeping_temp_dir("cli-lease-src-d-1");
        // Write a lease that expired an hour ago (simulating a crashed backup
        // whose TTL lapsed).
        let stale = LeaseFile {
            backup_id:              "dead".into(),
            pid:                    999_999,
            created_unix:           now_unix().saturating_sub(7200),
            expires_unix:           now_unix().saturating_sub(3600),
            protect_min_segment_id: 1,
            protect_max_segment_id: 9,
            watermark:              0,
        };
        write_lease(d.path(), &stale).expect("write stale lease");
        assert!(!stale.is_active(now_unix()));
        // active_leases filters it out and unlinks it.
        assert!(active_leases(d.path(), now_unix()).is_empty());
        assert!(
            read_all(d.path()).is_empty(),
            "stale lease file was cleaned up"
        );
    }

    #[test]
    fn renew_extends_expiry() {
        let d = mess_testkit::sweeping_temp_dir("cli-lease-src-d-2");
        let mut guard = acquire(d.path(), "b1", 1, 1, 0, 1).expect("acquire");
        let before = read_all(d.path())[0].expires_unix;
        // A longer TTL on renew pushes the expiry strictly forward.
        guard.ttl_secs = 10_000;
        guard.renew().expect("renew");
        let after = read_all(d.path())[0].expires_unix;
        assert!(after >= before, "renew never moves expiry backward");
        assert!(after > now_unix(), "renewed lease is active");
    }
}
