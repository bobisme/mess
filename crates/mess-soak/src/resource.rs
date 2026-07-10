//! Process resource probes: RSS, open-fd count, the tmpfs guard, and the
//! fresh-`--dir` guard.
//!
//! Linux-only (the engine already targets Unix; the soak's whole point is a
//! real device). On a non-Linux host the RSS/fd readers return `None` and the
//! driver simply skips those ceilings rather than firing a false abort.

use std::io;
use std::path::Path;

/// Resident set size of the current process, in bytes, or `None` if
/// `/proc/self/statm` is unavailable.
#[must_use]
pub fn current_rss_bytes() -> Option<u64> {
    let statm = std::fs::read_to_string("/proc/self/statm").ok()?;
    // Field 2 (0-based 1) is resident pages.
    let resident_pages: u64 = statm.split_whitespace().nth(1)?.parse().ok()?;
    let page_size = page_size_bytes();
    Some(resident_pages * page_size)
}

/// Number of file descriptors the current process holds open, or `None` if
/// `/proc/self/fd` is unavailable.
#[must_use]
pub fn current_fd_count() -> Option<usize> {
    let dir = std::fs::read_dir("/proc/self/fd").ok()?;
    // `read_dir` itself opens one fd for the directory handle; count entries as
    // reported (the extra handle is a constant the ceiling can absorb, and it
    // is released the moment the iterator drops).
    Some(dir.count())
}

fn page_size_bytes() -> u64 {
    // SAFETY: `sysconf` with a valid name is always safe; it only reads a
    // kernel constant. A negative return (unsupported) falls back to 4 KiB.
    let v = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    if v > 0 { v as u64 } else { 4096 }
}

/// Does `dir` live on a tmpfs/ramfs mount? The soak REFUSES such a directory:
/// on tmpfs `fdatasync` is a no-op, so every durability barrier the engine
/// thinks it is taking is a lie and a crash-recovery soak would validate
/// nothing. Resolves the longest mount-point prefix of the (canonicalized)
/// path in `/proc/mounts` and checks its filesystem type.
///
/// Returns `Ok(true)` if the backing filesystem is tmpfs/ramfs, `Ok(false)`
/// otherwise, and an error only if `/proc/mounts` cannot be read or the path
/// cannot be resolved to any mount.
pub fn is_tmpfs(dir: &Path) -> io::Result<bool> {
    // Canonicalize as far as possible: the leaf may not exist yet, so walk up
    // to the nearest existing ancestor and resolve that.
    let mut probe = dir.to_path_buf();
    let resolved = loop {
        match std::fs::canonicalize(&probe) {
            Ok(p) => break p,
            Err(_) => {
                if !probe.pop() {
                    return Err(io::Error::new(
                        io::ErrorKind::NotFound,
                        format!(
                            "cannot resolve any ancestor of {}",
                            dir.display()
                        ),
                    ));
                }
            }
        }
    };

    let mounts = std::fs::read_to_string("/proc/mounts")?;
    let mut best_len = 0usize;
    let mut best_fstype: Option<String> = None;
    for line in mounts.lines() {
        // fields: dev  mountpoint  fstype  opts ...
        let mut f = line.split_whitespace();
        let (Some(_dev), Some(mount_raw), Some(fstype)) =
            (f.next(), f.next(), f.next())
        else {
            continue;
        };
        let mount = unescape_mount(mount_raw);
        if resolved.starts_with(&mount) && mount.len() >= best_len {
            best_len = mount.len();
            best_fstype = Some(fstype.to_string());
        }
    }

    match best_fstype {
        Some(fs) => Ok(fs == "tmpfs" || fs == "ramfs"),
        None => Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!("no mount covers {}", resolved.display()),
        )),
    }
}

/// The fresh-`--dir` guard (bn-3dr root cause). The soak's shadow model starts
/// empty, so the store MUST too: opening a `--dir` that already holds a store
/// makes every pre-existing event an "extra" the shadow never acked and every
/// pre-existing stream head a permanent version-conflict source. Exactly this
/// produced the original bn-3dr finding — a killed first soak attempt left ~9s
/// of store in the dir, and the next run opened it: `engine_total −
/// shadow_total = 2727` leftovers (classifying as *fabricated* under the
/// reconcile probe, since the second run never submitted them) and 277k
/// conflicts (its writers racing the leftover stream heads).
///
/// Returns `Err(message)` naming the leftover artifacts when `dir` exists and
/// is non-empty; `Ok(())` for a missing or empty dir. There is deliberately no
/// resume/adopt mode: refusal is the whole contract.
pub fn guard_fresh_dir(dir: &Path) -> Result<(), String> {
    let entries = match std::fs::read_dir(dir) {
        Ok(e) => e,
        // Missing dir (or any unreadable state that create_dir_all will surface
        // properly a moment later): nothing to adopt, nothing to refuse.
        Err(_) => return Ok(()),
    };
    let mut segments = 0usize;
    let mut markers: Vec<String> = Vec::new();
    let mut others = 0usize;
    for entry in entries.flatten() {
        let name = entry.file_name().to_string_lossy().into_owned();
        if name.starts_with("seg-") && name.ends_with(".log") {
            segments += 1;
        } else if name == "LOCK" || name == "meta" || name == "sealed" {
            markers.push(name);
        } else {
            others += 1;
        }
    }
    if segments == 0 && markers.is_empty() && others == 0 {
        return Ok(());
    }
    markers.sort();
    Err(format!(
        "refusing to soak on non-empty --dir {}: found a leftover store \
         ({segments} seg-*.log segment file(s), markers: [{}]{}) — the shadow \
         model starts empty, so pre-existing events would read as fabricated \
         extras and pre-existing stream heads as version conflicts (the \
         bn-3dr false alarm). Use a fresh directory per run; there is no \
         resume mode.",
        dir.display(),
        markers.join(", "),
        if others > 0 {
            format!(", plus {others} other entr(ies)")
        } else {
            String::new()
        },
    ))
}

/// `/proc/mounts` octal-escapes spaces (`\040`) and a few other chars in the
/// mount-point field. Decode the escapes we care about so `starts_with` matches
/// paths with spaces.
fn unescape_mount(raw: &str) -> String {
    if !raw.contains('\\') {
        return raw.to_string();
    }
    let bytes = raw.as_bytes();
    let mut out = String::with_capacity(raw.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'\\' && i + 3 < bytes.len() {
            let oct = &raw[i + 1..i + 4];
            if let Ok(code) = u8::from_str_radix(oct, 8) {
                out.push(code as char);
                i += 4;
                continue;
            }
        }
        out.push(bytes[i] as char);
        i += 1;
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rss_is_plausible() {
        // On Linux CI this must return a nonzero RSS; elsewhere `None` is fine.
        if let Some(rss) = current_rss_bytes() {
            assert!(rss > 0, "rss should be positive");
        }
    }

    #[test]
    fn fd_count_is_plausible() {
        if let Some(n) = current_fd_count() {
            // stdin/stdout/stderr at minimum.
            assert!(n >= 3, "fd count {n} implausibly low");
        }
    }

    #[test]
    fn tmpfs_detects_dev_shm() {
        // `/dev/shm` is tmpfs on essentially every Linux box.
        if Path::new("/dev/shm").exists() {
            assert!(
                is_tmpfs(Path::new("/dev/shm")).unwrap(),
                "/dev/shm must read as tmpfs"
            );
        }
    }

    #[test]
    fn tmpfs_rejects_real_disk() {
        // The workspace/home is on a real disk (ext4 in this env). If the test
        // host happens to run home on tmpfs this is skipped rather than wrong.
        let home = std::env::var("HOME").unwrap_or_else(|_| "/".into());
        if let Ok(on_tmpfs) = is_tmpfs(Path::new(&home)) {
            // We only assert the negative when we can also confirm /dev/shm is
            // tmpfs, i.e. the detector is actually discriminating on this host.
            if Path::new("/dev/shm").exists()
                && is_tmpfs(Path::new("/dev/shm")).unwrap_or(false)
            {
                assert!(!on_tmpfs, "HOME unexpectedly detected as tmpfs");
            }
        }
    }

    // ---- fresh-dir guard (bn-3dr) ----

    #[test]
    fn fresh_dir_guard_accepts_missing_and_empty() {
        let t = tempfile::tempdir().unwrap();
        assert!(guard_fresh_dir(&t.path().join("does-not-exist")).is_ok());
        assert!(guard_fresh_dir(t.path()).is_ok());
    }

    #[test]
    fn fresh_dir_guard_refuses_leftover_store() {
        let t = tempfile::tempdir().unwrap();
        std::fs::write(t.path().join("seg-00000001.log"), b"x").unwrap();
        std::fs::write(t.path().join("LOCK"), b"").unwrap();
        std::fs::create_dir(t.path().join("meta")).unwrap();
        let err = guard_fresh_dir(t.path()).unwrap_err();
        assert!(err.contains("refusing"), "{err}");
        assert!(err.contains("1 seg-*.log"), "{err}");
        assert!(err.contains("LOCK") && err.contains("meta"), "{err}");
        assert!(err.contains("fresh directory"), "{err}");
    }

    #[test]
    fn fresh_dir_guard_refuses_any_nonempty_dir() {
        // Even a dir holding only unrelated files is refused: the run must own
        // its directory outright (an abort leaves it behind for post-mortem).
        let t = tempfile::tempdir().unwrap();
        std::fs::write(t.path().join("unrelated.txt"), b"x").unwrap();
        let err = guard_fresh_dir(t.path()).unwrap_err();
        assert!(err.contains("non-empty"), "{err}");
        assert!(err.contains("1 other entr"), "{err}");
    }

    #[test]
    fn unescape_handles_spaces() {
        assert_eq!(unescape_mount(r"/mnt/my\040disk"), "/mnt/my disk");
        assert_eq!(unescape_mount("/plain/path"), "/plain/path");
    }
}
