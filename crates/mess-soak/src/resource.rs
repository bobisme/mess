//! Process resource probes: RSS, open-fd count, and the tmpfs guard.
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
                        format!("cannot resolve any ancestor of {}", dir.display()),
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
        let (Some(_dev), Some(mount_raw), Some(fstype)) = (f.next(), f.next(), f.next()) else {
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
            assert!(is_tmpfs(Path::new("/dev/shm")).unwrap(), "/dev/shm must read as tmpfs");
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

    #[test]
    fn unescape_handles_spaces() {
        assert_eq!(unescape_mount(r"/mnt/my\040disk"), "/mnt/my disk");
        assert_eq!(unescape_mount("/plain/path"), "/plain/path");
    }
}
