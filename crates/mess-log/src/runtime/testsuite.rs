//! Runtime-agnostic behavioural suite. Each function is generic over
//! `R: Runtime` and is run against BOTH [`RealRuntime`](super::RealRuntime)
//! and [`SimRuntime`](super::SimRuntime) from their respective test
//! modules — the point of the whole abstraction is that the same code
//! drives real and simulated time/fs/spawn identically.

use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

// `now`/`sleep` come from `Clock`, a supertrait of `Runtime`, so they are
// in scope through the `R: Runtime` bound without importing `Clock` here.
use super::{FileHandle, Fs, OpenOpts, Runtime};

/// `pwrite` at two offsets, sync, and `pread` back — the framing the
/// committer + recovery scanner rely on. Verifies positioned I/O, EOF-short
/// reads, and `len()`.
pub(crate) fn fs_roundtrip<R: Runtime>(rt: &R, path: &Path) {
    let fs = rt.fs();
    let f = fs.open(path, OpenOpts::create_rw()).expect("open");

    assert_eq!(f.pwrite(0, b"header--").unwrap(), 8);
    assert_eq!(f.pwrite(8, b"BODYBODYBODY").unwrap(), 12);
    f.fdatasync().unwrap();
    assert_eq!(f.len().unwrap(), 20);

    let mut buf = [0u8; 8];
    assert_eq!(f.pread(0, &mut buf).unwrap(), 8);
    assert_eq!(&buf, b"header--");
    assert_eq!(f.pread(8, &mut buf).unwrap(), 8);
    assert_eq!(&buf, b"BODYBODY");

    // Short read at EOF (like pread(2)): only the remaining bytes.
    let mut tail = [0u8; 16];
    assert_eq!(f.pread(16, &mut tail).unwrap(), 4);
    assert_eq!(&tail[..4], b"BODY");

    // A reopened handle sees the same durable bytes.
    let g = fs.open(path, OpenOpts::read_only()).expect("reopen");
    let mut all = [0u8; 20];
    assert_eq!(g.pread(0, &mut all).unwrap(), 20);
    assert_eq!(&all[..8], b"header--");
}

/// `rename` moves the durable bytes to the new name and the old name is
/// gone (segment publish / manifest swap).
pub(crate) fn fs_rename<R: Runtime>(rt: &R, from: &Path, to: &Path) {
    let fs = rt.fs();
    {
        let f = fs.open(from, OpenOpts::create_rw()).unwrap();
        f.pwrite(0, b"segment-bytes").unwrap();
        f.fdatasync().unwrap();
    }
    fs.rename(from, to).unwrap();

    let g = fs.open(to, OpenOpts::read_only()).unwrap();
    let mut buf = [0u8; 13];
    assert_eq!(g.pread(0, &mut buf).unwrap(), 13);
    assert_eq!(&buf, b"segment-bytes");
    assert!(fs.open(from, OpenOpts::read_only()).is_err());
}

/// `now()` is monotonic, and awaiting `sleep(d)` advances it by at least
/// `d` (virtual on sim, real elapsed on real).
pub(crate) fn clock_advances<R: Runtime>(rt: &R) {
    let t0 = rt.now();
    let dur = Duration::from_millis(5);
    let rt2 = rt.clone();
    rt.block_on(async move {
        rt2.sleep(dur).await;
    });
    let t1 = rt.now();
    assert!(t1 >= t0, "clock went backwards");
    assert!(
        t1.saturating_duration_since(t0) >= dur,
        "sleep did not advance the clock by at least the requested duration"
    );
}

/// Spawn three actors that each write a distinct 8-byte marker at a
/// distinct offset of a shared segment file, sync, and return their id.
/// The root joins all three and verifies every marker landed — exercising
/// `spawn`, join handles, and a shared [`Fs`] under concurrency.
pub(crate) fn three_actors_share_a_segment<R: Runtime>(rt: &R, path: &Path) {
    let fs = rt.fs();
    {
        // Pre-create + size the file so each actor's positioned write lands.
        let f = fs.open(path, OpenOpts::create_rw()).unwrap();
        f.pwrite(0, &[0u8; 24]).unwrap();
        f.fdatasync().unwrap();
    }

    let sum = rt.block_on({
        let rt = rt.clone();
        async move {
            let mut joins = Vec::new();
            for id in 0u8..3 {
                let fs = rt.fs();
                let path = path.to_path_buf();
                joins.push(rt.spawn(async move {
                    let f = fs.open(&path, OpenOpts::create_rw()).unwrap();
                    let marker = [b'A' + id; 8];
                    f.pwrite(u64::from(id) * 8, &marker).unwrap();
                    f.fdatasync().unwrap();
                    u64::from(id)
                }));
            }
            let mut total = 0;
            for j in joins {
                total += j.await;
            }
            total
        }
    });
    assert_eq!(sum, 3, "0 + 1 + 2 = 3 actor ids");

    let f = fs.open(path, OpenOpts::read_only()).unwrap();
    let mut all = [0u8; 24];
    f.pread(0, &mut all).unwrap();
    assert_eq!(&all[0..8], &[b'A'; 8], "actor 0's marker missing");
    assert_eq!(&all[8..16], &[b'B'; 8], "actor 1's marker missing");
    assert_eq!(&all[16..24], &[b'C'; 8], "actor 2's marker missing");
}

/// Spawned actors' outputs reach the root in completion order. Returns the
/// order the actors finished in — deterministic (and seed-dependent) on the
/// sim runtime, unconstrained on the real one.
pub(crate) fn actor_completion_order<R: Runtime>(rt: &R) -> Vec<u64> {
    let log: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));
    rt.block_on({
        let rt = rt.clone();
        let log = log.clone();
        async move {
            let mut joins = Vec::new();
            for id in 0u64..3 {
                let log = log.clone();
                joins.push(rt.spawn(async move {
                    log.lock().unwrap().push(id);
                    id
                }));
            }
            for j in joins {
                j.await;
            }
        }
    });
    Arc::into_inner(log).unwrap().into_inner().unwrap()
}
