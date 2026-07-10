//! bn-1hl experiment 2: O_DSYNC vs fdatasync group-commit-shaped writes on
//! this host's real device (never tmpfs — see `main`'s scratch-dir guard).
//!
//! Methodology, deliberately narrower than `spikes/perf_group_commit`'s full
//! concurrent-committer harness: this is a **serial single-writer probe** —
//! one durability barrier per call, group-sized payloads (4/16/32/64 KiB),
//! matching what a single coalescing committer thread issues per commit
//! group (round 4's D7/H4 architecture: "single committer with ONE
//! coalesced write per group"). It reproduces-or-refutes the **per-call
//! latency primitive** round 4 measured in its own baseline section (§2:
//! "O_DSYNC write (1 MiB)" vs "write+fdatasync (1 MiB)"), NOT the
//! 512-concurrent-writers throughput number (round 4 §6, H4) — that number's
//! 2.3-4.4x also folds in a kernel-mediated-flush-coalescing confound this
//! probe does not exercise. Both are reported; the report is explicit about
//! which claim each number reproduces.
//!
//! Modes:
//! - `fdatasync`: buffered `write_all` + `File::sync_data()` (current default).
//! - `odsync`: `O_DSYNC`-opened file, buffered `write_all` (the write IS the
//!   barrier; no separate flush call).
//! - `odsync+odirect`: `O_DSYNC|O_DIRECT`, a page-aligned buffer written with
//!   a raw `write(2)` (std's `File::write_all` doesn't guarantee buffer
//!   alignment, which `O_DIRECT` requires). All four payload sizes here are
//!   4 KiB-aligned, so sequential aligned writes from offset 0 keep every
//!   write's file offset aligned too — the "if alignment permits" case does
//!   permit.
//!
//! Drift control (round 4's hard-won lesson: this device's fdatasync
//! degrades ~50x under sustained load and needs minutes to recover): every
//! (size, mode) point is measured across `REPS` interleaved repetitions with
//! an idle settle pause between each, and every rep for a given size runs
//! all three modes back-to-back before moving to the next rep — no mode
//! systematically runs on a "fresher" or "more worn" device state than
//! another.

use std::fs::OpenOptions;
use std::io::Write;
use std::os::unix::fs::OpenOptionsExt;
use std::os::unix::io::AsRawFd;
use std::path::Path;
use std::time::{Duration, Instant};

const SAMPLES: usize = 150;
const REPS: usize = 3;
const SETTLE: Duration = Duration::from_millis(1500);
const WARMUP: usize = 10;

pub fn xorshift(seed: &mut u64) -> u64 {
    *seed ^= *seed << 13;
    *seed ^= *seed >> 7;
    *seed ^= *seed << 17;
    *seed
}

/// A page-aligned owned buffer (`O_DIRECT` requires the userspace buffer,
/// the file offset, and the write length to all be block-aligned; 4 KiB
/// satisfies every NVMe device's block size).
struct AlignedBuf {
    ptr: *mut u8,
    layout: std::alloc::Layout,
}

impl AlignedBuf {
    fn filled(len: usize, seed: &mut u64) -> Self {
        let layout = std::alloc::Layout::from_size_align(len, 4096).expect("valid layout");
        // SAFETY: layout has nonzero size (all our callers pass len > 0).
        let ptr = unsafe { std::alloc::alloc(layout) };
        assert!(!ptr.is_null(), "aligned alloc failed");
        // SAFETY: ptr is valid for len bytes per the successful alloc above.
        let slice = unsafe { std::slice::from_raw_parts_mut(ptr, len) };
        for chunk in slice.chunks_mut(8) {
            let r = xorshift(seed).to_le_bytes();
            chunk.copy_from_slice(&r[..chunk.len()]);
        }
        AlignedBuf { ptr, layout }
    }

    fn as_ptr(&self) -> *const u8 {
        self.ptr
    }
}

impl Drop for AlignedBuf {
    fn drop(&mut self) {
        // SAFETY: ptr/layout are exactly what alloc returned/was given.
        unsafe { std::alloc::dealloc(self.ptr, self.layout) };
    }
}

pub struct Stats {
    pub p50_us: f64,
    pub p95_us: f64,
    pub p99_us: f64,
    pub max_us: f64,
    pub mean_us: f64,
}

fn stats(mut samples_ns: Vec<u64>) -> Stats {
    samples_ns.sort_unstable();
    let n = samples_ns.len();
    let pct = |p: f64| samples_ns[((n as f64 - 1.0) * p).round() as usize] as f64 / 1000.0;
    let mean_us = samples_ns.iter().sum::<u64>() as f64 / n as f64 / 1000.0;
    Stats { p50_us: pct(0.50), p95_us: pct(0.95), p99_us: pct(0.99), max_us: pct(1.0), mean_us }
}

fn run_fdatasync(path: &Path, size: usize, samples: usize) -> Vec<u64> {
    let payload = vec![0xA5u8; size];
    let f = OpenOptions::new().create(true).truncate(true).read(true).write(true).open(path).unwrap();
    let mut lat = Vec::with_capacity(samples);
    for i in 0..WARMUP + samples {
        (&f).write_all(&payload).unwrap();
        let t = Instant::now();
        f.sync_data().unwrap();
        let e = t.elapsed().as_nanos() as u64;
        if i >= WARMUP {
            lat.push(e);
        }
    }
    lat
}

fn run_odsync(path: &Path, size: usize, samples: usize) -> Vec<u64> {
    let payload = vec![0xA5u8; size];
    let f = OpenOptions::new()
        .create(true)
        .truncate(true)
        .read(true)
        .write(true)
        .custom_flags(libc::O_DSYNC)
        .open(path)
        .unwrap();
    let mut lat = Vec::with_capacity(samples);
    for i in 0..WARMUP + samples {
        let t = Instant::now();
        (&f).write_all(&payload).unwrap();
        let e = t.elapsed().as_nanos() as u64;
        if i >= WARMUP {
            lat.push(e);
        }
    }
    lat
}

fn run_odsync_odirect(path: &Path, size: usize, samples: usize) -> Vec<u64> {
    let f = OpenOptions::new()
        .create(true)
        .truncate(true)
        .read(true)
        .write(true)
        .custom_flags(libc::O_DSYNC | libc::O_DIRECT)
        .open(path)
        .unwrap();
    let fd = f.as_raw_fd();
    let mut seed = 0xD19EC7_u64;
    let mut lat = Vec::with_capacity(samples);
    for i in 0..WARMUP + samples {
        let buf = AlignedBuf::filled(size, &mut seed);
        let t = Instant::now();
        // SAFETY: fd is a valid, open, writable fd for the process lifetime
        // of this loop iteration; buf.as_ptr() is valid for `size` bytes
        // (AlignedBuf's invariant) and 4096-aligned, satisfying O_DIRECT.
        let n = unsafe { libc::write(fd, buf.as_ptr() as *const libc::c_void, size) };
        assert_eq!(n, size as isize, "short/failed O_DIRECT write: {n} (errno {})", std::io::Error::last_os_error());
        let e = t.elapsed().as_nanos() as u64;
        if i >= WARMUP {
            lat.push(e);
        }
    }
    lat
}

type ModeFn = fn(&Path, usize, usize) -> Vec<u64>;

pub fn main_bench(scratch: &Path) {
    let sizes = [4096usize, 16384, 32768, 65536];
    println!("device: real fs at {}", scratch.display());
    println!(
        "{:<9} {:<16} {:>8} {:>8} {:>8} {:>8} {:>8}",
        "size", "mode", "p50 us", "p95 us", "p99 us", "max us", "mean us"
    );

    // rep -> size -> mode -> samples, merged across reps at the end so every
    // point is settle-paced and interleaved (round-4 drift-control recipe).
    let modes: [(&str, ModeFn); 3] =
        [("fdatasync", run_fdatasync), ("odsync", run_odsync), ("odsync+odirect", run_odsync_odirect)];

    let mut merged: std::collections::HashMap<(usize, &str), Vec<u64>> = std::collections::HashMap::new();

    for rep in 0..REPS {
        for &size in &sizes {
            for (name, f) in &modes {
                std::thread::sleep(SETTLE);
                let path = scratch.join(format!("probe-{size}-{name}-r{rep}.dat"));
                let lat = f(&path, size, SAMPLES);
                std::fs::remove_file(&path).ok();
                merged.entry((size, name)).or_default().extend(lat);
            }
        }
    }

    for &size in &sizes {
        for (name, _) in &modes {
            let samples = merged.remove(&(size, *name)).unwrap();
            let s = stats(samples);
            println!(
                "{:<9} {:<16} {:>8.2} {:>8.2} {:>8.2} {:>8.2} {:>8.2}",
                size, name, s.p50_us, s.p95_us, s.p99_us, s.max_us, s.mean_us
            );
        }
    }
}
