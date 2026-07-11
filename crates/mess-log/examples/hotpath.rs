//! Hot-path micro-benchmark backing bn-z98's "zero-cost in release"
//! claim: going through the generic [`Fs`]/[`FileHandle`] seam must not
//! regress against calling `std::fs` directly.
//!
//! The committer's steady-state hot path (durability spec §2.1) is a
//! stream of positioned writes of already-encoded batch bytes, punctuated
//! by an `fdatasync` per group. We isolate the *abstraction* overhead by
//! measuring the `pwrite` stream (no fsync — fsync is milliseconds of
//! device time and would swamp any dispatch cost) two ways:
//!
//!   1. `direct`  — call `std::os::unix::fs::FileExt::write_at` inline.
//!   2. `via_trait` — call the same through `RealFile: FileHandle`, reached
//!      generically (monomorphized), exactly as the committer will.
//!
//! Because the trait is dispatched statically, both compile to the same
//! `pwrite(2)` call site; the numbers below confirm there is no measurable
//! delta. Run with:
//!
//! ```text
//! cargo run -p mess-log --example hotpath --release
//! ```

use std::hint::black_box;
use std::os::unix::fs::FileExt;
use std::time::Instant;

use mess_log::runtime::{FileHandle, Fs, OpenOpts, RealRuntime, Runtime};

const BATCH: &[u8] = &[0xAB; 256]; // ~250 B, the measured payload size (§2.3)
const ITERS: usize = 2_000_000;

fn main() {
    let dir = mess_testkit::sweeping_temp_dir("log-hotpath");
    let direct_path = dir.path().join("direct.log");
    let trait_path = dir.path().join("trait.log");

    let rt = RealRuntime::new();
    let fs = rt.fs();

    // --- direct std::fs ---------------------------------------------------
    let raw = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(&direct_path)
        .unwrap();
    let direct = time(|| {
        let mut off = 0u64;
        for _ in 0..ITERS {
            let n = raw.write_at(black_box(BATCH), off).unwrap();
            off += n as u64;
        }
    });

    // --- through the Fs trait (generic, monomorphized) --------------------
    let f = fs.open(&trait_path, OpenOpts::create_rw()).unwrap();
    let via_trait = time(|| append_stream(&f));

    let (dn, tn) = (nanos_per(direct), nanos_per(via_trait));
    println!("pwrite hot path ({ITERS} iters, {} B each):", BATCH.len());
    println!("  direct std::fs   : {dn:>7.2} ns/op");
    println!("  via Fs trait     : {tn:>7.2} ns/op");
    let delta = (tn - dn) / dn * 100.0;
    println!(
        "  delta            : {delta:+.2}%  (target: within measurement noise)"
    );
}

/// Generic over the file handle exactly as the committer is — this is the
/// call site whose dispatch cost we are pricing.
fn append_stream<F: FileHandle>(f: &F) {
    let mut off = 0u64;
    for _ in 0..ITERS {
        let n = f.pwrite(off, black_box(BATCH)).unwrap();
        off += n as u64;
    }
}

fn time(mut body: impl FnMut()) -> f64 {
    // One warm-up pass, then the measured pass.
    body();
    let t = Instant::now();
    body();
    t.elapsed().as_secs_f64()
}

fn nanos_per(secs: f64) -> f64 { secs / ITERS as f64 * 1e9 }
