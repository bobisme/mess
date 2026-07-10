//! bn-1hl experiment 2 entry point: `cargo run --release -- [bench|probe|all]`
//! (default `all`). See `bench.rs` for the group-commit-shaped latency sweep
//! and `probe.rs` for the startup capability-probe design sketch.
//!
//! Always point this at a **real, non-tmpfs filesystem** — durability
//! primitives (`fdatasync`, `O_DSYNC`) are no-ops or meaningless on tmpfs.
//! Set `TMPDIR` to a real-fs scratch dir before running, e.g.:
//!
//! ```text
//! mkdir -p ~/.cache/mess-bench-scratch
//! TMPDIR=~/.cache/mess-bench-scratch cargo run --release -- all
//! ```

mod bench;
mod probe;

use std::path::{Path, PathBuf};

fn scratch_dir() -> PathBuf {
    let root = std::env::var("TMPDIR").map(PathBuf::from).unwrap_or_else(|_| PathBuf::from("/tmp"));
    if root == Path::new("/tmp") {
        eprintln!(
            "WARNING: TMPDIR is unset or /tmp (commonly tmpfs) — fdatasync/O_DSYNC numbers \
             from a tmpfs backing are meaningless (fdatasync is a no-op there). Set TMPDIR to \
             a real-fs directory, e.g. TMPDIR=$HOME/.cache/mess-bench-scratch."
        );
    }
    let dir = root.join(format!("odsync-probe-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("create scratch dir");
    dir
}

fn run_probe_demo(scratch: &std::path::Path) {
    println!("\n=== capability probe demo (design sketch, not production-wired) ===");
    let cfg = probe::ProbeConfig { enabled: true, ..probe::ProbeConfig::default() };
    match probe::run_probe(&scratch.join("probe-demo.dat"), &cfg) {
        None => println!("probe: could not run / opted-out -> Capability::UseFdatasync (safe default)"),
        Some(r) => {
            println!(
                "probe: fdatasync p50={:.1}us  odsync p50={:.1}us  speedup={:.2}x  trap_bw_ratio={:.2}x  lying_signal={}",
                r.fdatasync_p50.as_secs_f64() * 1e6,
                r.odsync_p50.as_secs_f64() * 1e6,
                r.speedup,
                r.trap_implied_bw_ratio,
                r.lying_signal,
            );
            println!("verdict: {:?}", r.capability);
        }
    }
}

fn main() {
    let mode = std::env::args().nth(1).unwrap_or_else(|| "all".to_string());
    let scratch = scratch_dir();
    let cleanup = scopeguard(&scratch);

    match mode.as_str() {
        "bench" => bench::main_bench(&scratch),
        "probe" => run_probe_demo(&scratch),
        "all" => {
            bench::main_bench(&scratch);
            run_probe_demo(&scratch);
        }
        other => eprintln!("unknown mode {other:?}; expected bench|probe|all"),
    }
    drop(cleanup);
}

/// Tiny RAII scratch-dir cleanup (no `scopeguard` crate dependency).
struct Cleanup(std::path::PathBuf);
impl Drop for Cleanup {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).ok();
    }
}
fn scopeguard(p: &std::path::Path) -> Cleanup {
    Cleanup(p.to_path_buf())
}
