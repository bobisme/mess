//! bn-4pk: the envelope regression suite CLI.
//!
//! ```text
//! # full gated run (real-fs scratch REQUIRED; refuses tmpfs)
//! CLANG_PATH=/usr/bin/clang cargo run -p mess-bench --release -- run
//!
//! # smoke run: reduced sizes, floors never enforced (this is what
//! # `cargo test -p mess-bench` also exercises in-process, see tests/smoke.rs)
//! cargo run -p mess-bench -- run --mode smoke
//!
//! # compare an existing ledger against a floors file without re-running
//! cargo run -p mess-bench -- compare --ledger ledger.json --floors floors.json
//!
//! # one workload only, for developing/re-measuring it on a shared box
//! cargo run -p mess-bench --release -- run --only live_tail --no-compare
//! cargo run -p mess-bench -- list-workloads
//! ```
//!
//! CI/nightly wiring (documented here + `crates/mess-bench/README.md`):
//! per-PR CI runs the smoke path via the crate's own `cargo test -p
//! mess-bench` (fast, unconditional, no floor gate — catches harness rot,
//! not perf regressions). A nightly lane runs `run --mode full` in
//! `--release` on a real-fs runner and fails the job on a non-zero exit
//! (any metric below its floor's tolerance band), mirroring
//! `.github/workflows/phase3-exit-gate.yml`'s pattern.

use std::path::{Path, PathBuf};
use std::process::ExitCode;

use clap::{Parser, Subcommand};
use mess_bench::{
    Ledger, MachineProfile, RunSize, assert_real_fs, compare_report,
    default_scratch_root, load_floors, load_ledger, run_selected, today,
    write_ledger,
};

#[derive(Parser)]
#[command(name = "mess-bench", about = "bn-4pk envelope regression suite")]
struct Cli {
    #[command(subcommand)]
    cmd: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Run the reference workload set and emit a JSON ledger.
    Run {
        /// "full" (gated envelope sizes, compared against floors by default)
        /// or "smoke" (reduced sizes, floors never enforced).
        #[arg(long, default_value = "full")]
        mode:        String,
        /// Where to write the ledger JSON.
        #[arg(long, default_value = "ledger.json")]
        out:         PathBuf,
        /// Scratch root for the fs-backed workloads. Defaults to
        /// `MESS_BENCH_DIR` or `$HOME/.cache/mess-bench` — never `/tmp`.
        #[arg(long)]
        scratch:     Option<PathBuf>,
        /// Seconds to sleep between workloads (full mode only). See the
        /// crate docs' "Settle-pacing" section.
        #[arg(long, default_value_t = mess_bench::DEFAULT_SETTLE_SECS)]
        settle_secs: u64,
        /// Floors file to compare against (full mode only).
        #[arg(long, default_value = "floors.json")]
        floors:      PathBuf,
        /// Skip the floors comparison even in full mode.
        #[arg(long)]
        no_compare:  bool,
        /// Run only these workloads (repeat the flag or comma-separate).
        /// Names: see `list-workloads`. A narrowed run is a MEASUREMENT, not
        /// a gate — floors for workloads that did not run are skipped, so the
        /// nightly gate must never pass this.
        #[arg(long, value_delimiter = ',')]
        only:        Vec<String>,
    },
    /// Compare an existing ledger JSON against a floors file.
    Compare {
        #[arg(long)]
        ledger: PathBuf,
        #[arg(long, default_value = "floors.json")]
        floors: PathBuf,
    },
    /// Print the workload selector names accepted by `run --only`.
    ListWorkloads,
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    match cli.cmd {
        Command::Run {
            mode,
            out,
            scratch,
            settle_secs,
            floors,
            no_compare,
            only,
        } => {
            let unknown: Vec<&String> = only
                .iter()
                .filter(|n| !mess_bench::WORKLOAD_NAMES.contains(&n.as_str()))
                .collect();
            if !unknown.is_empty() {
                eprintln!(
                    "unknown --only workload(s): {}\nknown: {}",
                    unknown
                        .iter()
                        .map(|s| s.as_str())
                        .collect::<Vec<_>>()
                        .join(", "),
                    mess_bench::WORKLOAD_NAMES.join(", ")
                );
                return ExitCode::from(2);
            }
            let size = match mode.as_str() {
                "full" => RunSize::Full,
                "smoke" => RunSize::Smoke,
                other => {
                    eprintln!(
                        "unknown --mode {other:?}; use 'full' or 'smoke'"
                    );
                    return ExitCode::from(2);
                }
            };
            let scratch = scratch.unwrap_or_else(default_scratch_root);
            if let Err(e) = std::fs::create_dir_all(&scratch) {
                eprintln!(
                    "could not create scratch dir {}: {e}",
                    scratch.display()
                );
                return ExitCode::from(2);
            }
            if let Err(e) = assert_real_fs(&scratch) {
                eprintln!("{e}");
                return ExitCode::from(2);
            }

            let metrics = run_selected(
                size,
                &scratch,
                settle_secs,
                (!only.is_empty()).then_some(only.as_slice()),
            );
            let ledger = Ledger {
                date: today(),
                mode: mode.clone(),
                machine: MachineProfile::probe(&scratch),
                metrics,
            };
            if let Err(e) = write_ledger(&out, &ledger) {
                eprintln!("could not write ledger to {}: {e}", out.display());
                return ExitCode::from(2);
            }
            eprintln!("ledger written: {}", out.display());
            for m in &ledger.metrics {
                eprintln!("  {:<40} {:>16.2} {}", m.metric, m.value, m.unit);
            }

            if size == RunSize::Smoke || no_compare {
                eprintln!(
                    "(smoke mode or --no-compare: floors not enforced; run \
                     `compare` explicitly for a gated check)"
                );
                return ExitCode::SUCCESS;
            }
            if !only.is_empty() {
                eprintln!(
                    "NOTE: --only narrowed this run to {}; floors for every \
                     other workload are skipped because their metrics are \
                     absent from the ledger. Expect the `PASS (narrowed):` \
                     line below, never `PASS: all N ...` — this is a \
                     measurement, not a gate.",
                    only.join(", ")
                );
            }

            run_compare(&ledger, &floors)
        }
        Command::Compare { ledger, floors } => {
            let ledger = match load_ledger(&ledger) {
                Ok(l) => l,
                Err(e) => {
                    eprintln!(
                        "could not read ledger {}: {e}",
                        ledger.display()
                    );
                    return ExitCode::from(2);
                }
            };
            run_compare(&ledger, &floors)
        }
        Command::ListWorkloads => {
            for name in mess_bench::WORKLOAD_NAMES {
                println!("{name}");
            }
            ExitCode::SUCCESS
        }
    }
}

fn run_compare(ledger: &Ledger, floors_path: &Path) -> ExitCode {
    let floors = match load_floors(floors_path) {
        Ok(f) => f,
        Err(e) => {
            eprintln!(
                "could not read floors file {}: {e}",
                floors_path.display()
            );
            return ExitCode::from(2);
        }
    };
    let report = compare_report(ledger, &floors);
    if !report.regressions.is_empty() {
        eprintln!(
            "FAIL: {} metric(s) below floor tolerance:",
            report.regressions.len()
        );
        for r in &report.regressions {
            eprintln!("  {r}");
        }
        return ExitCode::FAILURE;
    }
    // Two different claims, so two different strings. "No regressions" is not
    // "the gate passed": `compare` skips a floor whose metric is absent from
    // the ledger, and since `run --only` (bn-1r9c) a ledger can legitimately
    // be missing most of them. The PASS line below is what CI and the lead's
    // gate checks grep for, so a narrowed run must not be able to emit it —
    // a `NOTE:` printed above a false assertion does not cure the assertion.
    if report.is_full_gate() {
        eprintln!(
            "PASS: all {} floor-gated metrics within tolerance of {}",
            report.total,
            floors_path.display()
        );
    } else {
        eprintln!(
            "PASS (narrowed): {} of {} floor-gated metrics checked; {} absent \
             from this ledger — NOT a full gate.",
            report.matched,
            report.total,
            report.absent()
        );
    }
    ExitCode::SUCCESS
}
