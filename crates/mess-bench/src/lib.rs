//! bn-4pk: the envelope regression suite.
//!
//! `docs/perf/envelope.md` is a hand-recorded ledger — the per-phase exit-gate
//! numbers, captured once by whichever bone happened to be closing a phase.
//! That is fine as a historical record but it is not a regression ratchet:
//! nothing re-runs those numbers, so a regression between recordings is
//! invisible until someone remembers to re-measure by hand. This crate turns
//! the same reference workload set into ONE runnable harness with a
//! committed floors file (`floors.json`) so a fresh run can be compared
//! against the last-known-good numbers automatically (CI, nightly, or a
//! local pre-merge check).
//!
//! # Design: in-process, not subprocess orchestration
//!
//! Every workload here is a direct port of an already-validated entry point
//! (`mess-log/examples/durable_bench.rs`, `fold_chain_bench.rs`,
//! `recover_bench.rs`; `mess-index`'s `sealed_read_paths` and
//! `payload_replay_bench`; `mess-store`'s `engine_envelope` bench;
//! `mess-log`'s `crash_verify::verify_throughput_bench`) — same corpus
//! shapes, same methodology (best-of-N wall time; the "device state and
//! scheduler jitter only ever make a run slower" argument from
//! `spikes/perf_group_commit`), parameterized by a [`RunSize`] instead of a
//! fixed constant. In-process (not `Command::new("cargo").arg("run")...`)
//! so:
//!
//!   - the smoke variant ([`RunSize::Smoke`]) exercises the *exact* code path
//!     the gated run does, just at tiny N — a separate shell script wired into
//!     CI could rot independently of the thing it is supposed to smoke test; a
//!     shared function parameterized by size cannot.
//!   - one binary, one JSON ledger, one exit code — no fragile stdout
//!     regex-scraping across `cargo` invocations with their own build output
//!     interleaved.
//!
//! # Settle-pacing (perf_group_commit finding)
//!
//! `spikes/perf_group_commit` §"Measurement trap found and controlled":
//! sequential cross-design benchmarking on this SSD poisons itself — tens of
//! GB of writes provoke GC / exhaust the SLC cache and `fdatasync` p50 walks
//! from ~3ms to 150+ms; idle recovery takes minutes. That spike's answer was
//! full interleaved repetition with 12-20s settles between *every* rep of
//! *every* design under comparison — appropriate for a design bake-off, far
//! too slow for a suite that runs nightly (or per-PR smoke) and tracks ONE
//! design's absolute numbers over time, not several designs against each
//! other.
//!
//! This harness's mitigation is narrower and documented as a real trade-off,
//! not glossed over:
//!
//!   1. **Best-of-N per workload** (matching each ported entry point's own
//!      methodology) — device jitter only ever slows a rep down, so the fastest
//!      rep is still the truest read of the code path.
//!   2. **A settle sleep between workloads** ([`settle`]), default
//!      [`DEFAULT_SETTLE_SECS`] seconds, so one workload's write burst does not
//!      directly poison the next workload's fsync latency. This is deliberately
//!      shorter than perf_group_commit's 12-20s — that duration was sized for
//!      *interleaved reps across designs* chasing a few-percent effect; this
//!      ratchet's threshold is a coarse -10% regression, and every
//!      gate-relevant workload already reports its own best-of-N.
//!
//!   What this does **not** claim: it is not a drift-controlled comparison
//!   in the perf_group_commit sense. If the compare step ever flags a
//!   metric close to its floor, re-run with a longer `--settle-secs` (or the
//!   full interleaved-rep methodology) before trusting the regression.
//!
//! # tmpfs refusal
//!
//! `fdatasync` on tmpfs is a no-op — any durability-barrier workload run
//! there fabricates its throughput. [`assert_real_fs`] reads `/proc/mounts`,
//! finds the longest-prefix-matching mount for the scratch root, and refuses
//! (returns `Err`) if its fstype is `tmpfs` or `ramfs`. Called once up front
//! over the whole scratch root — every workload here shares one root.

pub mod workloads;

use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::{Deserialize, Serialize};

/// Suite size: [`RunSize::Full`] is the gated envelope run (matches the
/// corpus sizes recorded in `docs/perf/envelope.md`); [`RunSize::Smoke`] is
/// the reduced-size variant wired into `cargo test -p mess-bench` so the
/// harness's own code paths run on every normal test pass. Smoke numbers are
/// NOT meaningful throughput figures and are never compared against floors.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunSize {
    Full,
    Smoke,
}

/// Default settle pause between workloads in [`RunSize::Full`] mode. See the
/// module docs' "Settle-pacing" section for why this is shorter than
/// `perf_group_commit`'s 12-20s interleaved-rep settles.
pub const DEFAULT_SETTLE_SECS: u64 = 5;

/// One ledger row: `{metric, value, unit, conditions}` plus the run-level
/// date/machine profile carried once on [`Ledger`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Metric {
    pub metric:     String,
    pub value:      f64,
    pub unit:       String,
    pub conditions: String,
}

impl Metric {
    pub fn new(
        metric: impl Into<String>,
        value: f64,
        unit: impl Into<String>,
        conditions: impl Into<String>,
    ) -> Self {
        Metric {
            metric: metric.into(),
            value,
            unit: unit.into(),
            conditions: conditions.into(),
        }
    }
}

/// CPU model, core count, kernel, and the scratch filesystem's type — enough
/// to explain a throughput swing between two ledger runs without re-deriving
/// it from scratch.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MachineProfile {
    pub cpu_model:   String,
    pub cpu_count:   usize,
    pub kernel:      String,
    pub scratch_dir: String,
    pub scratch_fs:  String,
}

impl MachineProfile {
    pub fn probe(scratch: &Path) -> Self {
        MachineProfile {
            cpu_model:   cpu_model(),
            cpu_count:   std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(1),
            kernel:      kernel_release(),
            scratch_dir: scratch.display().to_string(),
            scratch_fs:  fs_type_of(scratch)
                .unwrap_or_else(|_| "unknown".to_string()),
        }
    }
}

fn cpu_model() -> String {
    fs::read_to_string("/proc/cpuinfo")
        .ok()
        .and_then(|s| {
            s.lines()
                .find(|l| l.starts_with("model name"))
                .and_then(|l| l.split_once(':'))
                .map(|(_, v)| v.trim().to_string())
        })
        .unwrap_or_else(|| "unknown".to_string())
}

fn kernel_release() -> String {
    // /proc/version's first "Linux version X.Y.Z-..." token; avoids a `uname`
    // exec for one string.
    fs::read_to_string("/proc/version")
        .ok()
        .and_then(|s| s.split_whitespace().nth(2).map(str::to_string))
        .unwrap_or_else(|| "unknown".to_string())
}

/// A run ledger: date, machine profile, run mode, and every emitted metric.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Ledger {
    pub date:    String,
    pub mode:    String,
    pub machine: MachineProfile,
    pub metrics: Vec<Metric>,
}

// ---------------------------------------------------------------------------
// tmpfs refusal
// ---------------------------------------------------------------------------

/// Parse `/proc/mounts` and return the fstype of the longest-prefix-matching
/// mount point for `path` (which need not exist yet — only its nearest
/// existing ancestor is used for canonicalization).
pub fn fs_type_of(path: &Path) -> std::io::Result<String> {
    let mut probe = path.to_path_buf();
    while !probe.exists() {
        match probe.parent() {
            Some(p) => probe = p.to_path_buf(),
            None => break,
        }
    }
    let canon = fs::canonicalize(&probe).unwrap_or(probe);
    let mounts = fs::read_to_string("/proc/mounts")?;
    let mut best: Option<(usize, String)> = None;
    for line in mounts.lines() {
        let mut fields = line.split_whitespace();
        let _device = fields.next();
        let Some(mount_point) = fields.next() else { continue };
        let Some(fstype) = fields.next() else { continue };
        let mp = unescape_mount(mount_point);
        if canon.starts_with(&mp) {
            let len = mp.as_os_str().len();
            if best.as_ref().map(|(l, _)| len > *l).unwrap_or(true) {
                best = Some((len, fstype.to_string()));
            }
        }
    }
    Ok(best.map(|(_, t)| t).unwrap_or_else(|| "unknown".to_string()))
}

/// `/proc/mounts` octal-escapes spaces/tabs/backslashes/newlines in paths
/// (e.g. a space is `\040`); undo that before comparing prefixes.
fn unescape_mount(s: &str) -> PathBuf {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'\\'
            && i + 3 < bytes.len()
            && bytes[i + 1..i + 4].iter().all(u8::is_ascii_digit)
        {
            let oct = std::str::from_utf8(&bytes[i + 1..i + 4]).unwrap_or("0");
            let v = u8::from_str_radix(oct, 8).unwrap_or(bytes[i]);
            out.push(v);
            i += 4;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    PathBuf::from(String::from_utf8_lossy(&out).into_owned())
}

/// Refuse to proceed if `path` resolves onto a `tmpfs`/`ramfs` mount.
/// `fdatasync` on tmpfs is a no-op (nothing is ever actually flushed to a
/// backing device) so any durability-barrier workload measured there
/// fabricates its number — this is a hard error, not a warning.
pub fn assert_real_fs(path: &Path) -> Result<(), String> {
    let fstype = fs_type_of(path)
        .map_err(|e| format!("could not read /proc/mounts: {e}"))?;
    if fstype == "tmpfs" || fstype == "ramfs" {
        return Err(format!(
            "refusing to run: scratch dir {} resolves onto a {fstype} mount \
             (fdatasync is a no-op there; durable-append numbers would be a \
             lie). Point MESS_BENCH_DIR at a real-fs path (e.g. \
             $HOME/.cache/mess-bench).",
            path.display()
        ));
    }
    Ok(())
}

/// Resolve the scratch root: `MESS_BENCH_DIR` env override, else
/// `$HOME/.cache/mess-bench`. Deliberately never `std::env::temp_dir()` — on
/// this class of machine `/tmp` is tmpfs, which is exactly the trap
/// [`assert_real_fs`] exists to catch; defaulting somewhere real-fs means the
/// smoke test (which must run under plain `cargo test`, no env setup) does
/// not trip it by accident.
///
/// bn-2jr: deliberately NOT routed through `mess-testkit`'s
/// `sweeping_temp_dir` — this function backs the shipped `mess-bench`
/// binary's runtime scratch dir (`main.rs`, not test-only), and
/// `mess-testkit` must stay a dev-dependency everywhere in this workspace.
/// `tests/smoke.rs` nests a self-sweeping per-run subdir under this root's
/// resolved path instead (via `mess_testkit::temp_dir_in`, dev-dependency
/// only) rather than pulling this production helper into that edge.
pub fn default_scratch_root() -> PathBuf {
    if let Some(dir) = std::env::var_os("MESS_BENCH_DIR") {
        return PathBuf::from(dir);
    }
    let home = std::env::var("HOME").unwrap_or_else(|_| ".".to_string());
    PathBuf::from(home).join(".cache").join("mess-bench")
}

/// Sleep between workloads in [`RunSize::Full`] mode. No-op in
/// [`RunSize::Smoke`] mode (the smoke variant must stay fast enough for a
/// normal `cargo test` pass).
pub fn settle(size: RunSize, secs: u64) {
    if size == RunSize::Full && secs > 0 {
        std::thread::sleep(Duration::from_secs(secs));
    }
}

// ---------------------------------------------------------------------------
// Floors / compare
// ---------------------------------------------------------------------------

/// A metric's direction: does a regression mean the value went down (a
/// throughput floor) or up (a size/latency ceiling)?
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Direction {
    /// Regression = value fell below `floor * (1 - tolerance)`.
    Min,
    /// Regression = value rose above `floor * (1 + tolerance)`.
    Max,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Floor {
    pub metric:    String,
    pub direction: Direction,
    pub floor:     f64,
    /// Fractional tolerance, e.g. `0.10` for the default -10%. Always
    /// stored as a positive magnitude; [`Direction`] decides which way it
    /// shifts the bound.
    pub tolerance: f64,
    pub source:    String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FloorsFile {
    pub floors: Vec<Floor>,
}

impl Floor {
    /// The pass/fail bound after applying tolerance.
    pub fn bound(&self) -> f64 {
        match self.direction {
            Direction::Min => self.floor * (1.0 - self.tolerance),
            Direction::Max => self.floor * (1.0 + self.tolerance),
        }
    }

    pub fn passes(&self, value: f64) -> bool {
        match self.direction {
            Direction::Min => value >= self.bound(),
            Direction::Max => value <= self.bound(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Regression {
    pub metric:    String,
    pub value:     f64,
    pub bound:     f64,
    pub floor:     f64,
    pub direction: Direction,
}

impl std::fmt::Display for Regression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.direction {
            Direction::Min => write!(
                f,
                "{}: {:.4} < floor {:.4} * (1 - tol) = {:.4}",
                self.metric, self.value, self.floor, self.bound
            ),
            Direction::Max => write!(
                f,
                "{}: {:.4} > floor {:.4} * (1 + tol) = {:.4}",
                self.metric, self.value, self.floor, self.bound
            ),
        }
    }
}

/// The outcome of a [`compare_report`]: which floors were actually checked,
/// and which of those failed.
///
/// `matched < total` is the case that makes this struct necessary. [`compare`]
/// silently skips a floor whose metric is absent from the ledger, which is
/// correct behaviour — but it means "no regressions" and "the full gate
/// passed" are different statements, and a caller that reports the second when
/// it only established the first is lying about coverage. That skip path used
/// to be unreachable in the gate flow (a full-mode ledger always carried every
/// gated metric, and smoke mode never reached the comparison at all);
/// `run --only` (bn-1r9c) made it reachable, so the counts have to be
/// reportable.
#[derive(Debug, Clone)]
pub struct Comparison {
    /// Floors in the file whose metric was present in the ledger and was
    /// therefore actually checked.
    pub matched:     usize,
    /// Floors in the file, checked or not.
    pub total:       usize,
    /// Checked floors that fell outside their tolerance band.
    pub regressions: Vec<Regression>,
}

impl Comparison {
    /// Floors that were skipped because the ledger has no such metric.
    #[must_use]
    pub fn absent(&self) -> usize { self.total - self.matched }

    /// Whether every floor in the file was actually checked. Only a run for
    /// which this is true may claim the full gate passed.
    #[must_use]
    pub fn is_full_gate(&self) -> bool { self.matched == self.total }
}

/// Compare `ledger` against `floors`. Returns every metric that has a floor
/// AND is present in the ledger AND falls outside its tolerance band.
/// Metrics with no floor entry are ignored (recorded, not gated) — e.g. the
/// fold-chain/`load_verified` rows are tracked but not (yet) named gates in
/// `docs/perf/envelope.md`.
pub fn compare(ledger: &Ledger, floors: &FloorsFile) -> Vec<Regression> {
    compare_report(ledger, floors).regressions
}

/// [`compare`], plus how many floors were actually checked — see
/// [`Comparison`].
pub fn compare_report(ledger: &Ledger, floors: &FloorsFile) -> Comparison {
    let mut regressions = Vec::new();
    let mut matched = 0usize;
    for floor in &floors.floors {
        let Some(m) = ledger.metrics.iter().find(|m| m.metric == floor.metric)
        else {
            continue;
        };
        matched += 1;
        if !floor.passes(m.value) {
            regressions.push(Regression {
                metric:    floor.metric.clone(),
                value:     m.value,
                bound:     floor.bound(),
                floor:     floor.floor,
                direction: floor.direction,
            });
        }
    }
    Comparison { matched, total: floors.floors.len(), regressions }
}

pub fn load_floors(path: &Path) -> std::io::Result<FloorsFile> {
    let s = fs::read_to_string(path)?;
    serde_json::from_str(&s)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
}

pub fn write_ledger(path: &Path, ledger: &Ledger) -> std::io::Result<()> {
    let s = serde_json::to_string_pretty(ledger)?;
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(path, s)
}

pub fn load_ledger(path: &Path) -> std::io::Result<Ledger> {
    let s = fs::read_to_string(path)?;
    serde_json::from_str(&s)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
}

/// Today's date as `YYYY-MM-DD`, derived from the local system clock without
/// pulling in a date/time dependency (the ledger only needs a coarse day
/// stamp, not full RFC3339).
pub fn today() -> String {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let days = secs / 86_400;
    // Civil-from-days (Howard Hinnant's algorithm) — small, dependency-free,
    // proleptic Gregorian, valid for the ranges this tool will ever see.
    let z = days as i64 + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    format!("{y:04}-{m:02}-{d:02}")
}

/// Every workload's selector name, in run order — the vocabulary
/// [`run_selected`]'s `only` filter (and the CLI's `--only`) accepts.
pub const WORKLOAD_NAMES: &[&str] = &[
    "buffered_append",
    "durable_append",
    "sealed_pointer",
    "sealed_payload",
    "engine",
    "fold_chain",
    "load_verified",
    "recovery",
    "reader_contention",
    "live_tail",
];

/// Run every workload at `size`, settle-paced in between when `size ==
/// Full`. `scratch` MUST already have passed [`assert_real_fs`] — this
/// function does not re-check (callers run the check once over the shared
/// root; individual workload functions do not each open their own root).
pub fn run_all(size: RunSize, scratch: &Path, settle_secs: u64) -> Vec<Metric> {
    run_selected(size, scratch, settle_secs, None)
}

/// [`run_all`], restricted to the workloads named in `only` (see
/// [`WORKLOAD_NAMES`]); `None` runs all of them.
///
/// This exists because the suite's own workloads are the main tool for
/// *developing* a workload, and on a shared box re-running all ten to look at
/// one of them is both slow and antisocial — the settle-pacing that keeps one
/// workload's write burst from poisoning the next one's fsync latency is pure
/// waste when only one is selected. Selection changes nothing about how a
/// selected workload runs: same function, same size, same order, and settles
/// still separate whichever workloads did get selected.
///
/// **Gating caveat**: [`compare`] ignores floors whose metric is absent from
/// the ledger, so `run --mode full --only X` enforces X's floors and nothing
/// else. A narrowed run is a measurement, not a gate — and it must not be able
/// to *claim* to be one, which is why the CLI reports a narrowed comparison as
/// `PASS (narrowed): {matched} of {total} floor-gated metrics checked; ...`
/// and reserves the historical `PASS: all {N} floor-gated metrics within
/// tolerance of {path}` line for a ledger that actually covered every floor.
/// See [`Comparison`].
pub fn run_selected(
    size: RunSize,
    scratch: &Path,
    settle_secs: u64,
    only: Option<&[String]>,
) -> Vec<Metric> {
    let selected =
        |name: &str| only.is_none_or(|names| names.iter().any(|n| n == name));
    let mut metrics = Vec::new();
    let mut ran_any = false;

    macro_rules! step {
        ($name:literal, $label:literal, $f:expr) => {{
            if selected($name) {
                // Settle *before* each step but the first one that actually
                // runs, which is the same pause pattern as the original
                // between-every-pair placement when nothing is filtered out,
                // and avoids a pointless trailing sleep when it is.
                if ran_any {
                    settle(size, settle_secs);
                }
                ran_any = true;
                eprintln!("=== {} ({:?}) ===", $label, size);
                metrics.extend($f);
            }
        }};
    }

    step!(
        "buffered_append",
        "buffered append",
        workloads::buffered_append::run(size, scratch)
    );
    step!(
        "durable_append",
        "durable append",
        workloads::durable_append::run(size, scratch)
    );
    step!(
        "sealed_pointer",
        "sealed pointer replay",
        workloads::sealed_pointer::run(size)
    );
    step!(
        "sealed_payload",
        "sealed payload codec",
        workloads::sealed_payload::run(size)
    );
    step!(
        "engine",
        "engine buffered + sealed replay",
        workloads::engine::run(size, scratch)
    );
    step!(
        "fold_chain",
        "fold-chain overhead",
        workloads::fold_chain::run(size)
    );
    step!(
        "load_verified",
        "load_verified throughput",
        workloads::load_verified::run(size)
    );
    step!("recovery", "recovery time", workloads::recovery::run(size, scratch));
    step!(
        "reader_contention",
        "reader contention",
        workloads::reader_contention::run(size, scratch)
    );
    step!(
        "live_tail",
        "live tail / global read",
        workloads::live_tail::run(size, scratch)
    );

    // The last `step!` writes `ran_any` and nothing reads it again; consume it
    // here so adding a workload after this line stays a one-line change.
    let _ = ran_any;
    metrics
}
