//! Soak configuration — every knob the bone's `--flags` map to, plus the
//! resource ceilings the run exits on.

use std::path::PathBuf;
use std::time::Duration;

use mess_log::committer::Durability;
use mess_store::EngineOptions;

/// How a crash cycle is injected.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CrashMode {
    /// In-process: drop the [`LogEngine`](mess_store::LogEngine) handle (which
    /// gracefully shuts the committer and joins the seal thread) and reopen the
    /// same directory. Exercises the full open → recover → rehydrate →
    /// resume-in-place path continuously, with the shadow model preserved in
    /// the driver so every probe keeps checking across the "restart". This is
    /// what the CI smoke and the default binary run use.
    DropReopen,
    /// Out-of-process: fork the `soak-child` worker, let it drive a real
    /// concurrent workload against the same dir, `SIGKILL` it mid-flight, then
    /// reopen and reconcile recovery against the child's ack ledger. The
    /// faithful "no destructors ran" crash. Orchestrated by the binary, not the
    /// in-process driver.
    Sigkill,
}

/// The full soak configuration.
#[derive(Debug, Clone)]
pub struct Config {
    /// Total wall-clock run length.
    pub duration:            Duration,
    /// Number of distinct streams the Zipf sampler ranges over.
    pub streams:             usize,
    /// Concurrent writer tasks in the SIGKILL child (`--crash-mode sigkill`)
    /// ONLY. The in-process drop-reopen driver is strictly sequential — every
    /// append is awaited to a definite result before the next action — so this
    /// knob does not apply to it (an in-process concurrent-writer mode is
    /// future work).
    pub writers:             usize,
    /// Drop-reopen mode: number of actions between crash cycles; `0` disables
    /// crashes. Pinned to the deterministic action count — never wall-clock —
    /// so the store state at every crash point reproduces exactly across
    /// machines and CPU load (the action stream is a pure function of
    /// `(seed, config)`).
    pub crash_every_actions: u64,
    /// Sigkill mode ONLY: wall-clock delay before the child is `SIGKILL`ed.
    /// Wall-clock is inherent there (the kill races a live child process), so
    /// that mode is not action-deterministic. `ZERO` falls back to 5s.
    pub crash_every:         Duration,
    /// Master seed — deterministic per seed for a crash-free run.
    pub seed:                u64,
    /// Store directory. MUST NOT be tmpfs (checked at startup).
    pub dir:                 PathBuf,
    /// Zipf skew (0 == uniform; higher == hotter head).
    pub zipf_skew:           f64,
    /// Max events per appended batch (each batch is `1..=max_batch`).
    pub max_batch:           usize,
    /// Target number of concurrent subscribers.
    pub subscribers:         usize,
    /// Active-segment size. Deliberately small so rolls (and thus background
    /// seals + retention churn) are frequent.
    pub segment_size:        u64,
    /// Durability mode. Defaults to `Os` so every append is a real
    /// `fdatasync` and the fsync-p99 probe measures a genuine barrier.
    pub durability:          Durability,
    /// RSS ceiling in bytes (0 disables).
    pub rss_ceiling_bytes:   u64,
    /// Open-fd ceiling (0 disables).
    pub fd_ceiling:          usize,
    /// fsync p99 ceiling (`ZERO` disables).
    pub fsync_p99_ceiling:   Duration,
    /// How the binary injects crashes.
    pub crash_mode:          CrashMode,
    /// Interval between periodic metric prints.
    pub metrics_every:       Duration,
    /// Print per-action chatter.
    pub verbose:             bool,
    /// If set, when the post-reopen reconcile finds ILLEGAL extras
    /// (duplicate-of-acked or fabricated) the driver writes the full
    /// classification breakdown to this JSON path before aborting (bn-3dr
    /// diagnostics).
    pub dump_extras:         Option<PathBuf>,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            duration:            Duration::from_secs(120),
            streams:             256,
            writers:             8,
            // ~30s of actions at the observed ~650 actions/s on the dev box —
            // but deterministic: exactly this many actions between crashes on
            // ANY machine, however fast or loaded.
            crash_every_actions: 20_000,
            crash_every:         Duration::from_secs(30),
            seed:                0x50AC_5EED,
            dir:                 default_dir(),
            zipf_skew:           1.1,
            max_batch:           4,
            subscribers:         4,
            // 256 KiB: small enough that a soak rolls segments every few
            // hundred events, so the seal/retention path turns over
            // constantly.
            segment_size:        256 * 1024,
            durability:          Durability::Os,
            // 1 GiB RSS ceiling: a plateauing soak sits far below this; a leak
            // marches through it. 0 to disable.
            rss_ceiling_bytes:   1024 * 1024 * 1024,
            // 512 open fds: the engine holds a bounded handle set (active
            // segment, meta, a few sealed sidecars); an fd leak climbs past
            // this.
            fd_ceiling:          512,
            // Off by default: the probe still tracks and prints p99, but a
            // ceiling abort is opt-in (device-dependent).
            fsync_p99_ceiling:   Duration::ZERO,
            crash_mode:          CrashMode::DropReopen,
            metrics_every:       Duration::from_secs(10),
            verbose:             false,
            dump_extras:         None,
        }
    }
}

impl Config {
    /// The engine open options this config implies.
    #[must_use]
    pub fn engine_options(&self) -> EngineOptions {
        EngineOptions {
            durability: self.durability,
            segment_size: self.segment_size,
            ..EngineOptions::default()
        }
    }

    /// Multi-line human summary (header of every run and every abort dump).
    #[must_use]
    pub fn summary(&self) -> String {
        let crash = match self.crash_mode {
            CrashMode::DropReopen => format!(
                "crash_every_actions={} (sequential driver, no writer \
                 concurrency)",
                self.crash_every_actions
            ),
            CrashMode::Sigkill => {
                format!(
                    "kill_delay={:?} writers={}",
                    self.crash_every, self.writers
                )
            }
        };
        format!(
            "duration={:?} streams={} subscribers={} {crash} \
             crash_mode={:?}\n  seed={:#x} zipf_skew={} max_batch={} \
             durability={:?} segment_size={}B\n  dir={}\n  ceilings: rss={}B \
             fd={} fsync_p99={:?}",
            self.duration,
            self.streams,
            self.subscribers,
            self.crash_mode,
            self.seed,
            self.zipf_skew,
            self.max_batch,
            self.durability,
            self.segment_size,
            self.dir.display(),
            self.rss_ceiling_bytes,
            self.fd_ceiling,
            self.fsync_p99_ceiling,
        )
    }
}

/// Default store dir: `$HOME/.cache/mess-soak` (ext4 on this box), falling back
/// to the CWD. Never `/tmp` — that is tmpfs here and the driver would refuse
/// it.
fn default_dir() -> PathBuf {
    if let Ok(home) = std::env::var("HOME") {
        return PathBuf::from(home).join(".cache").join("mess-soak");
    }
    PathBuf::from("mess-soak-data")
}
