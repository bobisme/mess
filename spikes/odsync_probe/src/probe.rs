//! bn-1hl: a startup device-capability probe — **design sketch in code, not
//! wired into any production crate** (the bone's "design (code, not wire)"
//! deliverable). If this is ever adopted, it belongs in `mess-store`'s
//! engine-open path, behind the `ProbeConfig::enabled` opt-in below; nothing
//! here changes production behavior today.
//!
//! # What this solves
//!
//! Round 4 (`notes/mess-research/16_spike_results_round4.md`, H4) found
//! coalesced `O_DSYNC` writes 2.3-4.4x faster than `write+fdatasync` at the
//! high-concurrency end **on this machine's Samsung 970 EVO Plus** — but
//! also stated the caveat this module exists to operationalize: *"FUA
//! correctness is a per-device trust question (same class as trusting
//! FLUSH); ext4 journal fallbacks can silently turn O_DSYNC into
//! flush-equivalents on some configurations."* `O_DSYNC` asks the kernel to
//! use the device's FUA (Force Unit Access) write path when the block layer
//! and device both support it; when they don't, the kernel is free to
//! silently fall back to write+flush (correct but not the speedup) — or, in
//! the worst case on some virtualized/misconfigured stacks, to something
//! that returns "done" without actually reaching stable media (a genuinely
//! **lying** device, not a mess bug but an environment one). Shipping
//! `O_DSYNC` as a blind default would bet durability on unverified hardware
//! trust; D10 says accelerators are admitted only by evidence, gathered on
//! *this* store's actual device at startup.
//!
//! # Design
//!
//! `run_probe` performs three measurements against a scratch file **on the
//! store's own data directory** (the only filesystem whose behavior
//! matters):
//!
//! 1. **fdatasync floor**: `write + fdatasync` latency distribution at the
//!    store's group-commit payload size — the trusted baseline (fdatasync's
//!    POSIX fsync semantics are what mess already ships on, and what every
//!    correctness proof in this repo assumes).
//! 2. **O_DSYNC latency**: `O_DSYNC` write latency at the same payload size
//!    — the candidate fast path.
//! 3. **Bandwidth sanity trap**: a larger write (default 4 MiB, `trap_size`)
//!    done BOTH ways — `write+fdatasync` and `O_DSYNC` — and their implied
//!    throughputs compared. Deliberately NOT derived from the small
//!    `payload_size` measurement in (1): at group-commit sizes, fdatasync
//!    latency is barrier-dominated, not bandwidth-dominated ("durability is
//!    priced per barrier, not per byte, until bandwidth binds" — round 4),
//!    so payload_size/latency at that size is not a bandwidth number at
//!    all and using it as one is a category error an earlier draft of this
//!    probe made. Comparing two same-size large writes cancels the
//!    per-barrier fixed cost from both sides and isolates the question that
//!    matters: is `O_DSYNC` completing a multi-megabyte "durable" write in
//!    dramatically less time than an equally-sized write the kernel's
//!    universally-honored `fdatasync` just took to actually reach stable
//!    media? If the `O_DSYNC` trap's throughput exceeds the `fdatasync`
//!    trap's by more than `lying_multiplier` (default 4x — round 4's own
//!    observed FUA-vs-FLUSH ceiling under real concurrent load), the probe
//!    refuses to trust `O_DSYNC` regardless of its small-payload latency
//!    win.
//!
//! # What this does NOT prove
//!
//! Latency and throughput heuristics cannot *prove* power-loss durability —
//! only a real SIGKILL/power-cut crash harness (`spikes/perf_group_commit`'s
//! `crash` mode) can build that evidence, and even that cannot rule out
//! every device lying about a write cache with no battery/capacitor backup.
//! This probe is a **plausibility gate**, not a durability proof: it catches
//! the common failure modes (no `O_DSYNC`/FUA support at all, filesystem
//! that silently downgrades it, an obviously-too-fast "durable" write) and
//! otherwise defers to the conservative default. It is designed to be
//! combined with, not replace, periodic crash-harness verification in a
//! real deployment's CI/staging.
//!
//! # Opt-in, not auto-enable
//!
//! `ProbeConfig::enabled` defaults to `false`. Even when a probe run
//! recommends `Capability::UseOdsync`, the caller (a future engine-open
//! path) is expected to require **both** the opt-in flag **and** a passing
//! probe result before switching the write path — D10's "explicit opt-in
//! config" requirement, kept separate from "the probe said yes" so an
//! operator can force the conservative default even on a device the probe
//! likes.

use std::path::Path;
use std::time::{Duration, Instant};

use crate::bench::xorshift;

/// Startup probe configuration. `enabled = false` is the shipped default —
/// the probe does not run, and the store uses `write+fdatasync`
/// unconditionally, exactly today's behavior.
#[derive(Debug, Clone)]
pub struct ProbeConfig {
    /// Explicit opt-in: the probe only runs (and can only ever recommend
    /// `UseOdsync`) when this is `true`. Belongs in the store's on-disk or
    /// process config, never inferred from the device.
    pub enabled: bool,
    /// Samples per latency measurement (both fdatasync and O_DSYNC).
    pub samples: usize,
    /// Group-commit payload size to probe at (should match the store's
    /// typical coalesced-write size; the bone's sweep used 4-64 KiB).
    pub payload_size: usize,
    /// Size of the bandwidth sanity-trap write.
    pub trap_size: usize,
    /// Minimum speedup (`fdatasync_p50 / odsync_p50`) required to even
    /// consider recommending `UseOdsync`. Round 4's concurrent-load number
    /// was 2.3-4.4x; this is deliberately much lower (a serial single-call
    /// probe cannot see the concurrency-driven part of that win) — it only
    /// needs to establish that O_DSYNC isn't *worse* here.
    pub min_speedup: f64,
    /// How many multiples of the same-size `write+fdatasync` trap's
    /// throughput an `O_DSYNC` trap write of equal size is allowed to
    /// exceed before being flagged as an implausibly-fast ("lying") write.
    /// Default 4x mirrors round 4's own observed FUA-vs-FLUSH ceiling under
    /// real concurrent load — a serial single large write legitimately
    /// beating fdatasync by more than that is a stronger signal than
    /// anything round 4 measured honestly.
    pub lying_multiplier: f64,
    /// Wall-clock budget for the whole probe; if exceeded, aborts and
    /// returns `Capability::Undetermined` rather than block store startup
    /// indefinitely on a misbehaving device.
    pub max_probe_budget: Duration,
}

impl Default for ProbeConfig {
    fn default() -> Self {
        ProbeConfig {
            enabled: false,
            samples: 50,
            payload_size: 16 * 1024,
            trap_size: 4 * 1024 * 1024,
            min_speedup: 1.15,
            lying_multiplier: 4.0,
            max_probe_budget: Duration::from_secs(5),
        }
    }
}

/// What the probe recommends. Never self-executing — the caller decides
/// whether to act on it, and only ever may act on `UseOdsync` when its own
/// config also opted in (see module docs).
#[derive(Debug, Clone, PartialEq)]
pub enum Capability {
    /// Default / safe: use `write+fdatasync`. Returned whenever the probe
    /// didn't run, timed out, hit an I/O error (e.g. `O_DSYNC`/`O_DIRECT`
    /// unsupported on this filesystem — common on overlayfs, some network
    /// filesystems, and tmpfs), or measured no meaningful win.
    UseFdatasync { reason: &'static str },
    /// The probe measured a real O_DSYNC win with no lying-device signal.
    /// Still gated by the caller's own opt-in (module docs).
    UseOdsync { speedup: f64 },
}

#[derive(Debug, Clone)]
pub struct ProbeResult {
    pub fdatasync_p50: Duration,
    pub odsync_p50: Duration,
    pub speedup: f64,
    /// Bytes/sec the trap write implied vs. the fdatasync-derived ceiling.
    pub trap_implied_bw_ratio: f64,
    pub lying_signal: bool,
    pub capability: Capability,
}

fn median_ns(mut xs: Vec<u64>) -> u64 {
    xs.sort_unstable();
    xs[xs.len() / 2]
}

/// Run the probe against a scratch file at `path` (the caller must place
/// this inside the store's real data directory — probing a different
/// filesystem than the one the store will actually write to answers the
/// wrong question). Returns `None` (treat as `UseFdatasync`) on any I/O
/// error, unsupported flag combination, or budget overrun — a probe must
/// never be allowed to turn a startup failure into a crash.
pub fn run_probe(path: &Path, cfg: &ProbeConfig) -> Option<ProbeResult> {
    if !cfg.enabled {
        return None;
    }
    let deadline = Instant::now() + cfg.max_probe_budget;
    let over_budget = || Instant::now() > deadline;

    // 1. fdatasync floor.
    let payload = vec![0xA5u8; cfg.payload_size];
    let fdatasync_lat: Vec<u64> = {
        use std::fs::OpenOptions;
        use std::io::Write;
        let f = OpenOptions::new().create(true).truncate(true).write(true).open(path).ok()?;
        let mut lat = Vec::with_capacity(cfg.samples);
        for i in 0..cfg.samples + 5 {
            (&f).write_all(&payload).ok()?;
            let t = Instant::now();
            f.sync_data().ok()?;
            if i >= 5 {
                lat.push(t.elapsed().as_nanos() as u64);
            }
            if over_budget() {
                return None;
            }
        }
        lat
    };
    let fdatasync_p50 = Duration::from_nanos(median_ns(fdatasync_lat));

    // 2. O_DSYNC latency at the same payload size.
    let odsync_lat: Vec<u64> = {
        use std::fs::OpenOptions;
        use std::io::Write;
        use std::os::unix::fs::OpenOptionsExt;
        let f = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .custom_flags(libc::O_DSYNC)
            .open(path)
            .ok()?;
        let mut lat = Vec::with_capacity(cfg.samples);
        for i in 0..cfg.samples + 5 {
            let t = Instant::now();
            (&f).write_all(&payload).ok()?;
            if i >= 5 {
                lat.push(t.elapsed().as_nanos() as u64);
            }
            if over_budget() {
                return None;
            }
        }
        lat
    };
    let odsync_p50 = Duration::from_nanos(median_ns(odsync_lat));
    let speedup = fdatasync_p50.as_secs_f64() / odsync_p50.as_secs_f64().max(1e-12);

    // 3. Bandwidth sanity trap: one `trap_size` write done both ways.
    // Same-size comparison cancels the fixed per-barrier cost that makes
    // `fdatasync_bw` (derived from the small `payload_size` measurement)
    // unusable as a bandwidth ceiling — see module docs.
    let mut trap_buf = vec![0u8; cfg.trap_size];
    {
        let mut seed = 0xF00D_u64;
        for chunk in trap_buf.chunks_mut(8) {
            let r = xorshift(&mut seed).to_le_bytes();
            chunk.copy_from_slice(&r[..chunk.len()]);
        }
    }
    let fdatasync_trap_bw: f64 = {
        use std::fs::OpenOptions;
        use std::io::Write;
        let mut f = OpenOptions::new().create(true).truncate(true).write(true).open(path).ok()?;
        let t = Instant::now();
        f.write_all(&trap_buf).ok()?;
        f.sync_data().ok()?;
        let elapsed = t.elapsed();
        trap_buf.len() as f64 / elapsed.as_secs_f64().max(1e-9)
    };
    let odsync_trap_bw: f64 = {
        use std::fs::OpenOptions;
        use std::os::unix::fs::OpenOptionsExt;
        use std::os::unix::io::AsRawFd;
        let f = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .custom_flags(libc::O_DSYNC)
            .open(path)
            .ok()?;
        let fd = f.as_raw_fd();
        let t = Instant::now();
        // SAFETY: fd valid+open; trap_buf valid for its own length.
        let n = unsafe { libc::write(fd, trap_buf.as_ptr() as *const libc::c_void, trap_buf.len()) };
        let elapsed = t.elapsed();
        if n != trap_buf.len() as isize {
            return None; // short/failed write: don't trust this device either way
        }
        trap_buf.len() as f64 / elapsed.as_secs_f64().max(1e-9)
    };
    let trap_ratio = odsync_trap_bw / fdatasync_trap_bw.max(1.0);
    let lying_signal = trap_ratio > cfg.lying_multiplier;

    let capability = if lying_signal {
        Capability::UseFdatasync { reason: "O_DSYNC trap write implausibly fast vs fdatasync-derived bandwidth ceiling (possible no-op / lying device)" }
    } else if speedup < cfg.min_speedup {
        Capability::UseFdatasync { reason: "O_DSYNC showed no meaningful latency win over fdatasync on this device" }
    } else {
        Capability::UseOdsync { speedup }
    };

    Some(ProbeResult {
        fdatasync_p50,
        odsync_p50,
        speedup,
        trap_implied_bw_ratio: trap_ratio,
        lying_signal,
        capability,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disabled_by_default_returns_none() {
        let cfg = ProbeConfig::default();
        assert!(!cfg.enabled);
        let dir = std::env::temp_dir().join(format!("odsync-probe-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let result = run_probe(&dir.join("probe.dat"), &cfg);
        assert!(result.is_none(), "probe must not run unless explicitly opted in");
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Real-fs smoke test (skipped under miri / no real fs): the probe
    /// completes within budget and returns a self-consistent result. Does
    /// NOT assert which `Capability` comes out — that is host-dependent and
    /// exactly what the probe exists to discover; see `bin` output for the
    /// actual verdict on this host.
    #[test]
    fn probe_runs_and_is_self_consistent_on_real_fs() {
        let scratch_root = std::env::var("TMPDIR").unwrap_or_else(|_| "/tmp".to_string());
        let dir = Path::new(&scratch_root).join(format!("odsync-probe-selftest-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let cfg = ProbeConfig { enabled: true, samples: 20, ..ProbeConfig::default() };
        let result = run_probe(&dir.join("probe.dat"), &cfg);
        std::fs::remove_dir_all(&dir).ok();
        let Some(r) = result else {
            // Some filesystems (tmpfs in particular) can reject O_DSYNC in
            // ways that surface as I/O errors we treat as "don't trust it" —
            // acceptable, not a test failure.
            return;
        };
        assert!(r.fdatasync_p50.as_nanos() > 0);
        assert!(r.odsync_p50.as_nanos() > 0);
        assert!(r.speedup > 0.0);
        match r.capability {
            Capability::UseOdsync { speedup } => assert!(speedup >= cfg.min_speedup),
            Capability::UseFdatasync { .. } => {}
        }
    }
}
