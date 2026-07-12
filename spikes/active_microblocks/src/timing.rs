//! rdtsc/rdtscp timing + sorted-sample quantiles + quiet-guard, per the
//! measurement lessons in `spikes/state_kernel_dense_heads/REPORT.md` §2
//! (close every sample with `rdtscp`, never plain `rdtsc`) and
//! `spikes/epoch_dedupe/REPORT.md` §3 (competing-load guard: ambient load
//! ~4-5 on this host, floor 6.0; hold while compilers or sibling benches
//! run). Extended here to also hold while the concurrently-scheduled
//! static-directory spike (`directory_tournament`) is in a measured phase.

use std::time::Instant;

#[inline(always)]
pub fn rdtsc() -> u64 {
    unsafe { core::arch::x86_64::_rdtsc() }
}

/// Serializing-enough close: waits for prior instructions and load
/// visibility (plain rdtsc lets the CPU retire the timestamp ahead of an
/// in-flight load, flattering single-load candidates with 0 ns p50s).
#[inline(always)]
pub fn rdtscp() -> u64 {
    let mut aux = 0u32;
    unsafe { core::arch::x86_64::__rdtscp(&mut aux) }
}

pub struct Tsc {
    pub ns_per_tick:    f64,
    /// Median empty rdtsc..rdtscp delta (ticks): measurement overhead,
    /// subtracted from every sample.
    pub overhead_ticks: u64,
}

pub fn calibrate() -> Tsc {
    let t0 = Instant::now();
    let c0 = rdtsc();
    while t0.elapsed().as_millis() < 200 {
        std::hint::spin_loop();
    }
    let c1 = rdtsc();
    let ns_per_tick = t0.elapsed().as_nanos() as f64 / (c1 - c0) as f64;

    let mut deltas: Vec<u64> = (0..10_000)
        .map(|_| {
            let a = rdtsc();
            let b = rdtscp();
            b - a
        })
        .collect();
    deltas.sort_unstable();
    Tsc { ns_per_tick, overhead_ticks: deltas[deltas.len() / 2] }
}

#[derive(Clone, Copy)]
pub struct Quantiles {
    pub p50:  f64,
    pub p90:  f64,
    pub p99:  f64,
    pub p999: f64,
    pub max:  f64,
    pub mean: f64,
}

impl Tsc {
    /// Sorted-sample quantiles in ns, overhead subtracted (research/05
    /// §15.2: quantiles from full sorted samples, never averages of runs).
    pub fn quantiles_ns(&self, samples: &mut [u32]) -> Quantiles {
        assert!(!samples.is_empty());
        samples.sort_unstable();
        let oh = self.overhead_ticks;
        let q = |p: f64| -> f64 {
            let idx = ((samples.len() as f64 - 1.0) * p) as usize;
            (samples[idx] as f64 - oh as f64).max(0.0) * self.ns_per_tick
        };
        let sum: f64 = samples.iter().map(|&s| s as f64).sum();
        let mean =
            ((sum / samples.len() as f64) - oh as f64).max(0.0) * self.ns_per_tick;
        Quantiles { p50: q(0.50), p90: q(0.90), p99: q(0.99), p999: q(0.999), max: q(1.0), mean }
    }
}

/// Pin the calling thread to one core.
pub fn pin_to(core: usize) {
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        libc::CPU_SET(core, &mut set);
        let r = libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set);
        assert_eq!(r, 0, "sched_setaffinity failed");
    }
}

/// VmRSS in bytes from /proc/self/status.
pub fn rss_bytes() -> u64 {
    let s = std::fs::read_to_string("/proc/self/status").unwrap();
    for line in s.lines() {
        if let Some(rest) = line.strip_prefix("VmRSS:") {
            let kb: u64 = rest.trim().trim_end_matches("kB").trim().parse().unwrap();
            return kb * 1024;
        }
    }
    panic!("no VmRSS in /proc/self/status");
}

/// Competing-load guard. Returns a description of the contention, or None
/// if quiet. "Quiet" = no rustc/cc/ld anywhere, no cargo/bench processes
/// outside our own ancestor chain, no `directory_tournament` bench process
/// (the sibling spike's measured phases must not overlap ours — stagger
/// rule from the bone brief), and 1-min loadavg below 6.0 (host ambient is
/// ~4-5 from unrelated user processes on other cores).
pub fn contention() -> Option<String> {
    let mut ancestors = Vec::new();
    let mut pid = std::process::id();
    while pid != 0 {
        ancestors.push(pid);
        let stat = match std::fs::read_to_string(format!("/proc/{pid}/stat")) {
            Ok(s) => s,
            Err(_) => break,
        };
        pid = stat
            .rfind(')')
            .map(|i| &stat[i + 1..])
            .and_then(|rest| rest.split_whitespace().nth(1))
            .and_then(|p| p.parse().ok())
            .unwrap_or(0);
    }

    let mut offenders = Vec::new();
    if let Ok(rd) = std::fs::read_dir("/proc") {
        for e in rd.flatten() {
            let name = e.file_name();
            let Some(p) = name.to_str().and_then(|s| s.parse::<u32>().ok()) else {
                continue;
            };
            if ancestors.contains(&p) {
                continue;
            }
            let comm = std::fs::read_to_string(format!("/proc/{p}/comm")).unwrap_or_default();
            let comm = comm.trim();
            if matches!(comm, "rustc" | "cc" | "cc1" | "ld" | "lld" | "cargo" | "bench") {
                offenders.push(format!("{p}:{comm}"));
            }
            // The sibling spike's bench binary — matched by argv[0]
            // basename EQUALITY, not cmdline substring: a substring match
            // self-triggers on any shell whose command text merely mentions
            // the name (a monitoring loop deadlocked on exactly that).
            let cmdline =
                std::fs::read_to_string(format!("/proc/{p}/cmdline")).unwrap_or_default();
            let argv0 = cmdline.split('\0').next().unwrap_or("");
            let base = argv0.rsplit('/').next().unwrap_or("");
            if base.starts_with("directory_tournament") {
                offenders.push(format!("{p}:{base}"));
            }
        }
    }
    let load1: f64 = std::fs::read_to_string("/proc/loadavg")
        .ok()
        .and_then(|s| s.split_whitespace().next().map(str::to_string))
        .and_then(|s| s.parse().ok())
        .unwrap_or(0.0);
    if offenders.is_empty() && load1 < 6.0 {
        None
    } else {
        Some(format!("load1={load1:.2} procs=[{}]", offenders.join(",")))
    }
}

/// Block until the machine is quiet (sleep-and-retry). Panics after
/// `max_wait_s` — never publish contaminated quantiles.
pub fn ensure_quiet(max_wait_s: u64) {
    let t0 = Instant::now();
    loop {
        match contention() {
            None => return,
            Some(why) => {
                if t0.elapsed().as_secs() > max_wait_s {
                    panic!("machine still contended after {max_wait_s}s: {why}");
                }
                eprintln!("[quiet-guard] contended ({why}); sleeping 5s");
                std::thread::sleep(std::time::Duration::from_secs(5));
            }
        }
    }
}
