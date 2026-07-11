//! Per-op timing with rdtsc/rdtscp + sorted-sample quantiles.
//!
//! Instant::now() costs ~20-25 ns per call — useless against 5-50 ns reads.
//! Each op is timed as `t0 = rdtsc; op; t1 = rdtscp`: RDTSCP waits until all
//! previous instructions have executed and previous loads are globally
//! visible, so the sample cannot close before the op's load actually
//! completed (plain rdtsc at t1 let the CPU retire the timestamp ahead of an
//! in-flight independent load, flattering single-load candidates with 0 ns
//! p50s). We calibrate ticks->ns against `Instant` over 200 ms, measure the
//! empty rdtsc..rdtscp overhead, and subtract it from every sample.
//! Quantiles come from the full sorted sample array, never from per-run
//! averages (research/05 §15.2).

use std::time::Instant;

#[inline(always)]
pub fn rdtsc() -> u64 {
    unsafe { core::arch::x86_64::_rdtsc() }
}

/// Serializing-enough timestamp for closing a sample: waits for all prior
/// instructions to execute and prior loads to be globally visible.
#[inline(always)]
pub fn rdtscp() -> u64 {
    let mut aux = 0u32;
    unsafe { core::arch::x86_64::__rdtscp(&mut aux) }
}

pub struct Tsc {
    pub ns_per_tick: f64,
    /// Median empty rdtsc..rdtscp delta (ticks): the measurement overhead.
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

pub struct Quantiles {
    pub p50: f64,
    pub p90: f64,
    pub p99: f64,
    pub p999: f64,
    pub max: f64,
    pub mean: f64,
}

impl Tsc {
    /// Sorted-sample quantiles in ns, rdtsc overhead subtracted.
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
        Quantiles {
            p50: q(0.50),
            p90: q(0.90),
            p99: q(0.99),
            p999: q(0.999),
            max: q(1.0),
            mean,
        }
    }
}

/// u32 quantile over raw counts (used for retry distributions).
pub fn count_quantile(sorted: &[u32], p: f64) -> u32 {
    sorted[((sorted.len() as f64 - 1.0) * p) as usize]
}

/// Pin the calling thread to one core (reduces scheduler noise; cores noted
/// in the report).
pub fn pin_to(core: usize) {
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        libc::CPU_SET(core, &mut set);
        let r = libc::sched_setaffinity(
            0,
            std::mem::size_of::<libc::cpu_set_t>(),
            &set,
        );
        assert_eq!(r, 0, "sched_setaffinity failed");
    }
}

/// VmRSS in bytes from /proc/self/status.
pub fn rss_bytes() -> u64 {
    let s = std::fs::read_to_string("/proc/self/status").unwrap();
    for line in s.lines() {
        if let Some(rest) = line.strip_prefix("VmRSS:") {
            let kb: u64 = rest
                .trim()
                .trim_end_matches("kB")
                .trim()
                .parse()
                .unwrap();
            return kb * 1024;
        }
    }
    panic!("no VmRSS in /proc/self/status");
}
