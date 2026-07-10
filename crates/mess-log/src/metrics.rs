//! A tiny, dependency-free, lock-free metrics core (`bn-e2y`).
//!
//! The store MUST be able to show its own runtime health — above all its
//! durability-barrier (`fdatasync`) latency, which `docs/spec/03-durability.md`
//! §2.6 makes a normative requirement, not optional instrumentation ("a store
//! that cannot show its own p50/p99 barrier latency cannot be operated"). The
//! round-4 machine finding this bone exists to surface: a near-full consumer
//! SSD degrades `fdatasync` ~50× under sustained load (3.3 ms → 150+ ms), and
//! the store MUST surface that **loudly**.
//!
//! This module is the shared primitive layer for that: a fixed-bucket,
//! log-scale [`LatencyHistogram`] (percentiles over a bounded set of atomic
//! counters, no allocation and no lock on the record path), a plain
//! [`Counter`], and a [`DegradationAlarm`] that latches a store-status flag and
//! logs loudly (rate-limited) the first time a barrier crosses a configurable
//! threshold. It has **no external dependency** — the record path is a single
//! `fetch_add` plus a timestamp diff the caller already has (§2.6: the hot
//! append path overhead is "a counter increment + timestamp diff").
//!
//! # Why a log-scale histogram (and its error bound)
//!
//! Barrier latency spans microseconds (page-cache-warm settled device) to
//! hundreds of milliseconds (degraded device) — five orders of magnitude. A
//! linear histogram would need millions of buckets for millisecond resolution
//! at the top; a pure power-of-two histogram has 2× relative error. This uses
//! the HdrHistogram sub-bucket scheme: each octave (power-of-two band) is split
//! into [`SUB_COUNT`] linear sub-buckets, so the relative error of any reported
//! percentile is bounded by `1 / SUB_COUNT` (≈ 6% at `SUB_BITS = 4`) across the
//! whole range, at a fixed [`BUCKET_COUNT`] atomic-`u64` cost (~7.8 KiB).

use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

// ---------------------------------------------------------------------------
// Bucket geometry (HdrHistogram-style sub-buckets)
// ---------------------------------------------------------------------------

/// Sub-bucket precision: each octave is divided into `2^SUB_BITS` linear
/// sub-buckets. `4` → 16 sub-buckets per octave → ≤ ~6% relative error.
pub const SUB_BITS: u32 = 4;
/// Linear sub-buckets per octave (`2^SUB_BITS`).
pub const SUB_COUNT: usize = 1 << SUB_BITS;
/// Highest octave a `u64` nanosecond value can occupy (`floor(log2)` max).
const MAX_OCTAVE: u32 = 63;
/// Total fixed bucket count: the `[0, SUB_COUNT)` linear region plus one block
/// of `SUB_COUNT` sub-buckets per octave from `SUB_BITS..=MAX_OCTAVE`.
pub const BUCKET_COUNT: usize = ((MAX_OCTAVE - SUB_BITS + 1) as usize) * SUB_COUNT + SUB_COUNT;

/// The bucket index a nanosecond value falls into. Monotonic in `v`.
#[inline]
fn bucket_index(v: u64) -> usize {
    if v < SUB_COUNT as u64 {
        // Small values map linearly (exact) — the bottom octaves are the
        // sub-bucket region's natural extension.
        return v as usize;
    }
    let octave = MAX_OCTAVE - v.leading_zeros(); // floor(log2(v)) >= SUB_BITS
    let sub = ((v >> (octave - SUB_BITS)) & (SUB_COUNT as u64 - 1)) as usize;
    let base = ((octave - SUB_BITS + 1) as usize) * SUB_COUNT;
    base + sub
}

/// The inclusive lower bound (in nanoseconds) of bucket `i`.
#[inline]
fn bucket_lower(i: usize) -> u64 {
    if i < SUB_COUNT {
        return i as u64;
    }
    let rel = i - SUB_COUNT;
    let octave_group = (rel / SUB_COUNT) as u32; // 0 => octave == SUB_BITS
    let sub = (rel % SUB_COUNT) as u64;
    let shift = octave_group; // octave - SUB_BITS
    (SUB_COUNT as u64 + sub) << shift
}

/// The width (in nanoseconds) of bucket `i`.
#[inline]
fn bucket_width(i: usize) -> u64 {
    if i < SUB_COUNT {
        return 1;
    }
    let octave_group = ((i - SUB_COUNT) / SUB_COUNT) as u32;
    1u64 << octave_group
}

/// A representative value for bucket `i`: its midpoint. Reported as the
/// percentile estimate — true value is within `± bucket_width/2`, i.e. the
/// `1 / SUB_COUNT` relative bound above.
#[inline]
fn bucket_midpoint(i: usize) -> u64 {
    bucket_lower(i) + bucket_width(i) / 2
}

// ---------------------------------------------------------------------------
// Latency histogram
// ---------------------------------------------------------------------------

/// A lock-free, fixed-size log-scale latency histogram over nanoseconds.
///
/// `record` is a single atomic `fetch_add` on one bucket plus three cheap
/// atomic updates (count, running sum, running max) — no allocation, no lock,
/// safe to call from any thread. `count`/`sum`/`max` are tracked exactly
/// alongside the bucketed distribution so mean and max are not subject to the
/// histogram's bucket quantisation.
pub struct LatencyHistogram {
    buckets: Box<[AtomicU64]>,
    count: AtomicU64,
    sum_nanos: AtomicU64,
    max_nanos: AtomicU64,
}

impl Default for LatencyHistogram {
    fn default() -> Self {
        let buckets = (0..BUCKET_COUNT).map(|_| AtomicU64::new(0)).collect::<Vec<_>>();
        LatencyHistogram {
            buckets: buckets.into_boxed_slice(),
            count: AtomicU64::new(0),
            sum_nanos: AtomicU64::new(0),
            max_nanos: AtomicU64::new(0),
        }
    }
}

impl std::fmt::Debug for LatencyHistogram {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LatencyHistogram").field("snapshot", &self.snapshot()).finish()
    }
}

impl LatencyHistogram {
    /// A fresh, empty histogram.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Record one observed latency. The single mutating call on the hot path.
    #[inline]
    pub fn record(&self, dt: Duration) {
        let nanos = u64::try_from(dt.as_nanos()).unwrap_or(u64::MAX);
        self.record_nanos(nanos);
    }

    /// Record one observed latency already expressed in nanoseconds.
    #[inline]
    pub fn record_nanos(&self, nanos: u64) {
        self.buckets[bucket_index(nanos)].fetch_add(1, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
        self.sum_nanos.fetch_add(nanos, Ordering::Relaxed);
        // Monotonic-max via a relaxed CAS loop.
        let mut cur = self.max_nanos.load(Ordering::Relaxed);
        while nanos > cur {
            match self.max_nanos.compare_exchange_weak(
                cur,
                nanos,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(observed) => cur = observed,
            }
        }
    }

    /// Number of samples recorded so far.
    #[must_use]
    pub fn count(&self) -> u64 {
        self.count.load(Ordering::Relaxed)
    }

    /// The estimated value (nanoseconds) at quantile `q ∈ [0, 1]`. Returns `0`
    /// for an empty histogram. The estimate is the midpoint of the bucket the
    /// quantile's rank falls in (relative error ≤ `1 / SUB_COUNT`).
    #[must_use]
    pub fn quantile_nanos(&self, q: f64) -> u64 {
        let total = self.count.load(Ordering::Relaxed);
        if total == 0 {
            return 0;
        }
        let q = q.clamp(0.0, 1.0);
        // Rank of the target sample (1-based): the smallest 1-based index whose
        // cumulative count reaches `ceil(q * total)`.
        let rank = (q * total as f64).ceil().max(1.0) as u64;
        let mut cumulative = 0u64;
        for i in 0..BUCKET_COUNT {
            cumulative += self.buckets[i].load(Ordering::Relaxed);
            if cumulative >= rank {
                return bucket_midpoint(i);
            }
        }
        // Rounding could leave `rank` one past the last non-empty bucket; fall
        // back to the max sample so a percentile never under-reports the tail.
        self.max_nanos.load(Ordering::Relaxed)
    }

    /// Mean latency in nanoseconds (exact running sum ÷ count), `0` if empty.
    #[must_use]
    pub fn mean_nanos(&self) -> u64 {
        let n = self.count.load(Ordering::Relaxed);
        self.sum_nanos.load(Ordering::Relaxed).checked_div(n).unwrap_or(0)
    }

    /// The largest sample seen (exact), `0` if empty.
    #[must_use]
    pub fn max_nanos(&self) -> u64 {
        self.max_nanos.load(Ordering::Relaxed)
    }

    /// A consistent-enough point-in-time read of the standard percentiles.
    #[must_use]
    pub fn snapshot(&self) -> LatencySnapshot {
        LatencySnapshot {
            count: self.count(),
            p50_nanos: self.quantile_nanos(0.50),
            p95_nanos: self.quantile_nanos(0.95),
            p99_nanos: self.quantile_nanos(0.99),
            max_nanos: self.max_nanos(),
            mean_nanos: self.mean_nanos(),
        }
    }
}

/// A point-in-time read of a [`LatencyHistogram`]. All fields are nanoseconds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct LatencySnapshot {
    /// Samples recorded.
    pub count: u64,
    /// Estimated 50th percentile.
    pub p50_nanos: u64,
    /// Estimated 95th percentile.
    pub p95_nanos: u64,
    /// Estimated 99th percentile.
    pub p99_nanos: u64,
    /// Largest sample (exact).
    pub max_nanos: u64,
    /// Mean (exact).
    pub mean_nanos: u64,
}

// ---------------------------------------------------------------------------
// Counter
// ---------------------------------------------------------------------------

/// A monotonic lock-free `u64` counter.
#[derive(Debug, Default)]
pub struct Counter(AtomicU64);

impl Counter {
    /// A counter starting at zero.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Add one.
    #[inline]
    pub fn incr(&self) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }

    /// Add `n`.
    #[inline]
    pub fn add(&self, n: u64) {
        self.0.fetch_add(n, Ordering::Relaxed);
    }

    /// The current value.
    #[must_use]
    pub fn get(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }
}

// ---------------------------------------------------------------------------
// Degradation alarm
// ---------------------------------------------------------------------------

/// The default fsync-degradation threshold (`docs/spec/03-durability.md` §2.6):
/// on the reference hardware a settled `fdatasync` is ~3.3 ms; a degraded
/// near-full device jumps to 150+ ms. `50 ms` sits well clear of the healthy
/// band and well below the degraded one.
pub const DEFAULT_FSYNC_THRESHOLD: Duration = Duration::from_millis(50);

/// Minimum wall-clock gap between two loud degradation log lines, so a store
/// stuck degraded for minutes does not flood the log every barrier.
const LOG_RATE_LIMIT: Duration = Duration::from_secs(5);

/// A latching, rate-limited degradation alarm for a single latency signal.
///
/// [`observe`](Self::observe) is called with every barrier latency. When a
/// sample crosses the (configurable) threshold it **latches** a store-status
/// flag ([`is_tripped`](Self::is_tripped)) — the mandatory, non-optional signal
/// this bone exists to add — counts the crossing, and logs **loudly** to
/// stderr, rate-limited to at most one line per [`LOG_RATE_LIMIT`]. The flag is
/// sticky: once a store has demonstrably hit degraded-device latency an
/// operator must see it, even if the device later recovers; the trip count and
/// the live p99 tell them whether it is ongoing.
pub struct DegradationAlarm {
    threshold_nanos: AtomicU64,
    tripped: AtomicBool,
    trips: AtomicU64,
    /// Wall-clock instant of the last emitted log line, for rate limiting.
    /// `None` until the first trip. Behind a `Mutex` only because `Instant`
    /// is not atomic; it is never touched on a non-tripping observe.
    last_log: Mutex<Option<Instant>>,
    /// A human label for the signal, used in the loud log line.
    label: &'static str,
}

impl DegradationAlarm {
    /// A fresh alarm for `label`, tripping at `threshold`.
    #[must_use]
    pub fn new(label: &'static str, threshold: Duration) -> Self {
        DegradationAlarm {
            threshold_nanos: AtomicU64::new(
                u64::try_from(threshold.as_nanos()).unwrap_or(u64::MAX),
            ),
            tripped: AtomicBool::new(false),
            trips: AtomicU64::new(0),
            last_log: Mutex::new(None),
            label,
        }
    }

    /// Reconfigure the trip threshold at runtime.
    pub fn set_threshold(&self, threshold: Duration) {
        self.threshold_nanos
            .store(u64::try_from(threshold.as_nanos()).unwrap_or(u64::MAX), Ordering::Relaxed);
    }

    /// The current trip threshold, in nanoseconds.
    #[must_use]
    pub fn threshold_nanos(&self) -> u64 {
        self.threshold_nanos.load(Ordering::Relaxed)
    }

    /// Feed one observed latency. Returns `true` iff this sample crossed the
    /// threshold. Latches the flag and (rate-limited) logs loudly on a cross.
    pub fn observe(&self, latency: Duration) -> bool {
        let nanos = u64::try_from(latency.as_nanos()).unwrap_or(u64::MAX);
        if nanos < self.threshold_nanos.load(Ordering::Relaxed) {
            return false;
        }
        self.tripped.store(true, Ordering::Relaxed);
        let trips = self.trips.fetch_add(1, Ordering::Relaxed) + 1;
        self.maybe_log(nanos, trips);
        true
    }

    /// Whether the alarm has ever tripped (the sticky store-status flag).
    #[must_use]
    pub fn is_tripped(&self) -> bool {
        self.tripped.load(Ordering::Relaxed)
    }

    /// How many samples have crossed the threshold.
    #[must_use]
    pub fn trips(&self) -> u64 {
        self.trips.load(Ordering::Relaxed)
    }

    /// Emit the loud line if the rate limit allows. Separate so tests can
    /// exercise the latch without asserting on stderr.
    fn maybe_log(&self, nanos: u64, trips: u64) {
        let now = Instant::now();
        let mut last = self.last_log.lock().expect("degradation alarm log lock");
        let due = last.is_none_or(|t| now.duration_since(t) >= LOG_RATE_LIMIT);
        if due {
            *last = Some(now);
            drop(last);
            let threshold_ms = self.threshold_nanos.load(Ordering::Relaxed) as f64 / 1e6;
            eprintln!(
                "!!! mess DEGRADED: {} latency {:.1} ms crossed {:.1} ms threshold \
                 ({} crossings) — near-full/contended device fdatasync stall \
                 (docs/spec/03-durability.md §2.6); durable appends are stalling",
                self.label,
                nanos as f64 / 1e6,
                threshold_ms,
                trips,
            );
        }
    }
}

impl std::fmt::Debug for DegradationAlarm {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DegradationAlarm")
            .field("label", &self.label)
            .field("threshold_nanos", &self.threshold_nanos())
            .field("tripped", &self.is_tripped())
            .field("trips", &self.trips())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bucket_index_is_monotonic_and_contiguous() {
        // Index never decreases as the value grows, and the linear region joins
        // the sub-bucket region without a gap.
        let mut last = 0usize;
        for v in 0..10_000u64 {
            let i = bucket_index(v);
            assert!(i >= last, "index must be monotonic: v={v} i={i} last={last}");
            last = i;
        }
        // Boundary: the first sub-bucketed value lands right after the linear
        // region.
        assert_eq!(bucket_index(SUB_COUNT as u64 - 1), SUB_COUNT - 1);
        assert_eq!(bucket_index(SUB_COUNT as u64), SUB_COUNT);
    }

    #[test]
    fn bucket_bounds_contain_their_values() {
        for v in [0u64, 1, 15, 16, 31, 1000, 1_000_000, 150_000_000, u64::MAX / 2] {
            let i = bucket_index(v);
            let lo = bucket_lower(i);
            let hi = lo + bucket_width(i);
            assert!(lo <= v && v < hi, "v={v} not in [{lo},{hi}) bucket {i}");
        }
    }

    #[test]
    fn percentiles_over_uniform_distribution() {
        // 1 ms .. 1000 ms, one sample each. p50 ≈ 500 ms, p99 ≈ 990 ms, within
        // the histogram's ~6% relative bound.
        let h = LatencyHistogram::new();
        for i in 1..=1000u64 {
            h.record(Duration::from_millis(i));
        }
        assert_eq!(h.count(), 1000);
        let approx = |got: u64, want_ms: f64| {
            let got_ms = got as f64 / 1e6;
            let rel = (got_ms - want_ms).abs() / want_ms;
            assert!(rel <= 0.07, "got {got_ms:.2} ms want {want_ms} ms (rel {rel:.3})");
        };
        approx(h.quantile_nanos(0.50), 500.0);
        approx(h.quantile_nanos(0.95), 950.0);
        approx(h.quantile_nanos(0.99), 990.0);
        // Mean and max are exact (tracked outside the buckets).
        assert_eq!(h.max_nanos(), Duration::from_millis(1000).as_nanos() as u64);
        approx(h.mean_nanos(), 500.5);
    }

    #[test]
    fn percentiles_capture_a_bimodal_tail() {
        // 980 fast barriers (~3 ms) and 20 degraded ones (~150 ms): the p99 must
        // land in the degraded mode, not the fast one — the exact shape §2.6
        // says the metric MUST reveal.
        let h = LatencyHistogram::new();
        for _ in 0..980 {
            h.record(Duration::from_micros(3300));
        }
        for _ in 0..20 {
            h.record(Duration::from_millis(150));
        }
        let p50 = h.quantile_nanos(0.50) as f64 / 1e6;
        let p99 = h.quantile_nanos(0.99) as f64 / 1e6;
        assert!((3.0..4.0).contains(&p50), "p50 {p50:.2} ms should be the fast mode");
        assert!(p99 >= 140.0, "p99 {p99:.2} ms should reveal the degraded tail");
    }

    #[test]
    fn empty_histogram_reports_zero() {
        let h = LatencyHistogram::new();
        assert_eq!(h.snapshot(), LatencySnapshot::default());
    }

    #[test]
    fn counter_counts() {
        let c = Counter::new();
        c.incr();
        c.add(41);
        assert_eq!(c.get(), 42);
    }

    #[test]
    fn alarm_latches_only_past_threshold() {
        let alarm = DegradationAlarm::new("fsync", Duration::from_millis(50));
        // Below threshold: no trip, flag stays clear.
        assert!(!alarm.observe(Duration::from_millis(3)));
        assert!(!alarm.is_tripped());
        assert_eq!(alarm.trips(), 0);
        // At/above threshold: trips and latches.
        assert!(alarm.observe(Duration::from_millis(150)));
        assert!(alarm.is_tripped());
        assert_eq!(alarm.trips(), 1);
        // Sticky: a later healthy sample does not clear the flag.
        assert!(!alarm.observe(Duration::from_millis(2)));
        assert!(alarm.is_tripped());
        assert_eq!(alarm.trips(), 1);
    }

    #[test]
    fn alarm_threshold_is_reconfigurable() {
        let alarm = DegradationAlarm::new("fsync", Duration::from_millis(50));
        assert!(!alarm.observe(Duration::from_millis(10)));
        alarm.set_threshold(Duration::from_millis(5));
        assert!(alarm.observe(Duration::from_millis(10)));
        assert!(alarm.is_tripped());
    }
}
