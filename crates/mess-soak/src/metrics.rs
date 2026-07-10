//! A bounded-memory latency histogram for the fsync-p99 probe.
//!
//! Under `Durability::Os` (the soak default) every `append_batch` blocks on its
//! own `fdatasync`, so the wall time of an append is dominated by the device
//! barrier. We feed each append's duration here and read back p50/p99/max
//! periodically — the "fsync degradation on aging devices" signal the bone
//! calls out (the round-4 50x finding). A fixed bucket layout keeps memory
//! constant over a multi-hour run rather than growing an unbounded sample vec.

use std::time::Duration;

/// Log-ish bucketed histogram over microseconds. Bucket `i` covers
/// `[2^i, 2^(i+1))` µs for `i < 63`; everything `>= 2^63 µs` (never reached in
/// practice) saturates the top bucket. 64 `u64` counters — trivially small and
/// O(1) to update.
#[derive(Debug, Clone)]
pub struct LatencyHist {
    buckets: [u64; 64],
    count:   u64,
    sum_us:  u128,
    max_us:  u64,
}

impl Default for LatencyHist {
    fn default() -> Self { Self::new() }
}

impl LatencyHist {
    #[must_use]
    pub fn new() -> Self {
        LatencyHist { buckets: [0; 64], count: 0, sum_us: 0, max_us: 0 }
    }

    pub fn record(&mut self, d: Duration) {
        let us = d.as_micros().min(u64::MAX as u128) as u64;
        let idx = if us == 0 { 0 } else { (63 - us.leading_zeros()) as usize };
        self.buckets[idx] += 1;
        self.count += 1;
        self.sum_us += u128::from(us);
        self.max_us = self.max_us.max(us);
    }

    #[must_use]
    pub fn count(&self) -> u64 { self.count }

    #[must_use]
    pub fn max(&self) -> Duration { Duration::from_micros(self.max_us) }

    #[must_use]
    pub fn mean(&self) -> Duration {
        if self.count == 0 {
            return Duration::ZERO;
        }
        Duration::from_micros((self.sum_us / u128::from(self.count)) as u64)
    }

    /// Percentile `p` in `[0,100]`, returned as the *upper* edge of the bucket
    /// the p-th sample falls in (a conservative over-estimate — good for a
    /// ceiling check, which must never under-report tail latency). Empty
    /// histogram returns zero.
    #[must_use]
    pub fn percentile(&self, p: f64) -> Duration {
        if self.count == 0 {
            return Duration::ZERO;
        }
        let p = p.clamp(0.0, 100.0);
        // Rank of the target sample (1-based): ceil(p/100 * count).
        let target = ((p / 100.0) * self.count as f64).ceil().max(1.0) as u64;
        let mut cum = 0u64;
        for (i, &c) in self.buckets.iter().enumerate() {
            cum += c;
            if cum >= target {
                // Upper edge of bucket i is 2^(i+1) µs (bucket 0 covers [0,2)).
                let upper = if i >= 63 { u64::MAX } else { 1u64 << (i + 1) };
                return Duration::from_micros(upper);
            }
        }
        self.max()
    }

    /// One-line summary for the periodic print / abort dump.
    #[must_use]
    pub fn summary(&self) -> String {
        format!(
            "n={} mean={:.2}ms p50={:.2}ms p99={:.2}ms max={:.2}ms",
            self.count,
            self.mean().as_secs_f64() * 1e3,
            self.percentile(50.0).as_secs_f64() * 1e3,
            self.percentile(99.0).as_secs_f64() * 1e3,
            self.max().as_secs_f64() * 1e3,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_is_zero() {
        let h = LatencyHist::new();
        assert_eq!(h.percentile(99.0), Duration::ZERO);
        assert_eq!(h.max(), Duration::ZERO);
        assert_eq!(h.count(), 0);
    }

    #[test]
    fn p99_tracks_the_tail() {
        let mut h = LatencyHist::new();
        // 95 fast samples (~1ms) and 5 slow outliers (~100ms). With 5% in the
        // tail, the rank-99 sample (nearest-rank p99 of 100) IS a slow one, so
        // p99 must expose it — while p50 (rank 50) stays fast. (Note: a SINGLE
        // outlier in 100 sits at p100/max, not p99 — nearest-rank semantics.)
        for _ in 0..95 {
            h.record(Duration::from_millis(1));
        }
        for _ in 0..5 {
            h.record(Duration::from_millis(100));
        }
        assert_eq!(h.count(), 100);
        assert!(
            h.percentile(50.0) < Duration::from_millis(8),
            "p50={:?}",
            h.percentile(50.0)
        );
        assert!(
            h.percentile(99.0) >= Duration::from_millis(64),
            "p99={:?}",
            h.percentile(99.0)
        );
        assert!(h.max() >= Duration::from_millis(100), "max={:?}", h.max());
    }

    #[test]
    fn percentile_is_monotonic() {
        let mut h = LatencyHist::new();
        for ms in 1..=100 {
            h.record(Duration::from_millis(ms));
        }
        assert!(h.percentile(50.0) <= h.percentile(99.0));
        assert!(h.percentile(99.0) <= h.max() * 2 + Duration::from_millis(1));
    }
}
