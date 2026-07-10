//! Deterministic PRNG + Zipf stream sampler.
//!
//! The whole driver is seeded from one `u64` (`--seed`): given the same seed,
//! config, and a crash-free run, the sequence of workload actions is identical.
//! (A `sigkill` crash injects real wall-clock nondeterminism by construction —
//! see the README — so determinism is "per seed where feasible", as the bone
//! asks.) `splitmix64` is the same generator `mess-log`'s sigkill harness uses,
//! kept here so the two tiers share one well-understood stream.

/// SplitMix64 — a fast, statistically solid 64-bit generator. One `u64` of
/// state; each `next_u64` advances it and mixes.
#[derive(Debug, Clone)]
pub struct Rng {
    state: u64,
}

impl Rng {
    #[must_use]
    pub fn new(seed: u64) -> Self {
        // Avoid the all-zero fixed point degenerating the first few outputs.
        Rng { state: seed ^ 0x9E37_79B9_7F4A_7C15 }
    }

    pub fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Uniform in `[0, n)`. `n == 0` returns 0.
    pub fn below(&mut self, n: u64) -> u64 {
        if n == 0 {
            return 0;
        }
        self.next_u64() % n
    }

    /// Uniform `f64` in `[0, 1)`.
    pub fn unit(&mut self) -> f64 {
        // Top 53 bits → exact double in [0,1).
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }

    /// `true` with probability `p` (clamped to `[0,1]`).
    pub fn chance(&mut self, p: f64) -> bool { self.unit() < p.clamp(0.0, 1.0) }
}

/// A Zipf sampler over stream indices `0..n`: index `k` (1-based rank) is drawn
/// with probability proportional to `1 / (k+1)^skew`, so a few "hot" streams
/// take most of the traffic — the access pattern that stresses per-stream head
/// contention, dedupe-window turnover, and seal churn on the busy streams while
/// the long tail ages. `skew == 0` degenerates to uniform.
#[derive(Debug, Clone)]
pub struct Zipf {
    /// Cumulative distribution; `cdf[k]` is `P(index <= k)`. `cdf.len() == n`.
    cdf: Vec<f64>,
}

impl Zipf {
    #[must_use]
    pub fn new(n: usize, skew: f64) -> Self {
        assert!(n > 0, "Zipf needs at least one stream");
        let mut weights = Vec::with_capacity(n);
        let mut total = 0.0;
        for k in 0..n {
            let w = 1.0 / ((k as f64) + 1.0).powf(skew);
            total += w;
            weights.push(w);
        }
        let mut cdf = Vec::with_capacity(n);
        let mut acc = 0.0;
        for w in weights {
            acc += w / total;
            cdf.push(acc);
        }
        // Guard the last bucket against float drift so `sample` always lands.
        if let Some(last) = cdf.last_mut() {
            *last = 1.0;
        }
        Zipf { cdf }
    }

    /// Sample a stream index in `0..n`.
    pub fn sample(&self, rng: &mut Rng) -> usize {
        let u = rng.unit();
        // Binary search for the first cdf bucket >= u.
        match self.cdf.binary_search_by(|p| p.partial_cmp(&u).unwrap()) {
            Ok(i) | Err(i) => i.min(self.cdf.len() - 1),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rng_is_deterministic_per_seed() {
        let mut a = Rng::new(42);
        let mut b = Rng::new(42);
        for _ in 0..1000 {
            assert_eq!(a.next_u64(), b.next_u64());
        }
    }

    #[test]
    fn rng_diverges_across_seeds() {
        let mut a = Rng::new(1);
        let mut b = Rng::new(2);
        // Astronomically unlikely to collide on the first draw.
        assert_ne!(a.next_u64(), b.next_u64());
    }

    #[test]
    fn zipf_stays_in_range_and_is_skewed() {
        let z = Zipf::new(10, 1.2);
        let mut rng = Rng::new(7);
        let mut counts = [0u64; 10];
        for _ in 0..100_000 {
            let i = z.sample(&mut rng);
            assert!(i < 10);
            counts[i] += 1;
        }
        // Hot stream 0 must dominate the coldest stream 9 under skew 1.2.
        assert!(counts[0] > counts[9] * 3, "counts={counts:?}");
    }

    #[test]
    fn zipf_uniform_when_skew_zero() {
        let z = Zipf::new(4, 0.0);
        let mut rng = Rng::new(3);
        let mut counts = [0u64; 4];
        for _ in 0..40_000 {
            counts[z.sample(&mut rng)] += 1;
        }
        for c in counts {
            assert!((8_000..12_000).contains(&c), "counts={counts:?}");
        }
    }
}
