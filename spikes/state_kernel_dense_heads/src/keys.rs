//! Pre-generated key streams so no RNG runs inside timed loops.

use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};

/// `count` uniform keys over `0..n`.
pub fn uniform(n: u64, count: usize, seed: u64) -> Vec<u64> {
    let mut rng = StdRng::seed_from_u64(seed);
    (0..count).map(|_| rng.random_range(0..n)).collect()
}

/// `count` Zipf(s) keys over `0..n` via an explicit CDF + binary search.
/// (Hand-rolled to avoid a rand_distr version dance against rand 0.10; the
/// CDF is exact, built once outside any timed region.)
pub fn zipf(n: u64, s: f64, count: usize, seed: u64) -> Vec<u64> {
    let mut cdf = Vec::with_capacity(n as usize);
    let mut acc = 0.0f64;
    for i in 1..=n {
        acc += (i as f64).powf(-s);
        cdf.push(acc);
    }
    let total = acc;
    let mut rng = StdRng::seed_from_u64(seed);
    (0..count)
        .map(|_| {
            let r: f64 = rng.random_range(0.0..total);
            cdf.partition_point(|&c| c < r) as u64
        })
        .collect()
}

/// `count` keys confined to one page (`0..4096`) — the pathological
/// same-page scenario.
pub fn one_page(count: usize, seed: u64) -> Vec<u64> {
    uniform(crate::direct::PAGE_SIZE as u64, count, seed)
}
