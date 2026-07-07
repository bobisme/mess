//! Seeded workload: identical logical event sequence as spikes/vertical_slice
//! (same rand 0.10 StdRng, same seed 42, same payload generator), so numbers
//! are directly comparable. ~250-byte JSON-ish payloads, 10,000 streams
//! Zipf(s=1.1), batch size parameterized (10 = the vertical_slice shape).

use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use rand_distr::{Distribution, Zipf};

pub const STREAMS: u64 = 10_000;
pub const ZIPF_S: f64 = 1.1;

pub struct BatchSpec {
    pub stream: u64,
    pub expected: Option<u64>, // expected current head version (None = new stream)
    pub payloads: Vec<Vec<u8>>,
}

pub fn payload(rng: &mut StdRng, stream: u64, seq: u64) -> Vec<u8> {
    let amount: u32 = rng.random_range(1..100_000);
    let cents: u32 = rng.random_range(0..100);
    let user: u32 = rng.random_range(0..10_000);
    let note_len: usize = rng.random_range(10..80);
    let note: String = (0..note_len)
        .map(|_| (b'a' + rng.random_range(0..26u8)) as char)
        .collect();
    format!(
        r#"{{"type":"AccountCredited","stream":"stream-{stream:05}","seq":{seq},"amount":{amount}.{cents:02},"currency":"USD","actor":"user-{user:04}","source":"vertical-slice-bench","note":"{note}","occurred_at":"2026-07-07T12:34:56.789Z","schema":"v1"}}"#
    )
    .into_bytes()
}

/// Returns (batches, per-stream final event counts).
pub fn generate(n_events: usize, batch: usize, seed: u64) -> (Vec<BatchSpec>, Vec<u64>) {
    assert_eq!(n_events % batch, 0);
    let n_batches = n_events / batch;
    let mut rng = StdRng::seed_from_u64(seed);
    let zipf = Zipf::new(STREAMS as f64, ZIPF_S).unwrap();
    let mut counts = vec![0u64; STREAMS as usize];
    let mut out = Vec::with_capacity(n_batches);
    for _ in 0..n_batches {
        let s = (zipf.sample(&mut rng) as u64).clamp(1, STREAMS) - 1;
        let count = counts[s as usize];
        let expected = if count == 0 { None } else { Some(count - 1) };
        let first = count;
        let payloads = (0..batch as u64).map(|i| payload(&mut rng, s, first + i)).collect();
        counts[s as usize] += batch as u64;
        out.push(BatchSpec { stream: s, expected, payloads });
    }
    (out, counts)
}

/// Partition batches among writers by stream ownership (stream % writers),
/// preserving per-stream order. Returns index lists into `batches`.
pub fn partition(batches: &[BatchSpec], writers: usize) -> Vec<Vec<u32>> {
    let mut parts = vec![Vec::new(); writers];
    for (i, b) in batches.iter().enumerate() {
        parts[(b.stream % writers as u64) as usize].push(i as u32);
    }
    parts
}
