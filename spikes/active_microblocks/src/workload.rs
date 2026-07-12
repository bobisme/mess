//! Workload generation: SplitMix64 RNG, exact-CDF Zipf(1.1) sampling
//! (research/05 §3.1), and pre-generated batch schedules so timed regions
//! are pure index work (spikes convention: no keygen inside timed loops).

use mess_index::{BatchEntry, EventPtr};

/// SplitMix64 — deterministic, seedable, no dependency friction.
#[derive(Clone)]
pub struct Rng(pub u64);

impl Rng {
    pub fn new(seed: u64) -> Self {
        Rng(seed.wrapping_add(0x9E37_79B9_7F4A_7C15))
    }

    #[inline(always)]
    pub fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    #[inline(always)]
    pub fn below(&mut self, n: u64) -> u64 {
        self.next_u64() % n
    }

    #[inline(always)]
    pub fn f64(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 * (1.0 / (1u64 << 53) as f64)
    }
}

/// Exact-CDF Zipf sampler over `n` ranks with exponent `s`, ranks mapped
/// through a random permutation so hot streams are not id-adjacent.
pub struct Zipf {
    cdf:  Vec<f64>,
    perm: Vec<u32>,
}

impl Zipf {
    pub fn new(n: usize, s: f64, seed: u64) -> Self {
        let mut cdf = Vec::with_capacity(n);
        let mut acc = 0.0f64;
        for k in 1..=n {
            acc += 1.0 / (k as f64).powf(s);
            cdf.push(acc);
        }
        let total = acc;
        for v in cdf.iter_mut() {
            *v /= total;
        }
        let mut perm: Vec<u32> = (0..n as u32).collect();
        let mut rng = Rng::new(seed ^ 0xD1F);
        for i in (1..n).rev() {
            let j = rng.below(i as u64 + 1) as usize;
            perm.swap(i, j);
        }
        Zipf { cdf, perm }
    }

    /// Sample a stream id (0-based, permuted).
    #[inline]
    pub fn sample(&self, rng: &mut Rng) -> u64 {
        let u = rng.f64();
        let rank = self.cdf.partition_point(|&c| c < u);
        u64::from(self.perm[rank.min(self.perm.len() - 1)])
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Streams {
    /// One hot stream (id 0).
    Hot,
    /// Zipf(1.1) over `n` streams, each pre-seeded with one batch so every
    /// stream exists before measurement.
    Zipf(usize),
}

/// A pre-generated append schedule: `batches` in commit order plus the
/// final per-stream head versions (for recent-version read sampling).
pub struct Schedule {
    pub batches:  Vec<BatchEntry>,
    pub heads:    Vec<u64>, // final head version per stream (u64::MAX = none)
    pub segment:  u64,
    pub n_streams: usize,
}

pub const SEGMENT: u64 = 1;
/// Simulated batch framing: header + per-event payload (drives offsets).
const BATCH_HEADER: u64 = 64;
const EVENT_BYTES: u64 = 120;

pub fn build_schedule(
    streams: Streams,
    n_batches: usize,
    events_per_batch: u32,
    seed: u64,
) -> Schedule {
    let n_streams = match streams {
        Streams::Hot => 1,
        Streams::Zipf(n) => n,
    };
    let zipf = match streams {
        Streams::Hot => None,
        Streams::Zipf(n) => Some(Zipf::new(n, 1.1, seed)),
    };
    let mut rng = Rng::new(seed);
    let mut versions = vec![0u64; n_streams];
    let mut touched = vec![false; n_streams];
    let mut global = 0u64;
    let mut offset = 0u64;
    let mut batches = Vec::with_capacity(n_batches);

    let push = |sid: u64,
                    versions: &mut [u64],
                    touched: &mut [bool],
                    global: &mut u64,
                    offset: &mut u64,
                    batches: &mut Vec<BatchEntry>| {
        let b = BatchEntry {
            stream_id:            sid,
            first_stream_version: versions[sid as usize],
            frame_count:          events_per_batch,
            first_global_pos:     *global,
            ptr:                  EventPtr { segment_id: SEGMENT, offset: *offset },
        };
        versions[sid as usize] += u64::from(events_per_batch);
        touched[sid as usize] = true;
        *global += u64::from(events_per_batch);
        *offset += BATCH_HEADER + u64::from(events_per_batch) * EVENT_BYTES;
        batches.push(b);
    };

    // Seed every stream once (Zipf) so readers always find a head.
    if let Streams::Zipf(n) = streams {
        for sid in 0..n as u64 {
            push(sid, &mut versions, &mut touched, &mut global, &mut offset, &mut batches);
        }
    }
    while batches.len() < n_batches {
        let sid = match &zipf {
            None => 0,
            Some(z) => z.sample(&mut rng),
        };
        push(sid, &mut versions, &mut touched, &mut global, &mut offset, &mut batches);
    }

    let heads = versions
        .iter()
        .zip(touched.iter())
        .map(|(&v, &t)| if t { v - 1 } else { u64::MAX })
        .collect();
    Schedule { batches, heads, segment: SEGMENT, n_streams }
}

/// Apply a schedule slice as commit groups of `group` batches; the
/// watermark for each group is its last batch's end position (exactly the
/// committer's contract).
pub fn apply_groups<I: crate::Index>(idx: &I, batches: &[BatchEntry], group: usize) {
    for g in batches.chunks(group) {
        let wm = g.last().unwrap().end_pos();
        idx.apply(wm, g);
    }
}
