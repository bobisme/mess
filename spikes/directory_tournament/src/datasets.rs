//! Dataset generation: synthetic key distributions (research/05 §12 shapes)
//! plus extraction of REAL sealed-segment directory regions from `.pidx`
//! sidecars written by the current engine.

use std::collections::HashSet;
use std::path::Path;

use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use rand_distr::{Distribution, Zipf};

use crate::{Entry, rd_u32, rd_u64};

pub struct Dataset {
    pub name:  String,
    /// Canonical sorted (stream_id, entry) vector.
    pub pairs: Vec<(u64, Entry)>,
    /// Universe span / n (density measure; 1.0 = fully dense).
    pub u_over_n: f64,
}

impl Dataset {
    fn from_keys(name: String, keys: Vec<u64>, rng: &mut StdRng) -> Dataset {
        let n = keys.len();
        let u = (keys[n - 1] - keys[0] + 1) as f64;
        let pairs = keys
            .into_iter()
            .map(|k| (k, synth_entry(rng)))
            .collect::<Vec<_>>();
        Dataset { name, pairs, u_over_n: u / n as f64 }
    }

    pub fn n(&self) -> usize { self.pairs.len() }
}

/// A plausible 48-byte payload (monotone-ish offsets, small batch counts) —
/// content does not affect lookup speed, but exactness tests verify it
/// round-trips bit-exactly.
fn synth_entry(rng: &mut StdRng) -> Entry {
    Entry {
        first_version: rng.random_range(0..1u64 << 40),
        last_version:  rng.random_range(0..1u64 << 40),
        ptr_off:       rng.random_range(0..1u64 << 38),
        skip_off:      rng.random_range(0..1u64 << 38),
        ptr_len:       rng.random_range(1..1u32 << 20),
        n_batches:     rng.random_range(1..4096),
        skip_len:      rng.random_range(0..1u32 << 16),
        reserved:      0,
    }
}

/// Uniform-dense: n distinct keys sampled from a universe of ~1.5n
/// (U/n ~= 1.5 — the "interned dense ids" regime).
pub fn gen_dense(n: usize, seed: u64) -> Dataset {
    let mut rng = StdRng::seed_from_u64(seed);
    let base: u64 = rng.random_range(0..1u64 << 32);
    let u = (n as u64 * 3) / 2;
    // Partial Fisher-Yates over the universe to pick n distinct.
    let mut pool: Vec<u64> = (base..base + u).collect();
    for i in 0..n {
        let j = rng.random_range(i..pool.len());
        pool.swap(i, j);
    }
    let mut keys = pool[..n].to_vec();
    keys.sort_unstable();
    Dataset::from_keys(format!("dense-{}", human(n)), keys, &mut rng)
}

/// Uniform-sparse: n distinct keys from a universe of 1000n (U/n ~= 1k).
pub fn gen_sparse(n: usize, seed: u64) -> Dataset {
    let mut rng = StdRng::seed_from_u64(seed);
    let base: u64 = rng.random_range(0..1u64 << 32);
    let u = n as u64 * 1000;
    let mut set = HashSet::with_capacity(n * 2);
    let mut keys = Vec::with_capacity(n);
    while keys.len() < n {
        let k = base + rng.random_range(0..u);
        if set.insert(k) {
            keys.push(k);
        }
    }
    keys.sort_unstable();
    Dataset::from_keys(format!("sparse-{}", human(n)), keys, &mut rng)
}

/// Zipf-clustered: keys built from Zipf-distributed gaps — long
/// near-consecutive runs broken by rare large jumps (mixed local density,
/// the shape partitioned representations should like).
pub fn gen_zipf_cluster(n: usize, seed: u64) -> Dataset {
    let mut rng = StdRng::seed_from_u64(seed);
    let zipf = Zipf::new(100_000.0, 1.3).unwrap();
    let mut k: u64 = rng.random_range(0..1u64 << 32);
    let mut keys = Vec::with_capacity(n);
    for _ in 0..n {
        keys.push(k);
        k += zipf.sample(&mut rng) as u64; // gap >= 1
    }
    Dataset::from_keys(format!("zipfclust-{}", human(n)), keys, &mut rng)
}

pub fn human(n: usize) -> String {
    if n % 1_000_000 == 0 {
        format!("{}m", n / 1_000_000)
    } else if n % 1_000 == 0 {
        format!("{}k", n / 1_000)
    } else {
        n.to_string()
    }
}

// ---------------------------------------------------------------------------
// Query mixes
// ---------------------------------------------------------------------------

pub struct Queries {
    /// Present keys, uniform over the set.
    pub hit_uniform:  Vec<u64>,
    /// Present keys, Zipf(1.1) over a random popularity permutation.
    pub hit_zipf:     Vec<u64>,
    /// Absent keys drawn from the same [min, max] span.
    pub miss_uniform: Vec<u64>,
    /// 75% hit / 25% miss shuffled — the streaming batch mix.
    pub mixed:        Vec<u64>,
}

pub fn gen_queries(ds: &Dataset, per_phase: usize, seed: u64) -> Queries {
    let mut rng = StdRng::seed_from_u64(seed ^ 0xD1CE);
    let keys: Vec<u64> = ds.pairs.iter().map(|p| p.0).collect();
    let present: HashSet<u64> = keys.iter().copied().collect();
    let n = keys.len();
    let (min, max) = (keys[0], keys[n - 1]);

    let hit_uniform: Vec<u64> =
        (0..per_phase).map(|_| keys[rng.random_range(0..n)]).collect();

    // Zipf over ranks mapped through a random permutation, so popularity is
    // uncorrelated with key order.
    let mut perm: Vec<u32> = (0..n as u32).collect();
    for i in (1..n).rev() {
        perm.swap(i, rng.random_range(0..=i));
    }
    let zipf = Zipf::new(n as f64, 1.1).unwrap();
    let hit_zipf: Vec<u64> = (0..per_phase)
        .map(|_| {
            let rank = (zipf.sample(&mut rng) as usize - 1).min(n - 1);
            keys[perm[rank] as usize]
        })
        .collect();

    let span = max - min + 1;
    let mut miss_uniform = Vec::with_capacity(per_phase);
    while miss_uniform.len() < per_phase {
        // Mostly in-span (the hard misses), a few out-of-span.
        let k = if rng.random_range(0..8u32) == 0 {
            max + 1 + rng.random_range(0..span.max(2))
        } else {
            min + rng.random_range(0..span)
        };
        if !present.contains(&k) {
            miss_uniform.push(k);
        }
    }

    let mut mixed = Vec::with_capacity(per_phase);
    for i in 0..per_phase {
        if rng.random_range(0..4u32) == 0 {
            mixed.push(miss_uniform[i]);
        } else {
            mixed.push(hit_uniform[i]);
        }
    }

    Queries { hit_uniform, hit_zipf, miss_uniform, mixed }
}

// ---------------------------------------------------------------------------
// REAL datasets: sealed .pidx directory regions
// ---------------------------------------------------------------------------

/// Layout constants come straight from the production module so this can
/// never drift from the real sidecar format.
use mess_index::sealed::segment::{DIR_ENTRY_LEN, FOOTER_LEN};

pub struct RealSegment {
    pub segment_id: u64,
    pub dataset:    Dataset,
}

/// Extract every sealed segment's directory from `<store>/sealed/*.pidx`.
/// Each file is first validated by the REAL reader
/// (`SealedSegmentIndex::open` — magic/version/CRC and span checks), then
/// the DIR region is parsed into canonical pairs.
pub fn extract_real(store_dir: &Path) -> Vec<RealSegment> {
    let sealed = store_dir.join("sealed");
    let mut paths: Vec<_> = std::fs::read_dir(&sealed)
        .unwrap_or_else(|e| panic!("read {}: {e}", sealed.display()))
        .flatten()
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|x| x == "pidx"))
        .collect();
    paths.sort();
    let mut out = Vec::new();
    for path in paths {
        // 1) Validate through the production reader.
        let idx = mess_index::sealed::segment::SealedSegmentIndex::open(&path)
            .unwrap_or_else(|e| panic!("open {}: {e}", path.display()));
        let n = idx.stream_count();
        if n == 0 {
            continue;
        }
        // 2) Slice the DIR region out of the raw bytes.
        let bytes = std::fs::read(&path).unwrap();
        let foot = &bytes[bytes.len() - FOOTER_LEN..];
        let dir_off = rd_u64(foot, 0) as usize;
        let n_streams = rd_u32(&bytes, 32) as usize;
        assert_eq!(n_streams, n);
        let mut pairs = Vec::with_capacity(n);
        for i in 0..n {
            let b = dir_off + i * DIR_ENTRY_LEN;
            let (k, e) = Entry::read_record(&bytes[b..b + DIR_ENTRY_LEN]);
            pairs.push((k, e));
        }
        assert!(pairs.windows(2).all(|w| w[0].0 < w[1].0));
        // Cross-check against the reader's own view.
        assert_eq!(
            idx.stream_ids(),
            pairs.iter().map(|p| p.0).collect::<Vec<_>>().as_slice()
        );
        for &(k, e) in pairs.iter().take(64) {
            assert_eq!(
                idx.stream_range(k),
                Some((e.first_version, e.last_version))
            );
        }
        let u = (pairs[n - 1].0 - pairs[0].0 + 1) as f64;
        out.push(RealSegment {
            segment_id: idx.segment_id(),
            dataset:    Dataset {
                name: format!("real-seg{}-n{}", idx.segment_id(), n),
                u_over_n: u / n as f64,
                pairs,
            },
        });
    }
    out
}

/// Header-only peek used by `gen-real` reporting.
pub fn describe_real(segs: &[RealSegment]) -> String {
    let mut s = String::new();
    for seg in segs {
        let ds = &seg.dataset;
        let min = ds.pairs[0].0;
        let max = ds.pairs[ds.pairs.len() - 1].0;
        s.push_str(&format!(
            "  seg {:>3}  n={:<7} min={:<8} max={:<8} U/n={:.2}\n",
            seg.segment_id,
            ds.n(),
            min,
            max,
            ds.u_over_n
        ));
    }
    s
}
