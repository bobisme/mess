//! bn-1hl experiment 1: sealed stream-directory lookup.
//!
//! Compares four structures for the exact-match `stream_id -> pointer`
//! lookup that sits behind mess-index's `BinaryFuse16` membership filter
//! (`crates/mess-index/src/sealed/filter.rs`) and the sealed sidecar's
//! `HashMap<u64, DirEntry>` directory (`crates/mess-index/src/sealed/segment.rs`,
//! `SealedSegmentIndex::dir`), plus mess-index's fjall-backed `MetaStore`
//! (`crates/mess-index/src/meta/mod.rs`, the persistent stream-head registry):
//!
//! 1. `std::collections::HashMap<u64, V>` — what the sealed directory uses today.
//! 2. Sorted `Vec<(u64, V)>` + `binary_search_by_key` — the baseline the bone
//!    asks for.
//! 3. `mph::Mph<V>` — a hand-rolled fixed-seed FKS two-level perfect hash
//!    (see `src/mph.rs`), built once at "seal time" like the BinaryFuse16
//!    filter already is.
//! 4. `fjall` — the production on-disk KV backend already in the tree
//!    (mess-index's `MetaStore`), real-fs backed (not tmpfs), as the
//!    persistent-store comparison point the bone names explicitly.
//!
//! Sizes: 1,000 / 100,000 / 1,000,000 streams (the bone's "realistic
//! directory sizes"). Value = `Ptr { a, b, c }` (24 bytes, u64 x3) — sized
//! like a real `EventPtr`+range, not the full 56-byte `DirEntry`, to keep
//! the memory comparison about indexing overhead rather than payload size
//! (payload size is identical across all four structures for a fixed V).
//!
//! Run: `cargo run --release` from this directory (needs a real, non-tmpfs
//! scratch dir for fjall — set `TMPDIR=$HOME/.cache/mess-bench-scratch`,
//! never `/tmp`, matching the repo's durable-work convention).

mod mph;

use std::collections::HashMap;
use std::time::Instant;

use mph::Mph;

#[derive(Clone, Copy, Default, PartialEq, Eq, Debug)]
struct Ptr {
    a: u64,
    b: u64,
    c: u64,
}

/// Deterministic xorshift64 PRNG — kept dep-light (no `rand` dependency),
/// matching the seeding style already used by `mess-index`'s own
/// property tests (`sealed/filter.rs`).
fn xorshift(seed: &mut u64) -> u64 {
    *seed ^= *seed << 13;
    *seed ^= *seed >> 7;
    *seed ^= *seed << 17;
    *seed
}

/// Distinct pseudo-random `u64` stream ids — mirrors production where
/// `stream_id`s are pre-hashed (not sequential), the realistic case for a
/// hash table / MPH comparison. Sequential ids would flatter a sorted-array
/// binary search's cache behavior unrealistically.
fn gen_keys(n: usize, seed: &mut u64) -> Vec<u64> {
    let mut set = std::collections::BTreeSet::new();
    while set.len() < n {
        let k = xorshift(seed);
        if k != u64::MAX {
            set.insert(k);
        }
    }
    set.into_iter().collect()
}

struct Timing {
    build_ns: f64,
    hit_ns: f64,
    miss_ns: f64,
    approx_bytes: usize,
}

fn bench_hashmap(items: &[(u64, Ptr)], probe_hits: &[u64], probe_miss: &[u64]) -> Timing {
    let t0 = Instant::now();
    let mut map: HashMap<u64, Ptr> = HashMap::with_capacity(items.len());
    for &(k, v) in items {
        map.insert(k, v);
    }
    let build_ns = t0.elapsed().as_nanos() as f64;

    let t0 = Instant::now();
    let mut acc = 0u64;
    for &k in probe_hits {
        if let Some(v) = map.get(&k) {
            acc ^= v.a;
        }
    }
    std::hint::black_box(acc);
    let hit_ns = t0.elapsed().as_nanos() as f64 / probe_hits.len() as f64;

    let t0 = Instant::now();
    let mut hits = 0u64;
    for &k in probe_miss {
        if map.contains_key(&k) {
            hits += 1;
        }
    }
    assert_eq!(hits, 0);
    let miss_ns = t0.elapsed().as_nanos() as f64 / probe_miss.len() as f64;

    // hashbrown's RawTable: capacity buckets, each a control byte + (K,V).
    let cap = map.capacity();
    let approx_bytes = cap * (1 + std::mem::size_of::<(u64, Ptr)>());
    Timing { build_ns, hit_ns, miss_ns, approx_bytes }
}

fn bench_sorted(items: &[(u64, Ptr)], probe_hits: &[u64], probe_miss: &[u64]) -> Timing {
    let t0 = Instant::now();
    let mut v = items.to_vec();
    v.sort_unstable_by_key(|&(k, _)| k);
    let build_ns = t0.elapsed().as_nanos() as f64;

    let t0 = Instant::now();
    let mut acc = 0u64;
    for &k in probe_hits {
        if let Ok(idx) = v.binary_search_by_key(&k, |&(kk, _)| kk) {
            acc ^= v[idx].1.a;
        }
    }
    std::hint::black_box(acc);
    let hit_ns = t0.elapsed().as_nanos() as f64 / probe_hits.len() as f64;

    let t0 = Instant::now();
    let mut hits = 0u64;
    for &k in probe_miss {
        if v.binary_search_by_key(&k, |&(kk, _)| kk).is_ok() {
            hits += 1;
        }
    }
    assert_eq!(hits, 0);
    let miss_ns = t0.elapsed().as_nanos() as f64 / probe_miss.len() as f64;

    let approx_bytes = v.len() * std::mem::size_of::<(u64, Ptr)>();
    Timing { build_ns, hit_ns, miss_ns, approx_bytes }
}

fn bench_mph(items: &[(u64, Ptr)], probe_hits: &[u64], probe_miss: &[u64]) -> Timing {
    let t0 = Instant::now();
    let mph = Mph::build(items);
    let build_ns = t0.elapsed().as_nanos() as f64;
    eprintln!(
        "  mph: {} slots for {} keys ({:.2}x blowup)",
        mph.slot_count(),
        items.len(),
        mph.slot_count() as f64 / items.len().max(1) as f64
    );

    let t0 = Instant::now();
    let mut acc = 0u64;
    for &k in probe_hits {
        if let Some(v) = mph.get(k) {
            acc ^= v.a;
        }
    }
    std::hint::black_box(acc);
    let hit_ns = t0.elapsed().as_nanos() as f64 / probe_hits.len() as f64;

    let t0 = Instant::now();
    let mut hits = 0u64;
    for &k in probe_miss {
        if mph.get(k).is_some() {
            hits += 1;
        }
    }
    assert_eq!(hits, 0);
    let miss_ns = t0.elapsed().as_nanos() as f64 / probe_miss.len() as f64;

    Timing { build_ns, hit_ns, miss_ns, approx_bytes: mph.approx_bytes() }
}

fn bench_fjall(
    dir: &std::path::Path,
    items: &[(u64, Ptr)],
    probe_hits: &[u64],
    probe_miss: &[u64],
) -> Timing {
    let db = fjall::Database::builder(dir).open().expect("fjall open");
    let ks = db.keyspace("dir", fjall::KeyspaceCreateOptions::default).expect("open keyspace");

    let t0 = Instant::now();
    for &(k, v) in items {
        let mut buf = [0u8; 24];
        buf[0..8].copy_from_slice(&v.a.to_le_bytes());
        buf[8..16].copy_from_slice(&v.b.to_le_bytes());
        buf[16..24].copy_from_slice(&v.c.to_le_bytes());
        ks.insert(k.to_be_bytes(), buf).expect("insert");
    }
    db.persist(fjall::PersistMode::SyncAll).expect("persist");
    let build_ns = t0.elapsed().as_nanos() as f64;

    let t0 = Instant::now();
    let mut acc = 0u64;
    for &k in probe_hits {
        if let Some(buf) = ks.get(k.to_be_bytes()).expect("get") {
            acc ^= u64::from_le_bytes(buf[0..8].try_into().unwrap());
        }
    }
    std::hint::black_box(acc);
    let hit_ns = t0.elapsed().as_nanos() as f64 / probe_hits.len() as f64;

    let t0 = Instant::now();
    let mut hits = 0u64;
    for &k in probe_miss {
        if ks.get(k.to_be_bytes()).expect("get").is_some() {
            hits += 1;
        }
    }
    assert_eq!(hits, 0);
    let miss_ns = t0.elapsed().as_nanos() as f64 / probe_miss.len() as f64;

    // On-disk bytes (post-flush) as the memory-analogue for an LSM: du-style
    // allocated size of the partition's segment files under `dir`.
    drop(ks);
    drop(db);
    let approx_bytes = dir_size(dir);

    Timing { build_ns, hit_ns, miss_ns, approx_bytes }
}

fn dir_size(dir: &std::path::Path) -> usize {
    let mut total = 0usize;
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        if let Ok(rd) = std::fs::read_dir(&d) {
            for entry in rd.flatten() {
                let p = entry.path();
                if p.is_dir() {
                    stack.push(p);
                } else if let Ok(md) = entry.metadata() {
                    total += md.len() as usize;
                }
            }
        }
    }
    total
}

fn report(name: &str, n: usize, t: &Timing) {
    println!(
        "{name:<10} n={n:<9} build={:>10.1} ms  hit={:>8.1} ns  miss={:>8.1} ns  bytes/key={:>7.1}",
        t.build_ns / 1e6,
        t.hit_ns,
        t.miss_ns,
        t.approx_bytes as f64 / n as f64,
    );
}

fn main() {
    let sizes = [1_000usize, 100_000, 1_000_000];
    let n_probes = 200_000usize;

    let scratch_root = std::env::var("TMPDIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|_| std::path::PathBuf::from("/tmp"));
    if scratch_root == std::path::Path::new("/tmp") {
        eprintln!(
            "WARNING: TMPDIR not set to a real fs; fjall numbers below are tmpfs-backed \
             (fine for lookup latency, not representative of durable writes)."
        );
    }

    for &n in &sizes {
        let mut seed = 0x9E37_79B9_7F4A_7C15u64 ^ (n as u64);
        let keys = gen_keys(n, &mut seed);
        let items: Vec<(u64, Ptr)> =
            keys.iter().map(|&k| (k, Ptr { a: k, b: k.wrapping_mul(3), c: k.wrapping_add(7) })).collect();

        // Probe sets: `n_probes` hits (sampled with replacement from real
        // keys) and `n_probes` genuine misses (freshly generated, checked
        // disjoint from the key set).
        let present: std::collections::BTreeSet<u64> = keys.iter().copied().collect();
        let mut hit_seed = seed ^ 0xABCD;
        let probe_hits: Vec<u64> = (0..n_probes).map(|_| keys[(xorshift(&mut hit_seed) as usize) % n]).collect();
        let mut miss_seed = seed ^ 0xF00D;
        let mut probe_miss = Vec::with_capacity(n_probes);
        while probe_miss.len() < n_probes {
            let k = xorshift(&mut miss_seed);
            if k != u64::MAX && !present.contains(&k) {
                probe_miss.push(k);
            }
        }

        println!("\n=== n = {n} ===");
        report("hashmap", n, &bench_hashmap(&items, &probe_hits, &probe_miss));
        report("sorted", n, &bench_sorted(&items, &probe_hits, &probe_miss));
        report("mph", n, &bench_mph(&items, &probe_hits, &probe_miss));

        let fjall_dir = scratch_root.join(format!("phd-fjall-{n}-{}", std::process::id()));
        std::fs::create_dir_all(&fjall_dir).expect("mkdir scratch");
        let t = bench_fjall(&fjall_dir, &items, &probe_hits, &probe_miss);
        report("fjall", n, &t);
        std::fs::remove_dir_all(&fjall_dir).ok();
    }
}
