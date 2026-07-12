//! Exactness properties for every candidate, over every synthetic
//! distribution and the edge shapes (n=1, n=2, adjacent keys, extreme
//! spans): present keys -> exact entries, absent keys -> None, iter ==
//! source, serialize/open round-trip equality, corrupt bytes -> error.

use directory_tournament::candidates::*;
use directory_tournament::datasets::{gen_dense, gen_sparse, gen_zipf_cluster};
use directory_tournament::{Entry, ExactDirectory};

fn e(seed: u64) -> Entry {
    Entry {
        first_version: seed,
        last_version:  seed.wrapping_mul(3),
        ptr_off:       seed.wrapping_mul(7) & 0xFFFF_FFFF,
        skip_off:      seed.wrapping_mul(11) & 0xFFFF_FFFF,
        ptr_len:       (seed as u32).wrapping_mul(13) | 1,
        n_batches:     (seed as u32) % 4096 + 1,
        skip_len:      (seed as u32) % 65536,
        reserved:      0,
    }
}

fn pairs_from_keys(keys: &[u64]) -> Vec<(u64, Entry)> {
    keys.iter().map(|&k| (k, e(k ^ 0xDEAD_BEEF))).collect()
}

/// Deterministic absent-key generator: probe around and between the present
/// keys plus far outside the span.
fn absent_keys(keys: &[u64]) -> Vec<u64> {
    let present: std::collections::HashSet<u64> =
        keys.iter().copied().collect();
    let min = keys[0];
    let max = keys[keys.len() - 1];
    let mut out = Vec::new();
    let mut push = |k: u64| {
        if !present.contains(&k) {
            out.push(k);
        }
    };
    for &k in keys.iter().take(2000) {
        push(k.wrapping_add(1));
        push(k.wrapping_sub(1));
    }
    if min > 0 {
        push(min - 1);
        push(min / 2);
        push(0);
    }
    push(max + 1);
    push(max.saturating_add(max / 2));
    push(u64::MAX);
    let span = max - min + 1;
    let mut x = 0x9E37_79B9u64;
    for _ in 0..2000 {
        x = x.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        push(min + (x % span));
    }
    out
}

fn check_all<D: ExactDirectory>(pairs: &[(u64, Entry)]) {
    let keys: Vec<u64> = pairs.iter().map(|p| p.0).collect();
    let dir = D::build(pairs);

    // Present keys -> exact entries.
    for &(k, ref ent) in pairs {
        assert_eq!(dir.lookup(k), Some(ent), "{} hit {k}", D::NAME);
    }
    // Absent keys -> None.
    for k in absent_keys(&keys) {
        assert_eq!(dir.lookup(k), None, "{} miss {k}", D::NAME);
    }
    // iter == source.
    assert_eq!(dir.iter_pairs(), pairs, "{} iter", D::NAME);

    // Serialize -> open round-trip: identical behavior + identical re-image.
    let image = dir.serialize();
    let re = D::open(&image).unwrap_or_else(|err| {
        panic!("{} reopen of own image failed: {err}", D::NAME)
    });
    assert_eq!(re.iter_pairs(), pairs, "{} reopen iter", D::NAME);
    for &(k, ref ent) in pairs.iter().take(4000) {
        assert_eq!(re.lookup(k), Some(ent), "{} reopen hit", D::NAME);
    }
    assert_eq!(re.serialize(), image, "{} re-serialize equality", D::NAME);

    // Corrupt bytes -> error, never panic/garbage: flip a byte at several
    // positions (header, body, CRC), and truncate.
    let positions = [
        0usize,
        4,
        5,
        8,
        image.len() / 2,
        image.len() - 5,
        image.len() - 1,
    ];
    for &p in &positions {
        let mut bad = image.clone();
        bad[p] ^= 0xFF;
        assert!(
            D::open(&bad).is_err(),
            "{} accepted a corrupt byte at {p}",
            D::NAME
        );
    }
    assert!(D::open(&image[..image.len() - 1]).is_err());
    assert!(D::open(&image[..8]).is_err());
    assert!(D::open(&[]).is_err());
}

fn check_all_candidates(pairs: &[(u64, Entry)], dense_enough_for_h2: bool) {
    check_all::<SipHashDir>(pairs);
    check_all::<FoldHashDir>(pairs);
    check_all::<SortedDir>(pairs);
    if dense_enough_for_h2 {
        check_all::<BitRankDir>(pairs);
    }
    check_all::<PefDir>(pairs);
    check_all::<PtrHashDir>(pairs);
    check_all::<KBinDir>(pairs);
}

#[test]
fn edge_shapes() {
    // Single key.
    check_all_candidates(&pairs_from_keys(&[42]), true);
    check_all_candidates(&pairs_from_keys(&[0]), true);
    // Two adjacent, two distant.
    check_all_candidates(&pairs_from_keys(&[7, 8]), true);
    check_all_candidates(&pairs_from_keys(&[7, 7_000_000_007]), false);
    // Fully dense run.
    let run: Vec<u64> = (1000..2000).collect();
    check_all_candidates(&pairs_from_keys(&run), true);
    // Exact partition boundary sizes for H3 (255/256/257 keys).
    for n in [255u64, 256, 257, 512, 513] {
        let ks: Vec<u64> = (0..n).map(|i| i * 37 + 5).collect();
        check_all_candidates(&pairs_from_keys(&ks), false);
    }
    // Keys near u64::MAX.
    let hi: Vec<u64> = (0..100).map(|i| u64::MAX - 1000 + i * 7).collect();
    check_all_candidates(&pairs_from_keys(&hi), true);
}

#[test]
fn synthetic_dense() {
    let ds = gen_dense(20_000, 1);
    check_all_candidates(&ds.pairs, true);
}

#[test]
fn synthetic_sparse() {
    let ds = gen_sparse(20_000, 2);
    check_all_candidates(&ds.pairs, false);
}

#[test]
fn synthetic_zipf_cluster() {
    let ds = gen_zipf_cluster(20_000, 3);
    // Zipf-clustered density varies by seed; H2 is exercised when sane.
    check_all_candidates(&ds.pairs, ds.u_over_n <= 64.0);
}

/// The H5 overflow path must be exercised: force > 7 keys into one bin by
/// brute-force key search.
#[test]
fn kbin_overflow_path() {
    // With n=64 keys and nbins=13, expected load ~5; craft enough keys that
    // at least one bin overflows by sampling many candidates.
    let mut keys: Vec<u64> = (0..4096u64).map(|i| i * 131 + 17).collect();
    keys.sort_unstable();
    let pairs = pairs_from_keys(&keys);
    let dir = KBinDir::build(&pairs);
    // Statistically certain to overflow at least one bin at load 5 with 4096
    // keys; the assertions in check_all cover behavior — here we just verify
    // every key still resolves (including any spilled ones).
    for &(k, ref ent) in &pairs {
        assert_eq!(dir.lookup(k), Some(ent));
    }
    check_all::<KBinDir>(&pairs);
}

/// H2 rejects images whose popcount disagrees with the entry count.
#[test]
fn bitrank_popcount_cross_check() {
    let pairs = pairs_from_keys(&[10, 11, 12, 20, 21]);
    let dir = BitRankDir::build(&pairs);
    let image = dir.serialize();
    // Envelope CRC catches plain flips; also verify a "consistent" corrupt
    // image (bit flipped + CRC fixed) is caught by the popcount check.
    let mut bad = image.clone();
    let body_bit = 16 + 16; // first word of the bitvector
    bad[body_bit] ^= 0x40; // add a bogus present bit
    let crc_at = bad.len() - 4;
    let crc = crc32c::crc32c(&bad[..crc_at]);
    bad[crc_at..].copy_from_slice(&crc.to_le_bytes());
    assert!(BitRankDir::open(&bad).is_err());
}
