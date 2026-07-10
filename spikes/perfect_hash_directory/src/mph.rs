//! A hand-rolled static (FKS two-level) minimal-ish perfect hash table,
//! keyed by `u64` with a fixed-size `Copy` value. No external MPH crate is a
//! workspace dependency (`boomphf` etc. are not pulled in — bn-1hl scope says
//! avoid adding heavyweight deps for a benchmark-gated experiment), so this
//! is deliberately the simplest correct construction:
//!
//! - Level 1: `n_buckets = max(1, n)` buckets, `bucket(key) = mix(key, seed1) % n_buckets`.
//!   `seed1` is retried until `sum(bucket_len^2) <= 4n` (bounds total level-2
//!   table size to O(n); for Poisson(1)-ish bucket occupancy this holds on
//!   the first try almost always — expected sum is `2n`).
//! - Level 2: each non-empty bucket of size `b` gets its own slot table of
//!   size `m = b*b` (the birthday bound: a random `m`-slot placement of `b`
//!   keys is collision-free with probability >= 1/2, so a handful of seed
//!   retries succeeds in expectation). Slots use a `u64::MAX` sentinel key
//!   (parallel `Vec<u64>` + `Vec<V>`, not `Vec<Option<(u64,V)>>`) so empty
//!   slots cost 8 bytes, not a padded `Option` tag — this is the same
//!   "sentinel over `Option`" trick the sealed sidecar's DIR region uses
//!   implicitly by being a dense fixed-width array.
//!
//! Lookup is worst-case O(1): two `mix` calls and two array reads, with an
//! explicit key-equality check on the level-2 slot (a genuinely absent key
//! that hashes into another key's occupied slot must not return a false
//! hit — this is an *exact* structure, not a filter).
//!
//! Caveat: `u64::MAX` cannot be a real key (used as the empty-slot
//! sentinel). Fine for this synthetic benchmark (stream ids are hashed
//! `u64`s in production, `u64::MAX` is not reserved there); a production
//! port would need an explicit occupancy bitset instead of a sentinel.

#[inline]
fn mix(key: u64, seed: u64) -> u64 {
    // splitmix64 finalizer, keyed by XOR-ing the seed in first. Same
    // mixing family already in mess's toolbox conceptually (crc32c/xorf use
    // different primitives, but this is the standard public-domain splitmix
    // finalizer — good avalanche, no allocation, no deps).
    let mut x = key ^ seed;
    x ^= x >> 33;
    x = x.wrapping_mul(0xff51afd7ed558ccd);
    x ^= x >> 33;
    x = x.wrapping_mul(0xc4ceb9fe1a85ec53);
    x ^= x >> 33;
    x
}

const EMPTY: u64 = u64::MAX;

#[derive(Clone, Copy)]
struct BucketMeta {
    offset: u32,
    m: u32,
    seed: u64,
}

/// A static perfect hash table over `u64` keys with `Copy` values `V`.
/// Build once from a distinct key set; read-only thereafter.
pub struct Mph<V> {
    n_buckets: usize,
    seed1: u64,
    bucket_meta: Vec<BucketMeta>,
    slot_keys: Vec<u64>,
    slot_vals: Vec<V>,
}

impl<V: Copy + Default> Mph<V> {
    /// Build a perfect hash table over `items` (keys MUST be distinct and
    /// none may equal `u64::MAX`). Deterministic given the same input order
    /// (fixed starting seeds, linear retry).
    pub fn build(items: &[(u64, V)]) -> Self {
        let n = items.len();
        let n_buckets = n.max(1);

        let mut seed1 = 0x9E37_79B9_7F4A_7C15u64;
        let mut buckets: Vec<Vec<(u64, V)>>;
        let budget = 4 * n_buckets.max(1);
        let mut tries = 0;
        loop {
            buckets = vec![Vec::new(); n_buckets];
            for &(k, v) in items {
                debug_assert_ne!(k, EMPTY, "u64::MAX is reserved as the empty-slot sentinel");
                let idx = (mix(k, seed1) as usize) % n_buckets;
                buckets[idx].push((k, v));
            }
            let sum_sq: usize = buckets.iter().map(|b| b.len() * b.len()).sum();
            tries += 1;
            if sum_sq <= budget || tries > 64 {
                break;
            }
            seed1 = seed1.wrapping_add(0x2545_F491_4F6C_DD1D);
        }

        let mut bucket_meta = vec![BucketMeta { offset: 0, m: 0, seed: 0 }; n_buckets];
        let mut slot_keys: Vec<u64> = Vec::new();
        let mut slot_vals: Vec<V> = Vec::new();

        for (i, b) in buckets.iter().enumerate() {
            if b.is_empty() {
                continue;
            }
            let m = (b.len() * b.len()).max(1);
            let mut seed2 = 0x1234_5678_9ABC_DEF0u64;
            let mut table_keys;
            let mut table_vals;
            loop {
                table_keys = vec![EMPTY; m];
                table_vals = vec![V::default(); m];
                let mut ok = true;
                for &(k, v) in b {
                    let j = (mix(k, seed2) as usize) % m;
                    if table_keys[j] != EMPTY {
                        ok = false;
                        break;
                    }
                    table_keys[j] = k;
                    table_vals[j] = v;
                }
                if ok {
                    break;
                }
                seed2 = seed2.wrapping_add(1);
            }
            let offset = slot_keys.len() as u32;
            slot_keys.extend(table_keys);
            slot_vals.extend(table_vals);
            bucket_meta[i] = BucketMeta { offset, m: m as u32, seed: seed2 };
        }

        Mph { n_buckets, seed1, bucket_meta, slot_keys, slot_vals }
    }

    /// Exact lookup: `None` for any key not in the built set (including
    /// collisions into another key's slot — verified by key equality).
    #[inline]
    pub fn get(&self, key: u64) -> Option<V> {
        let idx = (mix(key, self.seed1) as usize) % self.n_buckets;
        let meta = self.bucket_meta[idx];
        if meta.m == 0 {
            return None;
        }
        let j = (mix(key, meta.seed) as usize) % meta.m as usize;
        let slot = meta.offset as usize + j;
        if self.slot_keys[slot] == key {
            Some(self.slot_vals[slot])
        } else {
            None
        }
    }

    /// Total slot count across all bucket tables (the O(n) blowup factor to
    /// report: `slot_count / n`).
    pub fn slot_count(&self) -> usize {
        self.slot_keys.len()
    }

    /// Approximate resident bytes: bucket metadata + both slot arrays.
    pub fn approx_bytes(&self) -> usize {
        self.bucket_meta.len() * std::mem::size_of::<BucketMeta>()
            + self.slot_keys.len() * std::mem::size_of::<u64>()
            + self.slot_vals.len() * std::mem::size_of::<V>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone, Copy, Default, PartialEq, Debug)]
    struct V(u64, u64, u64);

    #[test]
    fn exact_membership_no_false_hits() {
        fn xorshift(seed: &mut u64) -> u64 {
            *seed ^= *seed << 13;
            *seed ^= *seed >> 7;
            *seed ^= *seed << 17;
            *seed
        }
        let mut seed = 0xABCDu64;
        let mut present = std::collections::BTreeSet::new();
        while present.len() < 5_000 {
            let k = xorshift(&mut seed);
            if k != EMPTY {
                present.insert(k);
            }
        }
        let items: Vec<(u64, V)> = present.iter().map(|&k| (k, V(k, k + 1, k + 2))).collect();
        let mph = Mph::build(&items);

        for &(k, v) in &items {
            assert_eq!(mph.get(k), Some(v));
        }
        let mut checked = 0;
        let mut false_hits = 0;
        while checked < 50_000 {
            let k = xorshift(&mut seed);
            if k == EMPTY || present.contains(&k) {
                continue;
            }
            checked += 1;
            if mph.get(k).is_some() {
                false_hits += 1;
            }
        }
        assert_eq!(false_hits, 0, "exact structure must never return Some for an absent key");
    }

    #[test]
    fn empty_build() {
        let mph: Mph<V> = Mph::build(&[]);
        assert_eq!(mph.get(42), None);
    }
}
