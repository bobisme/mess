//! Spike G (bn-2j5): exact epoch dedupe tournament.
//!
//! Semantic model under test (design.md §13.1):
//!
//! ```text
//! WindowByGlobalPosition { span: W }
//! at durable end w, a key committed at position p is a duplicate iff p >= w - W
//! ```
//!
//! Candidates:
//! - G0 `g0::FjallDedupe` — SYNTHETIC baseline: fjall primary (scope,key) ->
//!   (position, seq) + order seq -> pk with FIFO eviction, mirroring the
//!   dormant `MetaStore` dedupe shape (crates/mess-index/src/meta/mod.rs)
//!   adapted to position-span semantics. Production dedupe is DORMANT
//!   (review V1): LogEngine never populates or queries those partitions, so
//!   this is a best-effort component baseline built for the comparison, not
//!   a migration source.
//! - G1 `epoch::EpochDedupe<HashActive>` — hashbrown active table (keyed
//!   128-bit fingerprint -> smallvec of (position, key_ptr)) + frozen epochs
//!   as sorted (fp, position, ptr) arrays with binary search.
//! - G2 — G1 + a BinaryFuse16 negative filter per frozen epoch (same xorf
//!   crate/filter the repo's sealed filters use).
//! - G3 `epoch::EpochDedupe<IcebergActive>` — Iceberg-style low-associativity
//!   fixed-bin active table (8-way bins + rare overflow map) + G2's frozen
//!   representation.
//!
//! Full keys are stored once in a simulated capsule arena ([`arena::Arena`],
//! a `Vec<u8>` blob store standing in for the log); the index stores only
//! fingerprint + position + arena ptr, and every fingerprint hit verifies the
//! full key bytes from the arena before declaring a duplicate — forced
//! fingerprint collisions (down to a zero-bit fingerprint where ALL keys
//! collide) must therefore never produce a false negative or false positive.

pub mod arena;
pub mod epoch;
pub mod g0;
pub mod reference;
pub mod timing;

use xxhash_rust::xxh3::Xxh3;

/// Dedupe scope: a key is deduplicated within one stream, or store-wide.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Scope {
    Global,
    Stream(u64),
}

impl Scope {
    /// 9-byte canonical encoding: kind byte + stream id (BE). Distinct from
    /// any key-byte confusion because it is a fixed-width prefix.
    #[inline]
    pub fn encode(&self) -> [u8; 9] {
        let mut out = [0u8; 9];
        match self {
            Scope::Global => {}
            Scope::Stream(id) => {
                out[0] = 1;
                out[1..9].copy_from_slice(&id.to_be_bytes());
            }
        }
        out
    }

    #[inline]
    pub fn decode(kind: u8, id: u64) -> Scope {
        if kind == 0 { Scope::Global } else { Scope::Stream(id) }
    }
}

/// Full primary key bytes for the reference model / fjall baseline:
/// `scope(9B) || key`.
pub fn scoped_key(scope: Scope, key: &[u8]) -> Vec<u8> {
    let mut v = Vec::with_capacity(9 + key.len());
    v.extend_from_slice(&scope.encode());
    v.extend_from_slice(key);
    v
}

/// Injectable keyed fingerprint function.
///
/// Production intent (design.md §13.2): a per-store keyed 128-bit hash.
/// The spike makes the WIDTH configurable so correctness tests can force
/// collisions: `bits = 0` makes every key fingerprint to 0 (all keys
/// collide); small widths force heavy partial collisions. Candidates must
/// stay exact regardless — the arena full-key compare is the authority.
#[derive(Clone, Copy, Debug)]
pub struct Fingerprinter {
    pub seed: u64,
    /// Effective fingerprint width in bits, 0..=128. 128 = production.
    pub bits: u32,
}

impl Fingerprinter {
    pub fn production(seed: u64) -> Self {
        Fingerprinter { seed, bits: 128 }
    }

    #[inline]
    pub fn fp(&self, scope: Scope, key: &[u8]) -> u128 {
        // One-shot xxh3 over a small stack buffer for the common key sizes:
        // the streaming hasher's init/finalize overhead is material against
        // an 80-100 ns latency gate. Both paths produce identical xxh3-128
        // values for identical bytes, and the path is a deterministic
        // function of key length, so answers never depend on it.
        let f = if key.len() <= 503 {
            let mut buf = [0u8; 512];
            buf[..9].copy_from_slice(&scope.encode());
            buf[9..9 + key.len()].copy_from_slice(key);
            xxhash_rust::xxh3::xxh3_128_with_seed(&buf[..9 + key.len()], self.seed)
        } else {
            let mut h = Xxh3::with_seed(self.seed);
            h.update(&scope.encode());
            h.update(key);
            h.digest128()
        };
        match self.bits {
            128.. => f,
            0 => 0,
            b => f & ((1u128 << b) - 1),
        }
    }
}

/// Fold a 128-bit fingerprint to the 64 bits the BinaryFuse filter and the
/// bin router consume.
#[inline]
pub fn fp64(fp: u128) -> u64 {
    (fp as u64) ^ ((fp >> 64) as u64)
}

/// Common candidate interface (single-threaded spike harness).
pub trait DedupeIndex {
    /// Exact duplicate check at durable end `w`: returns the LATEST live
    /// position holding an equal (scope, key), i.e. some `p >= w - span`,
    /// or `None`. `&mut self` only so G0 can flush its pending commit-group
    /// batch; epoch candidates do not mutate on check.
    fn check(&mut self, scope: Scope, key: &[u8], w: u64, arena: &arena::Arena) -> Option<u64>;

    /// Record a committed key at `position`, whose canonical record lives at
    /// `ptr` in the arena. Positions are strictly increasing across calls.
    fn insert(&mut self, scope: Scope, key: &[u8], position: u64, ptr: u64);

    /// Make all inserts visible / durable-shaped (G0 commit-group flush).
    fn flush(&mut self) {}

    /// Per-key delete/tombstone writes issued so far. Epoch candidates are
    /// structurally zero: their types expose no per-key remove operation and
    /// reclamation is whole-epoch `VecDeque::pop_front`.
    fn deletes_issued(&self) -> u64 {
        0
    }

    /// Estimated resident bytes of the index (excluding the arena, which
    /// stands in for the log both designs already pay for).
    fn resident_bytes(&self) -> u64;

    /// Bytes this index would persist (frozen epochs + filters + active
    /// checkpoint image for G1-G3; on-disk footprint for G0).
    fn serialized_bytes(&self) -> u64;
}

/// splitmix64: the spike's deterministic RNG (no rand-crate dependency).
#[derive(Clone)]
pub struct Rng(pub u64);

impl Rng {
    pub fn new(seed: u64) -> Self {
        Rng(seed.wrapping_add(0x9E3779B97F4A7C15))
    }

    #[inline]
    pub fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E3779B97F4A7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
        z ^ (z >> 31)
    }

    #[inline]
    pub fn below(&mut self, n: u64) -> u64 {
        debug_assert!(n > 0);
        self.next_u64() % n
    }

    #[inline]
    pub fn f64(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 * (1.0 / (1u64 << 53) as f64)
    }
}

/// Deterministic unique key generator: key `i` of length `len` starts with
/// `i` (LE) so keys are guaranteed distinct, followed by splitmix noise.
pub fn make_key(i: u64, len: usize, seed: u64) -> Vec<u8> {
    let mut key = vec![0u8; len];
    let n = len.min(8);
    key[..n].copy_from_slice(&i.to_le_bytes()[..n]);
    if len > 8 {
        let mut r = Rng::new(seed ^ i.wrapping_mul(0xA24BAED4963EE407));
        let mut off = 8;
        while off < len {
            let w = r.next_u64().to_le_bytes();
            let take = (len - off).min(8);
            key[off..off + take].copy_from_slice(&w[..take]);
            off += take;
        }
    }
    key
}
