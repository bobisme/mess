//! H4: PtrHash — the published `ptr_hash` crate (2.0.1, by the paper's
//! author). Slot -> packed key verify -> entry, exactly the research/03 §4.4
//! recipe: a PHF returns an arbitrary slot for nonmembers, so the full key
//! is stored per slot and compared.
//!
//! SAFETY FINDING (documented in the REPORT): the crate's minimal variant
//! (`DefaultPtrHash`, REMAP=true) is **unsafe for nonmember queries** — its
//! free-slot remap table is read with `get_unchecked`
//! (`ptr_hash-2.0.1/src/pack.rs`), and a nonmember key can hash to a free
//! slot past the last remapped one: our exactness tests SIGSEGV'd inside
//! `PtrHash::index`. Since the sealed directory MUST answer absent stream
//! ids exactly, H4 uses the non-minimal `FastPtrHash` (REMAP=false,
//! `params::default_fast()`, 2.67 bits/key): `index()` then returns a slot
//! in `[0, max_index())` (`max_index ~ 1.01n`) with no unchecked table, and
//! the verify arrays are sized to `max_index()`. Empty slots hold a sentinel
//! key chosen (deterministically) to not collide with any present key; a
//! query equal to the sentinel itself is answered `None` up front.
//!
//! Serialization: the canonical 56-byte records (sorted order); `open`
//! REBUILDS the PHF from the keys — deterministic, but it makes H4's
//! open/parse cost include full PHF construction. The production
//! alternative (the crate's `epserde` feature, pilots ~2.7 bits/key on
//! disk) was not wired in this spike; the measured `bits_per_element()` is
//! carried in the REPORT so that path's size is still quantified.

use ptr_hash::{FastPtrHash, PtrHashParams, hash::StrongerIntHash};

use crate::{Entry, ExactDirectory, OpenError, RECORD_LEN, envelope};

type Phf = FastPtrHash<StrongerIntHash, u64>;

const ZERO_ENTRY: Entry = Entry {
    first_version: 0,
    last_version:  0,
    ptr_off:       0,
    skip_off:      0,
    ptr_len:       0,
    n_batches:     0,
    skip_len:      0,
    reserved:      0,
};

pub struct PtrHashDir {
    phf:             Phf,
    keys_by_slot:    Vec<u64>,
    entries_by_slot: Vec<Entry>,
    /// Fills empty slots; never a present key; queried directly -> None.
    sentinel:        u64,
    n:               usize,
    /// (pilots + remap) metadata bytes — the crate's own accounting.
    pub meta_bytes:  usize,
}

impl PtrHashDir {
    fn build_pairs(pairs: &[(u64, Entry)]) -> Self {
        crate::assert_canonical(pairs);
        let keys: Vec<u64> = pairs.iter().map(|p| p.0).collect();
        let phf = <Phf>::new(&keys, PtrHashParams::default_fast());
        let n = keys.len();
        let slots = phf.max_index();
        // Deterministic sentinel: largest u64 not in the (sorted) key set.
        let mut sentinel = u64::MAX;
        for &k in keys.iter().rev() {
            if k != sentinel {
                break;
            }
            sentinel -= 1;
        }
        let mut keys_by_slot = vec![sentinel; slots];
        let mut entries_by_slot = vec![ZERO_ENTRY; slots];
        for &(k, e) in pairs {
            let slot = phf.index(&k);
            debug_assert_eq!(keys_by_slot[slot], sentinel, "PHF collision");
            keys_by_slot[slot] = k;
            entries_by_slot[slot] = e;
        }
        let (pilot_bits, remap_bits) = phf.bits_per_element();
        let meta_bytes =
            ((pilot_bits + remap_bits) * n as f64 / 8.0).ceil() as usize;
        PtrHashDir { phf, keys_by_slot, entries_by_slot, sentinel, n, meta_bytes }
    }
}

impl ExactDirectory for PtrHashDir {
    const NAME: &'static str = "h4_ptrhash";
    const KIND: u8 = 5;

    fn build(pairs: &[(u64, Entry)]) -> Self { Self::build_pairs(pairs) }

    #[inline]
    fn lookup(&self, key: u64) -> Option<&Entry> {
        if key == self.sentinel {
            return None;
        }
        let slot = self.phf.index(&key);
        if self.keys_by_slot[slot] == key {
            Some(&self.entries_by_slot[slot])
        } else {
            None
        }
    }

    fn iter_pairs(&self) -> Vec<(u64, Entry)> {
        let mut out: Vec<(u64, Entry)> = self
            .keys_by_slot
            .iter()
            .copied()
            .zip(self.entries_by_slot.iter().copied())
            .filter(|&(k, _)| k != self.sentinel)
            .collect();
        out.sort_unstable_by_key(|p| p.0);
        out
    }

    fn serialize(&self) -> Vec<u8> {
        let pairs = self.iter_pairs();
        let mut v = envelope::begin(Self::KIND, pairs.len() as u64);
        for (k, e) in &pairs {
            e.write_record(*k, &mut v);
        }
        envelope::finish(v)
    }

    fn open(bytes: &[u8]) -> Result<Self, OpenError> {
        let (n, body) = envelope::open(bytes, Self::KIND)?;
        let n = n as usize;
        if body.len() != n * RECORD_LEN {
            return Err(OpenError::Corrupt("record region size mismatch"));
        }
        let mut pairs = Vec::with_capacity(n);
        for i in 0..n {
            pairs.push(Entry::read_record(&body[i * RECORD_LEN..]));
        }
        if !pairs.windows(2).all(|w| w[0].0 < w[1].0) {
            return Err(OpenError::Corrupt("keys not strictly ascending"));
        }
        Ok(Self::build_pairs(&pairs))
    }

    fn resident_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.keys_by_slot.capacity() * 8
            + self.entries_by_slot.capacity() * 48
            + self.meta_bytes
    }
}
