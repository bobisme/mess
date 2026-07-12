//! H5 (stretch): cache-line k-bin static hash — an APPROXIMATION of the
//! July-2026 k-PHF cache-line-binning idea WITHOUT the paper's construction
//! (research/03 §4.5 says "label it experimental"; we label it
//! approximation). `bin = fastrange(mix(key), nbins)`; each 64-byte-aligned
//! bin holds up to 7 full keys + a base index into a bin-grouped entry
//! array; bin overflow spills to a shared sorted vector (binary search).
//! Lookup: one cache line, <=7 branch-light compares, one entry read.
//! Average load is ~5 keys/bin (nbins = ceil(n/5)), so overflow is rare but
//! nonzero — the deterministic fallback is built in.

use crate::{Entry, ExactDirectory, OpenError, RECORD_LEN, envelope};

const BIN_KEYS: usize = 7;
const AVG_LOAD: usize = 5;
/// `len` flag bit: this bin overflowed into the shared spill vector.
const OVERFLOW: u8 = 0x80;

#[repr(C, align(64))]
#[derive(Clone, Copy)]
struct Bin {
    keys:       [u64; BIN_KEYS],
    entry_base: u32,
    len:        u8, // low 7 bits: in-bin count; OVERFLOW: spilled
    _pad:       [u8; 3],
}

const _: () = assert!(std::mem::size_of::<Bin>() == 64);

const EMPTY_BIN: Bin = Bin {
    keys:       [0; BIN_KEYS],
    entry_base: 0,
    len:        0,
    _pad:       [0; 3],
};

#[inline]
fn mix(key: u64) -> u64 {
    // splitmix64 finalizer — cheap, full-avalanche.
    let mut z = key.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

#[inline]
fn bin_of(key: u64, nbins: usize) -> usize {
    // fastrange: (h * nbins) >> 64.
    ((mix(key) as u128 * nbins as u128) >> 64) as usize
}

pub struct KBinDir {
    nbins:    usize,
    bins:     Vec<Bin>,
    /// In-bin entries, grouped by bin (bin i's live at
    /// `entry_base..entry_base+len`).
    entries:  Vec<Entry>,
    /// Spilled `(key, entry)` pairs, sorted by key.
    overflow: Vec<(u64, Entry)>,
    n:        usize,
}

impl KBinDir {
    fn build_pairs(pairs: &[(u64, Entry)]) -> Self {
        crate::assert_canonical(pairs);
        let n = pairs.len();
        let nbins = n.div_ceil(AVG_LOAD).max(1);
        // Bucket the pairs.
        let mut per_bin: Vec<Vec<(u64, Entry)>> = vec![Vec::new(); nbins];
        for &(k, e) in pairs {
            per_bin[bin_of(k, nbins)].push((k, e));
        }
        let mut bins = vec![EMPTY_BIN; nbins];
        let mut entries = Vec::with_capacity(n);
        let mut overflow: Vec<(u64, Entry)> = Vec::new();
        for (b, mut items) in per_bin.into_iter().enumerate() {
            let bin = &mut bins[b];
            bin.entry_base = entries.len() as u32;
            let in_bin = items.len().min(BIN_KEYS);
            for (j, &(k, e)) in items.iter().take(BIN_KEYS).enumerate() {
                bin.keys[j] = k;
                entries.push(e);
            }
            bin.len = in_bin as u8;
            if items.len() > BIN_KEYS {
                bin.len |= OVERFLOW;
                overflow.extend(items.drain(BIN_KEYS..));
            }
        }
        overflow.sort_unstable_by_key(|p| p.0);
        KBinDir { nbins, bins, entries, overflow, n }
    }
}

impl ExactDirectory for KBinDir {
    const NAME: &'static str = "h5_kbin";
    const KIND: u8 = 6;

    fn build(pairs: &[(u64, Entry)]) -> Self { Self::build_pairs(pairs) }

    #[inline]
    fn lookup(&self, key: u64) -> Option<&Entry> {
        let bin = &self.bins[bin_of(key, self.nbins)];
        let len = (bin.len & !OVERFLOW) as usize;
        for j in 0..len {
            if bin.keys[j] == key {
                return Some(&self.entries[bin.entry_base as usize + j]);
            }
        }
        if bin.len & OVERFLOW != 0 {
            if let Ok(i) =
                self.overflow.binary_search_by_key(&key, |p| p.0)
            {
                return Some(&self.overflow[i].1);
            }
        }
        None
    }

    fn iter_pairs(&self) -> Vec<(u64, Entry)> {
        let mut out = Vec::with_capacity(self.n);
        for bin in &self.bins {
            let len = (bin.len & !OVERFLOW) as usize;
            for j in 0..len {
                out.push((
                    bin.keys[j],
                    self.entries[bin.entry_base as usize + j],
                ));
            }
        }
        out.extend_from_slice(&self.overflow);
        out.sort_unstable_by_key(|p| p.0);
        out
    }

    fn serialize(&self) -> Vec<u8> {
        // Canonical records; open() rebuilds (construction is one O(n)
        // bucketing pass, so rebuild-on-open is the production shape here
        // too, like the incumbent's HashMap fill).
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
            + self.bins.capacity() * 64
            + self.entries.capacity() * 48
            + self.overflow.capacity() * std::mem::size_of::<(u64, Entry)>()
    }
}
