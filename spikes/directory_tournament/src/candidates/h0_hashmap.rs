//! H0: the incumbent shape — `HashMap<u64, Entry>` rebuilt from the
//! serialized 56-byte-record DIR region on open, plus the ascending
//! `stream_ids` vec (`SealedSegmentIndex::from_bytes` keeps exactly these
//! two). H0 uses the std default hasher (SipHash-1-3, randomly seeded per
//! process) because that is literally what `std::collections::HashMap`
//! gives `segment.rs` today. H0f swaps in foldhash (hashbrown's default) —
//! the cheapest conceivable "improvement" and the honest hash-speed
//! counterfactual for the fancier structures.

use std::collections::HashMap;
use std::hash::BuildHasher;

use crate::{Entry, ExactDirectory, OpenError, RECORD_LEN, envelope};

pub struct HashDir<S: BuildHasher> {
    map: HashMap<u64, Entry, S>,
    /// Ascending stream ids — the incumbent keeps this for global replay and
    /// deterministic iteration; it is part of the shape's resident cost.
    ids: Vec<u64>,
}

impl<S: BuildHasher + Default> HashDir<S> {
    fn build_inner(pairs: &[(u64, Entry)]) -> Self {
        crate::assert_canonical(pairs);
        let mut map =
            HashMap::with_capacity_and_hasher(pairs.len(), S::default());
        let mut ids = Vec::with_capacity(pairs.len());
        for &(k, e) in pairs {
            map.insert(k, e);
            ids.push(k);
        }
        HashDir { map, ids }
    }

    fn serialize_inner(&self, kind: u8) -> Vec<u8> {
        let mut v = envelope::begin(kind, self.ids.len() as u64);
        for &k in &self.ids {
            self.map[&k].write_record(k, &mut v);
        }
        envelope::finish(v)
    }

    /// The incumbent's open cost: parse each 56-byte record, insert into a
    /// pre-sized map, push the id (mirrors `SealedSegmentIndex::from_bytes`'s
    /// directory loop).
    fn open_inner(bytes: &[u8], kind: u8) -> Result<Self, OpenError> {
        let (n, body) = envelope::open(bytes, kind)?;
        let n = n as usize;
        if body.len() != n * RECORD_LEN {
            return Err(OpenError::Corrupt("dir region size mismatch"));
        }
        let mut map = HashMap::with_capacity_and_hasher(n, S::default());
        let mut ids = Vec::with_capacity(n);
        for i in 0..n {
            let (k, e) = Entry::read_record(&body[i * RECORD_LEN..]);
            map.insert(k, e);
            ids.push(k);
        }
        if map.len() != n {
            return Err(OpenError::Corrupt("duplicate stream id"));
        }
        Ok(HashDir { map, ids })
    }

    fn resident_inner(&self) -> usize {
        // hashbrown RawTable: buckets = next_pow2(ceil(n/0.875)) (min 4 for
        // small n), one (K,V) slot + 1 ctrl byte per bucket + group padding.
        let n = self.map.len();
        let buckets = if n <= 3 {
            4
        } else {
            (n * 8 / 7).next_power_of_two()
        };
        let slot = std::mem::size_of::<(u64, Entry)>(); // 56
        std::mem::size_of::<Self>()
            + buckets * (slot + 1)
            + 16
            + self.ids.capacity() * 8
    }

    fn lookup_inner(&self, key: u64) -> Option<&Entry> { self.map.get(&key) }

    fn iter_inner(&self) -> Vec<(u64, Entry)> {
        self.ids.iter().map(|&k| (k, self.map[&k])).collect()
    }
}

/// H0: std default hasher — the incumbent.
pub struct SipHashDir(HashDir<std::hash::RandomState>);

impl ExactDirectory for SipHashDir {
    const NAME: &'static str = "h0_hashmap_sip";
    const KIND: u8 = 0;
    fn build(pairs: &[(u64, Entry)]) -> Self {
        SipHashDir(HashDir::build_inner(pairs))
    }
    #[inline]
    fn lookup(&self, key: u64) -> Option<&Entry> { self.0.lookup_inner(key) }
    fn iter_pairs(&self) -> Vec<(u64, Entry)> { self.0.iter_inner() }
    fn serialize(&self) -> Vec<u8> { self.0.serialize_inner(Self::KIND) }
    fn open(bytes: &[u8]) -> Result<Self, OpenError> {
        HashDir::open_inner(bytes, Self::KIND).map(SipHashDir)
    }
    fn resident_bytes(&self) -> usize { self.0.resident_inner() }
}

/// H0f: foldhash (hashbrown's default hasher since 0.15).
pub struct FoldHashDir(HashDir<foldhash::fast::RandomState>);

impl ExactDirectory for FoldHashDir {
    const NAME: &'static str = "h0f_hashmap_fold";
    const KIND: u8 = 1;
    fn build(pairs: &[(u64, Entry)]) -> Self {
        FoldHashDir(HashDir::build_inner(pairs))
    }
    #[inline]
    fn lookup(&self, key: u64) -> Option<&Entry> { self.0.lookup_inner(key) }
    fn iter_pairs(&self) -> Vec<(u64, Entry)> { self.0.iter_inner() }
    fn serialize(&self) -> Vec<u8> { self.0.serialize_inner(Self::KIND) }
    fn open(bytes: &[u8]) -> Result<Self, OpenError> {
        HashDir::open_inner(bytes, Self::KIND).map(FoldHashDir)
    }
    fn resident_bytes(&self) -> usize { self.0.resident_inner() }
}
