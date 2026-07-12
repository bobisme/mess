//! H1: sorted key array + entry array, binary search — research/03 §4.1's
//! baseline "every clever structure must beat". Serialized as two packed
//! columns (keys, then entries); open is a bounds-checked copy, no rebuild.

use crate::{Entry, ExactDirectory, OpenError, envelope};

pub struct SortedDir {
    keys:    Vec<u64>,
    entries: Vec<Entry>,
}

impl ExactDirectory for SortedDir {
    const NAME: &'static str = "h1_sorted";
    const KIND: u8 = 2;

    fn build(pairs: &[(u64, Entry)]) -> Self {
        crate::assert_canonical(pairs);
        SortedDir {
            keys:    pairs.iter().map(|p| p.0).collect(),
            entries: pairs.iter().map(|p| p.1).collect(),
        }
    }

    #[inline]
    fn lookup(&self, key: u64) -> Option<&Entry> {
        match self.keys.binary_search(&key) {
            Ok(i) => Some(&self.entries[i]),
            Err(_) => None,
        }
    }

    fn iter_pairs(&self) -> Vec<(u64, Entry)> {
        self.keys.iter().copied().zip(self.entries.iter().copied()).collect()
    }

    fn serialize(&self) -> Vec<u8> {
        let mut v = envelope::begin(Self::KIND, self.keys.len() as u64);
        for &k in &self.keys {
            v.extend_from_slice(&k.to_le_bytes());
        }
        for e in &self.entries {
            let mut rec = Vec::with_capacity(48);
            e.write_record(0, &mut rec); // key column already written
            v.extend_from_slice(&rec[8..]);
        }
        envelope::finish(v)
    }

    fn open(bytes: &[u8]) -> Result<Self, OpenError> {
        let (n, body) = envelope::open(bytes, Self::KIND)?;
        let n = n as usize;
        if body.len() != n * (8 + 48) {
            return Err(OpenError::Corrupt("column size mismatch"));
        }
        let mut keys = Vec::with_capacity(n);
        for i in 0..n {
            keys.push(crate::rd_u64(body, i * 8));
        }
        if !keys.windows(2).all(|w| w[0] < w[1]) {
            return Err(OpenError::Corrupt("keys not strictly ascending"));
        }
        let ecol = &body[n * 8..];
        let mut entries = Vec::with_capacity(n);
        for i in 0..n {
            // Entry columns are the record minus the 8-byte key.
            let mut rec = [0u8; 56];
            rec[8..].copy_from_slice(&ecol[i * 48..i * 48 + 48]);
            entries.push(Entry::read_record(&rec).1);
        }
        Ok(SortedDir { keys, entries })
    }

    fn resident_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.keys.capacity() * 8
            + self.entries.capacity() * 48
    }
}
