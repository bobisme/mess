//! H2: bitvector + rank over the shifted universe `[min, max]` — design.md
//! §12.2 / research/03 §4.2. Superblock rank every 512 bits (u32 absolute),
//! subblock rank every 64-bit word (u16 within-superblock), hardware
//! popcount for the in-word remainder. Entries stored in key order; the bit
//! position IS the key, so no key copy exists at all.
//!
//! Serialized image: `min u64 | n_words u64 | words | entries`; the rank
//! directories are rebuilt on open with one linear popcount pass (they are
//! derived data, like the incumbent's HashMap — but ~50x cheaper to derive).
//!
//! Density guard: the caller only builds this when `U/n` is small (the §12.2
//! auto-select rule is `U/n <= 8`); `build` itself only asserts U fits memory.

use crate::{Entry, ExactDirectory, OpenError, envelope};

pub struct BitRankDir {
    min:     u64,
    max:     u64,
    n:       usize,
    words:   Vec<u64>,
    /// Absolute rank (ones before) at each 512-bit superblock boundary.
    sup:     Vec<u32>,
    /// Rank within the superblock at each 64-bit word boundary.
    sub:     Vec<u16>,
    entries: Vec<Entry>,
}

impl BitRankDir {
    fn build_ranks(words: &[u64]) -> (Vec<u32>, Vec<u16>) {
        let n_sup = words.len().div_ceil(8);
        let mut sup = Vec::with_capacity(n_sup);
        let mut sub = Vec::with_capacity(words.len());
        let mut abs: u32 = 0;
        for (w, word) in words.iter().enumerate() {
            if w % 8 == 0 {
                sup.push(abs);
            }
            sub.push((abs - sup[w / 8]) as u16);
            abs += word.count_ones();
        }
        (sup, sub)
    }

    fn from_parts(
        min: u64,
        words: Vec<u64>,
        entries: Vec<Entry>,
    ) -> Result<Self, OpenError> {
        let (sup, sub) = Self::build_ranks(&words);
        let n = entries.len();
        let total: u32 =
            words.iter().map(|w| w.count_ones()).sum();
        if total as usize != n {
            return Err(OpenError::Corrupt("popcount != entry count"));
        }
        // Highest set bit = max.
        let Some(last_w) = words.iter().rposition(|&w| w != 0) else {
            return Err(OpenError::Corrupt("empty bitvector"));
        };
        let max =
            min + (last_w as u64) * 64 + 63 - words[last_w].leading_zeros() as u64;
        Ok(BitRankDir { min, max, n, words, sup, sub, entries })
    }
}

impl ExactDirectory for BitRankDir {
    const NAME: &'static str = "h2_bitrank";
    const KIND: u8 = 3;

    fn build(pairs: &[(u64, Entry)]) -> Self {
        crate::assert_canonical(pairs);
        let min = pairs[0].0;
        let max = pairs[pairs.len() - 1].0;
        let u = (max - min + 1) as usize;
        let mut words = vec![0u64; u.div_ceil(64)];
        let mut entries = Vec::with_capacity(pairs.len());
        for &(k, e) in pairs {
            let i = (k - min) as usize;
            words[i / 64] |= 1u64 << (i % 64);
            entries.push(e);
        }
        Self::from_parts(min, words, entries).expect("canonical input")
    }

    #[inline]
    fn lookup(&self, key: u64) -> Option<&Entry> {
        if key < self.min || key > self.max {
            return None;
        }
        let i = (key - self.min) as usize;
        let (w, b) = (i / 64, i % 64);
        let word = self.words[w];
        if word & (1u64 << b) == 0 {
            return None;
        }
        let slot = self.sup[w / 8] as usize
            + self.sub[w] as usize
            + (word & ((1u64 << b) - 1)).count_ones() as usize;
        Some(&self.entries[slot])
    }

    fn iter_pairs(&self) -> Vec<(u64, Entry)> {
        let mut out = Vec::with_capacity(self.n);
        let mut slot = 0usize;
        for (w, &word) in self.words.iter().enumerate() {
            let mut m = word;
            while m != 0 {
                let b = m.trailing_zeros() as u64;
                out.push((
                    self.min + (w as u64) * 64 + b,
                    self.entries[slot],
                ));
                slot += 1;
                m &= m - 1;
            }
        }
        out
    }

    fn serialize(&self) -> Vec<u8> {
        let mut v = envelope::begin(Self::KIND, self.n as u64);
        v.extend_from_slice(&self.min.to_le_bytes());
        v.extend_from_slice(&(self.words.len() as u64).to_le_bytes());
        for &w in &self.words {
            v.extend_from_slice(&w.to_le_bytes());
        }
        for e in &self.entries {
            let mut rec = Vec::with_capacity(56);
            e.write_record(0, &mut rec);
            v.extend_from_slice(&rec[8..]);
        }
        envelope::finish(v)
    }

    fn open(bytes: &[u8]) -> Result<Self, OpenError> {
        let (n, body) = envelope::open(bytes, Self::KIND)?;
        let n = n as usize;
        if body.len() < 16 {
            return Err(OpenError::Corrupt("short bitrank body"));
        }
        let min = crate::rd_u64(body, 0);
        let n_words = crate::rd_u64(body, 8) as usize;
        if body.len() != 16 + n_words * 8 + n * 48 {
            return Err(OpenError::Corrupt("bitrank body size mismatch"));
        }
        let mut words = Vec::with_capacity(n_words);
        for i in 0..n_words {
            words.push(crate::rd_u64(body, 16 + i * 8));
        }
        let ecol = &body[16 + n_words * 8..];
        let mut entries = Vec::with_capacity(n);
        for i in 0..n {
            let mut rec = [0u8; 56];
            rec[8..].copy_from_slice(&ecol[i * 48..i * 48 + 48]);
            entries.push(Entry::read_record(&rec).1);
        }
        Self::from_parts(min, words, entries)
    }

    fn resident_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.words.capacity() * 8
            + self.sup.capacity() * 4
            + self.sub.capacity() * 2
            + self.entries.capacity() * 48
    }
}
