//! H3: partitioned Elias–Fano — design.md §12.3 / research/03 §4.3. Fixed
//! 256-key partitions, each with a local base (its first key), `l` low bits
//! per key (`l = floor(log2(span/count))`), a unary high-bits vector (one
//! `1` per key at position `i + (rel_i >> l)`), and packed low bits. Lookup:
//! binary-search the partition first-key array, then an exact-equality scan
//! of the (<= ~3*256-bit, cache-local) high-bit run for `high == hb`,
//! comparing low bits — a predecessor-free exact membership test. Entries
//! stored in global key order; the EF rank IS the entry slot.
//!
//! The high-bit scan is a word-popcount skip (no select directory): a
//! partition's high vector is at most ~12 words, so a per-partition select
//! structure would cost more than it saves.

use crate::{Entry, ExactDirectory, OpenError, envelope};

pub const PART: usize = 256;

#[derive(Clone, Copy)]
struct Part {
    /// Bit offset of this partition's low bits in `lo`.
    lo_off: u64,
    /// Bit offset of this partition's high bits in `hi`.
    hi_off: u64,
    /// Keys in this partition (PART except possibly the last).
    count:  u32,
    /// High-bits length in bits: `count + (rel_max >> l)`.
    hi_len: u32,
    /// Low bits per key.
    l:      u8,
}

pub struct PefDir {
    n:          usize,
    /// First key of each partition, ascending (binary-search target).
    part_first: Vec<u64>,
    parts:      Vec<Part>,
    lo:         Vec<u64>,
    hi:         Vec<u64>,
    entries:    Vec<Entry>,
}

/// Read `width` (< 64) bits at absolute bit offset `off`.
#[inline]
fn get_bits(words: &[u64], off: u64, width: u8) -> u64 {
    if width == 0 {
        return 0;
    }
    let w = (off / 64) as usize;
    let b = (off % 64) as u32;
    let mask = (1u64 << width) - 1;
    let lo = words[w] >> b;
    if b + width as u32 <= 64 {
        lo & mask
    } else {
        (lo | (words[w + 1] << (64 - b))) & mask
    }
}

/// Set bit at absolute offset `off`.
#[inline]
fn set_bit(words: &mut [u64], off: u64) {
    words[(off / 64) as usize] |= 1u64 << (off % 64);
}

/// Write `width` bits of `v` at absolute bit offset `off`.
#[inline]
fn put_bits(words: &mut [u64], off: u64, width: u8, v: u64) {
    if width == 0 {
        return;
    }
    let w = (off / 64) as usize;
    let b = (off % 64) as u32;
    words[w] |= v << b;
    if b + width as u32 > 64 {
        words[w + 1] |= v >> (64 - b);
    }
}

impl PefDir {
    fn build_from_keys(
        keys: &[u64],
        entries: Vec<Entry>,
    ) -> Result<Self, OpenError> {
        let n = keys.len();
        if n == 0 {
            return Err(OpenError::Corrupt("empty pef"));
        }
        if !keys.windows(2).all(|w| w[0] < w[1]) {
            return Err(OpenError::Corrupt("keys not strictly ascending"));
        }
        let n_parts = n.div_ceil(PART);
        let mut part_first = Vec::with_capacity(n_parts);
        let mut parts = Vec::with_capacity(n_parts);
        let mut lo_bits = 0u64;
        let mut hi_bits = 0u64;
        // Pass 1: geometry.
        for p in 0..n_parts {
            let ks = &keys[p * PART..(p * PART + PART).min(n)];
            let base = ks[0];
            let count = ks.len();
            let span = ks[count - 1] - base + 1;
            let l: u8 = if span > count as u64 {
                (span / count as u64).ilog2() as u8
            } else {
                0
            };
            let rel_max = ks[count - 1] - base;
            let hi_len = count as u64 + (rel_max >> l);
            part_first.push(base);
            parts.push(Part {
                lo_off: lo_bits,
                hi_off: hi_bits,
                count: count as u32,
                hi_len: hi_len as u32,
                l,
            });
            lo_bits += count as u64 * l as u64;
            hi_bits += hi_len;
        }
        let mut lo = vec![0u64; (lo_bits as usize).div_ceil(64)];
        let mut hi = vec![0u64; (hi_bits as usize).div_ceil(64)];
        // Pass 2: fill.
        for (p, part) in parts.iter().enumerate() {
            let ks = &keys[p * PART..p * PART + part.count as usize];
            let base = part_first[p];
            for (i, &k) in ks.iter().enumerate() {
                let rel = k - base;
                set_bit(&mut hi, part.hi_off + i as u64 + (rel >> part.l));
                let mask =
                    if part.l == 0 { 0 } else { (1u64 << part.l) - 1 };
                put_bits(
                    &mut lo,
                    part.lo_off + i as u64 * part.l as u64,
                    part.l,
                    rel & mask,
                );
            }
        }
        Ok(PefDir { n, part_first, parts, lo, hi, entries })
    }

    /// Scan partition `p`'s high bits for a one with `high == hb` whose low
    /// bits equal `lowb`; returns the in-partition index.
    #[inline]
    fn find_in_part(&self, p: usize, hb: u64, lowb: u64) -> Option<usize> {
        let part = &self.parts[p];
        let hi_off = part.hi_off;
        let hi_len = part.hi_len as u64;
        // Walk to the hb'th zero (zeros_seen == hb), then candidates are the
        // run of ones that follows.
        let mut pos = 0u64; // bit position within the partition's high vector
        let mut zeros = 0u64;
        while pos < hi_len {
            // Load up to the next word boundary (take == 64 iff aligned).
            let take = (64 - ((hi_off + pos) % 64)).min(hi_len - pos);
            let word = if take == 64 {
                self.hi[((hi_off + pos) / 64) as usize]
            } else {
                get_bits(&self.hi, hi_off + pos, take as u8)
            };
            let word_zeros = take - (word.count_ones() as u64).min(take);
            if zeros + word_zeros < hb {
                zeros += word_zeros;
                pos += take;
                continue;
            }
            // The hb'th zero (or the start of the target run) is inside this
            // chunk; finish bit-by-bit from here.
            let mut w = word;
            let mut i = 0u64;
            while i < take {
                let bit = w & 1;
                if zeros == hb && bit == 1 {
                    // Candidate one: index = ones_before = (pos+i) - zeros.
                    let idx = pos + i - zeros;
                    let got = get_bits(
                        &self.lo,
                        part.lo_off + idx * part.l as u64,
                        part.l,
                    );
                    if got == lowb {
                        return Some(idx as usize);
                    }
                    // Low bits ascend within a high run: early-out.
                    if got > lowb {
                        return None;
                    }
                } else if bit == 0 {
                    if zeros == hb {
                        return None; // run over
                    }
                    zeros += 1;
                }
                w >>= 1;
                i += 1;
            }
            pos += take;
        }
        None
    }
}

impl ExactDirectory for PefDir {
    const NAME: &'static str = "h3_pef";
    const KIND: u8 = 4;

    fn build(pairs: &[(u64, Entry)]) -> Self {
        crate::assert_canonical(pairs);
        let keys: Vec<u64> = pairs.iter().map(|p| p.0).collect();
        let entries: Vec<Entry> = pairs.iter().map(|p| p.1).collect();
        Self::build_from_keys(&keys, entries).expect("canonical input")
    }

    #[inline]
    fn lookup(&self, key: u64) -> Option<&Entry> {
        let p = self.part_first.partition_point(|&f| f <= key);
        if p == 0 {
            return None;
        }
        let p = p - 1;
        let part = &self.parts[p];
        let rel = key - self.part_first[p];
        let hb = rel >> part.l;
        let lowb = rel & if part.l == 0 { 0 } else { (1u64 << part.l) - 1 };
        // Zeros available = hi_len - count; a high value needing more zeros
        // than exist cannot be present.
        if hb > (part.hi_len - part.count) as u64 {
            return None;
        }
        let idx = self.find_in_part(p, hb, lowb)?;
        Some(&self.entries[p * PART + idx])
    }

    fn iter_pairs(&self) -> Vec<(u64, Entry)> {
        let mut out = Vec::with_capacity(self.n);
        for (p, part) in self.parts.iter().enumerate() {
            let base = self.part_first[p];
            let mut idx = 0u64; // ones seen
            let mut zeros = 0u64;
            for pos in 0..part.hi_len as u64 {
                if get_bits(&self.hi, part.hi_off + pos, 1) == 1 {
                    let low = get_bits(
                        &self.lo,
                        part.lo_off + idx * part.l as u64,
                        part.l,
                    );
                    let key = base + (zeros << part.l) + low;
                    out.push((key, self.entries[p * PART + idx as usize]));
                    idx += 1;
                } else {
                    zeros += 1;
                }
            }
        }
        out
    }

    fn serialize(&self) -> Vec<u8> {
        let mut v = envelope::begin(Self::KIND, self.n as u64);
        v.extend_from_slice(&(self.parts.len() as u64).to_le_bytes());
        v.extend_from_slice(&(self.lo.len() as u64).to_le_bytes());
        v.extend_from_slice(&(self.hi.len() as u64).to_le_bytes());
        for &f in &self.part_first {
            v.extend_from_slice(&f.to_le_bytes());
        }
        for part in &self.parts {
            v.push(part.l);
        }
        for part in &self.parts {
            v.extend_from_slice(&part.hi_len.to_le_bytes());
        }
        for &w in &self.lo {
            v.extend_from_slice(&w.to_le_bytes());
        }
        for &w in &self.hi {
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
        if body.len() < 24 {
            return Err(OpenError::Corrupt("short pef body"));
        }
        let n_parts = crate::rd_u64(body, 0) as usize;
        let lo_words = crate::rd_u64(body, 8) as usize;
        let hi_words = crate::rd_u64(body, 16) as usize;
        if n_parts != n.div_ceil(PART) {
            return Err(OpenError::Corrupt("pef partition count mismatch"));
        }
        let expect = 24
            + n_parts * 8      // part_first
            + n_parts          // l
            + n_parts * 4      // hi_len
            + lo_words * 8
            + hi_words * 8
            + n * 48;
        if body.len() != expect {
            return Err(OpenError::Corrupt("pef body size mismatch"));
        }
        let mut at = 24;
        let mut part_first = Vec::with_capacity(n_parts);
        for i in 0..n_parts {
            part_first.push(crate::rd_u64(body, at + i * 8));
        }
        if !part_first.windows(2).all(|w| w[0] < w[1]) {
            return Err(OpenError::Corrupt("pef part_first not ascending"));
        }
        at += n_parts * 8;
        let ls = &body[at..at + n_parts];
        at += n_parts;
        let mut parts = Vec::with_capacity(n_parts);
        let mut lo_off = 0u64;
        let mut hi_off = 0u64;
        for p in 0..n_parts {
            let count =
                if p + 1 == n_parts { n - PART * p } else { PART } as u32;
            let l = ls[p];
            if l > 63 {
                return Err(OpenError::Corrupt("pef l out of range"));
            }
            let hi_len = crate::rd_u32(body, at + p * 4);
            if (hi_len as u64) < count as u64 {
                return Err(OpenError::Corrupt("pef hi_len < count"));
            }
            parts.push(Part { lo_off, hi_off, count, hi_len, l });
            lo_off += count as u64 * l as u64;
            hi_off += hi_len as u64;
        }
        if (lo_off as usize).div_ceil(64) != lo_words
            || (hi_off as usize).div_ceil(64) != hi_words
        {
            return Err(OpenError::Corrupt("pef bit-length mismatch"));
        }
        at += n_parts * 4;
        let mut lo = Vec::with_capacity(lo_words);
        for i in 0..lo_words {
            lo.push(crate::rd_u64(body, at + i * 8));
        }
        at += lo_words * 8;
        let mut hi = Vec::with_capacity(hi_words);
        for i in 0..hi_words {
            hi.push(crate::rd_u64(body, at + i * 8));
        }
        at += hi_words * 8;
        let ecol = &body[at..];
        let mut entries = Vec::with_capacity(n);
        for i in 0..n {
            let mut rec = [0u8; 56];
            rec[8..].copy_from_slice(&ecol[i * 48..i * 48 + 48]);
            entries.push(Entry::read_record(&rec).1);
        }
        Ok(PefDir { n, part_first, parts, lo, hi, entries })
    }

    fn resident_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.part_first.capacity() * 8
            + self.parts.capacity() * std::mem::size_of::<Part>()
            + self.lo.capacity() * 8
            + self.hi.capacity() * 8
            + self.entries.capacity() * 48
    }
}
