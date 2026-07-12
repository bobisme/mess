//! Canonical byte encodings shared by both state implementations and the
//! effect/manifest serializers: LEB128 varints and the canonical state
//! digest builder.
//!
//! The digest SPEC is shared (both states must emit identical bytes for
//! identical logical state); the state layouts feeding it are independent.

/// Append a LEB128 varint.
pub fn put_varint(out: &mut Vec<u8>, mut v: u64) {
    loop {
        let b = (v & 0x7f) as u8;
        v >>= 7;
        if v == 0 {
            out.push(b);
            return;
        }
        out.push(b | 0x80);
    }
}

/// Read a LEB128 varint. Returns `(value, bytes_consumed)`.
pub fn get_varint(buf: &[u8], at: usize) -> Option<(u64, usize)> {
    let mut v: u64 = 0;
    let mut shift = 0u32;
    let mut n = 0usize;
    loop {
        let b = *buf.get(at + n)?;
        n += 1;
        if shift > 63 || (shift == 63 && (b & 0x7e) != 0) {
            return None; // would overflow u64
        }
        v |= u64::from(b & 0x7f) << shift;
        if b & 0x80 == 0 {
            return Some((v, n));
        }
        shift += 7;
    }
}

/// Canonical state digest builder. Both the oracle and the kernel walk their
/// components IN THIS ORDER, calling these methods with logically sorted
/// entries; the digest is `BLAKE3` over the resulting canonical byte stream.
///
/// Components, in order: cursor, anchor, heads (ascending stream id),
/// snapshots (ascending stream id), frontiers (ascending (projection,
/// shard)), registry (ascending name), allocators (ascending slot), live
/// dedupe entries (ascending (fingerprint, position), EXACT window predicate
/// applied — representation slack must not leak into the digest).
pub struct DigestBuilder {
    h: blake3::Hasher,
    section: u8,
}

impl DigestBuilder {
    pub fn new(cursor_idx: u64, cursor_pos: u64, anchor: &[u8; 32]) -> Self {
        let mut h = blake3::Hasher::new();
        h.update(b"segeff-state-digest-v1");
        h.update(&cursor_idx.to_le_bytes());
        h.update(&cursor_pos.to_le_bytes());
        h.update(anchor);
        Self { h, section: 0 }
    }

    fn mark(&mut self, section: u8) {
        // Section separators make the encoding prefix-free across
        // components even when a component is empty.
        while self.section < section {
            self.section += 1;
            self.h.update(&[0xff, self.section]);
        }
        assert_eq!(self.section, section, "digest sections out of order");
    }

    pub fn head(&mut self, stream_id: u64, count: u64) {
        self.mark(1);
        self.h.update(&stream_id.to_le_bytes());
        self.h.update(&count.to_le_bytes());
    }

    pub fn snapshot(&mut self, stream_id: u64, version: u64, sref: u64) {
        self.mark(2);
        self.h.update(&stream_id.to_le_bytes());
        self.h.update(&version.to_le_bytes());
        self.h.update(&sref.to_le_bytes());
    }

    pub fn frontier(&mut self, projection: u32, shard: u32, pos: u64) {
        self.mark(3);
        self.h.update(&projection.to_le_bytes());
        self.h.update(&shard.to_le_bytes());
        self.h.update(&pos.to_le_bytes());
    }

    pub fn registration(&mut self, name: u64, id: u64) {
        self.mark(4);
        self.h.update(&name.to_le_bytes());
        self.h.update(&id.to_le_bytes());
    }

    pub fn alloc(&mut self, slot: u32, value: u64) {
        self.mark(5);
        self.h.update(&slot.to_le_bytes());
        self.h.update(&value.to_le_bytes());
    }

    pub fn dedupe(&mut self, fingerprint: u64, position: u64) {
        self.mark(6);
        self.h.update(&fingerprint.to_le_bytes());
        self.h.update(&position.to_le_bytes());
    }

    pub fn finish(mut self) -> [u8; 32] {
        self.mark(7);
        *self.h.finalize().as_bytes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn varint_roundtrip() {
        let mut buf = Vec::new();
        let vals = [
            0u64,
            1,
            127,
            128,
            16383,
            16384,
            u32::MAX as u64,
            u64::MAX,
        ];
        for &v in &vals {
            buf.clear();
            put_varint(&mut buf, v);
            let (got, n) = get_varint(&buf, 0).unwrap();
            assert_eq!(got, v);
            assert_eq!(n, buf.len());
        }
    }
}
