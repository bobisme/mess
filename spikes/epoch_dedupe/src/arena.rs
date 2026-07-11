//! Simulated capsule arena: a flat `Vec<u8>` blob store standing in for the
//! log's canonical capsules. The full dedupe key is stored here ONCE
//! (design.md §13.2); the resident indexes keep only fingerprint + position
//! + a ptr into this arena, and every fingerprint hit verifies the full key
//! bytes here before declaring a duplicate.
//!
//! Record layout at `ptr`:
//!
//! ```text
//! position  u64 LE      (for rebuild-from-arena, the checkpoint-loss case)
//! scope     kind u8 + stream_id u64 LE
//! key_len   u32 LE
//! key       key_len bytes
//! ```

use crate::Scope;

pub struct Arena {
    buf: Vec<u8>,
}

const HDR: usize = 8 + 1 + 8 + 4;

impl Arena {
    pub fn new() -> Self {
        Arena { buf: Vec::new() }
    }

    pub fn with_capacity(bytes: usize) -> Self {
        Arena { buf: Vec::with_capacity(bytes) }
    }

    /// Append one canonical record, returning its ptr.
    pub fn append(&mut self, scope: Scope, key: &[u8], position: u64) -> u64 {
        let ptr = self.buf.len() as u64;
        self.buf.extend_from_slice(&position.to_le_bytes());
        match scope {
            Scope::Global => {
                self.buf.push(0);
                self.buf.extend_from_slice(&0u64.to_le_bytes());
            }
            Scope::Stream(id) => {
                self.buf.push(1);
                self.buf.extend_from_slice(&id.to_le_bytes());
            }
        }
        self.buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
        self.buf.extend_from_slice(key);
        ptr
    }

    /// Exact verification: does the record at `ptr` carry exactly this
    /// (scope, key)? This is the read every fingerprint hit performs.
    #[inline]
    pub fn matches(&self, ptr: u64, scope: Scope, key: &[u8]) -> bool {
        let p = ptr as usize;
        let b = &self.buf[p..];
        let kind = b[8];
        let sid = u64::from_le_bytes(b[9..17].try_into().unwrap());
        if Scope::decode(kind, sid) != scope {
            return false;
        }
        let klen = u32::from_le_bytes(b[17..21].try_into().unwrap()) as usize;
        if klen != key.len() {
            return false;
        }
        &b[HDR..HDR + klen] == key
    }

    /// Iterate all records in append (= position) order:
    /// `(ptr, position, scope, key)`. Used by rebuild-from-arena.
    pub fn iter(&self) -> ArenaIter<'_> {
        ArenaIter { buf: &self.buf, off: 0 }
    }

    pub fn len_bytes(&self) -> u64 {
        self.buf.len() as u64
    }
}

pub struct ArenaIter<'a> {
    buf: &'a [u8],
    off: usize,
}

impl<'a> Iterator for ArenaIter<'a> {
    type Item = (u64, u64, Scope, &'a [u8]);

    fn next(&mut self) -> Option<Self::Item> {
        if self.off >= self.buf.len() {
            return None;
        }
        let ptr = self.off as u64;
        let b = &self.buf[self.off..];
        let pos = u64::from_le_bytes(b[..8].try_into().unwrap());
        let scope = Scope::decode(b[8], u64::from_le_bytes(b[9..17].try_into().unwrap()));
        let klen = u32::from_le_bytes(b[17..21].try_into().unwrap()) as usize;
        let key = &b[HDR..HDR + klen];
        self.off += HDR + klen;
        Some((ptr, pos, scope, key))
    }
}
