//! Columnar shredding prototype.
//!
//! Per event type (category), each event's msgpack is parsed and its scalar
//! fields are shredded into per-field column buffers (ints, strings, bools),
//! nested maps and arrays flattened by path. The residual "skeleton" — every
//! structural byte: map headers, key strings, array headers — is interned
//! per category (a handful of distinct skeletons per type), so a block
//! stores only a skeleton id per event plus the transposed columns.
//!
//! Int columns are stored zigzag-varint, either raw or delta-coded
//! (whichever is smaller per column per block — delta wins on timestamps,
//! seqs and other monotone-ish series). The whole block buffer (skeleton
//! ids ++ column directory ++ column data) is then zstd-compressed.
//!
//! Reassembly is BYTE-EXACT: values are re-encoded with minimal msgpack
//! encodings, which is exactly what rmp-serde named mode emits; the harness
//! proves this by memcmp of every reassembled event against the original
//! payload bytes (zero tolerance). Unknown msgpack markers fail loudly.
//!
//! Uncompressed columnar block layout:
//! ```text
//! u16 n_events, u16 n_cols
//! skeleton ids: n_events x u16 LE
//! col directory: n_cols x { u16 col_id, u8 kind, u8 enc, u32 data_len }
//! col data, concatenated in directory order
//! ```

use std::collections::HashMap;
use std::time::{Duration, Instant};

use crate::store::RawBlock;
use crate::workload::{Corpus, CATEGORIES};

pub const K_INT: u8 = 0;
pub const K_STR: u8 = 1;
pub const K_BOOL: u8 = 2;

pub const ENC_RAW: u8 = 0;
pub const ENC_DELTA: u8 = 1;

// ---------------------------------------------------------------------------
// varint / zigzag / minimal msgpack encoders
// ---------------------------------------------------------------------------

#[inline]
pub fn write_varint(out: &mut Vec<u8>, mut v: u64) {
    loop {
        if v < 0x80 {
            out.push(v as u8);
            return;
        }
        out.push((v as u8 & 0x7f) | 0x80);
        v >>= 7;
    }
}

#[inline]
pub fn read_varint(d: &[u8], p: &mut usize) -> u64 {
    let mut v = 0u64;
    let mut shift = 0;
    loop {
        let b = d[*p];
        *p += 1;
        v |= ((b & 0x7f) as u64) << shift;
        if b & 0x80 == 0 {
            return v;
        }
        shift += 7;
    }
}

#[inline]
pub fn varint_len(v: u64) -> usize {
    if v == 0 {
        1
    } else {
        (64 - v.leading_zeros() as usize).div_ceil(7)
    }
}

#[inline]
fn zz(v: i64) -> u64 {
    ((v << 1) ^ (v >> 63)) as u64
}

#[inline]
fn unzz(u: u64) -> i64 {
    ((u >> 1) as i64) ^ -((u & 1) as i64)
}

/// Minimal msgpack integer encoding — mirrors rmp's write_uint/write_sint.
#[inline]
fn emit_int(out: &mut Vec<u8>, v: i64) {
    if v >= 0 {
        let u = v as u64;
        if u < 0x80 {
            out.push(u as u8);
        } else if u < 0x100 {
            out.push(0xcc);
            out.push(u as u8);
        } else if u < 0x1_0000 {
            out.push(0xcd);
            out.extend_from_slice(&(u as u16).to_be_bytes());
        } else if u < 0x1_0000_0000 {
            out.push(0xce);
            out.extend_from_slice(&(u as u32).to_be_bytes());
        } else {
            out.push(0xcf);
            out.extend_from_slice(&u.to_be_bytes());
        }
    } else if v >= -32 {
        out.push(v as u8);
    } else if v >= -128 {
        out.push(0xd0);
        out.push(v as u8);
    } else if v >= -32768 {
        out.push(0xd1);
        out.extend_from_slice(&(v as i16).to_be_bytes());
    } else if v >= -(1i64 << 31) {
        out.push(0xd2);
        out.extend_from_slice(&(v as i32).to_be_bytes());
    } else {
        out.push(0xd3);
        out.extend_from_slice(&v.to_be_bytes());
    }
}

/// Minimal msgpack str header — mirrors rmp's write_str_len.
#[inline]
fn emit_str(out: &mut Vec<u8>, s: &[u8]) {
    let l = s.len();
    if l < 32 {
        out.push(0xa0 | l as u8);
    } else if l < 256 {
        out.push(0xd9);
        out.push(l as u8);
    } else if l < 0x1_0000 {
        out.push(0xda);
        out.extend_from_slice(&(l as u16).to_be_bytes());
    } else {
        out.push(0xdb);
        out.extend_from_slice(&(l as u32).to_be_bytes());
    }
    out.extend_from_slice(s);
}

// ---------------------------------------------------------------------------
// Per-category shred state
// ---------------------------------------------------------------------------

#[derive(PartialEq, Eq, Hash)]
struct PathStep {
    parent: u32,
    tlen: u8,
    token: [u8; 24],
}

pub enum ROp {
    Lit(Box<[u8]>),
    Val { col: u16, kind: u8 },
}

enum OpTmp {
    Lit(usize, usize), // byte range in the event
    Val(u16, u8),
}

pub struct CatShred {
    trie: HashMap<PathStep, u32>,
    node_col: Vec<i32>,
    node_path: Vec<String>,
    pub col_kind: Vec<u8>,
    pub col_path: Vec<String>,
    skel_lut: HashMap<Vec<u8>, u16>,
    pub skels: Vec<Vec<ROp>>,
    ops_scratch: Vec<OpTmp>,
    sig_scratch: Vec<u8>,
}

impl CatShred {
    pub fn new() -> Self {
        CatShred {
            trie: HashMap::new(),
            node_col: vec![-1],
            node_path: vec![String::new()],
            col_kind: Vec::new(),
            col_path: Vec::new(),
            skel_lut: HashMap::new(),
            skels: Vec::new(),
            ops_scratch: Vec::new(),
            sig_scratch: Vec::new(),
        }
    }

    /// Approximate durable metadata cost of this category's shred state
    /// (skeleton table + column paths), for the all-in size estimate.
    pub fn meta_bytes(&self) -> u64 {
        let mut b = 0u64;
        for sk in &self.skels {
            for op in sk {
                b += match op {
                    ROp::Lit(l) => 3 + l.len() as u64,
                    ROp::Val { .. } => 4,
                };
            }
        }
        for p in &self.col_path {
            b += 4 + p.len() as u64;
        }
        b
    }

    fn node(&mut self, parent: u32, token: &[u8]) -> u32 {
        assert!(token.len() <= 24, "path token too long: {:?}", token);
        let mut key = PathStep { parent, tlen: token.len() as u8, token: [0; 24] };
        key.token[..token.len()].copy_from_slice(token);
        if let Some(&id) = self.trie.get(&key) {
            return id;
        }
        let id = self.node_col.len() as u32;
        self.node_col.push(-1);
        let step = if token.first() == Some(&0xff) {
            format!("[{}]", token[1])
        } else {
            String::from_utf8_lossy(token).into_owned()
        };
        let path = if self.node_path[parent as usize].is_empty() {
            step
        } else {
            format!("{}.{}", self.node_path[parent as usize], step)
        };
        self.node_path.push(path);
        self.trie.insert(key, id);
        id
    }

    fn col_for(&mut self, node: u32, kind: u8) -> u16 {
        let c = self.node_col[node as usize];
        if c >= 0 {
            assert_eq!(self.col_kind[c as usize], kind, "kind flip at {}", self.node_path[node as usize]);
            return c as u16;
        }
        let id = self.col_kind.len() as u16;
        self.col_kind.push(kind);
        self.col_path.push(self.node_path[node as usize].clone());
        self.node_col[node as usize] = id as i32;
        id
    }

    /// Shred one event: append its values to `acc`'s columns, intern its
    /// skeleton, record its skeleton id.
    pub fn shred_event(&mut self, ev: &[u8], acc: &mut BlockAcc) {
        let mut ops = std::mem::take(&mut self.ops_scratch);
        ops.clear();
        let mut p = 0usize;
        let mut lit0 = 0usize;
        self.parse_val(ev, &mut p, 0, &mut lit0, acc, &mut ops);
        assert_eq!(p, ev.len(), "trailing bytes after msgpack value");
        if lit0 < ev.len() {
            ops.push(OpTmp::Lit(lit0, ev.len()));
        }
        // Signature -> skeleton id.
        let mut sig = std::mem::take(&mut self.sig_scratch);
        sig.clear();
        for op in &ops {
            match *op {
                OpTmp::Lit(a, b) => {
                    sig.push(0);
                    write_varint(&mut sig, (b - a) as u64);
                    sig.extend_from_slice(&ev[a..b]);
                }
                OpTmp::Val(col, kind) => {
                    sig.push(1);
                    sig.push(kind);
                    write_varint(&mut sig, col as u64);
                }
            }
        }
        let skel_id = match self.skel_lut.get(&sig) {
            Some(&id) => id,
            None => {
                let id = self.skels.len() as u16;
                let rops: Vec<ROp> = ops
                    .iter()
                    .map(|op| match *op {
                        OpTmp::Lit(a, b) => ROp::Lit(ev[a..b].to_vec().into_boxed_slice()),
                        OpTmp::Val(col, kind) => ROp::Val { col, kind },
                    })
                    .collect();
                self.skels.push(rops);
                self.skel_lut.insert(sig.clone(), id);
                id
            }
        };
        acc.skel_ids.push(skel_id);
        self.ops_scratch = ops;
        self.sig_scratch = sig;
    }

    fn parse_val(
        &mut self,
        ev: &[u8],
        p: &mut usize,
        node: u32,
        lit0: &mut usize,
        acc: &mut BlockAcc,
        ops: &mut Vec<OpTmp>,
    ) {
        let m = ev[*p];
        match m {
            // fixmap
            0x80..=0x8f => {
                let n = (m & 0x0f) as usize;
                *p += 1;
                for _ in 0..n {
                    // key: fixstr / str8 (rmp-serde named mode emits str keys)
                    let km = ev[*p];
                    let (klen, khdr) = match km {
                        0xa0..=0xbf => ((km & 0x1f) as usize, 1usize),
                        0xd9 => (ev[*p + 1] as usize, 2),
                        _ => panic!("unsupported map key marker {km:#x}"),
                    };
                    let key = &ev[*p + khdr..*p + khdr + klen];
                    let child = self.node(node, key);
                    *p += khdr + klen;
                    self.parse_val(ev, p, child, lit0, acc, ops);
                }
            }
            // fixarray
            0x90..=0x9f => {
                let n = (m & 0x0f) as usize;
                *p += 1;
                for i in 0..n {
                    let tok = [0xffu8, i as u8];
                    let child = self.node(node, &tok);
                    self.parse_val(ev, p, child, lit0, acc, ops);
                }
            }
            // ---- scalars ----
            _ => {
                let v0 = *p;
                enum SV<'a> {
                    I(i64),
                    S(&'a [u8]),
                    B(bool),
                }
                let (sv, end): (SV, usize) = match m {
                    0x00..=0x7f => (SV::I(m as i64), v0 + 1),
                    0xe0..=0xff => (SV::I(m as i8 as i64), v0 + 1),
                    0xcc => (SV::I(ev[v0 + 1] as i64), v0 + 2),
                    0xcd => (SV::I(u16::from_be_bytes([ev[v0 + 1], ev[v0 + 2]]) as i64), v0 + 3),
                    0xce => (
                        SV::I(u32::from_be_bytes(ev[v0 + 1..v0 + 5].try_into().unwrap()) as i64),
                        v0 + 5,
                    ),
                    0xcf => {
                        let u = u64::from_be_bytes(ev[v0 + 1..v0 + 9].try_into().unwrap());
                        assert!(u <= i64::MAX as u64, "u64 out of i64 range unsupported");
                        (SV::I(u as i64), v0 + 9)
                    }
                    0xd0 => (SV::I(ev[v0 + 1] as i8 as i64), v0 + 2),
                    0xd1 => (SV::I(i16::from_be_bytes([ev[v0 + 1], ev[v0 + 2]]) as i64), v0 + 3),
                    0xd2 => (
                        SV::I(i32::from_be_bytes(ev[v0 + 1..v0 + 5].try_into().unwrap()) as i64),
                        v0 + 5,
                    ),
                    0xd3 => (
                        SV::I(i64::from_be_bytes(ev[v0 + 1..v0 + 9].try_into().unwrap())),
                        v0 + 9,
                    ),
                    0xa0..=0xbf => {
                        let l = (m & 0x1f) as usize;
                        (SV::S(&ev[v0 + 1..v0 + 1 + l]), v0 + 1 + l)
                    }
                    0xd9 => {
                        let l = ev[v0 + 1] as usize;
                        (SV::S(&ev[v0 + 2..v0 + 2 + l]), v0 + 2 + l)
                    }
                    0xda => {
                        let l = u16::from_be_bytes([ev[v0 + 1], ev[v0 + 2]]) as usize;
                        (SV::S(&ev[v0 + 3..v0 + 3 + l]), v0 + 3 + l)
                    }
                    0xc2 => (SV::B(false), v0 + 1),
                    0xc3 => (SV::B(true), v0 + 1),
                    _ => panic!("unsupported msgpack marker {m:#x} at {v0}"),
                };
                let kind = match sv {
                    SV::I(_) => K_INT,
                    SV::S(_) => K_STR,
                    SV::B(_) => K_BOOL,
                };
                let col = self.col_for(node, kind);
                if *lit0 < v0 {
                    ops.push(OpTmp::Lit(*lit0, v0));
                }
                ops.push(OpTmp::Val(col, kind));
                match sv {
                    SV::I(v) => acc.push_int(col, v),
                    SV::S(s) => acc.push_str(col, s),
                    SV::B(b) => acc.push_bool(col, b),
                }
                *lit0 = end;
                *p = end;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Block accumulator + serialization
// ---------------------------------------------------------------------------

#[derive(Default)]
struct ColAcc {
    n: u32,
    ints: Vec<i64>,
    bytes: Vec<u8>,
}

pub struct BlockAcc {
    pub skel_ids: Vec<u16>,
    cols: Vec<ColAcc>,
    touched: Vec<u16>,
}

impl BlockAcc {
    pub fn new() -> Self {
        BlockAcc { skel_ids: Vec::new(), cols: Vec::new(), touched: Vec::new() }
    }

    #[inline]
    fn col(&mut self, id: u16) -> &mut ColAcc {
        if self.cols.len() <= id as usize {
            self.cols.resize_with(id as usize + 1, ColAcc::default);
        }
        let c = &mut self.cols[id as usize];
        if c.n == 0 {
            self.touched.push(id);
        }
        c
    }

    #[inline]
    fn push_int(&mut self, id: u16, v: i64) {
        let c = self.col(id);
        c.ints.push(v);
        c.n += 1;
    }

    #[inline]
    fn push_str(&mut self, id: u16, s: &[u8]) {
        let c = self.col(id);
        write_varint(&mut c.bytes, s.len() as u64);
        c.bytes.extend_from_slice(s);
        c.n += 1;
    }

    #[inline]
    fn push_bool(&mut self, id: u16, b: bool) {
        let c = self.col(id);
        c.bytes.push(b as u8);
        c.n += 1;
    }
}

/// Serialize the accumulated block and reset the accumulator.
pub fn finish_block(cs: &CatShred, acc: &mut BlockAcc) -> Vec<u8> {
    let n = acc.skel_ids.len();
    acc.touched.sort_unstable();
    let mut buf = Vec::with_capacity(64 + n * 3);
    buf.extend_from_slice(&(n as u16).to_le_bytes());
    buf.extend_from_slice(&(acc.touched.len() as u16).to_le_bytes());
    for &id in &acc.skel_ids {
        buf.extend_from_slice(&id.to_le_bytes());
    }
    // Column data segments (int enc chosen per column), directory, then data.
    let mut data: Vec<u8> = Vec::new();
    let mut dir: Vec<(u16, u8, u8, u32)> = Vec::with_capacity(acc.touched.len());
    for &id in &acc.touched {
        let c = &acc.cols[id as usize];
        let kind = cs.col_kind[id as usize];
        let start = data.len();
        let enc = match kind {
            K_INT => {
                let raw_len: usize = c.ints.iter().map(|&v| varint_len(zz(v))).sum();
                let mut prev = 0i64;
                let mut delta_len = 0usize;
                for &v in &c.ints {
                    delta_len += varint_len(zz(v.wrapping_sub(prev)));
                    prev = v;
                }
                if delta_len < raw_len {
                    let mut prev = 0i64;
                    for &v in &c.ints {
                        write_varint(&mut data, zz(v.wrapping_sub(prev)));
                        prev = v;
                    }
                    ENC_DELTA
                } else {
                    for &v in &c.ints {
                        write_varint(&mut data, zz(v));
                    }
                    ENC_RAW
                }
            }
            _ => {
                data.extend_from_slice(&c.bytes);
                ENC_RAW
            }
        };
        dir.push((id, kind, enc, (data.len() - start) as u32));
    }
    for (id, kind, enc, len) in dir {
        buf.extend_from_slice(&id.to_le_bytes());
        buf.push(kind);
        buf.push(enc);
        buf.extend_from_slice(&len.to_le_bytes());
    }
    buf.extend_from_slice(&data);
    // reset
    for &id in &acc.touched {
        let c = &mut acc.cols[id as usize];
        c.n = 0;
        c.ints.clear();
        c.bytes.clear();
    }
    acc.touched.clear();
    acc.skel_ids.clear();
    buf
}

/// Shred the whole corpus into columnar raw blocks of `be` events.
pub fn shred_all(
    c: &Corpus,
    be: usize,
) -> ([Vec<RawBlock>; CATEGORIES], Vec<CatShred>, Duration) {
    let t = Instant::now();
    let mut shreds: Vec<CatShred> = (0..CATEGORIES).map(|_| CatShred::new()).collect();
    let mut out: [Vec<RawBlock>; CATEGORIES] = Default::default();
    let mut acc = BlockAcc::new();
    for cat in 0..CATEGORIES {
        let cs = &mut shreds[cat];
        let lo = c.cat_start[cat];
        let hi = c.cat_start[cat + 1];
        let mut i = lo;
        while i < hi {
            let end = (i + be).min(hi);
            for k in i..end {
                cs.shred_event(c.bytes_of(k), &mut acc);
            }
            let count = (end - i) as u16;
            let buf = finish_block(cs, &mut acc);
            out[cat].push(RawBlock { count, buf });
            i = end;
        }
    }
    (out, shreds, t.elapsed())
}

// ---------------------------------------------------------------------------
// Reassembly
// ---------------------------------------------------------------------------

struct Cur<'a> {
    kind: u8,
    enc: u8,
    data: &'a [u8],
    pos: usize,
    prev: i64,
}

/// Reassemble events [a, b) of a columnar block, byte-exact, appending each
/// event's bytes to `out` and event boundaries to `offs` (b-a+1 entries).
pub fn reassemble_range(
    cs: &CatShred,
    block: &[u8],
    a: usize,
    b: usize,
    out: &mut Vec<u8>,
    offs: &mut Vec<u32>,
) {
    let n = u16::from_le_bytes([block[0], block[1]]) as usize;
    let n_cols = u16::from_le_bytes([block[2], block[3]]) as usize;
    debug_assert!(b <= n);
    let skel_ids = &block[4..4 + 2 * n];
    out.reserve((b - a) * 256);
    let mut p = 4 + 2 * n;
    let mut curs: Vec<Cur> = Vec::with_capacity(n_cols);
    let mut curs_idx = vec![usize::MAX; cs.col_kind.len()];
    let mut data_off = p + n_cols * 8;
    for _ in 0..n_cols {
        let id = u16::from_le_bytes([block[p], block[p + 1]]) as usize;
        let kind = block[p + 2];
        let enc = block[p + 3];
        let len = u32::from_le_bytes(block[p + 4..p + 8].try_into().unwrap()) as usize;
        p += 8;
        curs_idx[id] = curs.len();
        curs.push(Cur { kind, enc, data: &block[data_off..data_off + len], pos: 0, prev: 0 });
        data_off += len;
    }
    for e in 0..b {
        let emit = e >= a;
        if emit {
            offs.push(out.len() as u32);
        }
        let sk = u16::from_le_bytes([skel_ids[2 * e], skel_ids[2 * e + 1]]) as usize;
        for op in &cs.skels[sk] {
            match op {
                ROp::Lit(bytes) => {
                    if emit {
                        out.extend_from_slice(bytes);
                    }
                }
                ROp::Val { col, kind } => {
                    let cur = &mut curs[curs_idx[*col as usize]];
                    debug_assert_eq!(cur.kind, *kind);
                    match *kind {
                        K_INT => {
                            let d = unzz(read_varint(cur.data, &mut cur.pos));
                            let v = if cur.enc == ENC_DELTA {
                                cur.prev = cur.prev.wrapping_add(d);
                                cur.prev
                            } else {
                                d
                            };
                            if emit {
                                emit_int(out, v);
                            }
                        }
                        K_STR => {
                            let l = read_varint(cur.data, &mut cur.pos) as usize;
                            let s = &cur.data[cur.pos..cur.pos + l];
                            cur.pos += l;
                            if emit {
                                emit_str(out, s);
                            }
                        }
                        K_BOOL => {
                            let bv = cur.data[cur.pos];
                            cur.pos += 1;
                            if emit {
                                out.push(if bv != 0 { 0xc3 } else { 0xc2 });
                            }
                        }
                        _ => unreachable!(),
                    }
                }
            }
        }
    }
    offs.push(out.len() as u32);
}
