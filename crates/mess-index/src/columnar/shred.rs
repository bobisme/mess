//! Shredding: parse a run of MessagePack payloads into per-path columns +
//! interned skeletons, then serialize a self-describing columnar block. Any
//! payload the tokenizer cannot model — non-MessagePack, non-canonical scalar
//! encodings, unsupported markers, malformed/truncated/over-nested bytes —
//! routes the whole block to a byte-exact raw fallback. The shredder never
//! panics on adversarial input.

use std::collections::HashMap;

use super::{
    COLUMNAR_VERSION, ENC_DELTA, ENC_RAW, FLAG_COLUMNAR, FLAG_PERCOL, K_BOOL,
    K_INT, K_STR, MAX_BLOCK_EVENTS, emit_int, emit_str_header, varint_len,
    write_varint, zz,
};

/// Depth limit for nested maps/arrays; deeper input falls back to raw.
const MAX_DEPTH: u32 = 32;
/// Longest map key / path token kept inline in the trie; longer -> fallback.
const MAX_TOKEN: usize = 24;
/// Column-count ceiling per block (also keeps col ids inside u16); over ->
/// fallback. Bounds the column explosion an adversarial wide/array payload
/// could otherwise cause.
const MAX_COLS: usize = 16_384;
/// Per-map / per-array element ceiling; over -> fallback. Bounds skeleton op
/// counts (a u16) and column growth.
const MAX_CONTAINER: usize = 4096;
/// Skeleton op-count ceiling per event (op count is a u16); over -> fallback.
const MAX_OPS: usize = u16::MAX as usize;

/// Sentinel error meaning "this block is not shreddable — use raw fallback".
/// Never surfaces to callers; it only selects the block layout.
struct Unshreddable;
type Shred<T> = Result<T, Unshreddable>;

/// Encoder options for [`encode_block`] / [`BlockEncoder`].
#[derive(Clone, Copy, Debug)]
pub struct EncodeOpts {
    /// zstd level for block/column compression (round-4 default: 9).
    pub level:      i32,
    /// Emit the per-column layout (each column its own zstd frame) so single
    /// point reads decompress only the columns they touch. `false` (default)
    /// emits the whole-block layout, which packs tighter.
    pub per_column: bool,
}

impl Default for EncodeOpts {
    fn default() -> Self { EncodeOpts { level: 9, per_column: false } }
}

// ---------------------------------------------------------------------------
// Per-block shred state (reused across blocks via `reset`)
// ---------------------------------------------------------------------------

#[derive(PartialEq, Eq, Hash)]
struct PathKey {
    parent: u32,
    tlen:   u8,
    token:  [u8; MAX_TOKEN],
}

/// One event's ops, recorded as byte ranges into the event during parse.
enum OpTmp {
    Lit(usize, usize),
    Val(u16, u8),
}

/// An interned skeleton op.
enum ROp {
    Lit(Box<[u8]>),
    Val(u16, u8),
}

#[derive(Default)]
struct ColAcc {
    kind:  u8,
    ints:  Vec<i64>,
    bytes: Vec<u8>,
}

/// Reusable shredder scratch. Construct once, [`reset`](Shredder::reset)
/// between blocks. Block-local: skeletons and columns are interned per block
/// so every emitted block is self-describing.
struct Shredder {
    trie:     HashMap<PathKey, u32>,
    node_col: Vec<i32>,
    cols:     Vec<ColAcc>,
    skels:    Vec<Vec<ROp>>,
    skel_lut: HashMap<Vec<u8>, u16>,
    skel_ids: Vec<u16>,
    // scratch reused across events
    ops:      Vec<OpTmp>,
    sig:      Vec<u8>,
    canon:    Vec<u8>,
}

impl Shredder {
    fn new() -> Self {
        Shredder {
            trie:     HashMap::new(),
            node_col: vec![-1],
            cols:     Vec::new(),
            skels:    Vec::new(),
            skel_lut: HashMap::new(),
            skel_ids: Vec::new(),
            ops:      Vec::new(),
            sig:      Vec::new(),
            canon:    Vec::new(),
        }
    }

    fn reset(&mut self) {
        self.trie.clear();
        self.node_col.clear();
        self.node_col.push(-1);
        self.cols.clear();
        self.skels.clear();
        self.skel_lut.clear();
        self.skel_ids.clear();
        self.ops.clear();
        self.sig.clear();
        self.canon.clear();
    }

    fn node(&mut self, parent: u32, token: &[u8]) -> Shred<u32> {
        if token.len() > MAX_TOKEN {
            return Err(Unshreddable);
        }
        let mut key =
            PathKey { parent, tlen: token.len() as u8, token: [0; MAX_TOKEN] };
        key.token[..token.len()].copy_from_slice(token);
        if let Some(&id) = self.trie.get(&key) {
            return Ok(id);
        }
        let id = self.node_col.len() as u32;
        self.node_col.push(-1);
        self.trie.insert(key, id);
        Ok(id)
    }

    fn col_for(&mut self, node: u32, kind: u8) -> Shred<u16> {
        let c = self.node_col[node as usize];
        if c >= 0 {
            let col = c as u16;
            if self.cols[col as usize].kind != kind {
                // Same path, different scalar type across events — the spike
                // asserts; here it just routes to raw fallback.
                return Err(Unshreddable);
            }
            return Ok(col);
        }
        if self.cols.len() >= MAX_COLS {
            return Err(Unshreddable);
        }
        let id = self.cols.len() as u16;
        self.cols.push(ColAcc { kind, ..Default::default() });
        self.node_col[node as usize] = id as i32;
        Ok(id)
    }

    fn shred_all(&mut self, events: &[&[u8]]) -> Shred<()> {
        for ev in events {
            self.shred_event(ev)?;
        }
        Ok(())
    }

    fn shred_event(&mut self, ev: &[u8]) -> Shred<()> {
        let mut ops = std::mem::take(&mut self.ops);
        ops.clear();
        let mut p = 0usize;
        let mut lit0 = 0usize;
        let r = self.parse_val(ev, &mut p, 0, 0, &mut lit0, &mut ops);
        // Always restore scratch buffers before any early return.
        let outcome = (|| {
            r?;
            if p != ev.len() {
                return Err(Unshreddable); // trailing bytes after one value
            }
            if lit0 < ev.len() {
                ops.push(OpTmp::Lit(lit0, ev.len()));
            }
            if ops.len() > MAX_OPS {
                return Err(Unshreddable);
            }
            // Signature -> interned skeleton id.
            let mut sig = std::mem::take(&mut self.sig);
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
                    if self.skels.len() >= u16::MAX as usize {
                        self.sig = sig;
                        return Err(Unshreddable);
                    }
                    let id = self.skels.len() as u16;
                    let rops: Vec<ROp> = ops
                        .iter()
                        .map(|op| match *op {
                            OpTmp::Lit(a, b) => {
                                ROp::Lit(ev[a..b].to_vec().into_boxed_slice())
                            }
                            OpTmp::Val(col, kind) => ROp::Val(col, kind),
                        })
                        .collect();
                    self.skels.push(rops);
                    self.skel_lut.insert(sig.clone(), id);
                    id
                }
            };
            self.skel_ids.push(skel_id);
            self.sig = sig;
            Ok(())
        })();
        self.ops = ops;
        outcome
    }

    #[allow(clippy::too_many_arguments)]
    fn parse_val(
        &mut self,
        ev: &[u8],
        p: &mut usize,
        node: u32,
        depth: u32,
        lit0: &mut usize,
        ops: &mut Vec<OpTmp>,
    ) -> Shred<()> {
        if depth > MAX_DEPTH {
            return Err(Unshreddable);
        }
        let m = *ev.get(*p).ok_or(Unshreddable)?;
        match m {
            // ---- maps (keys stay verbatim in the literal run) ----
            0x80..=0x8F => {
                let n = (m & 0x0F) as usize;
                *p += 1;
                self.parse_map(ev, p, node, depth, lit0, ops, n)
            }
            0xDE => {
                let n = be16(ev, *p + 1)? as usize;
                *p += 3;
                self.parse_map(ev, p, node, depth, lit0, ops, n)
            }
            0xDF => {
                let n = be32(ev, *p + 1)? as usize;
                *p += 5;
                self.parse_map(ev, p, node, depth, lit0, ops, n)
            }
            // ---- arrays (headers stay verbatim in the literal run) ----
            0x90..=0x9F => {
                let n = (m & 0x0F) as usize;
                *p += 1;
                self.parse_array(ev, p, node, depth, lit0, ops, n)
            }
            0xDC => {
                let n = be16(ev, *p + 1)? as usize;
                *p += 3;
                self.parse_array(ev, p, node, depth, lit0, ops, n)
            }
            0xDD => {
                let n = be32(ev, *p + 1)? as usize;
                *p += 5;
                self.parse_array(ev, p, node, depth, lit0, ops, n)
            }
            // ---- nil: a zero-data constant, kept verbatim in the literal ----
            0xC0 => {
                *p += 1;
                Ok(())
            }
            // ---- scalars ----
            _ => self.parse_scalar(ev, p, node, lit0, ops, m),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn parse_map(
        &mut self,
        ev: &[u8],
        p: &mut usize,
        node: u32,
        depth: u32,
        lit0: &mut usize,
        ops: &mut Vec<OpTmp>,
        n: usize,
    ) -> Shred<()> {
        if n > MAX_CONTAINER {
            return Err(Unshreddable);
        }
        for _ in 0..n {
            // Key must be a MessagePack string (rmp-serde named mode). It stays
            // in the literal run verbatim, so we only skip past it here.
            let km = *ev.get(*p).ok_or(Unshreddable)?;
            let (klen, khdr) = match km {
                0xA0..=0xBF => ((km & 0x1F) as usize, 1usize),
                0xD9 => (*ev.get(*p + 1).ok_or(Unshreddable)? as usize, 2),
                0xDA => (be16(ev, *p + 1)? as usize, 3),
                0xDB => (be32(ev, *p + 1)? as usize, 5),
                _ => return Err(Unshreddable), // non-string key
            };
            let kstart = *p + khdr;
            let key = ev.get(kstart..kstart + klen).ok_or(Unshreddable)?;
            let child = self.node(node, key)?;
            *p += khdr + klen;
            self.parse_val(ev, p, child, depth + 1, lit0, ops)?;
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn parse_array(
        &mut self,
        ev: &[u8],
        p: &mut usize,
        node: u32,
        depth: u32,
        lit0: &mut usize,
        ops: &mut Vec<OpTmp>,
        n: usize,
    ) -> Shred<()> {
        if n > MAX_CONTAINER {
            return Err(Unshreddable);
        }
        for i in 0..n as u32 {
            let mut tok = [0u8; 5];
            tok[0] = 0xFF;
            tok[1..5].copy_from_slice(&i.to_le_bytes());
            let child = self.node(node, &tok)?;
            self.parse_val(ev, p, child, depth + 1, lit0, ops)?;
        }
        Ok(())
    }

    fn parse_scalar(
        &mut self,
        ev: &[u8],
        p: &mut usize,
        node: u32,
        lit0: &mut usize,
        ops: &mut Vec<OpTmp>,
        m: u8,
    ) -> Shred<()> {
        let v0 = *p;
        enum Sv<'a> {
            I(i64),
            S(&'a [u8]),
            B(bool),
        }
        let (sv, end): (Sv, usize) = match m {
            0x00..=0x7F => (Sv::I(m as i64), v0 + 1),
            0xE0..=0xFF => (Sv::I(m as i8 as i64), v0 + 1),
            0xCC => {
                (Sv::I(*ev.get(v0 + 1).ok_or(Unshreddable)? as i64), v0 + 2)
            }
            0xCD => (Sv::I(be16(ev, v0 + 1)? as i64), v0 + 3),
            0xCE => (Sv::I(be32(ev, v0 + 1)? as i64), v0 + 5),
            0xCF => {
                let u = be64(ev, v0 + 1)?;
                if u > i64::MAX as u64 {
                    return Err(Unshreddable);
                }
                (Sv::I(u as i64), v0 + 9)
            }
            0xD0 => (
                Sv::I(*ev.get(v0 + 1).ok_or(Unshreddable)? as i8 as i64),
                v0 + 2,
            ),
            0xD1 => (Sv::I(be16(ev, v0 + 1)? as i16 as i64), v0 + 3),
            0xD2 => (Sv::I(be32(ev, v0 + 1)? as i32 as i64), v0 + 5),
            0xD3 => (Sv::I(be64(ev, v0 + 1)? as i64), v0 + 9),
            0xA0..=0xBF => {
                let l = (m & 0x1F) as usize;
                (
                    Sv::S(ev.get(v0 + 1..v0 + 1 + l).ok_or(Unshreddable)?),
                    v0 + 1 + l,
                )
            }
            0xD9 => {
                let l = *ev.get(v0 + 1).ok_or(Unshreddable)? as usize;
                (
                    Sv::S(ev.get(v0 + 2..v0 + 2 + l).ok_or(Unshreddable)?),
                    v0 + 2 + l,
                )
            }
            0xDA => {
                let l = be16(ev, v0 + 1)? as usize;
                (
                    Sv::S(ev.get(v0 + 3..v0 + 3 + l).ok_or(Unshreddable)?),
                    v0 + 3 + l,
                )
            }
            0xDB => {
                let l = be32(ev, v0 + 1)? as usize;
                (
                    Sv::S(ev.get(v0 + 5..v0 + 5 + l).ok_or(Unshreddable)?),
                    v0 + 5 + l,
                )
            }
            0xC2 => (Sv::B(false), v0 + 1),
            0xC3 => (Sv::B(true), v0 + 1),
            // floats (0xca/0xcb), bin (0xc4-c6), ext, timestamp, reserved:
            // not modeled -> raw fallback.
            _ => return Err(Unshreddable),
        };
        // Canonicality gate: reassembly re-emits scalar values with minimal
        // encodings. If the original bytes are not minimal, byte-exactness
        // would break, so fall back to raw. (Structural bytes and map keys are
        // stored verbatim and need no such check.)
        let kind = match sv {
            Sv::I(v) => {
                self.canon.clear();
                emit_int(&mut self.canon, v);
                if self.canon.as_slice() != &ev[v0..end] {
                    return Err(Unshreddable);
                }
                K_INT
            }
            Sv::S(s) => {
                let hdr_end = end - s.len();
                self.canon.clear();
                emit_str_header(&mut self.canon, s.len());
                if self.canon.as_slice() != &ev[v0..hdr_end] {
                    return Err(Unshreddable);
                }
                K_STR
            }
            Sv::B(_) => K_BOOL,
        };
        let col = self.col_for(node, kind)?;
        if *lit0 < v0 {
            ops.push(OpTmp::Lit(*lit0, v0));
        }
        ops.push(OpTmp::Val(col, kind));
        match sv {
            Sv::I(v) => self.cols[col as usize].ints.push(v),
            Sv::S(s) => {
                let b = &mut self.cols[col as usize].bytes;
                write_varint(b, s.len() as u64);
                b.extend_from_slice(s);
            }
            Sv::B(x) => self.cols[col as usize].bytes.push(x as u8),
        }
        *lit0 = end;
        *p = end;
        Ok(())
    }

    // -- serialization -------------------------------------------------------

    /// Encode each column's data, choosing the smaller int encoding. Returns
    /// `(kind, enc, data)` per column in directory order.
    fn encode_columns(&self) -> Vec<(u8, u8, Vec<u8>)> {
        self.cols
            .iter()
            .map(|c| match c.kind {
                K_INT => {
                    let raw_len: usize =
                        c.ints.iter().map(|&v| varint_len(zz(v))).sum();
                    let mut prev = 0i64;
                    let mut delta_len = 0usize;
                    for &v in &c.ints {
                        delta_len += varint_len(zz(v.wrapping_sub(prev)));
                        prev = v;
                    }
                    let mut data = Vec::new();
                    if delta_len < raw_len {
                        let mut prev = 0i64;
                        for &v in &c.ints {
                            write_varint(&mut data, zz(v.wrapping_sub(prev)));
                            prev = v;
                        }
                        (K_INT, ENC_DELTA, data)
                    } else {
                        for &v in &c.ints {
                            write_varint(&mut data, zz(v));
                        }
                        (K_INT, ENC_RAW, data)
                    }
                }
                kind => (kind, ENC_RAW, c.bytes.clone()),
            })
            .collect()
    }

    fn write_skeletons(&self, out: &mut Vec<u8>) {
        for sk in &self.skels {
            out.extend_from_slice(&(sk.len() as u16).to_le_bytes());
            for op in sk {
                match op {
                    ROp::Lit(b) => {
                        out.push(0);
                        out.extend_from_slice(&(b.len() as u32).to_le_bytes());
                        out.extend_from_slice(b);
                    }
                    ROp::Val(col, kind) => {
                        out.push(1);
                        out.extend_from_slice(&col.to_le_bytes());
                        out.push(*kind);
                    }
                }
            }
        }
    }

    /// Build the uncompressed columnar image (whole-block payload).
    fn columnar_image(
        &self,
        n_events: usize,
        cols: &[(u8, u8, Vec<u8>)],
    ) -> Vec<u8> {
        let mut img = Vec::new();
        img.extend_from_slice(&(n_events as u16).to_le_bytes());
        img.extend_from_slice(&(self.skels.len() as u16).to_le_bytes());
        img.extend_from_slice(&(cols.len() as u16).to_le_bytes());
        self.write_skeletons(&mut img);
        for &id in &self.skel_ids {
            img.extend_from_slice(&id.to_le_bytes());
        }
        for (kind, enc, data) in cols {
            img.push(*kind);
            img.push(*enc);
            img.extend_from_slice(&(data.len() as u32).to_le_bytes());
        }
        for (_, _, data) in cols {
            img.extend_from_slice(data);
        }
        img
    }

    fn write_whole(&self, n_events: usize, level: i32, out: &mut Vec<u8>) {
        let cols = self.encode_columns();
        let img = self.columnar_image(n_events, &cols);
        let comp = zstd::bulk::compress(&img, level).expect("zstd compress");
        out.push(COLUMNAR_VERSION);
        out.push(FLAG_COLUMNAR);
        out.extend_from_slice(&(img.len() as u32).to_le_bytes());
        out.extend_from_slice(&comp);
    }

    fn write_percol(&self, n_events: usize, level: i32, out: &mut Vec<u8>) {
        let cols = self.encode_columns();
        out.push(COLUMNAR_VERSION);
        out.push(FLAG_COLUMNAR | FLAG_PERCOL);
        out.extend_from_slice(&(n_events as u16).to_le_bytes());
        out.extend_from_slice(&(self.skels.len() as u16).to_le_bytes());
        out.extend_from_slice(&(cols.len() as u16).to_le_bytes());
        self.write_skeletons(out);
        for &id in &self.skel_ids {
            out.extend_from_slice(&id.to_le_bytes());
        }
        let comps: Vec<Vec<u8>> = cols
            .iter()
            .map(|(_, _, data)| {
                zstd::bulk::compress(data, level).expect("zstd compress")
            })
            .collect();
        for ((kind, enc, data), comp) in cols.iter().zip(&comps) {
            out.push(*kind);
            out.push(*enc);
            out.extend_from_slice(&(comp.len() as u32).to_le_bytes());
            out.extend_from_slice(&(data.len() as u32).to_le_bytes());
        }
        for comp in &comps {
            out.extend_from_slice(comp);
        }
    }
}

/// Serialize `events` verbatim as a raw fallback block (byte-exact, lossless).
fn write_raw(events: &[&[u8]], level: i32, out: &mut Vec<u8>) {
    let total: usize = events.iter().map(|e| e.len()).sum();
    let mut img = Vec::with_capacity(2 + 4 * (events.len() + 1) + total);
    img.extend_from_slice(&(events.len() as u16).to_le_bytes());
    let mut off = 0u32;
    img.extend_from_slice(&off.to_le_bytes());
    for e in events {
        off += e.len() as u32;
        img.extend_from_slice(&off.to_le_bytes());
    }
    for e in events {
        img.extend_from_slice(e);
    }
    let comp = zstd::bulk::compress(&img, level).expect("zstd compress");
    out.push(COLUMNAR_VERSION);
    out.push(0); // FLAG_COLUMNAR clear = raw
    out.extend_from_slice(&(img.len() as u32).to_le_bytes());
    out.extend_from_slice(&comp);
}

// ---------------------------------------------------------------------------
// big-endian bounds-checked reads (return Unshreddable on truncation)
// ---------------------------------------------------------------------------

#[inline]
fn be16(d: &[u8], a: usize) -> Shred<u16> {
    let s = d.get(a..a + 2).ok_or(Unshreddable)?;
    Ok(u16::from_be_bytes(s.try_into().unwrap()))
}

#[inline]
fn be32(d: &[u8], a: usize) -> Shred<u32> {
    let s = d.get(a..a + 4).ok_or(Unshreddable)?;
    Ok(u32::from_be_bytes(s.try_into().unwrap()))
}

#[inline]
fn be64(d: &[u8], a: usize) -> Shred<u64> {
    let s = d.get(a..a + 8).ok_or(Unshreddable)?;
    Ok(u64::from_be_bytes(s.try_into().unwrap()))
}

// ---------------------------------------------------------------------------
// Public encoder API
// ---------------------------------------------------------------------------

/// A reusable columnar block encoder. Amortizes the shred scratch across many
/// blocks — the throughput path the sealer (bn-zge) will use.
pub struct BlockEncoder {
    sh:   Shredder,
    opts: EncodeOpts,
}

impl BlockEncoder {
    /// New encoder with the given options.
    pub fn new(opts: EncodeOpts) -> Self {
        BlockEncoder { sh: Shredder::new(), opts }
    }

    /// Encode one block. `events` are the raw payloads in stored order (in
    /// practice one clustered stream run, `<= 128`). Never fails: unshreddable
    /// input produces a byte-exact raw block. Panics only if `events` exceeds
    /// [`MAX_BLOCK_EVENTS`], which the caller controls (blocks are ~128).
    pub fn encode(&mut self, events: &[&[u8]]) -> Vec<u8> {
        assert!(
            events.len() <= MAX_BLOCK_EVENTS,
            "block event count {} exceeds MAX_BLOCK_EVENTS",
            events.len()
        );
        self.sh.reset();
        let mut out = Vec::new();
        match self.sh.shred_all(events) {
            Ok(()) => {
                if self.opts.per_column {
                    self.sh.write_percol(
                        events.len(),
                        self.opts.level,
                        &mut out,
                    );
                } else {
                    self.sh.write_whole(
                        events.len(),
                        self.opts.level,
                        &mut out,
                    );
                }
            }
            Err(Unshreddable) => write_raw(events, self.opts.level, &mut out),
        }
        out
    }
}

/// Encode a single block with a fresh encoder. For many blocks prefer
/// [`BlockEncoder`] to amortize the scratch.
pub fn encode_block(events: &[&[u8]], opts: EncodeOpts) -> Vec<u8> {
    BlockEncoder::new(opts).encode(events)
}
