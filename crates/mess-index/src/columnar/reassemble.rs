//! Reassembly: decode a columnar (or raw) block back to the byte-exact
//! original payloads. Handles the whole-block layout, the per-column
//! point-read layout, and the raw fallback. All reads are bounds-checked and
//! surface [`CodecError`] rather than panicking on corrupt block bytes.

use super::{
    ENC_DELTA, FLAG_COLUMNAR, FLAG_PERCOL, FLAG_RESERVED_MASK, K_BOOL, K_INT, K_STR,
    COLUMNAR_VERSION, emit_int, emit_str, read_varint, unzz,
};

/// Upper bound on a block's decompressed size (guards a hostile `ulen`).
const MAX_ULEN: usize = 64 << 20;

/// Errors decoding or reassembling a columnar block. These indicate corrupt
/// or truncated block bytes (a media-integrity failure), never a shred-time
/// condition — the shredder's fallback is internal and never lossy.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum CodecError {
    /// Frame version byte is not [`COLUMNAR_VERSION`].
    #[error("unsupported columnar block version {0}")]
    UnsupportedVersion(u8),
    /// A reserved FLAGS bit was set.
    #[error("reserved flag bits set: {0:#010b}")]
    ReservedFlags(u8),
    /// Block bytes ended mid-field.
    #[error("truncated columnar block")]
    Truncated,
    /// A skeleton op tag was neither Lit nor Val.
    #[error("malformed skeleton")]
    MalformedSkeleton,
    /// A skeleton referenced a column id past the directory.
    #[error("column id out of range")]
    ColumnOutOfRange,
    /// A directory entry carried an unknown column kind.
    #[error("unknown column kind {0}")]
    UnknownKind(u8),
    /// Row index passed to a point read was past the block's event count.
    #[error("row index out of range")]
    IndexOutOfRange,
    /// zstd rejected the frame (corrupt) or `ulen` was implausibly large.
    #[error("decompression failed")]
    Decompress,
    /// A column ran dry mid-reassembly (corrupt column data).
    #[error("corrupt column data")]
    CorruptColumn,
    /// A per-column block's directory `ulen` fields summed past
    /// [`MAX_ULEN`] — the whole-block/raw layouts cap total decompressed
    /// size via one `ulen` field, but the per-column layout has one `ulen`
    /// per column (up to `n_cols` of them); without this check a block with
    /// many columns, each a tiny (highly compressible, e.g. all-zero) zstd
    /// frame claiming a large `ulen`, could force allocating far more memory
    /// in aggregate than the input bytes justify.
    #[error("per-column block's aggregate decompressed size exceeds the block cap")]
    TotalUlenExceeded,
}

/// A decoded skeleton op. `Lit` ranges point into the block's structural
/// buffer (`buf`); `Val` pulls one value from column `col`.
enum ROp {
    Lit { start: usize, len: usize },
    Val { col: u16, kind: u8 },
}

struct ColMeta {
    kind: u8,
    enc: u8,
    // whole-block: uncompressed data range in `buf`.
    // per-column: compressed data range in `buf` plus decompressed length.
    start: usize,
    len: usize,
    ulen: usize,
}

/// A decoded columnar (or raw fallback) block, ready for byte-exact
/// reassembly. Opaque: interact through [`decode`](Block::decode) and the
/// reassembly methods.
pub struct Block(Inner);

impl std::fmt::Debug for Block {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let layout = match &self.0 {
            Inner::Raw { .. } => "raw",
            Inner::Whole { .. } => "columnar-whole",
            Inner::PerCol { .. } => "columnar-percol",
        };
        f.debug_struct("Block").field("layout", &layout).field("events", &self.len()).finish()
    }
}

enum Inner {
    /// Raw fallback: original payloads stored verbatim.
    Raw {
        /// Row-image buffer (decompressed).
        buf: Vec<u8>,
        /// `n+1` offsets into the payload region.
        offsets: Vec<u32>,
        /// Start of the payload region within `buf`.
        base: usize,
    },
    /// Whole-block columnar: the columnar image is decompressed into `buf`.
    Whole {
        buf: Vec<u8>,
        n: usize,
        skels: Vec<Vec<ROp>>,
        skel_ids: Vec<u16>,
        cols: Vec<ColMeta>,
    },
    /// Per-column columnar: preamble in `buf`; each column zstd frame in `buf`.
    PerCol {
        buf: Vec<u8>,
        n: usize,
        skels: Vec<Vec<ROp>>,
        skel_ids: Vec<u16>,
        cols: Vec<ColMeta>,
    },
}

/// Bounds-checked little-endian cursor over a byte buffer.
struct Rdr<'a> {
    d: &'a [u8],
    p: usize,
}

impl<'a> Rdr<'a> {
    fn new(d: &'a [u8]) -> Self {
        Rdr { d, p: 0 }
    }
    fn u8(&mut self) -> Result<u8, CodecError> {
        let b = *self.d.get(self.p).ok_or(CodecError::Truncated)?;
        self.p += 1;
        Ok(b)
    }
    fn u16(&mut self) -> Result<u16, CodecError> {
        let s = self.d.get(self.p..self.p + 2).ok_or(CodecError::Truncated)?;
        self.p += 2;
        Ok(u16::from_le_bytes(s.try_into().unwrap()))
    }
    fn u32(&mut self) -> Result<u32, CodecError> {
        let s = self.d.get(self.p..self.p + 4).ok_or(CodecError::Truncated)?;
        self.p += 4;
        Ok(u32::from_le_bytes(s.try_into().unwrap()))
    }
    /// Advance over `n` bytes, returning their start offset.
    fn take(&mut self, n: usize) -> Result<usize, CodecError> {
        let start = self.p;
        let end = start.checked_add(n).ok_or(CodecError::Truncated)?;
        if end > self.d.len() {
            return Err(CodecError::Truncated);
        }
        self.p = end;
        Ok(start)
    }
}

fn parse_skeletons(r: &mut Rdr, n_skels: usize) -> Result<Vec<Vec<ROp>>, CodecError> {
    let mut skels = Vec::with_capacity(n_skels);
    for _ in 0..n_skels {
        let nops = r.u16()? as usize;
        let mut ops = Vec::with_capacity(nops);
        for _ in 0..nops {
            match r.u8()? {
                0 => {
                    let len = r.u32()? as usize;
                    let start = r.take(len)?;
                    ops.push(ROp::Lit { start, len });
                }
                1 => {
                    let col = r.u16()?;
                    let kind = r.u8()?;
                    ops.push(ROp::Val { col, kind });
                }
                _ => return Err(CodecError::MalformedSkeleton),
            }
        }
        skels.push(ops);
    }
    Ok(skels)
}

fn parse_skel_ids(r: &mut Rdr, n: usize) -> Result<Vec<u16>, CodecError> {
    let mut ids = Vec::with_capacity(n);
    for _ in 0..n {
        ids.push(r.u16()?);
    }
    Ok(ids)
}

fn decompress(comp: &[u8], ulen: usize) -> Result<Vec<u8>, CodecError> {
    if ulen > MAX_ULEN {
        return Err(CodecError::Decompress);
    }
    zstd::bulk::decompress(comp, ulen).map_err(|_| CodecError::Decompress)
}

impl Block {
    /// Decode a block's frame. Decompresses the whole-block/raw payload up
    /// front; per-column blocks keep their columns compressed until a read
    /// touches them.
    pub fn decode(bytes: &[u8]) -> Result<Block, CodecError> {
        let ver = *bytes.first().ok_or(CodecError::Truncated)?;
        if ver != COLUMNAR_VERSION {
            return Err(CodecError::UnsupportedVersion(ver));
        }
        let flags = *bytes.get(1).ok_or(CodecError::Truncated)?;
        if flags & FLAG_RESERVED_MASK != 0 {
            return Err(CodecError::ReservedFlags(flags));
        }
        let body = &bytes[2..];
        if flags & FLAG_COLUMNAR == 0 {
            return Self::decode_raw(body);
        }
        if flags & FLAG_PERCOL != 0 {
            Self::decode_percol(body)
        } else {
            Self::decode_whole(body)
        }
    }

    fn decode_raw(body: &[u8]) -> Result<Block, CodecError> {
        let mut r = Rdr::new(body);
        let ulen = r.u32()? as usize;
        let buf = decompress(&body[r.p..], ulen)?;
        let mut ri = Rdr::new(&buf);
        let n = ri.u16()? as usize;
        let mut offsets = Vec::with_capacity(n + 1);
        for _ in 0..=n {
            offsets.push(ri.u32()?);
        }
        let base = ri.p;
        // Every offset must be non-decreasing, and the payload region must
        // cover the last one. Both checks are required for memory safety:
        // `reassemble_range` slices `buf[base + offsets[e]..base +
        // offsets[e + 1]]` for every consecutive pair with no further
        // bounds-checking, trusting this decode step. Checking only the
        // *last* offset (as this used to) is not enough — a corrupt table
        // can set some interior offset arbitrarily large (or the sequence
        // non-monotonic) while the last entry still looks in-bounds, and
        // slicing then panics instead of failing loudly.
        if offsets.windows(2).any(|w| w[0] > w[1]) {
            return Err(CodecError::Truncated);
        }
        let last = *offsets.last().unwrap() as usize;
        if base + last > buf.len() {
            return Err(CodecError::Truncated);
        }
        Ok(Block(Inner::Raw { buf, offsets, base }))
    }

    fn decode_whole(body: &[u8]) -> Result<Block, CodecError> {
        let mut r = Rdr::new(body);
        let ulen = r.u32()? as usize;
        let buf = decompress(&body[r.p..], ulen)?;
        let mut ri = Rdr::new(&buf);
        let n = ri.u16()? as usize;
        let n_skels = ri.u16()? as usize;
        let n_cols = ri.u16()? as usize;
        let skels = parse_skeletons(&mut ri, n_skels)?;
        let skel_ids = parse_skel_ids(&mut ri, n)?;
        // directory
        let mut kinds_encs_lens = Vec::with_capacity(n_cols);
        for _ in 0..n_cols {
            let kind = ri.u8()?;
            let enc = ri.u8()?;
            let len = ri.u32()? as usize;
            kinds_encs_lens.push((kind, enc, len));
        }
        // column data, concatenated
        let mut cols = Vec::with_capacity(n_cols);
        for (kind, enc, len) in kinds_encs_lens {
            let start = ri.take(len)?;
            cols.push(ColMeta { kind, enc, start, len, ulen: len });
        }
        // buf lifetime: skels/cols reference `buf` by range; move buf in.
        let block = Block(Inner::Whole { buf, n, skels, skel_ids, cols });
        Ok(block)
    }

    fn decode_percol(body: &[u8]) -> Result<Block, CodecError> {
        // Preamble + directory + compressed columns all live in `body`; hold
        // it as the block's buffer and reference ranges into it.
        let buf = body.to_vec();
        let mut r = Rdr::new(&buf);
        let n = r.u16()? as usize;
        let n_skels = r.u16()? as usize;
        let n_cols = r.u16()? as usize;
        let skels = parse_skeletons(&mut r, n_skels)?;
        let skel_ids = parse_skel_ids(&mut r, n)?;
        let mut dir = Vec::with_capacity(n_cols);
        // Aggregate `ulen` budget across every column (see
        // `CodecError::TotalUlenExceeded`): each column's `ulen` is checked
        // individually against MAX_ULEN by `decompress`, but that alone
        // doesn't bound the *sum* over up to `n_cols` (u16::MAX) columns —
        // cap the running total here, before any column is decompressed.
        let mut total_ulen: usize = 0;
        for _ in 0..n_cols {
            let kind = r.u8()?;
            let enc = r.u8()?;
            let clen = r.u32()? as usize;
            let ulen = r.u32()? as usize;
            total_ulen = total_ulen.checked_add(ulen).ok_or(CodecError::TotalUlenExceeded)?;
            if total_ulen > MAX_ULEN {
                return Err(CodecError::TotalUlenExceeded);
            }
            dir.push((kind, enc, clen, ulen));
        }
        let mut cols = Vec::with_capacity(n_cols);
        for (kind, enc, clen, ulen) in dir {
            let start = r.take(clen)?;
            cols.push(ColMeta { kind, enc, start, len: clen, ulen });
        }
        // `r` (which borrows `buf`) is unused past here, so `buf` may move.
        Ok(Block(Inner::PerCol { buf, n, skels, skel_ids, cols }))
    }

    /// Number of events in the block.
    pub fn len(&self) -> usize {
        match &self.0 {
            Inner::Raw { offsets, .. } => offsets.len() - 1,
            Inner::Whole { n, .. } | Inner::PerCol { n, .. } => *n,
        }
    }

    /// Whether the block holds no events.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Reassemble every event, appending bytes to `out` and `n+1` event
    /// boundaries to `offs`.
    pub fn reassemble_all(
        &self,
        out: &mut Vec<u8>,
        offs: &mut Vec<u32>,
    ) -> Result<(), CodecError> {
        let n = self.len();
        self.reassemble_range(0, n, out, offs)
    }

    /// Reassemble events `[a, b)`, appending bytes to `out` and `b-a+1`
    /// boundaries to `offs`. Byte-exact.
    pub fn reassemble_range(
        &self,
        a: usize,
        b: usize,
        out: &mut Vec<u8>,
        offs: &mut Vec<u32>,
    ) -> Result<(), CodecError> {
        if a > b || b > self.len() {
            return Err(CodecError::IndexOutOfRange);
        }
        match &self.0 {
            Inner::Raw { buf, offsets, base } => {
                for e in a..b {
                    offs.push(out.len() as u32);
                    let o0 = offsets[e] as usize;
                    let o1 = offsets[e + 1] as usize;
                    out.extend_from_slice(&buf[base + o0..base + o1]);
                }
                offs.push(out.len() as u32);
                Ok(())
            }
            Inner::Whole { buf, skels, skel_ids, cols, .. } => {
                let refs: Vec<ColRef> = cols
                    .iter()
                    .map(|c| ColRef { kind: c.kind, enc: c.enc, data: &buf[c.start..c.start + c.len] })
                    .collect();
                reassemble_core(buf, skels, skel_ids, &refs, a, b, out, offs)
            }
            Inner::PerCol { buf, skels, skel_ids, cols, .. } => {
                // Range reassembly needs every column referenced up to `b`;
                // decompress all columns.
                let owned: Vec<Vec<u8>> = cols
                    .iter()
                    .map(|c| decompress(&buf[c.start..c.start + c.len], c.ulen))
                    .collect::<Result<_, _>>()?;
                let refs: Vec<ColRef> = cols
                    .iter()
                    .zip(&owned)
                    .map(|(c, d)| ColRef { kind: c.kind, enc: c.enc, data: d })
                    .collect();
                reassemble_core(buf, skels, skel_ids, &refs, a, b, out, offs)
            }
        }
    }

    /// Reassemble a single event (point read), returning its exact bytes.
    ///
    /// For the per-column layout this decompresses **only** the columns the
    /// row's skeleton references, leaving unrelated columns untouched — the
    /// selective-decode win. The whole-block and raw layouts decompress once
    /// at [`decode`](Block::decode); this then reassembles the one row.
    pub fn reassemble_one(&self, idx: usize) -> Result<Vec<u8>, CodecError> {
        if idx >= self.len() {
            return Err(CodecError::IndexOutOfRange);
        }
        match &self.0 {
            Inner::Raw { buf, offsets, base } => {
                let o0 = offsets[idx] as usize;
                let o1 = offsets[idx + 1] as usize;
                Ok(buf[base + o0..base + o1].to_vec())
            }
            Inner::Whole { .. } => {
                let mut out = Vec::new();
                let mut offs = Vec::new();
                self.reassemble_range(idx, idx + 1, &mut out, &mut offs)?;
                Ok(out)
            }
            Inner::PerCol { buf, skels, skel_ids, cols, .. } => {
                self.point_read_percol(buf, skels, skel_ids, cols, idx)
            }
        }
    }

    /// Selective per-column point read: decompress only referenced columns and
    /// position each at the target row's element via a single count pass.
    fn point_read_percol(
        &self,
        buf: &[u8],
        skels: &[Vec<ROp>],
        skel_ids: &[u16],
        cols: &[ColMeta],
        idx: usize,
    ) -> Result<Vec<u8>, CodecError> {
        let sk = *skel_ids.get(idx).ok_or(CodecError::IndexOutOfRange)? as usize;
        let ops = skels.get(sk).ok_or(CodecError::MalformedSkeleton)?;
        // Which columns does this row touch?
        let mut referenced = vec![false; cols.len()];
        for op in ops {
            if let ROp::Val { col, .. } = op {
                let c = *col as usize;
                *referenced.get_mut(c).ok_or(CodecError::ColumnOutOfRange)? = true;
            }
        }
        // Count elements each referenced column contributes before row `idx`
        // (a single pass over the prior skeleton ids).
        let mut skip = vec![0u32; cols.len()];
        for &id in &skel_ids[..idx] {
            let prev_ops = skels.get(id as usize).ok_or(CodecError::MalformedSkeleton)?;
            for op in prev_ops {
                if let ROp::Val { col, .. } = op {
                    let c = *col as usize;
                    if referenced[c] {
                        skip[c] += 1;
                    }
                }
            }
        }
        // Decompress only referenced columns and advance each to its element.
        let mut decoded: Vec<Option<Vec<u8>>> = vec![None; cols.len()];
        let mut curs: Vec<Option<Cur>> = Vec::with_capacity(cols.len());
        for _ in 0..cols.len() {
            curs.push(None);
        }
        for (c, meta) in cols.iter().enumerate() {
            if !referenced[c] {
                continue;
            }
            let data = decompress(&buf[meta.start..meta.start + meta.len], meta.ulen)?;
            decoded[c] = Some(data);
        }
        for (c, meta) in cols.iter().enumerate() {
            if !referenced[c] {
                continue;
            }
            let data = decoded[c].as_deref().unwrap();
            let mut cur = Cur { kind: meta.kind, enc: meta.enc, data, pos: 0, prev: 0 };
            for _ in 0..skip[c] {
                cur.next()?; // advance past prior rows' elements
            }
            curs[c] = Some(cur);
        }
        // Emit the row.
        let mut out = Vec::new();
        for op in ops {
            match op {
                ROp::Lit { start, len } => {
                    out.extend_from_slice(
                        buf.get(*start..*start + *len).ok_or(CodecError::Truncated)?,
                    );
                }
                ROp::Val { col, kind } => {
                    let cur = curs
                        .get_mut(*col as usize)
                        .and_then(|c| c.as_mut())
                        .ok_or(CodecError::ColumnOutOfRange)?;
                    let sv = cur.next()?;
                    emit_scalar(&mut out, *kind, sv)?;
                }
            }
        }
        Ok(out)
    }
}

/// A column's kind/encoding and its decompressed data slice.
struct ColRef<'a> {
    kind: u8,
    enc: u8,
    data: &'a [u8],
}

/// A per-column decode cursor.
struct Cur<'a> {
    kind: u8,
    enc: u8,
    data: &'a [u8],
    pos: usize,
    prev: i64,
}

enum Scalar<'a> {
    I(i64),
    S(&'a [u8]),
    B(bool),
}

impl<'a> Cur<'a> {
    fn next(&mut self) -> Result<Scalar<'a>, CodecError> {
        match self.kind {
            K_INT => {
                let raw = read_varint(self.data, &mut self.pos).ok_or(CodecError::CorruptColumn)?;
                let d = unzz(raw);
                let v = if self.enc == ENC_DELTA {
                    self.prev = self.prev.wrapping_add(d);
                    self.prev
                } else {
                    d
                };
                Ok(Scalar::I(v))
            }
            K_STR => {
                let l = read_varint(self.data, &mut self.pos).ok_or(CodecError::CorruptColumn)?
                    as usize;
                let s = self
                    .data
                    .get(self.pos..self.pos + l)
                    .ok_or(CodecError::CorruptColumn)?;
                self.pos += l;
                Ok(Scalar::S(s))
            }
            K_BOOL => {
                let b = *self.data.get(self.pos).ok_or(CodecError::CorruptColumn)?;
                self.pos += 1;
                Ok(Scalar::B(b != 0))
            }
            k => Err(CodecError::UnknownKind(k)),
        }
    }
}

fn emit_scalar(out: &mut Vec<u8>, kind: u8, sv: Scalar) -> Result<(), CodecError> {
    match (kind, sv) {
        (K_INT, Scalar::I(v)) => emit_int(out, v),
        (K_STR, Scalar::S(s)) => emit_str(out, s),
        (K_BOOL, Scalar::B(b)) => out.push(if b { 0xc3 } else { 0xc2 }),
        _ => return Err(CodecError::CorruptColumn),
    }
    Ok(())
}

/// Core columnar reassembly over already-materialized column data. Walks
/// events `[0, b)` advancing cursors, emitting only `[a, b)`.
#[allow(clippy::too_many_arguments)]
fn reassemble_core(
    buf: &[u8],
    skels: &[Vec<ROp>],
    skel_ids: &[u16],
    cols: &[ColRef],
    a: usize,
    b: usize,
    out: &mut Vec<u8>,
    offs: &mut Vec<u32>,
) -> Result<(), CodecError> {
    let mut curs: Vec<Cur> = cols
        .iter()
        .map(|c| Cur { kind: c.kind, enc: c.enc, data: c.data, pos: 0, prev: 0 })
        .collect();
    for e in 0..b {
        let emit = e >= a;
        if emit {
            offs.push(out.len() as u32);
        }
        let sk = *skel_ids.get(e).ok_or(CodecError::IndexOutOfRange)? as usize;
        let ops = skels.get(sk).ok_or(CodecError::MalformedSkeleton)?;
        for op in ops {
            match op {
                ROp::Lit { start, len } => {
                    if emit {
                        out.extend_from_slice(
                            buf.get(*start..*start + *len).ok_or(CodecError::Truncated)?,
                        );
                    }
                }
                ROp::Val { col, kind } => {
                    let cur = curs.get_mut(*col as usize).ok_or(CodecError::ColumnOutOfRange)?;
                    let sv = cur.next()?;
                    if emit {
                        emit_scalar(out, *kind, sv)?;
                    }
                }
            }
        }
    }
    offs.push(out.len() as u32);
    Ok(())
}
