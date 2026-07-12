//! Merkle-page kernel checkpoints (design §10).
//!
//! State is partitioned into fixed logical pages (4096 head cells / 4096
//! snapshot cells per page) plus small component blobs (registry, frontiers,
//! allocators, dedupe epochs), all content-addressed:
//! `page_hash = BLAKE3(kind || logical_page_id || canonical_page_bytes)`.
//! A manifest names the commit cursor, the log-prefix anchor, and the page
//! table, framed with CRC32C. Install follows §10.3 exactly:
//! temp write → hash verify → fsync → rename → dir fsync → manifest →
//! rename → dir fsync → alternating `current.a`/`current.b` pointer →
//! dir fsync. Open follows §10.4: highest manifest whose CRC, pages,
//! cursor, and anchor all validate; otherwise try older; otherwise none.
//!
//! I/O goes through the [`Dir`] seam: [`MemDir`] models durability
//! (volatile vs fsynced content, volatile vs fsynced directory entries,
//! torn unsynced files) with step-indexed fault injection so the install
//! and GC protocols can be crashed at EVERY step; [`FsDir`] is the real
//! thing for perf runs.

use std::collections::VecDeque;
use std::io;

use hashbrown::{HashMap, HashSet};

use crate::PAGE_CELLS;
use crate::codec::{get_varint, put_varint};
use crate::kernel::{FrozenEpoch, KernelState};
use crate::model::Cursor;

pub const KIND_HEADS: u8 = 0;
pub const KIND_SNAPSHOTS: u8 = 1;
pub const KIND_REGISTRY: u8 = 2;
pub const KIND_FRONTIERS: u8 = 3;
pub const KIND_ALLOC: u8 = 4;
pub const KIND_DEDUPE: u8 = 5;

// ---------------------------------------------------------------------------
// Dir seam
// ---------------------------------------------------------------------------

/// Flat-namespace directory with explicit durability points.
pub trait Dir {
    fn write_file(&mut self, name: &str, bytes: &[u8]) -> io::Result<()>;
    fn fsync_file(&mut self, name: &str) -> io::Result<()>;
    fn rename(&mut self, from: &str, to: &str) -> io::Result<()>;
    fn fsync_dir(&mut self) -> io::Result<()>;
    fn read(&self, name: &str) -> Option<Vec<u8>>;
    fn exists(&self, name: &str) -> bool {
        self.read(name).is_some()
    }
    fn list(&self) -> Vec<String>;
    fn remove(&mut self, name: &str) -> io::Result<()>;
}

#[derive(Clone, Debug, Default)]
struct Inode {
    /// Bytes guaranteed to survive a crash (`fsync_file` happened).
    durable: Option<Vec<u8>>,
    volatile: Vec<u8>,
}

/// Crash-modeling in-memory directory with step-indexed fault injection.
///
/// Mutating operations count steps; when the counter reaches `fail_at` the
/// operation (and every later one) fails. [`MemDir::crash`] then discards
/// everything volatile: un-fsynced directory entries vanish, un-fsynced
/// file content is torn to a half-length prefix.
#[derive(Clone, Debug, Default)]
pub struct MemDir {
    inodes: HashMap<u64, Inode>,
    next_inode: u64,
    entries: HashMap<String, u64>,
    entries_durable: HashMap<String, u64>,
    pub ops: u64,
    pub fail_at: Option<u64>,
    failed: bool,
}

impl MemDir {
    pub fn new() -> Self {
        Self::default()
    }

    fn step(&mut self) -> io::Result<()> {
        if self.failed {
            return Err(io::Error::other("injected fault (post)"));
        }
        self.ops += 1;
        if Some(self.ops) == self.fail_at {
            self.failed = true;
            return Err(io::Error::other("injected fault"));
        }
        Ok(())
    }

    /// Power loss: keep only durable entries and durable content; tear
    /// un-fsynced content to a half prefix.
    pub fn crash(&mut self) {
        self.entries = self.entries_durable.clone();
        let live: HashSet<u64> = self.entries.values().copied().collect();
        self.inodes.retain(|id, _| live.contains(id));
        for ino in self.inodes.values_mut() {
            let content = match &ino.durable {
                Some(d) => d.clone(),
                None => {
                    let half = ino.volatile.len() / 2;
                    ino.volatile[..half].to_vec()
                }
            };
            ino.durable = Some(content.clone());
            ino.volatile = content;
        }
        self.failed = false;
        self.fail_at = None;
    }
}

impl Dir for MemDir {
    fn write_file(&mut self, name: &str, bytes: &[u8]) -> io::Result<()> {
        self.step()?;
        let id = self.next_inode;
        self.next_inode += 1;
        self.inodes
            .insert(id, Inode { durable: None, volatile: bytes.to_vec() });
        self.entries.insert(name.to_string(), id);
        Ok(())
    }

    fn fsync_file(&mut self, name: &str) -> io::Result<()> {
        self.step()?;
        let id = *self
            .entries
            .get(name)
            .ok_or_else(|| io::Error::other("fsync missing file"))?;
        let ino = self.inodes.get_mut(&id).unwrap();
        ino.durable = Some(ino.volatile.clone());
        Ok(())
    }

    fn rename(&mut self, from: &str, to: &str) -> io::Result<()> {
        self.step()?;
        let id = self
            .entries
            .remove(from)
            .ok_or_else(|| io::Error::other("rename missing file"))?;
        self.entries.insert(to.to_string(), id);
        Ok(())
    }

    fn fsync_dir(&mut self) -> io::Result<()> {
        self.step()?;
        self.entries_durable = self.entries.clone();
        Ok(())
    }

    fn read(&self, name: &str) -> Option<Vec<u8>> {
        let id = self.entries.get(name)?;
        Some(self.inodes.get(id)?.volatile.clone())
    }

    fn list(&self) -> Vec<String> {
        let mut v: Vec<String> = self.entries.keys().cloned().collect();
        v.sort();
        v
    }

    fn remove(&mut self, name: &str) -> io::Result<()> {
        self.step()?;
        self.entries.remove(name);
        Ok(())
    }
}

/// Real directory for perf runs. Same protocol, real fsyncs.
pub struct FsDir {
    root: std::path::PathBuf,
}

impl FsDir {
    pub fn new(root: std::path::PathBuf) -> io::Result<Self> {
        std::fs::create_dir_all(&root)?;
        Ok(Self { root })
    }
    fn p(&self, name: &str) -> std::path::PathBuf {
        self.root.join(name)
    }
}

impl Dir for FsDir {
    fn write_file(&mut self, name: &str, bytes: &[u8]) -> io::Result<()> {
        std::fs::write(self.p(name), bytes)
    }
    fn fsync_file(&mut self, name: &str) -> io::Result<()> {
        std::fs::File::open(self.p(name))?.sync_all()
    }
    fn rename(&mut self, from: &str, to: &str) -> io::Result<()> {
        std::fs::rename(self.p(from), self.p(to))
    }
    fn fsync_dir(&mut self) -> io::Result<()> {
        std::fs::File::open(&self.root)?.sync_all()
    }
    fn read(&self, name: &str) -> Option<Vec<u8>> {
        std::fs::read(self.p(name)).ok()
    }
    fn list(&self) -> Vec<String> {
        let mut v = Vec::new();
        if let Ok(rd) = std::fs::read_dir(&self.root) {
            for e in rd.flatten() {
                if let Some(n) = e.file_name().to_str() {
                    v.push(n.to_string());
                }
            }
        }
        v.sort();
        v
    }
    fn remove(&mut self, name: &str) -> io::Result<()> {
        std::fs::remove_file(self.p(name))
    }
}

// ---------------------------------------------------------------------------
// Pages
// ---------------------------------------------------------------------------

pub fn page_hash(kind: u8, page_id: u64, bytes: &[u8]) -> [u8; 32] {
    let mut h = blake3::Hasher::new();
    h.update(&[kind]);
    h.update(&page_id.to_le_bytes());
    h.update(bytes);
    *h.finalize().as_bytes()
}

fn page_name(hash: &[u8; 32]) -> String {
    let mut s = String::with_capacity(75);
    s.push_str("page-");
    for b in hash {
        s.push_str(&format!("{b:02x}"));
    }
    s.push_str(".page");
    s
}

fn head_page_bytes(state: &KernelState, page_id: u64) -> Vec<u8> {
    let mut out = vec![0u8; (PAGE_CELLS * 8) as usize];
    let base = (page_id * PAGE_CELLS) as usize;
    for i in 0..PAGE_CELLS as usize {
        let v = state.heads.get(base + i).copied().unwrap_or(0);
        out[i * 8..i * 8 + 8].copy_from_slice(&v.to_le_bytes());
    }
    out
}

fn snap_page_bytes(state: &KernelState, page_id: u64) -> Vec<u8> {
    let mut out = vec![0u8; (PAGE_CELLS * 16) as usize];
    let base = (page_id * PAGE_CELLS) as usize;
    for i in 0..PAGE_CELLS as usize {
        let (v, r) = state.snapshots.get(base + i).copied().unwrap_or((0, 0));
        out[i * 16..i * 16 + 8].copy_from_slice(&v.to_le_bytes());
        out[i * 16 + 8..i * 16 + 16].copy_from_slice(&r.to_le_bytes());
    }
    out
}

fn registry_blob(state: &KernelState) -> Vec<u8> {
    let mut reg: Vec<(u64, u64)> =
        state.registry.iter().map(|(&n, &i)| (n, i)).collect();
    reg.sort_unstable();
    let mut out = Vec::new();
    put_varint(&mut out, reg.len() as u64);
    for (n, i) in reg {
        put_varint(&mut out, n);
        put_varint(&mut out, i);
    }
    out
}

fn frontier_blob(state: &KernelState) -> Vec<u8> {
    let mut fr: Vec<((u32, u32), u64)> =
        state.frontiers.iter().map(|(&k, &v)| (k, v)).collect();
    fr.sort_unstable();
    let mut out = Vec::new();
    put_varint(&mut out, fr.len() as u64);
    for ((p, s), pos) in fr {
        put_varint(&mut out, u64::from(p));
        put_varint(&mut out, u64::from(s));
        put_varint(&mut out, pos);
    }
    out
}

fn alloc_blob(state: &KernelState) -> Vec<u8> {
    let mut al: Vec<(u32, u64)> =
        state.alloc.iter().map(|(&s, &v)| (s, v)).collect();
    al.sort_unstable();
    let mut out = Vec::new();
    put_varint(&mut out, al.len() as u64);
    for (s, v) in al {
        put_varint(&mut out, u64::from(s));
        put_varint(&mut out, v);
    }
    out
}

fn dedupe_blob(state: &KernelState) -> Vec<u8> {
    // Frozen epochs in deque order, plus the active set as a final epoch.
    let mut out = Vec::new();
    let extra = usize::from(!state.active.is_empty());
    put_varint(&mut out, (state.epochs.len() + extra) as u64);
    let enc = |out: &mut Vec<u8>, max_pos: u64, entries: &[(u64, u64)]| {
        put_varint(out, max_pos);
        put_varint(out, entries.len() as u64);
        for &(f, p) in entries {
            out.extend_from_slice(&f.to_le_bytes());
            put_varint(out, p);
        }
    };
    for e in &state.epochs {
        enc(&mut out, e.max_pos, &e.entries);
    }
    if extra == 1 {
        let mut act = state.active.clone();
        act.sort_unstable();
        let max_pos = act.iter().map(|&(_, p)| p).max().unwrap_or(0);
        enc(&mut out, max_pos, &act);
    }
    out
}

fn decode_dedupe_blob(bytes: &[u8]) -> Option<VecDeque<FrozenEpoch>> {
    let mut at = 0usize;
    let (n, k) = get_varint(bytes, at)?;
    at += k;
    let mut out = VecDeque::with_capacity(n as usize);
    for _ in 0..n {
        let (max_pos, k) = get_varint(bytes, at)?;
        at += k;
        let (m, k) = get_varint(bytes, at)?;
        at += k;
        let mut entries = Vec::with_capacity(m as usize);
        for _ in 0..m {
            if bytes.len() < at + 8 {
                return None;
            }
            let f = u64::from_le_bytes(bytes[at..at + 8].try_into().unwrap());
            at += 8;
            let (p, k) = get_varint(bytes, at)?;
            at += k;
            entries.push((f, p));
        }
        out.push_back(FrozenEpoch { max_pos, entries });
    }
    if at != bytes.len() {
        return None;
    }
    Some(out)
}

// ---------------------------------------------------------------------------
// Manifest
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Manifest {
    pub cursor: Cursor,
    pub anchor: [u8; 32],
    pub dedupe_span: u64,
    pub epoch_span: u64,
    /// `(kind, logical page id, page hash)`, sorted.
    pub pages: Vec<(u8, u64, [u8; 32])>,
}

const MMAGIC: &[u8; 4] = b"SMF1";

impl Manifest {
    pub fn name(&self) -> String {
        format!("manifest-{:020}.ckpt", self.cursor.idx)
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(MMAGIC);
        put_varint(&mut out, self.cursor.idx);
        put_varint(&mut out, self.cursor.pos);
        out.extend_from_slice(&self.anchor);
        put_varint(&mut out, self.dedupe_span);
        put_varint(&mut out, self.epoch_span);
        put_varint(&mut out, self.pages.len() as u64);
        for &(kind, id, hash) in &self.pages {
            out.push(kind);
            put_varint(&mut out, id);
            out.extend_from_slice(&hash);
        }
        let crc = crc32c::crc32c(&out);
        out.extend_from_slice(&crc.to_le_bytes());
        out
    }

    pub fn decode(bytes: &[u8]) -> Option<Manifest> {
        if bytes.len() < 8 {
            return None;
        }
        let (body, crc_bytes) = bytes.split_at(bytes.len() - 4);
        let want = u32::from_le_bytes(crc_bytes.try_into().ok()?);
        if crc32c::crc32c(body) != want {
            return None;
        }
        if &body[..4] != MMAGIC {
            return None;
        }
        let mut at = 4usize;
        let v = |at: &mut usize| -> Option<u64> {
            let (x, n) = get_varint(body, *at)?;
            *at += n;
            Some(x)
        };
        let cursor = Cursor { idx: v(&mut at)?, pos: v(&mut at)? };
        if body.len() < at + 32 {
            return None;
        }
        let mut anchor = [0u8; 32];
        anchor.copy_from_slice(&body[at..at + 32]);
        at += 32;
        let dedupe_span = v(&mut at)?;
        let epoch_span = v(&mut at)?;
        let n = v(&mut at)?;
        let mut pages = Vec::with_capacity(n as usize);
        for _ in 0..n {
            if body.len() < at + 1 {
                return None;
            }
            let kind = body[at];
            at += 1;
            let id = v(&mut at)?;
            if body.len() < at + 32 {
                return None;
            }
            let mut h = [0u8; 32];
            h.copy_from_slice(&body[at..at + 32]);
            at += 32;
            pages.push((kind, id, h));
        }
        if at != body.len() {
            return None;
        }
        Some(Manifest { cursor, anchor, dedupe_span, epoch_span, pages })
    }
}

// ---------------------------------------------------------------------------
// Install (design §10.3)
// ---------------------------------------------------------------------------

/// Outcome of an install: the manifest plus how many blob bytes were
/// actually written (the dirty-page proportionality measurement).
pub struct Installed {
    pub manifest: Manifest,
    pub blob_bytes_written: u64,
    pub blobs_written: u64,
}

/// Compute the full page table for `state`, reusing hashes from `prev` for
/// pages the kernel did not dirty (incremental checkpointing, §10.5).
/// `prev = None` (or a fully dirty state) is a full checkpoint.
fn page_table(
    state: &KernelState,
    prev: Option<&Manifest>,
) -> Vec<(u8, u64, Option<Vec<u8>>, [u8; 32])> {
    let prev_hash = |kind: u8, id: u64| -> Option<[u8; 32]> {
        prev?.pages.iter().find(|&&(k, i, _)| k == kind && i == id).map(|&(_, _, h)| h)
    };
    let mut out: Vec<(u8, u64, Option<Vec<u8>>, [u8; 32])> = Vec::new();
    // Heads pages.
    let head_pages = state.heads.len().div_ceil(PAGE_CELLS as usize) as u64;
    for pid in 0..head_pages {
        let clean = !state.dirty_head_pages.contains(&pid);
        if clean && let Some(h) = prev_hash(KIND_HEADS, pid) {
            out.push((KIND_HEADS, pid, None, h));
            continue;
        }
        let bytes = head_page_bytes(state, pid);
        if bytes.iter().all(|&b| b == 0) {
            continue; // all-absent page: not stored
        }
        let h = page_hash(KIND_HEADS, pid, &bytes);
        out.push((KIND_HEADS, pid, Some(bytes), h));
    }
    // Snapshot pages.
    let snap_pages = state.snapshots.len().div_ceil(PAGE_CELLS as usize) as u64;
    for pid in 0..snap_pages {
        let clean = !state.dirty_snap_pages.contains(&pid);
        if clean && let Some(h) = prev_hash(KIND_SNAPSHOTS, pid) {
            out.push((KIND_SNAPSHOTS, pid, None, h));
            continue;
        }
        let bytes = snap_page_bytes(state, pid);
        if bytes.iter().all(|&b| b == 0) {
            continue;
        }
        let h = page_hash(KIND_SNAPSHOTS, pid, &bytes);
        out.push((KIND_SNAPSHOTS, pid, Some(bytes), h));
    }
    // Component blobs.
    let blobs: [(u8, bool, fn(&KernelState) -> Vec<u8>); 4] = [
        (KIND_REGISTRY, state.registry_dirty, registry_blob),
        (KIND_FRONTIERS, state.frontier_dirty, frontier_blob),
        (KIND_ALLOC, state.alloc_dirty, alloc_blob),
        (KIND_DEDUPE, state.dedupe_dirty, dedupe_blob),
    ];
    for (kind, dirty, f) in blobs {
        if !dirty && let Some(h) = prev_hash(kind, 0) {
            out.push((kind, 0, None, h));
            continue;
        }
        let bytes = f(state);
        let h = page_hash(kind, 0, &bytes);
        out.push((kind, 0, Some(bytes), h));
    }
    out.sort_by_key(|&(k, i, _, _)| (k, i));
    out
}

/// Install a checkpoint of `state` into `dir` following §10.3. Steps, in
/// order, each an injectable I/O op on the [`Dir`] seam:
/// per missing blob: write temp → (hash verify) → fsync temp → rename;
/// then dir fsync; manifest temp → fsync → rename → dir fsync; pointer
/// temp → fsync → rename (alternating a/b) → dir fsync.
pub fn install<D: Dir>(
    dir: &mut D,
    state: &KernelState,
    prev: Option<&Manifest>,
) -> io::Result<Installed> {
    let table = page_table(state, prev);
    let mut blob_bytes_written = 0u64;
    let mut blobs_written = 0u64;
    let mut renamed_any = false;
    for (kind, id, bytes, hash) in &table {
        let name = page_name(hash);
        if dir.exists(&name) {
            continue; // content-addressed reuse
        }
        let bytes = match bytes {
            Some(b) => b.clone(),
            // Clean-by-dirty-tracking but blob missing (e.g. GC raced or
            // first install into an empty dir): serialize it after all.
            None => match *kind {
                KIND_HEADS => head_page_bytes(state, *id),
                KIND_SNAPSHOTS => snap_page_bytes(state, *id),
                KIND_REGISTRY => registry_blob(state),
                KIND_FRONTIERS => frontier_blob(state),
                KIND_ALLOC => alloc_blob(state),
                KIND_DEDUPE => dedupe_blob(state),
                _ => unreachable!(),
            },
        };
        // §10.3 step 2: verify hashes before making them durable.
        assert_eq!(page_hash(*kind, *id, &bytes), *hash, "page hash verify");
        let tmp = format!("tmp-{name}");
        dir.write_file(&tmp, &bytes)?;
        dir.fsync_file(&tmp)?;
        dir.rename(&tmp, &name)?;
        renamed_any = true;
        blob_bytes_written += bytes.len() as u64;
        blobs_written += 1;
    }
    if renamed_any {
        dir.fsync_dir()?;
    }
    let manifest = Manifest {
        cursor: state.cursor,
        anchor: state.anchor,
        dedupe_span: state.dedupe_span,
        epoch_span: state.epoch_span,
        pages: table.iter().map(|&(k, i, _, h)| (k, i, h)).collect(),
    };
    let mbytes = manifest.encode();
    blob_bytes_written += mbytes.len() as u64;
    let mname = manifest.name();
    let tmp = format!("tmp-{mname}");
    dir.write_file(&tmp, &mbytes)?;
    dir.fsync_file(&tmp)?;
    dir.rename(&tmp, &mname)?;
    dir.fsync_dir()?;
    // Alternating advisory pointer.
    let slot = if state.cursor.idx % 2 == 0 { "current.a" } else { "current.b" };
    let mut pbytes = Vec::new();
    put_varint(&mut pbytes, mname.len() as u64);
    pbytes.extend_from_slice(mname.as_bytes());
    let crc = crc32c::crc32c(&pbytes);
    pbytes.extend_from_slice(&crc.to_le_bytes());
    let tmp = format!("tmp-{slot}");
    dir.write_file(&tmp, &pbytes)?;
    dir.fsync_file(&tmp)?;
    dir.rename(&tmp, slot)?;
    dir.fsync_dir()?;
    Ok(Installed { manifest, blob_bytes_written, blobs_written })
}

// ---------------------------------------------------------------------------
// Open + anchor validation (design §10.4)
// ---------------------------------------------------------------------------

/// What the log can answer about its recovered end and sealed boundaries.
pub trait AnchorSource {
    fn end_idx(&self) -> u64;
    /// `(footer anchor, cursor)` if `idx` is a sealed boundary (or 0).
    fn boundary(&self, idx: u64) -> Option<([u8; 32], Cursor)>;
}

impl AnchorSource for crate::model::Log {
    fn end_idx(&self) -> u64 {
        self.end_cursor().idx
    }
    fn boundary(&self, idx: u64) -> Option<([u8; 32], Cursor)> {
        self.boundary_anchor(idx)
    }
}

/// Validate one manifest per §10.4 and reconstruct the kernel state from
/// its pages. Any failure → `None` (caller tries an older manifest).
pub fn load_manifest<D: Dir>(
    dir: &D,
    m: &Manifest,
    log: &impl AnchorSource,
) -> Option<KernelState> {
    // Cursor within the recovered log, anchored at a sealed boundary that
    // matches byte-for-byte.
    if m.cursor.idx > log.end_idx() {
        return None;
    }
    let (anchor, cur) = log.boundary(m.cursor.idx)?;
    if anchor != m.anchor || cur != m.cursor {
        return None;
    }
    // Every referenced page must exist and hash-validate.
    let mut state = KernelState::new(m.dedupe_span, m.epoch_span);
    state.cursor = m.cursor;
    state.anchor = m.anchor;
    for &(kind, id, hash) in &m.pages {
        let bytes = dir.read(&page_name(&hash))?;
        if page_hash(kind, id, &bytes) != hash {
            return None;
        }
        match kind {
            KIND_HEADS => {
                if bytes.len() != (PAGE_CELLS * 8) as usize {
                    return None;
                }
                let base = (id * PAGE_CELLS) as usize;
                if state.heads.len() < base + PAGE_CELLS as usize {
                    state.heads.resize(base + PAGE_CELLS as usize, 0);
                }
                for i in 0..PAGE_CELLS as usize {
                    state.heads[base + i] = u64::from_le_bytes(
                        bytes[i * 8..i * 8 + 8].try_into().unwrap(),
                    );
                }
            }
            KIND_SNAPSHOTS => {
                if bytes.len() != (PAGE_CELLS * 16) as usize {
                    return None;
                }
                let base = (id * PAGE_CELLS) as usize;
                if state.snapshots.len() < base + PAGE_CELLS as usize {
                    state.snapshots.resize(base + PAGE_CELLS as usize, (0, 0));
                }
                for i in 0..PAGE_CELLS as usize {
                    let v = u64::from_le_bytes(
                        bytes[i * 16..i * 16 + 8].try_into().unwrap(),
                    );
                    let r = u64::from_le_bytes(
                        bytes[i * 16 + 8..i * 16 + 16].try_into().unwrap(),
                    );
                    state.snapshots[base + i] = (v, r);
                }
            }
            KIND_REGISTRY => {
                let mut at = 0usize;
                let (n, k) = get_varint(&bytes, at)?;
                at += k;
                for _ in 0..n {
                    let (name, k) = get_varint(&bytes, at)?;
                    at += k;
                    let (id, k) = get_varint(&bytes, at)?;
                    at += k;
                    state.registry.insert(name, id);
                    state.registry_rev.insert(id, name);
                }
                if at != bytes.len() {
                    return None;
                }
            }
            KIND_FRONTIERS => {
                let mut at = 0usize;
                let (n, k) = get_varint(&bytes, at)?;
                at += k;
                for _ in 0..n {
                    let (p, k) = get_varint(&bytes, at)?;
                    at += k;
                    let (s, k) = get_varint(&bytes, at)?;
                    at += k;
                    let (pos, k) = get_varint(&bytes, at)?;
                    at += k;
                    state.frontiers.insert((p as u32, s as u32), pos);
                }
                if at != bytes.len() {
                    return None;
                }
            }
            KIND_ALLOC => {
                let mut at = 0usize;
                let (n, k) = get_varint(&bytes, at)?;
                at += k;
                for _ in 0..n {
                    let (s, k) = get_varint(&bytes, at)?;
                    at += k;
                    let (v, k) = get_varint(&bytes, at)?;
                    at += k;
                    state.alloc.insert(s as u32, v);
                }
                if at != bytes.len() {
                    return None;
                }
            }
            KIND_DEDUPE => {
                state.epochs = decode_dedupe_blob(&bytes)?;
            }
            _ => return None,
        }
    }
    Some(state)
}

/// §10.4 open: choose the highest fully-valid manifest, preferring the
/// advisory pointers but falling back to a directory listing.
pub fn open<D: Dir>(dir: &D, log: &impl AnchorSource) -> Option<(KernelState, Manifest)> {
    let mut names: Vec<String> = Vec::new();
    for slot in ["current.a", "current.b"] {
        if let Some(bytes) = dir.read(slot)
            && bytes.len() >= 4
        {
            let (body, crc_bytes) = bytes.split_at(bytes.len() - 4);
            if let Ok(crc_arr) = <[u8; 4]>::try_from(crc_bytes)
                && crc32c::crc32c(body) == u32::from_le_bytes(crc_arr)
                && let Some((len, k)) = get_varint(body, 0)
                && body.len() == k + len as usize
                && let Ok(s) = std::str::from_utf8(&body[k..])
            {
                names.push(s.to_string());
            }
        }
    }
    for n in dir.list() {
        if n.starts_with("manifest-") && n.ends_with(".ckpt") {
            names.push(n);
        }
    }
    names.sort();
    names.dedup();
    let mut manifests: Vec<Manifest> = names
        .iter()
        .filter_map(|n| dir.read(n))
        .filter_map(|b| Manifest::decode(&b))
        .collect();
    manifests.sort_by_key(|m| std::cmp::Reverse(m.cursor.idx));
    for m in manifests {
        if let Some(state) = load_manifest(dir, &m, log) {
            return Some((state, m));
        }
    }
    None
}

// ---------------------------------------------------------------------------
// GC (design §10.5): never delete a page reachable from a retained manifest
// ---------------------------------------------------------------------------

/// Retain the newest `retain` valid manifests; delete older manifests, then
/// pages unreachable from the retained set. The durable reachability set is
/// established BEFORE any delete (§10.5); manifests are removed before the
/// pages only they reference, so a crash anywhere leaves every retained
/// manifest fully loadable (at worst it leaves orphaned garbage).
pub fn gc<D: Dir>(dir: &mut D, retain: usize) -> io::Result<()> {
    let mut manifests: Vec<(String, Manifest)> = dir
        .list()
        .into_iter()
        .filter(|n| n.starts_with("manifest-") && n.ends_with(".ckpt"))
        .filter_map(|n| {
            let m = Manifest::decode(&dir.read(&n)?)?;
            Some((n, m))
        })
        .collect();
    manifests.sort_by_key(|(_, m)| std::cmp::Reverse(m.cursor.idx));
    let (keep, drop) = manifests.split_at(retain.min(manifests.len()));
    let mut reachable: HashSet<String> = HashSet::new();
    for (_, m) in keep {
        for &(_, _, h) in &m.pages {
            reachable.insert(page_name(&h));
        }
    }
    // Deletes begin only now — the reachable set is computed from durable
    // manifest content.
    for (name, _) in drop {
        dir.remove(name)?;
    }
    dir.fsync_dir()?;
    for n in dir.list() {
        if n.starts_with("page-") && n.ends_with(".page") && !reachable.contains(&n) {
            dir.remove(&n)?;
        }
        if n.starts_with("tmp-") {
            dir.remove(&n)?; // interrupted-install leftovers
        }
    }
    dir.fsync_dir()?;
    Ok(())
}
