//! In-memory simulation filesystem with the spike fault layers ported
//! behind the [`Fs`] trait.
//!
//! Two media, one per spike, slotted behind the same [`FileHandle`]:
//!
//! - [`Fault::Sector`] ports `spikes/torn_write`'s **`SectorDisk`**: a
//!   file's un-`fdatasync`ed sectors persist independently and in
//!   arbitrary order on crash, and one may tear at an arbitrary byte cut.
//!   This is the ALICE-style block-write-reordering model that the 24k
//!   torn-write conformance cases (the future bar) run against — A4's
//!   killer (marker sector before frame sectors) lives here.
//! - [`Fault::Tail`] ports `spikes/crash_log`'s **`FaultWriter`**: the
//!   un-synced tail survives only up to some length (torn prefix), with
//!   optional byte scrambling in the surviving-but-unsynced region.
//!
//! Both make `fdatasync` a **true barrier** (matching the spec's
//! no-volatile-cache-lies fault model): everything written before a
//! successful sync is durable and survives any crash plan.

use std::collections::{BTreeSet, HashMap};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use super::sim::Rng;
use super::{FileHandle, Fs, OpenOpts};

/// Which fault model backs a file. The default for a fresh [`SimFs`] is
/// [`Fault::Sector`] with 512-byte sectors (the spike's headline size).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Fault {
    /// Sector-granular reordering + tear (`torn_write::SectorDisk`).
    Sector { sector_size: usize },
    /// Torn-tail prefix truncation (`crash_log::FaultWriter`).
    Tail,
}

impl Fault {
    /// The spike's headline configuration: 512-byte sectors.
    pub const SECTOR_512: Fault = Fault::Sector { sector_size: 512 };
}

/// A deterministic crash plan for a [`Fault::Sector`] file: which pending
/// sectors persisted, plus an optional torn sector `(sector, keep_bytes)`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SectorPlan {
    /// Pending sectors that reached the platter. All others revert to the
    /// last durable content.
    pub persist: Vec<usize>,
    /// One sector persisted only up to `keep_bytes`; the rest keeps its
    /// prior durable bytes (a partial-sector write).
    pub tear: Option<(usize, usize)>,
}

/// A deterministic crash plan for a [`Fault::Tail`] file.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TailPlan {
    /// The surviving prefix length. MUST be in `[synced, len]`; the tail
    /// past it is lost.
    pub keep: usize,
    /// Byte offsets in `(synced, keep)` to scramble (torn page).
    pub scramble: Vec<usize>,
}

/// A crash plan, tagged to the medium it applies to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CrashPlan {
    Sector(SectorPlan),
    Tail(TailPlan),
}

// ---------------------------------------------------------------------------
// SectorDisk — ported from spikes/torn_write
// ---------------------------------------------------------------------------

/// Sector-granular disk. Positioned writes land in `shadow` and mark
/// touched sectors pending; `fdatasync` promotes `shadow` to `durable`; a
/// crash materializes `durable` + an arbitrary subset of pending sectors,
/// optionally tearing one.
#[derive(Debug, Clone)]
struct SectorDisk {
    sector_size: usize,
    durable: Vec<u8>,
    shadow: Vec<u8>,
    logical_len: usize,
    pending: BTreeSet<usize>,
}

impl SectorDisk {
    fn new(sector_size: usize, background: Vec<u8>) -> Self {
        assert!(
            sector_size.is_power_of_two(),
            "sector size must be a power of two"
        );
        let cap = background.len().div_ceil(sector_size) * sector_size;
        let mut durable = background;
        durable.resize(cap, 0);
        SectorDisk {
            sector_size,
            shadow: durable.clone(),
            logical_len: durable
                .iter()
                .rposition(|&b| b != 0)
                .map_or(0, |i| i + 1),
            durable,
            pending: BTreeSet::new(),
        }
    }

    fn grow_to(&mut self, len: usize) {
        if len > self.shadow.len() {
            let cap = len.div_ceil(self.sector_size) * self.sector_size;
            self.durable.resize(cap, 0);
            self.shadow.resize(cap, 0);
        }
    }

    fn pwrite(&mut self, off: usize, buf: &[u8]) -> usize {
        let end = off + buf.len();
        self.grow_to(end);
        self.shadow[off..end].copy_from_slice(buf);
        if !buf.is_empty() {
            for s in off / self.sector_size..=(end - 1) / self.sector_size {
                self.pending.insert(s);
            }
        }
        self.logical_len = self.logical_len.max(end);
        buf.len()
    }

    fn pread(&self, off: usize, buf: &mut [u8]) -> usize {
        if off >= self.logical_len {
            return 0;
        }
        let n = buf.len().min(self.logical_len - off);
        buf[..n].copy_from_slice(&self.shadow[off..off + n]);
        n
    }

    fn fdatasync(&mut self) {
        self.durable.copy_from_slice(&self.shadow);
        self.pending.clear();
    }

    fn pending_sectors(&self) -> Vec<usize> {
        self.pending.iter().copied().collect()
    }

    /// Materialize the post-crash image and reopen onto it.
    fn crash(&mut self, plan: &SectorPlan) {
        let ss = self.sector_size;
        let mut img = self.durable.clone();
        for &s in &plan.persist {
            if self.pending.contains(&s) {
                img[s * ss..(s + 1) * ss]
                    .copy_from_slice(&self.shadow[s * ss..(s + 1) * ss]);
            }
        }
        if let Some((s, keep)) = plan.tear
            && self.pending.contains(&s)
        {
            let keep = keep.min(ss);
            img[s * ss..s * ss + keep]
                .copy_from_slice(&self.shadow[s * ss..s * ss + keep]);
            img[s * ss + keep..(s + 1) * ss]
                .copy_from_slice(&self.durable[s * ss + keep..(s + 1) * ss]);
        }
        self.durable = img.clone();
        self.shadow = img;
        self.pending.clear();
    }
}

// ---------------------------------------------------------------------------
// TailDisk — ported from spikes/crash_log FaultWriter
// ---------------------------------------------------------------------------

/// Append-with-torn-tail disk. `synced` is the fdatasync watermark; a
/// crash keeps a prefix anywhere in `[synced, len]`, optionally scrambling
/// bytes in the surviving-but-unsynced region.
#[derive(Debug, Clone)]
struct TailDisk {
    buf: Vec<u8>,
    synced: usize,
}

impl TailDisk {
    fn new(background: Vec<u8>) -> Self {
        let synced = background.len();
        TailDisk { buf: background, synced }
    }

    fn pwrite(&mut self, off: usize, buf: &[u8]) -> usize {
        let end = off + buf.len();
        if end > self.buf.len() {
            self.buf.resize(end, 0);
        }
        self.buf[off..end].copy_from_slice(buf);
        buf.len()
    }

    fn pread(&self, off: usize, buf: &mut [u8]) -> usize {
        if off >= self.buf.len() {
            return 0;
        }
        let n = buf.len().min(self.buf.len() - off);
        buf[..n].copy_from_slice(&self.buf[off..off + n]);
        n
    }

    fn fdatasync(&mut self) {
        self.synced = self.buf.len();
    }

    fn crash(&mut self, plan: &TailPlan) {
        let keep = plan.keep.clamp(self.synced, self.buf.len());
        self.buf.truncate(keep);
        for &i in &plan.scramble {
            if i >= self.synced && i < self.buf.len() {
                // Deterministic corruption: flip all bits.
                self.buf[i] = !self.buf[i];
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Medium: the per-file fault-model backing store
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
enum Medium {
    Sector(SectorDisk),
    Tail(TailDisk),
}

impl Medium {
    fn new(fault: Fault, background: Vec<u8>) -> Self {
        match fault {
            Fault::Sector { sector_size } => {
                Medium::Sector(SectorDisk::new(sector_size, background))
            }
            Fault::Tail => Medium::Tail(TailDisk::new(background)),
        }
    }

    fn pwrite(&mut self, off: usize, buf: &[u8]) -> usize {
        match self {
            Medium::Sector(d) => d.pwrite(off, buf),
            Medium::Tail(d) => d.pwrite(off, buf),
        }
    }

    fn pread(&self, off: usize, buf: &mut [u8]) -> usize {
        match self {
            Medium::Sector(d) => d.pread(off, buf),
            Medium::Tail(d) => d.pread(off, buf),
        }
    }

    fn fdatasync(&mut self) {
        match self {
            Medium::Sector(d) => d.fdatasync(),
            Medium::Tail(d) => d.fdatasync(),
        }
    }

    fn len(&self) -> u64 {
        match self {
            Medium::Sector(d) => d.logical_len as u64,
            Medium::Tail(d) => d.buf.len() as u64,
        }
    }

    fn crash(&mut self, plan: &CrashPlan) -> io::Result<()> {
        match (self, plan) {
            (Medium::Sector(d), CrashPlan::Sector(p)) => {
                d.crash(p);
                Ok(())
            }
            (Medium::Tail(d), CrashPlan::Tail(p)) => {
                d.crash(p);
                Ok(())
            }
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "crash plan does not match the file's fault medium",
            )),
        }
    }
}

// ---------------------------------------------------------------------------
// SimFs / SimFile — the Fs trait impl
// ---------------------------------------------------------------------------

type Inode = Arc<Mutex<Medium>>;

/// An in-memory filesystem whose files inject the spike fault models.
///
/// Cheap to clone (shared inode table): all clones name the same files.
#[derive(Clone)]
pub struct SimFs {
    inner: Arc<Mutex<Inner>>,
}

struct Inner {
    files: HashMap<PathBuf, Inode>,
    default_fault: Fault,
}

impl SimFs {
    /// A fresh filesystem whose newly-created files use `default_fault`.
    pub fn new(default_fault: Fault) -> Self {
        SimFs {
            inner: Arc::new(Mutex::new(Inner {
                files: HashMap::new(),
                default_fault,
            })),
        }
    }

    fn inode(&self, path: &Path) -> Option<Inode> {
        self.inner.lock().unwrap().files.get(path).cloned()
    }

    /// Pre-seed a file with durable `background` bytes and a specific fault
    /// model — models a recycled segment file whose disk region still holds
    /// a stale prior generation (the A9 precondition). Overwrites any
    /// existing file at `path`.
    pub fn seed(
        &self,
        path: impl Into<PathBuf>,
        fault: Fault,
        background: Vec<u8>,
    ) {
        let inode = Arc::new(Mutex::new(Medium::new(fault, background)));
        self.inner.lock().unwrap().files.insert(path.into(), inode);
    }

    /// Crash `path`, materializing its post-crash on-disk image per `plan`.
    /// Subsequent reads on any handle to that file see the survived bytes.
    pub fn crash(&self, path: &Path, plan: CrashPlan) -> io::Result<()> {
        let inode = self.inode(path).ok_or_else(|| {
            io::Error::new(io::ErrorKind::NotFound, "no such sim file")
        })?;
        inode.lock().unwrap().crash(&plan)
    }

    /// Roll a random-but-seeded [`SectorPlan`] over `path`'s currently
    /// pending sectors: each persists with probability 1/2, and with
    /// probability `tear_prob` one applied sector tears at a random cut —
    /// the `torn_write::crash_random` shape the 24k conformance sweep uses.
    /// Returns the plan actually applied (for shrinking/replay). No-op plan
    /// on a non-sector file.
    pub fn crash_random(
        &self,
        path: &Path,
        rng: &mut Rng,
        tear_prob: f64,
    ) -> io::Result<SectorPlan> {
        let inode = self.inode(path).ok_or_else(|| {
            io::Error::new(io::ErrorKind::NotFound, "no such sim file")
        })?;
        let mut guard = inode.lock().unwrap();
        let Medium::Sector(disk) = &*guard else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "crash_random is sector-model only",
            ));
        };
        let ss = disk.sector_size;
        let pending = disk.pending_sectors();
        let persist: Vec<usize> =
            pending.iter().copied().filter(|_| rng.bool()).collect();
        let tear = if !persist.is_empty() && rng.chance(tear_prob) {
            let s = persist[rng.below(persist.len() as u64) as usize];
            Some((s, rng.below(ss as u64) as usize))
        } else {
            None
        };
        let plan = SectorPlan { persist, tear };
        guard.crash(&CrashPlan::Sector(plan.clone()))?;
        Ok(plan)
    }
}

impl Fs for SimFs {
    type File = SimFile;

    fn open(&self, path: &Path, opts: OpenOpts) -> io::Result<SimFile> {
        let mut inner = self.inner.lock().unwrap();
        let default_fault = inner.default_fault;
        let inode = match inner.files.get(path) {
            Some(inode) => inode.clone(),
            None => {
                if !opts.create {
                    return Err(io::Error::new(
                        io::ErrorKind::NotFound,
                        "no such sim file",
                    ));
                }
                let inode = Arc::new(Mutex::new(Medium::new(
                    default_fault,
                    Vec::new(),
                )));
                inner.files.insert(path.to_path_buf(), inode.clone());
                inode
            }
        };
        if opts.truncate {
            *inode.lock().unwrap() = Medium::new(default_fault, Vec::new());
        }
        Ok(SimFile { inode })
    }

    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        let mut inner = self.inner.lock().unwrap();
        let inode = inner.files.remove(from).ok_or_else(|| {
            io::Error::new(io::ErrorKind::NotFound, "rename source missing")
        })?;
        inner.files.insert(to.to_path_buf(), inode);
        Ok(())
    }
}

/// A handle to a [`SimFs`] file. Shared: clones (or reopens) address the
/// same inode, matching a segment file held by both the committer and a
/// background sealer.
#[derive(Clone)]
pub struct SimFile {
    inode: Inode,
}

impl FileHandle for SimFile {
    fn pwrite(&self, off: u64, buf: &[u8]) -> io::Result<usize> {
        Ok(self.inode.lock().unwrap().pwrite(off as usize, buf))
    }

    fn pread(&self, off: u64, buf: &mut [u8]) -> io::Result<usize> {
        Ok(self.inode.lock().unwrap().pread(off as usize, buf))
    }

    fn fdatasync(&self) -> io::Result<()> {
        self.inode.lock().unwrap().fdatasync();
        Ok(())
    }

    fn len(&self) -> io::Result<u64> {
        Ok(self.inode.lock().unwrap().len())
    }
}
