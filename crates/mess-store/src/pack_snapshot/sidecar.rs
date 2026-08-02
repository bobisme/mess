//! The storage core: [`Sidecar`], a storage-neutral pack-based snapshot
//! directory with one exclusive writer, immutable sealed packs, and bounded
//! root-descriptor discovery.
//!
//! This type knows nothing about [`Backend`](crate::Backend),
//! [`Snapshottable`](crate::Snapshottable), or aggregates — it stores and
//! retrieves self-describing [`Record`]s keyed by stream name. The trait
//! adapter lives in the parent module, and the crash tests drive *this* type
//! directly so they can inject a failure at every durability boundary without
//! standing up an event log.

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{self, Seek, SeekFrom, Write};
use std::num::NonZeroU32;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use mess_log::lock::{LockError, StoreLock};

use super::format::{
    self, Footer, IDENTITY_FILE, IDENTITY_LEN, IndexEntry, MAX_FRAME_LEN,
    PACK_HEADER_LEN, Record, Root, RootEntry, SaveMode, StoreUuid, TMP_SUFFIX,
    decode_footer, decode_frame, decode_identity, decode_pack_header,
    decode_record, decode_root, encode_frame, encode_identity,
    encode_index_and_footer, encode_pack_header, encode_record, encode_root,
    pack_name, parse_pack_name, parse_root_name, peek_frame_len, root_name,
    trust_from_u8,
};
use crate::snapshot::{
    CurrentHead, PinnedSnapshotRoot, SnapshotCompatibility, SnapshotLookup,
    SnapshotMiss, SnapshotRootId, SnapshotSaveOutcome, SnapshotScanCursor,
    SnapshotScanDiagnostic, SnapshotScanEntry, SnapshotScanKey,
    SnapshotScanPage, clamp_scan_limit, publication_decision,
};

/// A failure that could not be degraded into "no snapshot".
///
/// The sidecar's whole contract is that *snapshot* problems degrade to
/// replay-on-miss, so this enum is deliberately small: a second writer, a
/// read-only handle asked to write, and raw I/O failures on the write path.
/// Nothing here is ever produced by a read.
#[derive(Debug, thiserror::Error)]
pub enum PackSidecarError {
    /// Another live process already holds this sidecar's writer lock. This is
    /// the one case that fails loudly instead of degrading.
    #[error("snapshot sidecar writer lock: {0}")]
    Lock(#[from] LockError),
    /// A write was attempted through a read-only (offline reader) handle.
    #[error("snapshot sidecar at {0} is open read-only")]
    ReadOnly(PathBuf),
    /// Filesystem I/O failed on the write path.
    #[error("snapshot sidecar io: {0}")]
    Io(#[from] io::Error),
}

/// Tunables for a sidecar. Every default is chosen so a caller that passes
/// [`Default::default`] gets the superseded head table's semantics (buffered,
/// discardable) with the new crash contract.
#[derive(Debug, Clone, Copy)]
pub struct SidecarOptions {
    /// Publication mode for saves. [`SaveMode::Buffered`] by default — a
    /// discardable cache write, matching the superseded backend's
    /// journal-buffered head. [`SaveMode::Durable`] acknowledges only
    /// after the covering pack frames and directory entries are synced.
    pub mode:           SaveMode,
    /// Roll the active build pack once it would exceed this many bytes. A
    /// single record larger than the limit still gets its own pack rather
    /// than being rejected.
    pub max_pack_bytes: u64,
    /// How many complete root generations to retain when pruning. Clamped to
    /// a minimum of 2 (ADR 0002: at least two complete roots remain usable).
    pub retain_roots:   usize,
    /// Prune obsolete root descriptors once this many are on disk. Pruning is
    /// a durability boundary (see [`Sidecar::prune_roots_now`]), so it is
    /// amortized rather than run on every save.
    pub prune_roots_at: usize,
}

impl Default for SidecarOptions {
    fn default() -> Self {
        Self {
            mode:           SaveMode::Buffered,
            max_pack_bytes: 64 * 1024 * 1024,
            retain_roots:   2,
            prune_roots_at: 16,
        }
    }
}

/// Counters an operator (or `mess doctor`) can read to tell a healthy sidecar
/// from one that is quietly degrading to replay.
#[derive(Debug, Default)]
pub struct SidecarMetrics {
    /// Records appended.
    pub records_written:      AtomicU64,
    /// Saves that were a no-op because the current head already held
    /// byte-identical bytes at the same coverage.
    pub idempotent_saves:     AtomicU64,
    /// Saves skipped because they would have regressed coverage.
    pub coverage_regressions: AtomicU64,
    /// Saves that superseded a head whose bytes did not validate. A nonzero
    /// value means something damaged the sidecar and a save healed it.
    pub repairs:              AtomicU64,
    /// Saves refused because a *valid*, different record already held the same
    /// coverage under the same identity (ADR 0002 §1 rule 3). A nonzero value
    /// means two writers disagree about the same fold — a real bug, not noise.
    pub conflicts:            AtomicU64,
    /// Loads that found a head but could not use it (missing pack, corrupt
    /// frame, identity mismatch) and therefore reported a miss.
    pub degraded_loads:       AtomicU64,
    /// Build packs rolled and sealed.
    pub packs_rolled:         AtomicU64,
    /// Root descriptors published.
    pub roots_published:      AtomicU64,
    /// Obsolete root descriptors unlinked.
    pub roots_pruned:         AtomicU64,
    /// Explicit durability barriers issued (file + directory fsyncs).
    pub barriers:             AtomicU64,
    /// Bytes promoted (synced) by durable closure promotion.
    pub promoted_bytes:       AtomicU64,
    /// Roots rejected at open because they did not independently resolve.
    pub roots_rejected:       AtomicU64,
    /// Bytes truncated from a torn active-pack tail at open.
    pub tail_truncated_bytes: AtomicU64,
}

impl SidecarMetrics {
    fn bump(counter: &AtomicU64) { counter.fetch_add(1, Ordering::Relaxed); }
}

/// A hook the crash tests use to abort exactly at one named durability
/// boundary. Always `None` in production (the field costs one word).
pub(crate) type FaultHook =
    Arc<dyn Fn(&str) -> io::Result<()> + Send + Sync + 'static>;

/// The active, footerless build pack.
struct ActivePack {
    seq:  u64,
    file: File,
    len:  u64,
    /// Frame offsets/lengths, for the index a roll appends.
    idx:  Vec<IndexEntry>,
}

/// How many ids one durable reservation covers.
///
/// The allocator persists a high-water into `IDENTITY` every `STRIDE` ids, so
/// the amortized cost is one durable identity rewrite per `STRIDE` packs +
/// roots. (A `Buffered` save therefore issues a barrier once every `STRIDE`
/// saves. That barrier is about *identity safety*, not data durability, and
/// buying it is strictly better than risking a reused generation number.)
const ID_RESERVE_STRIDE: u64 = 4096;

/// Hands out the shared, monotone, never-reused id that names both packs and
/// root generations.
///
/// # Why a persisted reservation and not just "max on disk + 1"
///
/// Scanning the directory recovers the counter only while an artifact bearing
/// the highest number still exists. Delete every root descriptor (an operator
/// "cleaning up", a partial restore) and the counter would silently rewind,
/// letting a *new* root reuse a generation an old, still-cached descriptor
/// used — the ABA the contract forbids. Reserving ids in batches and recording
/// the high-water under the writer lock closes that hole for the price of one
/// durable write per [`ID_RESERVE_STRIDE`] ids.
struct IdAllocator {
    next:     u64,
    reserved: u64,
}

impl IdAllocator {
    fn alloc(
        &mut self,
        dir: &Path,
        uuid: StoreUuid,
        metrics: &SidecarMetrics,
        hook: Option<&FaultHook>,
    ) -> io::Result<u64> {
        if self.next >= self.reserved {
            let reserved = self.next.saturating_add(ID_RESERVE_STRIDE);
            atomic_write(
                &dir.join(IDENTITY_FILE),
                &encode_identity(uuid, reserved),
                true,
                metrics,
                hook,
                "identity",
            )?;
            self.reserved = reserved;
        }
        let id = self.next;
        self.next = self.next.saturating_add(1);
        Ok(id)
    }
}

/// Writer-only state, serialized by one mutex so every clone of a
/// [`Sidecar`] shares a single writer owner (ADR 0002: "all in-process
/// clones share the same writer owner and serialize append, head comparison,
/// and root publication").
struct Writer {
    active:          ActivePack,
    ids:             IdAllocator,
    sealed:          Vec<u64>,
    /// Generations of root descriptors currently on disk, ascending.
    roots_on_disk:   Vec<u64>,
    /// Durability-proof ledger: per-pack byte frontier already known synced.
    ///
    /// Deliberately **in-memory only** in v1: a reopen forgets every proof and
    /// therefore re-promotes. That errs in the safe direction (more barriers,
    /// never fewer) and costs no on-disk format, so the persisted ledger ADR
    /// 0002 describes can be added later without a format change.
    proven_frontier: HashMap<u64, u64>,
    /// Whether the sidecar directory's entries are known durable.
    dir_proven:      bool,
    hook:            Option<FaultHook>,
}

/// The published discovery state of one root generation.
///
/// # Why two views of the same heads
///
/// The ordinary path wants `O(1)` point lookup by `(stream, compatibility)`;
/// the administrative path wants a bounded, ordered walk. `by_stream` serves
/// the first and is mutated in place per save; `sorted` serves the second and
/// is exactly the `Vec` a save already builds to encode the root, retained
/// behind an `Arc` so [`Sidecar::pin_root`] is a refcount bump rather than a
/// traversal.
///
/// The duplication is the flat root's, not this type's: ADR 0002's
/// copy-on-write discovery tree removes both views together (see
/// [`select_root`]). Until it does, resident cost is roughly two `RootEntry`
/// per head, each carrying its identity inline — the on-disk root interns
/// identities, this does not, because the point of the resident copy is an
/// `O(1)` compare on the lookup path.
struct Published {
    /// Generation of the root these heads came from, or `None` when no root
    /// resolved and the store simply has no snapshots.
    generation: Option<u64>,
    /// Heads by stream name — usually one, one more per compatibility that
    /// stream has ever been snapshotted under.
    by_stream:  HashMap<String, Vec<RootEntry>>,
    /// The same heads in scan order: by stream name, then compatibility.
    sorted:     Arc<Vec<RootEntry>>,
}

impl Published {
    fn empty() -> Self {
        Self {
            generation: None,
            by_stream:  HashMap::new(),
            sorted:     Arc::new(Vec::new()),
        }
    }

    /// Build both views from one root's leaf list.
    fn from_root(root: Root) -> Self {
        let mut sorted = root.entries;
        sorted.sort_by(scan_order);
        let mut by_stream: HashMap<String, Vec<RootEntry>> = HashMap::new();
        for e in &sorted {
            by_stream.entry(e.stream_name.clone()).or_default().push(e.clone());
        }
        Self {
            generation: Some(root.generation),
            by_stream,
            sorted: Arc::new(sorted),
        }
    }

    /// The head for exactly this key, if one is published.
    fn head(
        &self,
        stream: &str,
        compat: &SnapshotCompatibility,
    ) -> Option<&RootEntry> {
        self.by_stream.get(stream)?.iter().find(|e| e.compatibility == *compat)
    }

    /// The highest-coverage head this stream has under *any* other identity —
    /// what makes a post-deploy miss say "incompatible" instead of "absent".
    fn foreign_head(
        &self,
        stream: &str,
        compat: &SnapshotCompatibility,
    ) -> Option<&RootEntry> {
        self.by_stream
            .get(stream)?
            .iter()
            .filter(|e| e.compatibility != *compat)
            .max_by_key(|e| e.coverage)
    }
}

/// The total order every scan and cursor uses: stream name, then identity.
fn scan_order(a: &RootEntry, b: &RootEntry) -> std::cmp::Ordering {
    a.stream_name
        .cmp(&b.stream_name)
        .then_with(|| a.compatibility.cmp(&b.compatibility))
}

/// A pack-based snapshot sidecar directory.
///
/// Cheap to clone via `Arc` at the layer above; a `Sidecar` itself is held
/// once and shared.
pub struct Sidecar {
    dir:         PathBuf,
    uuid:        StoreUuid,
    options:     SidecarOptions,
    /// The published discovery state. Rebuilt from the selected root at open
    /// and updated only after a successful root publication.
    published:   RwLock<Published>,
    /// Read handles per pack sequence. An already-open handle keeps working
    /// across the `.open` -> `.pack` rename (same inode), which is what makes
    /// a stale descriptor yield *old correct bytes or a miss*, never a
    /// different snapshot.
    readers:     RwLock<HashMap<u64, Arc<File>>>,
    writer:      Option<Mutex<Writer>>,
    /// Held for the sidecar's lifetime by a writer; `None` for a reader.
    _lock:       Option<StoreLock>,
    /// Observability.
    pub metrics: SidecarMetrics,
}

impl std::fmt::Debug for Sidecar {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Sidecar")
            .field("dir", &self.dir)
            .field("uuid", &self.uuid)
            .field("writable", &self.writer.is_some())
            .finish_non_exhaustive()
    }
}

// ---------------------------------------------------------------------------
// Small filesystem helpers
// ---------------------------------------------------------------------------

/// Positional read — never disturbs any file offset, so a reader can pread a
/// committed range of the very pack the writer is appending to.
#[cfg(unix)]
fn pread_exact(file: &File, buf: &mut [u8], offset: u64) -> io::Result<()> {
    use std::os::unix::fs::FileExt;
    file.read_exact_at(buf, offset)
}

#[cfg(not(unix))]
fn pread_exact(file: &File, buf: &mut [u8], offset: u64) -> io::Result<()> {
    use std::io::Read;
    // Portable fallback: a private handle so no shared offset is touched.
    let mut f = file.try_clone()?;
    f.seek(SeekFrom::Start(offset))?;
    f.read_exact(buf)
}

/// fsync a directory so a creation/rename entry survives a power cut.
/// A no-op on platforms where directories cannot be opened for sync.
fn sync_dir(dir: &Path) -> io::Result<()> {
    #[cfg(unix)]
    {
        File::open(dir)?.sync_all()
    }
    #[cfg(not(unix))]
    {
        let _ = dir;
        Ok(())
    }
}

/// Write `bytes` to `path` via a reserved-suffix temp sibling and rename.
/// `durable` adds the file and directory barriers.
fn atomic_write(
    path: &Path,
    bytes: &[u8],
    durable: bool,
    metrics: &SidecarMetrics,
    hook: Option<&FaultHook>,
    step_prefix: &str,
) -> io::Result<()> {
    let tmp = with_tmp_suffix(path);
    if let Some(h) = hook {
        h(&format!("{step_prefix}:write"))?;
    }
    {
        let mut f = File::create(&tmp)?;
        f.write_all(bytes)?;
        if durable {
            if let Some(h) = hook {
                h(&format!("{step_prefix}:sync"))?;
            }
            f.sync_all()?;
            SidecarMetrics::bump(&metrics.barriers);
        }
    }
    if let Some(h) = hook {
        h(&format!("{step_prefix}:rename"))?;
    }
    std::fs::rename(&tmp, path)?;
    if durable {
        if let Some(h) = hook {
            h(&format!("{step_prefix}:dirsync"))?;
        }
        if let Some(parent) = path.parent() {
            sync_dir(parent)?;
            SidecarMetrics::bump(&metrics.barriers);
        }
    }
    Ok(())
}

fn with_tmp_suffix(path: &Path) -> PathBuf {
    let mut s = path.as_os_str().to_os_string();
    s.push(TMP_SUFFIX);
    PathBuf::from(s)
}

/// Mint 128 bits of identity.
///
/// Prefers the OS CSPRNG (`/dev/urandom`); falls back to a mix of the
/// process's ASLR'd addresses, `RandomState`'s per-process random keys, the
/// wall clock and the pid. The fallback is not cryptographic, but the store
/// UUID only has to be *unique*, and it is never a secret.
fn mint_uuid() -> StoreUuid {
    use std::io::Read;
    if let Ok(mut f) = File::open("/dev/urandom") {
        let mut b = [0u8; 16];
        if f.read_exact(&mut b).is_ok() {
            return StoreUuid(b);
        }
    }
    use std::collections::hash_map::RandomState;
    use std::hash::{BuildHasher, Hasher};
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    let mix = |salt: u64| -> u64 {
        let mut h = RandomState::new().build_hasher();
        h.write_u64(salt);
        h.write_u64(nanos);
        h.write_u32(std::process::id());
        h.write_usize(&salt as *const u64 as usize);
        h.finish()
    };
    let mut out = [0u8; 16];
    out[0..8].copy_from_slice(&mix(0xA5A5_5A5A_1234_5678).to_le_bytes());
    out[8..16].copy_from_slice(&mix(0x0F0F_F0F0_8765_4321).to_le_bytes());
    StoreUuid(out)
}

// ---------------------------------------------------------------------------
// Open
// ---------------------------------------------------------------------------

/// What a directory listing turned up for one store UUID.
#[derive(Default)]
struct Listing {
    /// Sealed pack sequences.
    sealed:                Vec<u64>,
    /// Unsealed (`.open`) pack sequences.
    open:                  Vec<u64>,
    /// Root generations, ascending.
    roots:                 Vec<u64>,
    /// The highest sequence/generation seen for ANY uuid, so a new namespace
    /// never reuses a number an old artifact already used.
    max_pack_seq_any_uuid: Option<u64>,
    max_root_gen_any_uuid: Option<u64>,
}

fn list_dir(dir: &Path, uuid: StoreUuid) -> io::Result<Listing> {
    let mut out = Listing::default();
    let rd = match std::fs::read_dir(dir) {
        Ok(rd) => rd,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(out),
        Err(e) => return Err(e),
    };
    for entry in rd {
        let entry = entry?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        if let Some((u, seq, sealed)) = parse_pack_name(name) {
            out.max_pack_seq_any_uuid = Some(
                out.max_pack_seq_any_uuid.map_or(seq, |m: u64| m.max(seq)),
            );
            if u == uuid {
                if sealed {
                    out.sealed.push(seq);
                } else {
                    out.open.push(seq);
                }
            }
        } else if let Some((u, generation)) = parse_root_name(name) {
            out.max_root_gen_any_uuid = Some(
                out.max_root_gen_any_uuid
                    .map_or(generation, |m: u64| m.max(generation)),
            );
            if u == uuid {
                out.roots.push(generation);
            }
        }
        // Anything else — LOCK, IDENTITY, a `.tmp` staging file, junk — is
        // structurally not a candidate. Nothing is repaired or deleted.
    }
    out.sealed.sort_unstable();
    out.open.sort_unstable();
    out.roots.sort_unstable();
    Ok(out)
}

impl Sidecar {
    /// Open (creating if absent) the sidecar at `dir` as **the** writer.
    ///
    /// Fails loudly if another live process holds the writer lock. Every other
    /// problem — a missing directory, a corrupt identity, an unreadable root, a
    /// torn active tail — is recovered or degraded into "no snapshots yet".
    pub fn open_writer(
        dir: impl AsRef<Path>,
        options: SidecarOptions,
    ) -> Result<Self, PackSidecarError> {
        Self::open_writer_inner(dir.as_ref(), options, None)
    }

    pub(crate) fn open_writer_inner(
        dir: &Path,
        options: SidecarOptions,
        hook: Option<FaultHook>,
    ) -> Result<Self, PackSidecarError> {
        std::fs::create_dir_all(dir)?;
        let lock = StoreLock::acquire(dir)?;
        let metrics = SidecarMetrics::default();

        // 1. Store identity, created exactly once under the writer lock.
        let id_path = dir.join(IDENTITY_FILE);
        let existing = std::fs::read(&id_path).ok().and_then(|raw| {
            (raw.len() == IDENTITY_LEN).then(|| decode_identity(&raw)).flatten()
        });
        let (uuid, reserved_ids) = match existing {
            Some(pair) => pair,
            None => {
                // Absent OR corrupt: mint a brand-new namespace. ADR 0002
                // explicitly permits ignoring old artifacts but never adopting
                // their IDs, which is what prevents ABA after a restore.
                let u = mint_uuid();
                atomic_write(
                    &id_path,
                    &encode_identity(u, ID_RESERVE_STRIDE),
                    true,
                    &metrics,
                    hook.as_ref(),
                    "identity",
                )?;
                (u, ID_RESERVE_STRIDE)
            }
        };

        let listing = list_dir(dir, uuid)?;

        // 2. Recover the active build pack BEFORE choosing a root, because
        //    truncating a torn tail can invalidate a root that named bytes past
        //    the last committed frame.
        let mut sealed = listing.sealed.clone();
        let mut active: Option<ActivePack> = None;
        // Highest `.open` sequence is the active build pack; any lower one is
        // an orphan from an interrupted roll and is left strictly alone.
        if let Some(&seq) = listing.open.last() {
            match Self::recover_open_pack(dir, uuid, seq, &metrics)? {
                RecoveredPack::Active(p) => active = Some(p),
                RecoveredPack::Sealed => sealed.push(seq),
                RecoveredPack::Unusable => {}
            }
        }
        sealed.sort_unstable();
        sealed.dedup();

        // 3. Discovery: highest final, independently resolvable, valid root;
        //    fall back progressively to an older root, then to empty.
        let published = select_root(dir, uuid, &listing.roots, &metrics)
            .map_or_else(Published::empty, Published::from_root);

        // 4. Ids never restart: the shared counter resumes at the durably
        //    reserved high-water, and (belt and braces, in case the identity
        //    file was rolled back) past the highest number ANY namespace in
        //    this directory has used.
        let seen = listing
            .max_pack_seq_any_uuid
            .into_iter()
            .chain(listing.max_root_gen_any_uuid)
            .chain(active.as_ref().map(|a| a.seq))
            .max()
            .map_or(0, |m| m.saturating_add(1));
        let mut ids = IdAllocator {
            next:     reserved_ids.max(seen),
            reserved: reserved_ids,
        };

        let active = match active {
            Some(a) => a,
            None => {
                let seq = ids.alloc(dir, uuid, &metrics, hook.as_ref())?;
                Self::create_pack(dir, uuid, seq)?
            }
        };

        Ok(Self {
            dir: dir.to_path_buf(),
            uuid,
            options,
            published: RwLock::new(published),
            readers: RwLock::new(HashMap::new()),
            writer: Some(Mutex::new(Writer {
                active,
                ids,
                sealed,
                roots_on_disk: listing.roots,
                proven_frontier: HashMap::new(),
                dir_proven: false,
                hook,
            })),
            _lock: Some(lock),
            metrics,
        })
    }

    /// Open the sidecar at `dir` **read-only**.
    ///
    /// Takes no lock and never creates, truncates, repairs, renames or deletes
    /// anything. A sidecar that does not exist, has no identity, or has no
    /// resolvable root opens successfully with zero heads — every load then
    /// misses and the caller replays.
    pub fn open_reader(dir: impl AsRef<Path>) -> Self {
        let dir = dir.as_ref().to_path_buf();
        let metrics = SidecarMetrics::default();
        let uuid = std::fs::read(dir.join(IDENTITY_FILE))
            .ok()
            .and_then(|raw| decode_identity(&raw))
            .map(|(uuid, _reserved)| uuid);
        let published = match uuid {
            Some(uuid) => list_dir(&dir, uuid)
                .ok()
                .and_then(|l| select_root(&dir, uuid, &l.roots, &metrics))
                .map_or_else(Published::empty, Published::from_root),
            None => Published::empty(),
        };
        Self {
            dir,
            // A reader with no identity still functions; it simply has no
            // heads, so nothing ever resolves to a pack.
            uuid: uuid.unwrap_or(StoreUuid([0; 16])),
            options: SidecarOptions::default(),
            published: RwLock::new(published),
            readers: RwLock::new(HashMap::new()),
            writer: None,
            _lock: None,
            metrics,
        }
    }

    /// The sidecar directory.
    #[must_use]
    pub fn dir(&self) -> &Path { &self.dir }

    /// This sidecar's persisted store identity.
    #[must_use]
    pub fn uuid(&self) -> StoreUuid { self.uuid }

    /// Number of published heads. One stream contributes one head per distinct
    /// identity it has been snapshotted under.
    #[must_use]
    pub fn head_count(&self) -> usize { self.read_published().sorted.len() }

    /// Every published head's stream name, deduplicated and sorted. Because a
    /// pack record is self-describing, no reverse `id -> name` side map is
    /// needed (the retired path required one).
    ///
    /// This materializes every name and is therefore **not** the administrative
    /// enumeration path: use [`pin_root`](Self::pin_root) and
    /// [`scan`](Self::scan), which are bounded.
    #[must_use]
    pub fn stream_names(&self) -> Vec<String> {
        let published = self.read_published();
        let mut v: Vec<String> = published.by_stream.keys().cloned().collect();
        v.sort();
        v
    }

    fn read_published(&self) -> std::sync::RwLockReadGuard<'_, Published> {
        self.published.read().unwrap_or_else(|e| e.into_inner())
    }

    // -----------------------------------------------------------------
    // Load
    // -----------------------------------------------------------------

    /// Resolve the record published for exactly `(stream, compatibility)`.
    ///
    /// **Never fails.** A missing head, a head under a different identity, a
    /// missing pack, a corrupt frame, a record whose bytes do not match the
    /// leaf's `(offset, length, hash)`, a record naming a different stream, an
    /// unknown record format — every one of them is a
    /// [`SnapshotLookup::Miss`] carrying its reason, and every reason means the
    /// caller replays.
    #[must_use]
    pub fn load(
        &self,
        stream: &str,
        compatibility: SnapshotCompatibility,
    ) -> SnapshotLookup<Record> {
        let entry = {
            let published = self.read_published();
            match published.head(stream, &compatibility) {
                Some(e) => e.clone(),
                None => {
                    return SnapshotLookup::Miss(
                        match published.foreign_head(stream, &compatibility) {
                            Some(other) => SnapshotMiss::Incompatible {
                                stored: other.compatibility,
                            },
                            None => SnapshotMiss::Absent,
                        },
                    );
                }
            }
        };
        match self.resolve(&entry) {
            Some(rec) if rec.stream_name == stream => SnapshotLookup::Hit(rec),
            _ => {
                SidecarMetrics::bump(&self.metrics.degraded_loads);
                SnapshotLookup::Miss(SnapshotMiss::Unreadable)
            }
        }
    }

    /// Read and fully validate the record a root leaf names.
    fn resolve(&self, entry: &RootEntry) -> Option<Record> {
        if entry.frame_len > MAX_FRAME_LEN {
            return None;
        }
        let file = self.pack_reader(entry.pack_seq)?;
        let mut buf = vec![0u8; entry.frame_len as usize];
        pread_exact(&file, &mut buf, entry.offset).ok()?;
        let frame = decode_frame(&buf)?;
        // Bind the leaf to exact bytes: the frame must be the one the root
        // named, not merely *a* valid frame at that offset.
        if frame.body_crc != entry.record_crc
            || frame.total_len != entry.frame_len
        {
            return None;
        }
        let rec = decode_record(&frame.body)?;
        // The key and coverage the leaf routes on must be the ones the record
        // itself claims, or the leaf is not describing this record.
        if rec.compatibility != entry.compatibility
            || rec.coverage != entry.coverage
        {
            return None;
        }
        // An unknown trust mode is invalid (ADR 0002 §1), never "probably
        // fine".
        trust_from_u8(rec.trust_mode)?;
        Some(rec)
    }

    /// A cached read handle for a pack, accepting either name during the
    /// `.open` -> `.pack` seal transition.
    fn pack_reader(&self, seq: u64) -> Option<Arc<File>> {
        if let Some(f) =
            self.readers.read().unwrap_or_else(|e| e.into_inner()).get(&seq)
        {
            return Some(Arc::clone(f));
        }
        let sealed = self.dir.join(pack_name(self.uuid, seq, true));
        let open = self.dir.join(pack_name(self.uuid, seq, false));
        let file = File::open(&sealed).or_else(|_| File::open(&open)).ok()?;
        let file = Arc::new(file);
        self.readers
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .insert(seq, Arc::clone(&file));
        Some(file)
    }

    // -----------------------------------------------------------------
    // Save
    // -----------------------------------------------------------------

    /// Persist `record` and publish a new root, using this sidecar's
    /// configured [`SaveMode`].
    ///
    /// # Errors
    ///
    /// [`PackSidecarError`] only when the write plumbing failed. A refused save
    /// is a successful call reporting its [`SnapshotSaveOutcome`].
    pub fn save(
        &self,
        record: &Record,
    ) -> Result<SnapshotSaveOutcome, PackSidecarError> {
        self.save_with_mode(record, self.options.mode)
    }

    /// Persist `record` under an explicit publication mode.
    ///
    /// # The publication rule
    ///
    /// Heads are keyed by `(stream, compatibility)`, so a record under a new
    /// identity never touches an old one's head and a *late* save under an old
    /// identity can neither hide nor delete a new one (ADR 0002 §1). Within one
    /// key the decision is [`publication_decision`]'s, including the
    /// equal-coverage rule: the current record is read and fully validated
    /// before anything supersedes it, so a corrupt head can be repaired without
    /// weakening split-brain detection.
    ///
    /// # Errors
    ///
    /// [`PackSidecarError`] only when the write plumbing failed.
    pub fn save_with_mode(
        &self,
        record: &Record,
        mode: SaveMode,
    ) -> Result<SnapshotSaveOutcome, PackSidecarError> {
        let Some(writer) = self.writer.as_ref() else {
            return Err(PackSidecarError::ReadOnly(self.dir.clone()));
        };
        let mut w = writer.lock().unwrap_or_else(|e| e.into_inner());

        let body = encode_record(record);
        let body_crc = format::crc32(&body);

        // --- head comparison, serialized with publication -----------------
        //
        // The identity bytes compared at equal coverage are the encoded record
        // body: everything that distinguishes two records, and nothing that
        // does not (the frame around it, its offset, which pack it landed in).
        let current = {
            let published = self.read_published();
            published
                .head(&record.stream_name, &record.compatibility)
                .map_or(CurrentHead::Vacant, |e| {
                    CurrentHead::Published(e.coverage)
                })
        };
        let decision =
            publication_decision(current, record.coverage, &body, || {
                let entry = self
                    .read_published()
                    .head(&record.stream_name, &record.compatibility)
                    .cloned()?;
                self.resolve(&entry).map(|rec| encode_record(&rec))
            });
        match decision {
            SnapshotSaveOutcome::Idempotent => {
                SidecarMetrics::bump(&self.metrics.idempotent_saves);
                return Ok(decision);
            }
            SnapshotSaveOutcome::CoverageRegressed { .. } => {
                SidecarMetrics::bump(&self.metrics.coverage_regressions);
                return Ok(decision);
            }
            SnapshotSaveOutcome::Conflict { .. } => {
                SidecarMetrics::bump(&self.metrics.conflicts);
                return Ok(decision);
            }
            SnapshotSaveOutcome::Repaired => {
                SidecarMetrics::bump(&self.metrics.repairs);
            }
            _ => {}
        }

        // --- 1. append + re-read/validate the frame -----------------------
        let frame = encode_frame(&body);
        let frame_len = frame.len() as u32;
        self.maybe_roll(&mut w, u64::from(frame_len))?;
        let offset = w.active.len;
        let pack_seq = w.active.seq;
        hook(&w, "record:append")?;
        w.active.file.write_all(&frame)?;
        w.active.len += u64::from(frame_len);
        w.active.idx.push(IndexEntry { offset, frame_len });
        SidecarMetrics::bump(&self.metrics.records_written);

        // Re-read what we just wrote through a positional read and validate
        // the complete frame (ADR 0002 publication step 1).
        {
            let mut back = vec![0u8; frame_len as usize];
            pread_exact(&w.active.file, &mut back, offset)?;
            let ok =
                decode_frame(&back).is_some_and(|f| f.body_crc == body_crc);
            if !ok {
                return Err(PackSidecarError::Io(io::Error::other(
                    "snapshot record failed read-back validation",
                )));
            }
        }

        // --- 2. build the new head set ------------------------------------
        let entry = RootEntry {
            stream_name: record.stream_name.clone(),
            compatibility: record.compatibility,
            coverage: record.coverage,
            pack_seq,
            offset,
            frame_len,
            record_crc: body_crc,
        };
        let mut entries: Vec<RootEntry> = {
            let published = self.read_published();
            published
                .sorted
                .iter()
                .filter(|e| {
                    e.stream_name != record.stream_name
                        || e.compatibility != record.compatibility
                })
                .cloned()
                .collect()
        };
        entries.push(entry.clone());
        entries.sort_by(scan_order);

        // --- 3. durable closure promotion ---------------------------------
        //
        // The closure of the new root is every pack byte range it names, plus
        // the directory entries that make those packs findable. Promotion
        // consults the proof ledger and syncs whatever it cannot prove:
        //
        // - the **active** pack through the highest record end the new root
        //   references, which sweeps in every inherited `Buffered`-only record
        //   for unrelated streams;
        // - any **other** referenced pack with no ledger proof. In steady state
        //   `roll` already proved every pack it sealed, so this costs nothing;
        //   after a reopen the ledger is empty by design, so the first Durable
        //   save re-promotes rather than trusting mere readability. (An fsync
        //   on a read-only descriptor is valid and is what these already-sealed
        //   files need.)
        // - the directory, whenever a creation/rename entry is unproven.
        if mode == SaveMode::Durable {
            let required = entries
                .iter()
                .filter(|e| e.pack_seq == pack_seq)
                .map(|e| e.offset + u64::from(e.frame_len))
                .max()
                .unwrap_or(w.active.len);
            let proven = w.proven_frontier.get(&pack_seq).copied().unwrap_or(0);
            if required > proven {
                hook(&w, "record:sync")?;
                w.active.file.sync_data()?;
                SidecarMetrics::bump(&self.metrics.barriers);
                self.metrics
                    .promoted_bytes
                    .fetch_add(required - proven, Ordering::Relaxed);
                w.proven_frontier.insert(pack_seq, required);
            }
            let mut unproven: Vec<u64> = entries
                .iter()
                .map(|e| e.pack_seq)
                .filter(|s| {
                    *s != pack_seq && !w.proven_frontier.contains_key(s)
                })
                .collect();
            unproven.sort_unstable();
            unproven.dedup();
            for seq in unproven {
                let Some(file) = self.pack_reader(seq) else { continue };
                hook(&w, "closure:sync")?;
                file.sync_data()?;
                SidecarMetrics::bump(&self.metrics.barriers);
                let len = file.metadata().map(|m| m.len()).unwrap_or(0);
                self.metrics.promoted_bytes.fetch_add(len, Ordering::Relaxed);
                w.proven_frontier.insert(seq, len);
            }
            if !w.dir_proven {
                hook(&w, "pack:dirsync")?;
                sync_dir(&self.dir)?;
                SidecarMetrics::bump(&self.metrics.barriers);
                w.dir_proven = true;
            }
        }

        // --- 4/5. publish the root ----------------------------------------
        let generation = self.publish_root(&mut w, entries.clone(), mode)?;

        // --- 6. in-memory publication -------------------------------------
        {
            let mut published =
                self.published.write().unwrap_or_else(|e| e.into_inner());
            let heads = published
                .by_stream
                .entry(record.stream_name.clone())
                .or_default();
            match heads
                .iter_mut()
                .find(|e| e.compatibility == record.compatibility)
            {
                Some(slot) => *slot = entry,
                None => {
                    heads.push(entry);
                    heads.sort_by(scan_order);
                }
            }
            published.generation = Some(generation);
            published.sorted = Arc::new(entries);
        }

        self.maybe_prune(&mut w)?;
        Ok(decision)
    }

    /// Write and atomically install the next root generation, returning its
    /// generation number.
    fn publish_root(
        &self,
        w: &mut Writer,
        entries: Vec<RootEntry>,
        mode: SaveMode,
    ) -> Result<u64, PackSidecarError> {
        let hook = w.hook.clone();
        let generation =
            w.ids.alloc(&self.dir, self.uuid, &self.metrics, hook.as_ref())?;
        let root = Root {
            uuid: self.uuid,
            generation,
            mode,
            active_pack: Some(w.active.seq),
            active_pack_len: w.active.len,
            sealed_packs: w.sealed.clone(),
            entries,
        };
        let path = self.dir.join(root_name(self.uuid, generation));
        atomic_write(
            &path,
            &encode_root(&root),
            mode == SaveMode::Durable,
            &self.metrics,
            hook.as_ref(),
            "root",
        )?;
        w.roots_on_disk.push(generation);
        SidecarMetrics::bump(&self.metrics.roots_published);
        if mode == SaveMode::Durable {
            // The root's own directory entry is now durable, which also
            // proves every earlier creation/rename entry in this directory.
            w.dir_proven = true;
        } else {
            // A buffered rename made the new root atomically visible but
            // proved nothing about the directory.
            w.dir_proven = false;
        }
        Ok(generation)
    }

    // -----------------------------------------------------------------
    // Bounded administrative enumeration
    // -----------------------------------------------------------------

    /// Pin the discovery root this sidecar currently serves, or `None` when no
    /// root resolved and the store has no snapshots.
    ///
    /// Takes no lock, creates nothing and repairs nothing, so an offline tool
    /// may pin a live store's root. The lease is the published head list
    /// itself: while the pin lives, that immutable view stays readable no
    /// matter how many generations the writer publishes on top of it.
    ///
    /// # Its cost, honestly
    ///
    /// `O(1)` — a refcount bump. What is *not* `O(1)` is the flat root behind
    /// it: `bn-ozi5` documented that v1 keeps every head resident, so a pin
    /// borrows an `O(N)` list rather than walking an `O(height)` page path.
    /// When ADR 0002's copy-on-write discovery tree lands, the lease becomes
    /// the page path and this signature does not change.
    #[must_use]
    pub fn pin_root(&self) -> Option<PinnedSnapshotRoot> {
        let published = self.read_published();
        let generation = published.generation?;
        let lease: Arc<dyn std::any::Any + Send + Sync> =
            Arc::clone(&published.sorted) as Arc<Vec<RootEntry>>;
        Some(PinnedSnapshotRoot::new(
            SnapshotRootId::new(self.uuid.0, generation),
            lease,
        ))
    }

    /// Read one bounded page of `pin`'s heads, resuming strictly after
    /// `cursor`.
    ///
    /// # Its law
    ///
    /// - the page holds at most `limit` entries, itself capped at
    ///   [`MAX_SNAPSHOT_SCAN_LIMIT`](crate::snapshot::MAX_SNAPSHOT_SCAN_LIMIT);
    /// - a cursor minted against a different root is
    ///   [`Rejected`](SnapshotScanDiagnostic::Rejected), never reinterpreted;
    /// - every returned entry is **validated** — the frame is read, the leaf's
    ///   `(offset, length, hash)` binding checked and the record decoded;
    /// - a head that does not validate is *skipped* and counted in
    ///   [`Partial`](SnapshotScanDiagnostic::Partial), never fabricated and
    ///   never repaired. Only [`Complete`](SnapshotScanDiagnostic::Complete)
    ///   licenses a destructive caller to act.
    ///
    /// Work is `O(log N + limit)`; page memory is `O(limit)`.
    #[must_use]
    pub fn scan(
        &self,
        pin: &PinnedSnapshotRoot,
        cursor: Option<&SnapshotScanCursor>,
        limit: NonZeroU32,
    ) -> SnapshotScanPage {
        let published = self.read_published();
        let root_id = match published.generation {
            Some(g) => SnapshotRootId::new(self.uuid.0, g),
            None => return SnapshotScanPage::rejected(),
        };
        if pin.id() != root_id {
            return SnapshotScanPage::rejected();
        }
        if let Some(c) = cursor
            && c.root() != root_id
        {
            return SnapshotScanPage::rejected();
        }
        let heads = Arc::clone(&published.sorted);
        drop(published);

        // Seek: O(log N) to the first key strictly after the cursor.
        let start = match cursor {
            None => 0,
            Some(c) => heads.partition_point(|e| {
                (e.stream_name.as_str(), &e.compatibility)
                    <= (c.after().stream_id.as_str(), &c.after().compatibility)
            }),
        };

        let limit = clamp_scan_limit(limit) as usize;
        let end = heads.len().min(start.saturating_add(limit));
        let mut entries = Vec::with_capacity(end - start);
        let mut unresolved = 0u64;
        for head in &heads[start..end] {
            let Some(rec) = self.resolve(head) else {
                unresolved += 1;
                continue;
            };
            let Some(trust) = trust_from_u8(rec.trust_mode) else {
                unresolved += 1;
                continue;
            };
            entries.push(SnapshotScanEntry {
                key: SnapshotScanKey {
                    stream_id:     head.stream_name.clone(),
                    compatibility: head.compatibility,
                },
                coverage: head.coverage,
                trust,
                state_len: rec.state.len() as u64,
                frame_len: u64::from(head.frame_len),
            });
        }

        // The cursor names the last key *visited*, not the last one returned,
        // so a page whose entries were all skipped still makes progress.
        let next_cursor = (end < heads.len()).then(|| {
            let last = &heads[end - 1];
            SnapshotScanCursor::new(
                root_id,
                SnapshotScanKey {
                    stream_id:     last.stream_name.clone(),
                    compatibility: last.compatibility,
                },
            )
        });
        SnapshotScanPage {
            entries,
            next_cursor,
            diagnostic: if unresolved == 0 {
                SnapshotScanDiagnostic::Complete
            } else {
                SnapshotScanDiagnostic::Partial { unresolved }
            },
        }
    }

    // -----------------------------------------------------------------
    // Roll / seal
    // -----------------------------------------------------------------

    fn maybe_roll(
        &self,
        w: &mut Writer,
        incoming: u64,
    ) -> Result<(), PackSidecarError> {
        let is_empty = w.active.len <= PACK_HEADER_LEN;
        if is_empty || w.active.len + incoming <= self.options.max_pack_bytes {
            return Ok(());
        }
        self.roll(w)
    }

    /// Seal the active build pack and start a new one.
    ///
    /// Ordering (ADR 0002 "Build-pack protocol"), each step interruptible by
    /// the crash tests: append index+footer, sync through the footer,
    /// rename/seal, sync the directory, then create the successor. A sealed
    /// pack is never opened for append again.
    fn roll(&self, w: &mut Writer) -> Result<(), PackSidecarError> {
        let seq = w.active.seq;
        let index_offset = w.active.len;
        let blob = encode_index_and_footer(index_offset, &w.active.idx);
        hook(w, "roll:index")?;
        w.active.file.write_all(&blob)?;
        w.active.len += blob.len() as u64;

        hook(w, "roll:sync")?;
        w.active.file.sync_all()?;
        SidecarMetrics::bump(&self.metrics.barriers);

        hook(w, "roll:rename")?;
        std::fs::rename(
            self.dir.join(pack_name(self.uuid, seq, false)),
            self.dir.join(pack_name(self.uuid, seq, true)),
        )?;

        hook(w, "roll:dirsync")?;
        sync_dir(&self.dir)?;
        SidecarMetrics::bump(&self.metrics.barriers);
        w.dir_proven = true;
        // The whole sealed pack is durable, so anything the next root names
        // inside it needs no further promotion.
        w.proven_frontier.insert(seq, w.active.len);
        w.sealed.push(seq);
        SidecarMetrics::bump(&self.metrics.packs_rolled);

        hook(w, "roll:newpack")?;
        let fault = w.hook.clone();
        let next =
            w.ids.alloc(&self.dir, self.uuid, &self.metrics, fault.as_ref())?;
        w.active = Self::create_pack(&self.dir, self.uuid, next)?;
        // A brand-new `.open` file has an unproven directory entry.
        w.dir_proven = false;
        // Drop the cached read handle for the sealed sequence so later reads
        // reopen by the new name. (An already-handed-out `Arc<File>` keeps
        // working — same inode — which is exactly the stale-descriptor case
        // the contract permits.)
        self.readers.write().unwrap_or_else(|e| e.into_inner()).remove(&seq);
        Ok(())
    }

    /// Force a roll (test/administrative hook; also what a future compaction
    /// would call before publishing the root that names the new pack).
    pub fn roll_now(&self) -> Result<(), PackSidecarError> {
        let Some(writer) = self.writer.as_ref() else {
            return Err(PackSidecarError::ReadOnly(self.dir.clone()));
        };
        let mut w = writer.lock().unwrap_or_else(|e| e.into_inner());
        self.roll(&mut w)
    }

    fn create_pack(
        dir: &Path,
        uuid: StoreUuid,
        seq: u64,
    ) -> io::Result<ActivePack> {
        let path = dir.join(pack_name(uuid, seq, false));
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)?;
        let header = encode_pack_header(uuid, seq);
        file.write_all(&header)?;
        Ok(ActivePack { seq, file, len: header.len() as u64, idx: Vec::new() })
    }

    // -----------------------------------------------------------------
    // Root pruning (the GC ordering, without content deletion)
    // -----------------------------------------------------------------

    fn maybe_prune(&self, w: &mut Writer) -> Result<(), PackSidecarError> {
        if w.roots_on_disk.len() <= self.options.prune_roots_at {
            return Ok(());
        }
        self.prune_roots(w)
    }

    /// Remove obsolete root descriptors, retaining at least
    /// [`SidecarOptions::retain_roots`] (minimum 2) complete generations.
    ///
    /// This is the *destructive* half of GC, and it is a durability boundary
    /// independent of the save mode (ADR 0002's GC amendment): the surviving
    /// roots' directory entries are made durable **before** anything is
    /// unlinked, and the directory is synced again afterwards. v1 stops here —
    /// no pack or record content is ever deleted — but the ordering is already
    /// the one content deletion has to slot into, so adding reclamation later
    /// needs no format change. The active build pack is never collected.
    pub fn prune_roots_now(&self) -> Result<(), PackSidecarError> {
        let Some(writer) = self.writer.as_ref() else {
            return Err(PackSidecarError::ReadOnly(self.dir.clone()));
        };
        let mut w = writer.lock().unwrap_or_else(|e| e.into_inner());
        self.prune_roots(&mut w)
    }

    fn prune_roots(&self, w: &mut Writer) -> Result<(), PackSidecarError> {
        let retain = self.options.retain_roots.max(2);
        if w.roots_on_disk.len() <= retain {
            return Ok(());
        }
        w.roots_on_disk.sort_unstable();
        let cut = w.roots_on_disk.len() - retain;
        let doomed: Vec<u64> = w.roots_on_disk[..cut].to_vec();

        // Barrier 1: the roots we are keeping must survive a crash that
        // happens between the unlinks below.
        hook(w, "prune:presync")?;
        sync_dir(&self.dir)?;
        SidecarMetrics::bump(&self.metrics.barriers);

        hook(w, "prune:unlink")?;
        for generation in &doomed {
            let path = self.dir.join(root_name(self.uuid, *generation));
            match std::fs::remove_file(&path) {
                Ok(()) => SidecarMetrics::bump(&self.metrics.roots_pruned),
                Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                Err(e) => return Err(e.into()),
            }
        }

        // Barrier 2: the removals are durable before any dependent content
        // deletion could be considered (v1 does none).
        hook(w, "prune:dirsync")?;
        sync_dir(&self.dir)?;
        SidecarMetrics::bump(&self.metrics.barriers);
        w.roots_on_disk.drain(..cut);
        Ok(())
    }

    // -----------------------------------------------------------------
    // Recovery
    // -----------------------------------------------------------------

    /// Recover one `.open` pack: complete an interrupted seal, or truncate a
    /// torn tail. Only ever called by a writer holding the lock.
    fn recover_open_pack(
        dir: &Path,
        uuid: StoreUuid,
        seq: u64,
        metrics: &SidecarMetrics,
    ) -> io::Result<RecoveredPack> {
        let path = dir.join(pack_name(uuid, seq, false));
        let mut file = OpenOptions::new().read(true).write(true).open(&path)?;
        let len = file.metadata()?.len();

        // A valid pack header is the price of entry; without it the file is
        // not ours to append to (and is left untouched).
        if len < PACK_HEADER_LEN {
            return Ok(RecoveredPack::Unusable);
        }
        let mut hdr = vec![0u8; PACK_HEADER_LEN as usize];
        pread_exact(&file, &mut hdr, 0)?;
        match decode_pack_header(&hdr) {
            Some((u, s)) if u == uuid && s == seq => {}
            _ => return Ok(RecoveredPack::Unusable),
        }

        // Case A: the roll wrote and synced index+footer but the rename never
        // landed. Finish the seal rather than truncating a durable footer.
        if let Some(footer) = read_trailing_footer(&file, len)?
            && footer.index_offset
                + footer.index_len
                + format::FOOTER_LEN as u64
                == len
        {
            std::fs::rename(&path, dir.join(pack_name(uuid, seq, true)))?;
            sync_dir(dir)?;
            SidecarMetrics::bump(&metrics.barriers);
            return Ok(RecoveredPack::Sealed);
        }

        // Case B: scan forward frame by frame; the first frame that does not
        // fully validate ends the pack. Only the writer truncates.
        let mut off = PACK_HEADER_LEN;
        let mut buf = Vec::new();
        loop {
            let mut head = [0u8; format::FRAME_HEADER_LEN];
            if off + format::FRAME_HEADER_LEN as u64 > len {
                break;
            }
            if pread_exact(&file, &mut head, off).is_err() {
                break;
            }
            let Some(total) = peek_frame_len(&head) else { break };
            if off + u64::from(total) > len {
                break;
            }
            buf.resize(total as usize, 0);
            if pread_exact(&file, &mut buf, off).is_err() {
                break;
            }
            if decode_frame(&buf).is_none() {
                break;
            }
            off += u64::from(total);
        }

        let mut idx = Vec::new();
        // Rebuild the index entries for the frames that survived, so a later
        // roll can still write a complete index.
        {
            let mut scan = PACK_HEADER_LEN;
            while scan < off {
                let mut head = [0u8; format::FRAME_HEADER_LEN];
                pread_exact(&file, &mut head, scan)?;
                let Some(total) = peek_frame_len(&head) else { break };
                idx.push(IndexEntry { offset: scan, frame_len: total });
                scan += u64::from(total);
            }
        }

        if off < len {
            file.set_len(off)?;
            file.sync_all()?;
            SidecarMetrics::bump(&metrics.barriers);
            metrics
                .tail_truncated_bytes
                .fetch_add(len - off, Ordering::Relaxed);
        }
        file.seek(SeekFrom::Start(off))?;
        Ok(RecoveredPack::Active(ActivePack { seq, file, len: off, idx }))
    }
}

enum RecoveredPack {
    Active(ActivePack),
    Sealed,
    Unusable,
}

fn read_trailing_footer(file: &File, len: u64) -> io::Result<Option<Footer>> {
    let n = format::FOOTER_LEN as u64;
    if len < PACK_HEADER_LEN + n {
        return Ok(None);
    }
    let mut buf = vec![0u8; format::FOOTER_LEN];
    pread_exact(file, &mut buf, len - n)?;
    Ok(decode_footer(&buf))
}

fn hook(w: &Writer, step: &str) -> io::Result<()> {
    match &w.hook {
        Some(h) => h(step),
        None => Ok(()),
    }
}

/// Select the highest final, independently resolvable, valid root.
///
/// Falls back progressively: a root that does not decode, belongs to another
/// UUID, or names a pack that is missing or shorter than the frontier it
/// recorded is rejected and the next-older generation is tried. Exhausting
/// them yields `None` — "no snapshots", i.e. replay everything.
///
/// # Where sharded discovery would slot in
///
/// This root descriptor is a single flat list of heads, which makes a save
/// `O(N)` in published heads and open `O(N)` too. ADR 0002's bounded design
/// replaces the flat entry list with an immutable content-addressed
/// copy-on-write radix tree so a save rewrites `O(k log N)` pages. The
/// substitution is local: the root would carry a `root_page_hash` instead of
/// `entries`, `select_root` would validate the page path lazily, and `load`
/// would walk `O(log N)` pages instead of an in-memory map. Nothing outside
/// this file and `format::Root` would change.
fn select_root(
    dir: &Path,
    uuid: StoreUuid,
    generations: &[u64],
    metrics: &SidecarMetrics,
) -> Option<Root> {
    for generation in generations.iter().rev() {
        let path = dir.join(root_name(uuid, *generation));
        let Ok(raw) = std::fs::read(&path) else {
            SidecarMetrics::bump(&metrics.roots_rejected);
            continue;
        };
        let Some(root) = decode_root(&raw) else {
            SidecarMetrics::bump(&metrics.roots_rejected);
            continue;
        };
        if root.uuid != uuid || root.generation != *generation {
            SidecarMetrics::bump(&metrics.roots_rejected);
            continue;
        }
        if root_resolves(dir, uuid, &root) {
            return Some(root);
        }
        SidecarMetrics::bump(&metrics.roots_rejected);
    }
    None
}

/// Bounded resolvability check: every pack the root references must exist,
/// and the active pack must be at least as long as the frontier the root
/// recorded. Per-record validation stays lazy (a bad leaf is a miss, and a
/// miss is a replay), so this is `O(#packs)`, not `O(#heads)`.
fn root_resolves(dir: &Path, uuid: StoreUuid, root: &Root) -> bool {
    let mut needed: Vec<u64> = root.sealed_packs.clone();
    needed.extend(root.entries.iter().map(|e| e.pack_seq));
    if let Some(a) = root.active_pack {
        needed.push(a);
    }
    needed.sort_unstable();
    needed.dedup();
    for seq in needed {
        let sealed = dir.join(pack_name(uuid, seq, true));
        let open = dir.join(pack_name(uuid, seq, false));
        let meta =
            std::fs::metadata(&sealed).or_else(|_| std::fs::metadata(&open));
        let Ok(meta) = meta else { return false };
        if Some(seq) == root.active_pack && meta.len() < root.active_pack_len {
            // The root names bytes that are no longer there (a torn tail was
            // truncated, or a buffered write never reached the disk).
            return false;
        }
    }
    true
}
