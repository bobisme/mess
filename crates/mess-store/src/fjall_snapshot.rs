//! [`FjallSnapshotBackend`]: the **production** snapshot path — snapshot *heads*
//! in mess-index's fjall [`SnapshotHeads`](mess_index::meta::MetaTable::SnapshotHeads)
//! table (O(1) point lookup, journal-buffered, rebuildable per I5) plus state
//! **blobs** in a plain content-addressed directory.
//!
//! # Why this shape, and why the API does not change
//!
//! Phase 1 shipped the snapshot *API* — [`SnapshotStore`], `save_snapshot::<A>`,
//! `load_cached::<A>` — backed by an explicitly throwaway in-memory map on
//! [`MockBackend`](crate::MockBackend). This module is the Phase 4 body behind
//! that same API: a [`SnapshotStore`] whose internals are the real
//! "`snapshot_head` lookup → blob read → tail replay" load path of
//! `docs/spec/05-fold-certificates.md`. Because it is *another* [`SnapshotStore`]
//! impl, nothing in [`EventStore`](crate::EventStore) changes:
//! `save_snapshot` / `load_cached` are generic over the backend, the mock still
//! serves the pure unit/property tests, and swapping the mock for
//! `FjallSnapshotBackend<_>` is a construction-site choice, not an API change.
//!
//! # Composition, not a monolith
//!
//! [`SnapshotStore`] is a **supertrait of [`Backend`]** (a stored snapshot is
//! orthogonal to the commit authority — D1 — but the trait ties the two error
//! types together). So the production snapshot store is a *wrapper*: it holds
//! the real event-log [`Backend`] `B` and **delegates** every log operation to
//! it, adding only the snapshot keyspace. Its error type
//! [`SnapshotBackendError`] unifies `B`'s error with the fjall/blob failures the
//! snapshot side can hit, so the whole thing is still one `Backend` with one
//! `Error`. For tests, `FjallSnapshotBackend<MockBackend>` is a fully real
//! snapshot store over the in-memory log.
//!
//! # Blob storage: a positional blob dir, deliberately **not** content-hashed
//!
//! Each state blob is written to `<root>/blobs/<stream_id_hex>/<version>.blob`
//! — addressed by `(interned stream id, covered version)`, the exact pair the
//! [`SnapshotHead`] carries. Two properties fall out, both load-bearing:
//!
//! - **Crash-safe pointer/blob agreement.** Save writes the blob **first**,
//!   then advances the fjall head. The head only ever names a version whose
//!   blob is already on disk. A crash after the blob write but before the head
//!   advance leaves the head pointing at the *previous* version's blob (still
//!   present) — stale, never dangling. A crash that loses the buffered head
//!   advance leaves a head naming a missing blob → [`load_snapshot`] reads
//!   `None` → the load self-heals by full replay (I5). Either way the answer is
//!   correct.
//! - **No 64-bit collision correctness risk.** A content-addressed dir would
//!   need a digest; `mess-store` pulls in no crypto hash, and a 64-bit
//!   non-crypto digest can (astronomically rarely) collide, and a collision
//!   there means one blob overwrites another and a later load decodes the
//!   *wrong* state — silently law-violating. Positional addressing is
//!   collision-free by construction, and dedup is not a goal at v1. Each blob
//!   file still carries a length + checksum header, so a torn/corrupt file is
//!   detected and also degrades to full replay rather than a wrong answer.
//!
//! [`SnapshotHeads`]: mess_index::meta::MetaTable::SnapshotHeads
//! [`SnapshotHead`]: mess_index::meta::SnapshotHead

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use mess_index::meta::{
    CommitGroup, MetaError, MetaStore, MetaTable, SnapshotHead, StreamId,
};

use crate::backend::{
    AppendError, Appended, Backend, RecordToAppend, StoredRecord,
};
use crate::snapshot::{
    BlobPtr, SnapshotRef, SnapshotStore, StoredSnapshot, interim_stream_id,
};
use crate::version::Version;

/// Failure of a [`FjallSnapshotBackend`] operation.
///
/// Unifies the wrapped log backend's error with the snapshot side's fjall and
/// blob-I/O failures so the composed type is still a single [`Backend`] with a
/// single `Error`.
#[derive(Debug, thiserror::Error)]
pub enum SnapshotBackendError<E> {
    /// The wrapped event-log backend failed.
    #[error("event-log backend: {0}")]
    Inner(#[source] E),
    /// The fjall snapshot-heads table failed.
    #[error("snapshot meta: {0}")]
    Meta(#[from] MetaError),
    /// A snapshot blob could not be read or written.
    #[error("snapshot blob io: {0}")]
    Blob(#[from] std::io::Error),
}

/// Magic prefix on every blob file, so a truncated-to-empty or foreign file is
/// rejected before its length/checksum are trusted.
const BLOB_MAGIC: &[u8; 4] = b"MSB1";
/// Fixed blob header: magic(4) + payload_len(8 LE) + checksum(8 LE).
const BLOB_HEADER: usize = 4 + 8 + 8;

/// The production snapshot store: a wrapper that delegates the event log to `B`
/// and adds fjall-backed snapshot heads + a positional blob dir.
///
/// Cheap to clone when `B` is (the fjall handle, blob root, and save counter
/// are all shared behind `Arc`), matching [`EventStore`](crate::EventStore)'s
/// clone-is-share contract.
#[derive(Clone)]
pub struct FjallSnapshotBackend<B> {
    inner: B,
    meta: Arc<MetaStore>,
    blob_root: Arc<PathBuf>,
    /// Monotonic pseudo-global position stamped on each saved head. The event
    /// [`Backend`] seam exposes no global position for a stream head, so saves
    /// carry a strictly increasing counter instead — enough to keep the fjall
    /// high-water monotonic and to order snapshots; the true log position lands
    /// with the Phase 5 certificate. Seeded from the persisted high-water on
    /// open so it stays monotonic across a reopen.
    next_pos: Arc<AtomicU64>,
}

impl<B: std::fmt::Debug> std::fmt::Debug for FjallSnapshotBackend<B> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FjallSnapshotBackend")
            .field("inner", &self.inner)
            .field("blob_root", &self.blob_root)
            .finish_non_exhaustive()
    }
}

impl<B> FjallSnapshotBackend<B> {
    /// Open (creating if absent) a production snapshot store rooted at `root`,
    /// wrapping the event-log backend `inner`.
    ///
    /// `root/meta` holds the fjall snapshot-heads table; `root/blobs` holds the
    /// state-blob dir. Reopening the same `root` recovers every persisted
    /// snapshot head (that is the reopen-persistence capability the interim
    /// in-memory store never had).
    pub fn open(inner: B, root: impl AsRef<Path>) -> Result<Self, MetaError> {
        let root = root.as_ref();
        let meta = MetaStore::open(root.join("meta"))?;
        // Seed the save counter past the highest position the persisted heads
        // already reached, so positions stay monotonic across reopen.
        let seed = meta.high_water(MetaTable::SnapshotHeads)?;
        let blob_root = root.join("blobs");
        Ok(Self {
            inner,
            meta: Arc::new(meta),
            blob_root: Arc::new(blob_root),
            next_pos: Arc::new(AtomicU64::new(seed)),
        })
    }

    /// Borrow the wrapped event-log backend.
    pub fn inner(&self) -> &B {
        &self.inner
    }

    /// Force the snapshot-heads table's buffered writes to disk (fsync).
    ///
    /// Not needed for correctness — a lost buffered head just self-heals to
    /// full replay (I5) — but useful for a clean shutdown and to make a
    /// reopen-persistence test deterministic. Blobs are written+renamed
    /// eagerly, so only the fjall head buffer is at issue here.
    pub fn persist(&self) -> Result<(), MetaError> {
        self.meta.persist()
    }

    /// Path to the blob file for `(id, version)`:
    /// `<blobs>/<stream_id_hex>/<version>.blob`.
    fn blob_path(&self, id: StreamId, version: u64) -> PathBuf {
        self.blob_root
            .join(format!("{:016x}", id.0))
            .join(format!("{version}.blob"))
    }

    /// Write a blob file: `magic || len(8 LE) || checksum(8 LE) || bytes`.
    /// Written before the head advances, so the head never outruns the blob.
    fn write_blob(
        &self,
        id: StreamId,
        version: u64,
        bytes: &[u8],
    ) -> std::io::Result<()> {
        let path = self.blob_path(id, version);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let mut file = Vec::with_capacity(BLOB_HEADER + bytes.len());
        file.extend_from_slice(BLOB_MAGIC);
        file.extend_from_slice(&(bytes.len() as u64).to_le_bytes());
        file.extend_from_slice(&checksum(bytes).to_le_bytes());
        file.extend_from_slice(bytes);
        // Write to a temp sibling then rename, so a reader never sees a
        // half-written blob (the rename is atomic on the same dir).
        let tmp = path.with_extension("blob.tmp");
        std::fs::write(&tmp, &file)?;
        std::fs::rename(&tmp, &path)?;
        Ok(())
    }

    /// Read and verify a blob file. `Ok(None)` means "missing or corrupt" — a
    /// reason to fall back to full replay (I5), never an error to surface.
    fn read_blob(
        &self,
        id: StreamId,
        version: u64,
    ) -> std::io::Result<Option<Vec<u8>>> {
        let path = self.blob_path(id, version);
        let raw = match std::fs::read(&path) {
            Ok(raw) => raw,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Ok(None);
            }
            Err(e) => return Err(e),
        };
        Ok(decode_blob(&raw))
    }
}

/// Decode a blob file body, verifying magic, length and checksum. Any mismatch
/// yields `None` (treated as "no usable snapshot").
fn decode_blob(raw: &[u8]) -> Option<Vec<u8>> {
    if raw.len() < BLOB_HEADER || &raw[0..4] != BLOB_MAGIC {
        return None;
    }
    let len = u64::from_le_bytes(raw[4..12].try_into().ok()?) as usize;
    let want = u64::from_le_bytes(raw[12..20].try_into().ok()?);
    let body = raw.get(BLOB_HEADER..BLOB_HEADER + len)?;
    if body.len() != len || checksum(body) != want {
        return None;
    }
    Some(body.to_vec())
}

/// FNV-1a 64-bit — a dependency-free integrity check for the blob body. Not a
/// security hash; it only needs to catch a torn or truncated file (a corrupt
/// blob must degrade to full replay, never to a wrong answer).
fn checksum(bytes: &[u8]) -> u64 {
    const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;
    let mut h = OFFSET;
    for b in bytes {
        h ^= u64::from(*b);
        h = h.wrapping_mul(PRIME);
    }
    h
}

/// Encode the opaque `snapshot_ref` bytes the fjall [`SnapshotHead`] carries
/// verbatim: everything about a [`SnapshotRef`] that the head's own fields
/// (`covered_version`, `global_position`) do not already hold.
///
/// Layout: `fold_version(4 LE) || flags(1) || snapshot_ptr(8 LE)`.
/// `flags` bit 0 = `covers_empty_prefix`. The interned `stream_id`,
/// `stream_version`, and the reserved hashes are all recovered without being
/// stored (id from the stream name, version from the head, hashes are `None`).
fn encode_ref(snap: &SnapshotRef) -> Vec<u8> {
    let mut out = Vec::with_capacity(13);
    out.extend_from_slice(&snap.fold_version.to_le_bytes());
    out.push(u8::from(snap.covers_empty_prefix));
    out.extend_from_slice(&snap.snapshot_ptr.0.to_le_bytes());
    out
}

/// Reconstruct the parts of a [`SnapshotRef`] stored in the opaque head bytes.
/// Returns `(fold_version, covers_empty_prefix, snapshot_ptr)`.
fn decode_ref(bytes: &[u8]) -> Option<(u32, bool, u64)> {
    if bytes.len() < 13 {
        return None;
    }
    let fold_version = u32::from_le_bytes(bytes[0..4].try_into().ok()?);
    let covers_empty_prefix = bytes[4] != 0;
    let snapshot_ptr = u64::from_le_bytes(bytes[5..13].try_into().ok()?);
    Some((fold_version, covers_empty_prefix, snapshot_ptr))
}

impl<B: Backend> Backend for FjallSnapshotBackend<B> {
    type Error = SnapshotBackendError<B::Error>;

    async fn head(&self, stream_id: &str) -> Result<Version, Self::Error> {
        self.inner.head(stream_id).await.map_err(SnapshotBackendError::Inner)
    }

    async fn read_stream(
        &self,
        stream_id: &str,
        after: Version,
        limit: usize,
    ) -> Result<Vec<StoredRecord>, Self::Error> {
        self.inner
            .read_stream(stream_id, after, limit)
            .await
            .map_err(SnapshotBackendError::Inner)
    }

    async fn read_global(
        &self,
        after: Option<u64>,
        limit: usize,
    ) -> Result<Vec<StoredRecord>, Self::Error> {
        self.inner
            .read_global(after, limit)
            .await
            .map_err(SnapshotBackendError::Inner)
    }

    async fn append_batch(
        &self,
        stream_id: &str,
        expected: Version,
        records: &[RecordToAppend],
    ) -> Result<Appended, AppendError<Self::Error>> {
        match self.inner.append_batch(stream_id, expected, records).await {
            Ok(appended) => Ok(appended),
            Err(AppendError::Conflict { expected, actual }) => {
                Err(AppendError::Conflict { expected, actual })
            }
            Err(AppendError::Backend(e)) => Err(AppendError::Backend(
                SnapshotBackendError::Inner(e),
            )),
        }
    }
}

impl<B: Backend> SnapshotStore for FjallSnapshotBackend<B> {
    async fn save_snapshot(
        &self,
        stream_id: &str,
        snapshot: StoredSnapshot,
    ) -> Result<(), Self::Error> {
        let id = StreamId(interim_stream_id(stream_id));
        let snap = &snapshot.snapshot_ref;
        // An empty-prefix snapshot covers version 0 by convention; the flag is
        // what disambiguates "folds the empty prefix" from "folds index 0".
        let covered_version =
            if snap.covers_empty_prefix { 0 } else { snap.stream_version };

        // Blob first: the head must never name a version whose blob is absent.
        self.write_blob(id, covered_version, &snapshot.state_blob)?;

        // Then advance the fjall head in one atomic group. `global_position`
        // (also the group's high-water) is the monotonic save counter.
        let pos = self.next_pos.fetch_add(1, Ordering::SeqCst) + 1;
        let head = SnapshotHead {
            covered_version,
            global_position: pos,
            snapshot_ref: encode_ref(snap),
        };
        let mut group = CommitGroup::new(pos);
        group.snapshot_heads.push((id, head));
        self.meta.apply_group(&group)?;
        Ok(())
    }

    async fn load_snapshot(
        &self,
        stream_id: &str,
    ) -> Result<Option<StoredSnapshot>, Self::Error> {
        let id = StreamId(interim_stream_id(stream_id));
        // 1. snapshot_head lookup — a single fjall point read.
        let Some(head) = self.meta.snapshot_head(id)? else {
            return Ok(None);
        };
        let Some((fold_version, covers_empty_prefix, snapshot_ptr)) =
            decode_ref(&head.snapshot_ref)
        else {
            // A garbled head reads as "no usable snapshot": self-heal by replay.
            return Ok(None);
        };
        // 2. blob read — missing/corrupt blob => fall back to full replay (I5).
        let Some(state_blob) = self.read_blob(id, head.covered_version)? else {
            return Ok(None);
        };
        // 3. reconstruct the SnapshotRef the caller stored; the tail replay is
        //    the EventStore's job (`load_cached`).
        let snapshot_ref = SnapshotRef {
            stream_id: id.0,
            stream_version: head.covered_version,
            fold_version,
            covers_empty_prefix,
            event_prefix_hash: None,
            state_hash: None,
            snapshot_ptr: BlobPtr(snapshot_ptr),
        };
        Ok(Some(StoredSnapshot { snapshot_ref, state_blob }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn blob_roundtrip_and_corruption() {
        let payload = b"hello snapshot".to_vec();
        let mut raw = Vec::new();
        raw.extend_from_slice(BLOB_MAGIC);
        raw.extend_from_slice(&(payload.len() as u64).to_le_bytes());
        raw.extend_from_slice(&checksum(&payload).to_le_bytes());
        raw.extend_from_slice(&payload);
        assert_eq!(decode_blob(&raw).as_deref(), Some(payload.as_slice()));

        // Flip a payload byte: checksum mismatch -> None (no wrong answer).
        let mut torn = raw.clone();
        *torn.last_mut().unwrap() ^= 0xff;
        assert_eq!(decode_blob(&torn), None);

        // Truncated header / wrong magic -> None.
        assert_eq!(decode_blob(&raw[..3]), None);
        let mut bad_magic = raw.clone();
        bad_magic[0] = b'X';
        assert_eq!(decode_blob(&bad_magic), None);
    }

    #[test]
    fn ref_roundtrip() {
        let snap = SnapshotRef {
            stream_id: 42,
            stream_version: 7,
            fold_version: 3,
            covers_empty_prefix: true,
            event_prefix_hash: None,
            state_hash: None,
            snapshot_ptr: BlobPtr(0xdead_beef),
        };
        let (fv, empty, ptr) = decode_ref(&encode_ref(&snap)).unwrap();
        assert_eq!(fv, 3);
        assert!(empty);
        assert_eq!(ptr, 0xdead_beef);
        assert_eq!(decode_ref(&[0u8; 4]), None);
    }
}
