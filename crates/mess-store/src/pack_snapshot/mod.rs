//! [`PackSnapshotBackend`]: the storage-neutral, pack-based snapshot sidecar —
//! a drop-in [`SnapshotStore`] that replaces
//! [`FjallSnapshotBackend`](crate::FjallSnapshotBackend)'s fjall head table
//! and positional blob directory with **immutable self-describing packs plus a
//! bounded discovery root** (ADR 0002 §1, bone `bn-ozi5`).
//!
//! # The one law everything else serves
//!
//! *A snapshot is discardable acceleration.* Absence, corruption, an unknown
//! format, a deleted pack, a truncated root, a sidecar directory removed
//! wholesale — every one of them degrades to **replay on miss**. Nothing in
//! this module can make the event store unavailable, and nothing here ever
//! authorizes event-log retention. [`Sidecar::load`] is infallible by
//! construction: its return type is `Option<Record>`, not `Result`.
//!
//! # Shape on disk
//!
//! ```text
//! <root>/
//!   LOCK                                     one exclusive OS writer lock
//!   IDENTITY                                 one 128-bit store UUID, once
//!   pack-<uuid>-<0000000000000000>.pack      sealed: immutable, footer+index
//!   pack-<uuid>-<0000000000000001>.open      active build pack: footerless
//!   root-<uuid>-<0000000000000007>.root      discovery root generation 7
//!   *.tmp                                    reserved staging suffix
//! ```
//!
//! - **One writer.** [`Sidecar::open_writer`] takes an OS advisory lock
//!   (`mess_log::lock::StoreLock` — the same `flock`-backed primitive the log
//!   uses, so no new dependency and no second locking scheme). A second writer
//!   process fails loudly with [`PackSidecarError::Lock`]. Every clone of a
//!   [`PackSnapshotBackend`] shares one `Arc<Sidecar>`, whose single writer
//!   mutex serializes append, head comparison, and root publication.
//!   [`PackSnapshotBackend::open_read_only`] takes no lock and never creates,
//!   truncates, repairs, renames, or deletes anything.
//! - **Identity that cannot ABA.** One store UUID is minted under the writer
//!   lock and durably recorded. Pack and root filenames embed it plus a
//!   monotone id drawn from **one shared counter** that is never reused. The
//!   counter's high-water is reserved durably in batches inside `IDENTITY`, so
//!   it cannot rewind even if every artifact that would otherwise witness it is
//!   deleted; a fresh UUID namespace still continues past the highest number
//!   the directory has used. `.tmp` staging names are never discovery
//!   candidates: they cannot parse as a pack or root name at all.
//! - **Commit-framed records.** The active pack has no footer. Each record is
//!   an independently framed unit: bounded header with a CRC32C over itself, a
//!   body, a body CRC32C, and a trailing commit marker. A torn write leaves a
//!   prefix that cannot decode, so writer recovery truncates at the last valid
//!   frame — and only the writer ever truncates.
//! - **Roll is a state transition.** Append index + footer, sync through the
//!   footer, rename `.open` -> `.pack`, sync the directory, then create the
//!   successor. Sealed packs are immutable and never appended.
//! - **Discovery falls back.** Open selects the highest final, independently
//!   resolvable, valid root, then the next older, then "no snapshots".
//!
//! # Buffered vs Durable
//!
//! The existing [`SnapshotStore`] seam has no per-call durability knob, so the
//! mode is a constructor option ([`SidecarOptions::mode`]) and a direct
//! [`Sidecar::save_with_mode`] call:
//!
//! - [`SaveMode::Buffered`] (the default, matching today's Fjall semantics) is
//!   a discardable cache write. It still validates the frame it wrote and still
//!   installs the root by atomic rename, but issues no barrier and promises
//!   nothing about power loss.
//! - [`SaveMode::Durable`] acknowledges only after the closure the new root
//!   names is synced: the active pack through the highest referenced record end
//!   (which sweeps in every inherited `Buffered`-only record for unrelated
//!   streams), any unproven directory entry, then the root file, its rename,
//!   and the directory.
//!
//! # What this v1 deliberately does not do
//!
//! - **No public trait break.** ADR 0002's typed `SnapshotCompatibility` /
//!   `SnapshotCoverage` API, aggregate/schema IDs and codec IDs are a
//!   source-breaking change to [`Snapshottable`](crate::Snapshottable) /
//!   [`SnapshotStore`] and are deferred to a follow-up bone. The concepts are
//!   present internally: `fold_version` is the compatibility key and
//!   [`format::Coverage`] is the ordered coverage lattice (`Empty < Through(0)
//!   < Through(1)`).
//! - **Flat root, not a COW radix tree.** The root descriptor lists every head,
//!   so a save is `O(N)` in published heads. The bounded `O(k log N)` sharded
//!   design is documented at the exact substitution point in
//!   `sidecar::select_root`.
//! - **No content reclamation.** Obsolete *root descriptors* are pruned (with
//!   the full durability ordering the GC amendment demands), but no pack or
//!   record bytes are ever deleted. Superseded records and orphaned frames
//!   accumulate. The active build pack is never collected.
//! - **No grouped publication.** Each save publishes its own root generation.
//!
//! [`SnapshotStore`]: crate::SnapshotStore

pub mod format;
mod sidecar;

#[cfg(test)]
mod tests;

use std::path::Path;
use std::sync::Arc;

pub use format::{Coverage, Record, SaveMode, StoreUuid};
pub use sidecar::{PackSidecarError, Sidecar, SidecarMetrics, SidecarOptions};

use crate::backend::{
    AppendError, Appended, Backend, OwnedAppendBatch, RecordToAppend,
    StoredRecord, SubscribeBackend,
};
use crate::snapshot::{
    BlobPtr, SnapshotRef, SnapshotStore, StoredSnapshot, interim_stream_id,
};
use crate::version::Version;

/// Failure of a [`PackSnapshotBackend`] operation.
///
/// Mirrors [`SnapshotBackendError`](crate::SnapshotBackendError)'s shape: the
/// wrapped log backend's error, plus the sidecar's own. Note what is *not*
/// here — there is no "snapshot corrupt" variant, because corruption is never
/// an error, only a miss.
#[derive(Debug, thiserror::Error)]
pub enum PackBackendError<E> {
    /// The wrapped event-log backend failed.
    #[error("event-log backend: {0}")]
    Inner(#[source] E),
    /// The snapshot sidecar failed on a write path.
    #[error("snapshot sidecar: {0}")]
    Sidecar(#[from] PackSidecarError),
}

/// A [`SnapshotStore`] over the pack sidecar, delegating the event log to `B`.
///
/// Cheap to clone: the sidecar (and therefore the single writer owner) is
/// shared behind an `Arc`, matching [`EventStore`](crate::EventStore)'s
/// clone-is-share contract.
#[derive(Clone)]
pub struct PackSnapshotBackend<B> {
    inner:   B,
    sidecar: Arc<Sidecar>,
}

impl<B: std::fmt::Debug> std::fmt::Debug for PackSnapshotBackend<B> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PackSnapshotBackend")
            .field("inner", &self.inner)
            .field("sidecar", &self.sidecar)
            .finish_non_exhaustive()
    }
}

impl<B> PackSnapshotBackend<B> {
    /// Open (creating if absent) the sidecar at `root` as the writer, with
    /// default options ([`SaveMode::Buffered`]).
    ///
    /// Fails only if another live process holds the writer lock, or the
    /// directory itself cannot be created/locked. A corrupt identity, an
    /// unreadable root, or a torn active tail are all recovered.
    pub fn open(
        inner: B,
        root: impl AsRef<Path>,
    ) -> Result<Self, PackSidecarError> {
        Self::open_with(inner, root, SidecarOptions::default())
    }

    /// Open as the writer with explicit options.
    pub fn open_with(
        inner: B,
        root: impl AsRef<Path>,
        options: SidecarOptions,
    ) -> Result<Self, PackSidecarError> {
        let sidecar = Sidecar::open_writer(root, options)?;
        Ok(Self { inner, sidecar: Arc::new(sidecar) })
    }

    /// Open the sidecar **read-only** (an offline reader / diagnostic tool).
    ///
    /// Takes no writer lock, so it coexists with a live writer, and it never
    /// mutates the directory in any way. Saves through this handle fail with
    /// [`PackSidecarError::ReadOnly`]; loads serve whatever root was final at
    /// open time.
    pub fn open_read_only(inner: B, root: impl AsRef<Path>) -> Self {
        Self { inner, sidecar: Arc::new(Sidecar::open_reader(root)) }
    }

    /// Borrow the wrapped event-log backend.
    pub fn inner(&self) -> &B { &self.inner }

    /// Borrow the sidecar, for diagnostics
    /// ([`Sidecar::stream_names`], [`Sidecar::metrics`]) and for
    /// [`Sidecar::save_with_mode`] when a caller wants a one-off
    /// [`SaveMode::Durable`] publication.
    pub fn sidecar(&self) -> &Arc<Sidecar> { &self.sidecar }
}

impl<B: Backend> Backend for PackSnapshotBackend<B> {
    type Error = PackBackendError<B::Error>;

    async fn head(&self, stream_id: &str) -> Result<Version, Self::Error> {
        self.inner.head(stream_id).await.map_err(PackBackendError::Inner)
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
            .map_err(PackBackendError::Inner)
    }

    async fn read_global(
        &self,
        after: Option<u64>,
        limit: usize,
    ) -> Result<Vec<StoredRecord>, Self::Error> {
        self.inner
            .read_global(after, limit)
            .await
            .map_err(PackBackendError::Inner)
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
            Err(AppendError::Backend(e)) => {
                Err(AppendError::Backend(PackBackendError::Inner(e)))
            }
        }
    }

    async fn append_batch_owned(
        &self,
        stream_id: &str,
        expected: Version,
        batch: OwnedAppendBatch,
    ) -> Result<Appended, AppendError<Self::Error>> {
        match self.inner.append_batch_owned(stream_id, expected, batch).await {
            Ok(appended) => Ok(appended),
            Err(AppendError::Conflict { expected, actual }) => {
                Err(AppendError::Conflict { expected, actual })
            }
            Err(AppendError::Backend(e)) => {
                Err(AppendError::Backend(PackBackendError::Inner(e)))
            }
        }
    }
}

impl<B: Backend> SnapshotStore for PackSnapshotBackend<B> {
    async fn save_snapshot(
        &self,
        stream_id: &str,
        snapshot: StoredSnapshot,
    ) -> Result<(), Self::Error> {
        let snap = &snapshot.snapshot_ref;
        // An empty-prefix snapshot covers version 0 by convention; the flag is
        // what disambiguates "folds the empty prefix" from "folds index 0",
        // and `Coverage` keeps them strictly ordered.
        let stream_version =
            if snap.covers_empty_prefix { 0 } else { snap.stream_version };
        let record = Record {
            stream_name: stream_id.to_owned(),
            fold_version: snap.fold_version,
            covers_empty_prefix: snap.covers_empty_prefix,
            stream_version,
            snapshot_ptr: snap.snapshot_ptr.0,
            trust_mode: format::TRUST_UNVERIFIED_CACHE,
            state: snapshot.state_blob,
        };
        self.sidecar.save(&record)?;
        Ok(())
    }

    async fn load_snapshot(
        &self,
        stream_id: &str,
    ) -> Result<Option<StoredSnapshot>, Self::Error> {
        // Infallible by design: any sidecar problem is `None`, which the
        // `EventStore` answers with a full replay.
        Ok(self.sidecar.load(stream_id).map(|rec| StoredSnapshot {
            snapshot_ref: SnapshotRef {
                stream_id:           interim_stream_id(stream_id),
                stream_version:      rec.stream_version,
                fold_version:        rec.fold_version,
                covers_empty_prefix: rec.covers_empty_prefix,
                // Reserved until the Phase 5 fold chain: this sidecar writes
                // only `UnverifiedCache` records, and a record that claimed
                // certification without both semantic hashes would be a lie.
                event_prefix_hash:   None,
                state_hash:          None,
                snapshot_ptr:        BlobPtr(rec.snapshot_ptr),
            },
            state_blob:   rec.state,
        }))
    }
}

/// Pure delegation, exactly as the fjall backend does: the sidecar adds no
/// subscription state, so a live-tail subscription over
/// `PackSnapshotBackend<B>` is a subscription over `B`.
impl<B: SubscribeBackend> SubscribeBackend for PackSnapshotBackend<B> {
    async fn watermark(&self) -> Result<u64, Self::Error> {
        self.inner.watermark().await.map_err(PackBackendError::Inner)
    }

    async fn await_watermark_past(&self, pos: u64) -> Result<(), Self::Error> {
        self.inner
            .await_watermark_past(pos)
            .await
            .map_err(PackBackendError::Inner)
    }
}
