//! Exact-KV metadata tables on [fjall] (doc 04's stream/snapshot heads,
//! projection checkpoints, and the recent-dedupe window), the pointer-index
//! spike's chosen backend.
//!
//! # Why fjall, and why journal-buffered durability is enough (I5)
//!
//! Every table here is a **derived cache of the log**, never a source of
//! truth. The append-only log (`mess-log`) is the only durable authority; a
//! stream's head, its latest snapshot pointer, a projection's checkpoint, and
//! the recent-dedupe window can all be reconstructed by scanning the log
//! (invariant **I5** — "every index is rebuildable"). That is exactly what
//! licenses running these tables at fjall's default *journal-buffered*
//! durability: writes land in fjall's journal buffer and are **not** fsynced
//! per commit. A crash may lose the un-flushed tail — and that is fine,
//! because recovery replays the lost tail from the log. We never pay an fsync
//! on the metadata path; the log's own durability (`03-durability.md`) is the
//! only fsync that matters.
//!
//! This is the F1 lesson made concrete: writes are **batched per commit
//! group**, never per event ([`MetaStore::apply_group`]). One fjall
//! [`WriteBatch`](fjall::OwnedWriteBatch) carries every row a commit group
//! touches *plus* the high-water advance, atomically.
//!
//! # Rebuild story, per table
//!
//! On `rm -rf` of the metadata directory (or a crash that lost buffered
//! writes), each table is rebuilt by replaying the log in position order and
//! re-applying, idempotently:
//!
//! | table | key → value | rebuilt from the log by |
//! |---|---|---|
//! | `stream_heads` | `stream_id` → `(version, global_pos)` | the last event seen for each stream — its stream position and global position |
//! | `snapshot_heads` | `stream_id` → `(covered_version, global_pos, ref)` | the newest snapshot record seen for each stream |
//! | `checkpoints` | `projection_id` → `position` | replaying that projection and recording how far it got |
//! | `dedupe` (+`dedupe_order`) | `(stream_id, dedupe_key)` → `position` | re-inserting each committed event's dedupe key in commit order, aging out all but the most recent `capacity` |
//!
//! Because replay is deterministic and in commit order, a full rebuild
//! produces **byte-identical** table contents (tested in `tests/`).
//!
//! # Crash-consistency: per-table high-water + lag detection
//!
//! Each table records a **high-water position**: the exclusive log end
//! (watermark units) through which its rows are current. The high-water is
//! written *in the same atomic batch* as the rows it accounts for, so a table
//! and its high-water can never disagree — a crash truncates both together.
//!
//! On reopen a table's high-water may **lag** the log's durable watermark
//! (the buffered tail was lost). Recovery calls [`MetaStore::lag`] /
//! [`MetaStore::checkpoint_lag`] to detect the gap `[table_hw, log_end)` and
//! replays exactly those log positions back through [`MetaStore::apply_group`]
//! to close it (tested in `tests/`). Re-application is idempotent: replaying
//! an already-applied head just rewrites the same bytes.
//!
//! # Performance
//!
//! Head lookup and the dedupe-window check are single fjall point reads served
//! from the in-memory memtable / block cache — no fsync, no log scan. Measured
//! on this machine (release, warm cache; see `tests/bench_lookup.rs`):
//!
//! - `stream_head` point lookup: ~0.49 µs/op (492 ns)
//! - `dedupe_lookup` (absorb check): ~0.55 µs/op (550 ns)
//!
//! comfortably in the sub-microsecond-ish band the bone asks for. (The bench
//! test prints the exact numbers it measured on the current host and asserts a
//! generous ceiling so it never flakes in CI.)
//!
//! [fjall]: https://docs.rs/fjall

pub mod codec;

use std::path::Path;
use std::sync::Mutex;

pub use codec::{DecodeError, Head, SnapshotHead, StreamId};
use codec::{
    decode_dedupe, decode_head, decode_pos, decode_snapshot, dedupe_key,
    encode_dedupe, encode_head, encode_pos, encode_snapshot, order_key,
    stream_key,
};
use fjall::{Database, Keyspace, KeyspaceCreateOptions};

const P_STREAM_HEADS: &str = "stream_heads";
const P_SNAPSHOT_HEADS: &str = "snapshot_heads";
const P_CHECKPOINTS: &str = "checkpoints";
const P_DEDUPE: &str = "dedupe";
const P_DEDUPE_ORDER: &str = "dedupe_order";
const P_HW: &str = "hw";
// bn-20b: interner bijections. The append-only log stores only interned
// numeric ids (a `stream_id u64` per batch, an `event_type_id u32` per event) —
// never their names — so a materializing reader that must return
// `(stream_name, message_type)` bytes cannot reconstruct names from the log
// alone. These two tables persist the id→name maps the engine's in-process
// interner assigns, so a fresh open over a populated dir resolves names again.
//
// Unlike the other meta tables (derived caches, rebuildable from the log by
// I5), these are the durable *source of truth* for the name↔id bijection —
// exactly the role `$registry` plays in the full design, kept here as the
// smallest durable surface for the engine's lightweight interner. They live
// in the meta store because that is already the store's durable
// derived-metadata home, but — unlike every other table here — they are NOT
// run at plain journal-buffered durability: bn-20b originally documented a
// buffered write here as "an acceptable, documented limit shared with every
// other buffered meta table", but it is not, because I5 (rebuildable from
// the log) does not hold for these two tables. bn-150 closes that gap: the
// engine (`mess-store`'s `engine.rs`, `LogEngine::persist_new_names`) forces
// a newly-interned name's row durable (`MetaStore::persist`, a real
// `fsync`) strictly before its covering append can become durable, so a
// power-loss crash can no longer separate the two. See that function's doc
// for the full rationale.
const P_STREAM_NAMES: &str = "stream_names";
const P_TYPE_NAMES: &str = "type_names";

// High-water keys inside the `hw` partition. Checkpoints need no entry here:
// a projection's checkpoint value *is* its high-water.
const HW_STREAM_HEADS: &[u8] = b"stream_heads";
const HW_SNAPSHOT_HEADS: &[u8] = b"snapshot_heads";
const HW_DEDUPE: &[u8] = b"dedupe";

/// Default cap on the recent-dedupe window: the number of most-recent
/// `(stream, dedupe_key)` entries kept before the oldest ages out. Must exceed
/// the largest single commit group (eviction never touches an entry added by
/// the batch currently committing).
pub const DEFAULT_DEDUPE_CAPACITY: usize = 65_536;

/// A table dump: key/value byte pairs in key order (test/diagnostic use).
pub type Dump = Vec<(Vec<u8>, Vec<u8>)>;

/// Which high-water-tracked table a [`MetaStore::lag`] query is about.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum MetaTable {
    /// The `stream_heads` table.
    StreamHeads,
    /// The `snapshot_heads` table.
    SnapshotHeads,
    /// The recent-dedupe window.
    Dedupe,
}

impl MetaTable {
    fn hw_key(self) -> &'static [u8] {
        match self {
            MetaTable::StreamHeads => HW_STREAM_HEADS,
            MetaTable::SnapshotHeads => HW_SNAPSHOT_HEADS,
            MetaTable::Dedupe => HW_DEDUPE,
        }
    }

    fn name(self) -> &'static str {
        match self {
            MetaTable::StreamHeads => P_STREAM_HEADS,
            MetaTable::SnapshotHeads => P_SNAPSHOT_HEADS,
            MetaTable::Dedupe => P_DEDUPE,
        }
    }
}

/// Anything that can go wrong talking to the metadata tables.
#[derive(Debug, thiserror::Error)]
pub enum MetaError {
    /// The underlying fjall engine failed.
    #[error(transparent)]
    Fjall(#[from] fjall::Error),
    /// A stored value did not decode (corruption or a torn write).
    #[error(transparent)]
    Decode(#[from] DecodeError),
}

/// One commit group's worth of metadata updates, applied to fjall in a single
/// atomic batch (the F1 "never per event" rule).
///
/// `end_position` is the **exclusive** log end (watermark units) the tables
/// reach once this group is applied: for a group whose last event sits at
/// global position `p`, pass `p + 1`. It advances the high-water of every
/// writer-maintained table.
#[derive(Clone, Debug, Default)]
pub struct CommitGroup {
    /// Exclusive log end reached after applying this group.
    pub end_position:   u64,
    /// `stream_id → new head`.
    pub stream_heads:   Vec<(StreamId, Head)>,
    /// `stream_id → new snapshot pointer`.
    pub snapshot_heads: Vec<(StreamId, SnapshotHead)>,
    /// `(stream_id, dedupe_key, committed global position)` to remember.
    pub dedupe:         Vec<(StreamId, Vec<u8>, u64)>,
}

impl CommitGroup {
    /// A new, empty group that will advance high-water to `end_position`.
    #[must_use]
    pub fn new(end_position: u64) -> Self {
        CommitGroup { end_position, ..Default::default() }
    }
}

struct DedupeBounds {
    /// Seq of the oldest live entry (front of the FIFO). Equals `next_seq`
    /// when the window is empty.
    min_seq:  u64,
    /// Seq the next inserted entry will take (back of the FIFO). Live count is
    /// exactly `next_seq - min_seq`.
    next_seq: u64,
}

/// The metadata store: one fjall [`Database`] holding the four tables, opened
/// at journal-buffered durability.
pub struct MetaStore {
    db:                     Database,
    stream_heads:           Keyspace,
    snapshot_heads:         Keyspace,
    checkpoints:            Keyspace,
    dedupe:                 Keyspace,
    dedupe_order:           Keyspace,
    hw:                     Keyspace,
    stream_names:           Keyspace,
    type_names:             Keyspace,
    dedupe_bounds:          Mutex<DedupeBounds>,
    dedupe_capacity:        usize,
    /// Test/diagnostic: counts calls to [`Self::persist`] (bn-150). Lets a
    /// caller (the engine's name-durability regression tests) assert the
    /// hot append path (no newly-interned name) makes zero durable-flush
    /// calls, and that a new-name append makes exactly the expected number —
    /// a call-count assertion is used because a real power-loss event
    /// cannot be simulated portably in-process (an `fsync` survives a mere
    /// process crash/exit; only a real OS crash or power cut loses a
    /// page-cache write that was never `fsync`ed).
    persist_calls:          std::sync::atomic::AtomicU64,
    /// Test/diagnostic counter for [`Self::persist_buffered`] — the
    /// barrier-free page-cache flush used under `Durability::Process`
    /// (bn-2cj). Kept distinct from `persist_calls` so a test can tell a
    /// real `fsync` barrier apart from a page-cache-only flush.
    buffered_persist_calls: std::sync::atomic::AtomicU64,
}

impl MetaStore {
    /// Open (creating if absent) the metadata store at `path`, with the
    /// default dedupe-window capacity.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, MetaError> {
        Self::open_with_capacity(path, DEFAULT_DEDUPE_CAPACITY)
    }

    /// Open with an explicit dedupe-window capacity (must be `>= 1` and larger
    /// than any single commit group's dedupe count).
    pub fn open_with_capacity(
        path: impl AsRef<Path>,
        dedupe_capacity: usize,
    ) -> Result<Self, MetaError> {
        assert!(dedupe_capacity >= 1, "dedupe capacity must be >= 1");
        // Default builder => auto journal persist at PersistMode::Buffer:
        // journal-buffered, no per-commit fsync. That is the I5 contract.
        let db = Database::builder(path.as_ref()).open()?;
        let stream_heads =
            db.keyspace(P_STREAM_HEADS, KeyspaceCreateOptions::default)?;
        let snapshot_heads =
            db.keyspace(P_SNAPSHOT_HEADS, KeyspaceCreateOptions::default)?;
        let checkpoints =
            db.keyspace(P_CHECKPOINTS, KeyspaceCreateOptions::default)?;
        let dedupe = db.keyspace(P_DEDUPE, KeyspaceCreateOptions::default)?;
        let dedupe_order =
            db.keyspace(P_DEDUPE_ORDER, KeyspaceCreateOptions::default)?;
        let hw = db.keyspace(P_HW, KeyspaceCreateOptions::default)?;
        let stream_names =
            db.keyspace(P_STREAM_NAMES, KeyspaceCreateOptions::default)?;
        let type_names =
            db.keyspace(P_TYPE_NAMES, KeyspaceCreateOptions::default)?;

        // Recover the FIFO seq bounds from the order index: the window's live
        // seqs are the contiguous range [first_key, last_key].
        let bounds = match dedupe_order.first_key_value() {
            None => DedupeBounds { min_seq: 0, next_seq: 0 },
            Some(first) => {
                let fk = first.key()?;
                let min_seq = be8(&fk);
                let last = dedupe_order
                    .last_key_value()
                    .expect("non-empty order index has a last key")
                    .key()?;
                let next_seq = be8(&last) + 1;
                DedupeBounds { min_seq, next_seq }
            }
        };

        Ok(MetaStore {
            db,
            stream_heads,
            snapshot_heads,
            checkpoints,
            dedupe,
            dedupe_order,
            hw,
            stream_names,
            type_names,
            dedupe_bounds: Mutex::new(bounds),
            dedupe_capacity,
            persist_calls: std::sync::atomic::AtomicU64::new(0),
            buffered_persist_calls: std::sync::atomic::AtomicU64::new(0),
        })
    }

    // ---- writes (grouped) --------------------------------------------

    /// Apply one commit group's updates in a single atomic fjall batch,
    /// advancing the writer-maintained tables' high-water to
    /// `group.end_position`.
    ///
    /// Journal-buffered: the batch is not fsynced (I5). Idempotent for the
    /// head/snapshot rows — replaying the same group rewrites the same bytes —
    /// which is what makes recovery's gap-replay safe.
    pub fn apply_group(&self, group: &CommitGroup) -> Result<(), MetaError> {
        let mut bounds = self.dedupe_bounds.lock().unwrap();
        let mut batch = self.db.batch();

        for (id, head) in &group.stream_heads {
            batch.insert(
                &self.stream_heads,
                stream_key(*id),
                encode_head(*head),
            );
        }
        for (id, snap) in &group.snapshot_heads {
            batch.insert(
                &self.snapshot_heads,
                stream_key(*id),
                encode_snapshot(snap),
            );
        }

        for (stream, key, pos) in &group.dedupe {
            let pk = dedupe_key(*stream, key);
            // Overwrite of a still-live key: drop its stale order entry so the
            // FIFO stays 1:1 with the primary table and the count stays exact.
            if let Some(existing) = self.dedupe.get(&pk)? {
                let (_, old_seq) = decode_dedupe(&existing)?;
                // Drop the stale order entry; a fresh one is added below at the
                // new seq. Net live count is unchanged (one out, one in).
                batch.remove(&self.dedupe_order, order_key(old_seq));
            }
            let seq = bounds.next_seq;
            bounds.next_seq += 1;
            batch.insert(&self.dedupe, pk.clone(), encode_dedupe(*pos, seq));
            batch.insert(&self.dedupe_order, order_key(seq), pk);
        }

        // Age out the oldest entries beyond capacity. Only prior-group entries
        // (already committed, so `get` sees them) are ever evicted, because
        // `dedupe_capacity` exceeds any single group.
        while bounds.next_seq - bounds.min_seq > self.dedupe_capacity as u64 {
            let ok = order_key(bounds.min_seq);
            if let Some(pk) = self.dedupe_order.get(ok)? {
                batch.remove(&self.dedupe, pk.to_vec());
                batch.remove(&self.dedupe_order, ok);
            }
            bounds.min_seq += 1;
        }

        // High-water advances in the same batch as the rows it accounts for.
        let end = encode_pos(group.end_position);
        batch.insert(&self.hw, HW_STREAM_HEADS, end);
        batch.insert(&self.hw, HW_SNAPSHOT_HEADS, end);
        batch.insert(&self.hw, HW_DEDUPE, end);

        batch.commit()?; // durability None => journal-buffered, no fsync
        Ok(())
    }

    // ---- interner bijections (bn-20b) --------------------------------

    /// Persist a `stream_id → name` interner mapping (idempotent — the same id
    /// always maps to the same name; re-writing is a no-op-shaped overwrite).
    /// Called by the engine the first time a stream name is interned, so a
    /// later reopen can resolve the name the log's numeric `stream_id` stands
    /// for.
    pub fn put_stream_name(
        &self,
        stream_id: u64,
        name: &str,
    ) -> Result<(), MetaError> {
        self.stream_names.insert(stream_id.to_be_bytes(), name.as_bytes())?;
        Ok(())
    }

    /// Persist an `event_type_id → name` interner mapping (see
    /// [`put_stream_name`](Self::put_stream_name)).
    pub fn put_type_name(
        &self,
        event_type_id: u32,
        name: &str,
    ) -> Result<(), MetaError> {
        self.type_names.insert(event_type_id.to_be_bytes(), name.as_bytes())?;
        Ok(())
    }

    /// Every persisted `(stream_id, name)` interner mapping, for rebuilding the
    /// engine's interner on reopen. Order is unspecified (the caller sorts by
    /// id to reconstruct the dense assignment).
    pub fn stream_names(&self) -> Result<Vec<(u64, String)>, MetaError> {
        let mut out = Vec::new();
        for kv in self.stream_names.iter() {
            let (k, v) = kv.into_inner()?;
            let id =
                u64::from_be_bytes(k.as_ref().try_into().map_err(|_| {
                    DecodeError::Corrupt {
                        table:  P_STREAM_NAMES,
                        reason: "stream-name key must be 8 bytes".into(),
                    }
                })?);
            let name = String::from_utf8(v.to_vec()).map_err(|e| {
                DecodeError::Corrupt {
                    table:  P_STREAM_NAMES,
                    reason: format!("name not utf-8: {e}"),
                }
            })?;
            out.push((id, name));
        }
        Ok(out)
    }

    /// Every persisted `(event_type_id, name)` interner mapping (see
    /// [`stream_names`](Self::stream_names)).
    pub fn type_names(&self) -> Result<Vec<(u32, String)>, MetaError> {
        let mut out = Vec::new();
        for kv in self.type_names.iter() {
            let (k, v) = kv.into_inner()?;
            let id =
                u32::from_be_bytes(k.as_ref().try_into().map_err(|_| {
                    DecodeError::Corrupt {
                        table:  P_TYPE_NAMES,
                        reason: "type-name key must be 4 bytes".into(),
                    }
                })?);
            let name = String::from_utf8(v.to_vec()).map_err(|e| {
                DecodeError::Corrupt {
                    table:  P_TYPE_NAMES,
                    reason: format!("name not utf-8: {e}"),
                }
            })?;
            out.push((id, name));
        }
        Ok(out)
    }

    /// Record a projection's checkpoint (the exclusive log position it has
    /// consumed through). The value is also this projection's high-water.
    pub fn set_checkpoint(
        &self,
        projection_id: &str,
        position: u64,
    ) -> Result<(), MetaError> {
        self.checkpoints
            .insert(projection_id.as_bytes(), encode_pos(position))?;
        Ok(())
    }

    /// Force any buffered writes to disk with a real durability **barrier**
    /// (`fjall::PersistMode::SyncAll` — an `fsync`). Not needed for
    /// correctness of the I5 derived-cache tables — a convenience for clean
    /// shutdown or to bound recovery replay by periodically checkpointing
    /// durability — but IS the barrier the `stream_names`/`type_names`
    /// interner bijection tables (the one durable source of truth this store
    /// holds) ride under a **barriered** engine durability mode
    /// (`Durability::Os`/`Group`), so a covering log append can only become
    /// durable once its new name(s) already are (bn-150; see [`MetaStore`]'s
    /// and `mess-store`'s `engine.rs` `persist_new_names` doc for why). Under
    /// the barrier-free `Durability::Process` mode the caller uses
    /// [`persist_buffered`](Self::persist_buffered) instead — see bn-2cj.
    pub fn persist(&self) -> Result<(), MetaError> {
        self.persist_calls.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.db.persist(fjall::PersistMode::SyncAll)?;
        Ok(())
    }

    /// Flush buffered writes to the OS **page cache** only
    /// (`fjall::PersistMode::Buffer` — a `write(2)`, never an `fsync`), with
    /// **no** durability barrier (bn-2cj).
    ///
    /// This is the name-persist path chosen when the engine runs
    /// `Durability::Process`: that mode acks a log append the instant its own
    /// covering `write(2)` reaches the OS page cache and issues no barrier of
    /// its own (`docs/spec/03-durability.md` §1.1), so a per-name `SyncAll`
    /// (a multi-millisecond `fsync`) would be *strictly stronger* than the
    /// durability the operator asked for — and it is exactly that `fsync`
    /// that dominated new-stream latency (spike bn-1jg: ~3.4 ms/new stream,
    /// 98.8% of the cost).
    ///
    /// Calling this after a `put_stream_name`/`put_type_name` still matters
    /// even though fjall's default insert path already flushes each write to
    /// the page cache (`Keyspace::insert` issues its own
    /// `PersistMode::Buffer` when `manual_journal_persist` is off, as it is
    /// here): it makes the "name bytes have reached the kernel" fact an
    /// explicit, load-bearing ordering point the caller can rely on
    /// regardless of fjall's insert-time defaults, and gives the tests a
    /// counter distinct from the `SyncAll` barrier count. It is cheap — a
    /// `BufWriter::flush` that is a no-op when the insert already flushed —
    /// so, unlike [`persist`](Self::persist), it does **not** need
    /// `spawn_blocking`.
    ///
    /// # Durability
    ///
    /// Survives a **process** crash (a completed `write(2)` outlives process
    /// death), NOT an OS crash / power loss — identical to what
    /// `Durability::Process` promises for the event bytes themselves. See the
    /// `engine.rs` flush-site contract for why "name `write(2)` ordered
    /// before the covering event `write(2)`" excludes a surviving-event /
    /// lost-name gap for that crash class.
    pub fn persist_buffered(&self) -> Result<(), MetaError> {
        self.buffered_persist_calls
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.db.persist(fjall::PersistMode::Buffer)?;
        Ok(())
    }

    /// Test/diagnostic: how many times [`Self::persist`] (the `SyncAll`
    /// **barrier**) has been called (bn-150). Lets a caller assert the hot
    /// append path (no newly-interned name) makes zero durable-flush calls,
    /// and that under a barriered mode a new-name append makes exactly one.
    #[doc(hidden)]
    #[must_use]
    pub fn persist_call_count(&self) -> u64 {
        self.persist_calls.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Test/diagnostic: how many times [`Self::persist_buffered`] (the
    /// barrier-free page-cache flush) has been called (bn-2cj). Lets a caller
    /// assert that under `Durability::Process` a new-name append pushes the
    /// name to the page cache without an `fsync` barrier.
    #[doc(hidden)]
    #[must_use]
    pub fn buffered_persist_call_count(&self) -> u64 {
        self.buffered_persist_calls.load(std::sync::atomic::Ordering::Relaxed)
    }

    // ---- reads --------------------------------------------------------

    /// The current head of `stream`, or `None` if the stream is unknown.
    pub fn stream_head(
        &self,
        stream: StreamId,
    ) -> Result<Option<Head>, MetaError> {
        match self.stream_heads.get(stream_key(stream))? {
            Some(v) => Ok(Some(decode_head(&v)?)),
            None => Ok(None),
        }
    }

    /// The latest snapshot pointer for `stream`, or `None`.
    pub fn snapshot_head(
        &self,
        stream: StreamId,
    ) -> Result<Option<SnapshotHead>, MetaError> {
        match self.snapshot_heads.get(stream_key(stream))? {
            Some(v) => Ok(Some(decode_snapshot(&v)?)),
            None => Ok(None),
        }
    }

    /// A projection's checkpoint position, or `None` if it has none yet.
    pub fn checkpoint(
        &self,
        projection_id: &str,
    ) -> Result<Option<u64>, MetaError> {
        match self.checkpoints.get(projection_id.as_bytes())? {
            Some(v) => Ok(Some(decode_pos(P_CHECKPOINTS, &v)?)),
            None => Ok(None),
        }
    }

    /// The **A6 absorb** check: if `(stream, dedupe_key)` is in the recent
    /// window, return the global position the original append committed at, so
    /// the caller can absorb the duplicate and return that position instead of
    /// writing again. `None` means "not seen recently" — proceed with a normal
    /// append. (A key aged out of the window reads as `None`; dedupe is a
    /// best-effort recency guarantee, bounded by `capacity`.)
    pub fn dedupe_lookup(
        &self,
        stream: StreamId,
        dkey: &[u8],
    ) -> Result<Option<u64>, MetaError> {
        match self.dedupe.get(dedupe_key(stream, dkey))? {
            Some(v) => Ok(Some(decode_dedupe(&v)?.0)),
            None => Ok(None),
        }
    }

    // ---- high-water / lag --------------------------------------------

    /// A table's high-water: the exclusive log end its rows are current
    /// through. `0` if the table has never been written.
    pub fn high_water(&self, table: MetaTable) -> Result<u64, MetaError> {
        match self.hw.get(table.hw_key())? {
            Some(v) => Ok(decode_pos("hw", &v)?),
            None => Ok(0),
        }
    }

    /// Detect lag for `table` against the log's durable end (`log_end`,
    /// watermark value). Returns `Some((from, to))` — the half-open position
    /// range `[from, to)` recovery must replay through [`Self::apply_group`] to
    /// close the gap — or `None` if the table is already caught up.
    pub fn lag(
        &self,
        table: MetaTable,
        log_end: u64,
    ) -> Result<Option<(u64, u64)>, MetaError> {
        let hw = self.high_water(table)?;
        Ok((hw < log_end).then_some((hw, log_end)))
    }

    /// Lag for a projection's checkpoint against `log_end`; `Some((from, to))`
    /// is the range the projection must reprocess.
    pub fn checkpoint_lag(
        &self,
        projection_id: &str,
        log_end: u64,
    ) -> Result<Option<(u64, u64)>, MetaError> {
        let cp = self.checkpoint(projection_id)?.unwrap_or(0);
        Ok((cp < log_end).then_some((cp, log_end)))
    }

    // ---- test / diagnostic support -----------------------------------

    /// Dump a content table's key/value pairs in key order, for byte-equal
    /// rebuild verification. The dedupe-window's `dedupe_order` and the `hw`
    /// bookkeeping partitions are excluded — only the four logical tables.
    #[doc(hidden)]
    pub fn dump(&self, table: MetaTable) -> Result<Dump, MetaError> {
        self.dump_named(table.name())
    }

    /// Dump the checkpoints table.
    #[doc(hidden)]
    pub fn dump_checkpoints(&self) -> Result<Dump, MetaError> {
        self.dump_named(P_CHECKPOINTS)
    }

    fn dump_named(&self, name: &str) -> Result<Dump, MetaError> {
        let ks = match name {
            P_STREAM_HEADS => &self.stream_heads,
            P_SNAPSHOT_HEADS => &self.snapshot_heads,
            P_CHECKPOINTS => &self.checkpoints,
            P_DEDUPE => &self.dedupe,
            _ => unreachable!("unknown table {name}"),
        };
        let mut out = Vec::new();
        for kv in ks.iter() {
            let (k, v) = kv.into_inner()?;
            out.push((k.to_vec(), v.to_vec()));
        }
        Ok(out)
    }
}

fn be8(b: &[u8]) -> u64 {
    u64::from_be_bytes(b[..8].try_into().expect("8-byte big-endian key"))
}
