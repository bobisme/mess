//! G0 — SYNTHETIC fjall baseline.
//!
//! Mirrors the dormant `MetaStore` dedupe shape (crates/mess-index/src/meta/
//! mod.rs, `apply_group` ~:321-347): a `dedupe` primary keyspace
//! `(scope || key) -> (position, seq)` plus a `dedupe_order` FIFO keyspace
//! `seq -> (position, pk)`, one atomic journal-buffered batch per commit
//! group (durability None => no fsync, the I5 contract), read-before-write
//! on overwrite to drop the stale order row so the FIFO stays 1:1 with the
//! primary table.
//!
//! Adapted to the position-span semantics under test (the dormant code is
//! entry-count-bounded, `DEFAULT_DEDUPE_CAPACITY = 65_536`; nothing depends
//! on that — review V1): eviction pops the order FIFO while the FRONT
//! entry's position has expired (`pos < w - span`) instead of while the
//! entry count exceeds a capacity. The order value carries the position so
//! eviction does not need a second primary read. Expiry between commit
//! groups is handled at query time by the same exact position check every
//! candidate applies.
//!
//! This baseline pays, per key: a read-before-write get, a primary insert,
//! an order insert, and later TWO per-key deletes (primary + order) —
//! exactly the mutable-KV write/delete work the epoch design eliminates.

use std::path::PathBuf;

use fjall::{Database, Keyspace, KeyspaceCreateOptions};

use crate::arena::Arena;
use crate::{DedupeIndex, Scope, scoped_key};

pub struct FjallDedupe {
    db: Database,
    primary: Keyspace,
    order: Keyspace,
    span: u64,
    next_seq: u64,
    min_seq: u64,
    /// Buffered (pk, position) rows for the current commit group.
    pending: Vec<(Vec<u8>, u64)>,
    group_size: usize,
    /// Per-key delete/tombstone writes issued (primary + order removes).
    deletes: u64,
    /// Logical bytes written into batches (inserts + removes), for the
    /// write-amplification story.
    bytes_written: u64,
    dir: PathBuf,
    sweep: bool,
}

fn encode_val(pos: u64, seq: u64) -> [u8; 16] {
    let mut v = [0u8; 16];
    v[..8].copy_from_slice(&pos.to_le_bytes());
    v[8..].copy_from_slice(&seq.to_le_bytes());
    v
}

impl FjallDedupe {
    /// `group_size` = keys per commit-group batch (1 in the differential
    /// tests; production groups many appends per batch).
    pub fn open(dir: PathBuf, span: u64, group_size: usize, sweep: bool) -> Self {
        // Default builder => journal persist at PersistMode::Buffer:
        // journal-buffered, no per-commit fsync (same as MetaStore::open).
        let db = Database::builder(&dir).open().expect("fjall open");
        let primary = db.keyspace("dedupe", KeyspaceCreateOptions::default).unwrap();
        let order = db.keyspace("dedupe_order", KeyspaceCreateOptions::default).unwrap();
        FjallDedupe {
            db,
            primary,
            order,
            span,
            next_seq: 0,
            min_seq: 0,
            pending: Vec::with_capacity(group_size),
            group_size: group_size.max(1),
            deletes: 0,
            bytes_written: 0,
            dir,
            sweep,
        }
    }

    fn commit_group(&mut self) {
        if self.pending.is_empty() {
            return;
        }
        let first_seq_of_batch = self.next_seq;
        let mut batch = self.db.batch();
        let mut group_end = 0u64;
        // pks written by THIS group -> their new seq. Needed for exactness:
        // `get` only sees committed rows, so without this (a) a key
        // re-appearing within one group would leave two live order rows
        // (FIFO no longer 1:1), and (b) the eviction loop below could see a
        // committed-but-stale order row for a pk this very batch overwrites
        // and clobber the fresh primary insert — a false-negative bug the
        // differential harness caught on the first run. The dormant
        // MetaStore sidesteps (a)/(b) only by the "capacity exceeds any
        // single group" assumption; position-window eviction has no such
        // slack, so the guard must be explicit.
        let mut group_seqs: std::collections::HashMap<Vec<u8>, u64> = std::collections::HashMap::new();
        for (pk, pos) in std::mem::take(&mut self.pending) {
            group_end = group_end.max(pos);
            // Overwrite of a still-live key: drop its stale order entry so
            // the FIFO stays 1:1 with the primary table (MetaStore shape).
            if let Some(&prev_seq) = group_seqs.get(&pk) {
                batch.remove(&self.order, prev_seq.to_be_bytes());
                self.deletes += 1;
                self.bytes_written += 8;
            } else if let Some(existing) = self.primary.get(&pk).unwrap() {
                let old_seq = u64::from_le_bytes(existing[8..16].try_into().unwrap());
                batch.remove(&self.order, old_seq.to_be_bytes());
                self.deletes += 1;
                self.bytes_written += 8;
            }
            let seq = self.next_seq;
            self.next_seq += 1;
            let mut ov = Vec::with_capacity(8 + pk.len());
            ov.extend_from_slice(&pos.to_le_bytes());
            ov.extend_from_slice(&pk);
            self.bytes_written += (pk.len() + 16 + 8 + ov.len()) as u64;
            batch.insert(&self.primary, pk.clone(), encode_val(pos, seq));
            batch.insert(&self.order, seq.to_be_bytes(), ov);
            group_seqs.insert(pk, seq);
        }

        // Age out expired entries: pop the order FIFO while the front's
        // position is below the window. Only previously COMMITTED rows are
        // visible to `get`, so stop before this batch's own seqs.
        let lo = group_end.saturating_sub(self.span);
        while self.min_seq < first_seq_of_batch {
            match self.order.get(self.min_seq.to_be_bytes()).unwrap() {
                // Gap: this seq's order row was dropped by an overwrite.
                None => {
                    self.min_seq += 1;
                }
                Some(v) => {
                    let pos = u64::from_le_bytes(v[..8].try_into().unwrap());
                    if pos >= lo {
                        break;
                    }
                    let pk = &v[8..];
                    if group_seqs.contains_key(pk) {
                        // This batch re-inserted the pk: its stale order row
                        // is already being removed above; do NOT touch the
                        // fresh primary row.
                        self.min_seq += 1;
                        continue;
                    }
                    // The order row owns the primary row (1:1 invariant), so
                    // this delete is safe.
                    batch.remove(&self.primary, pk.to_vec());
                    batch.remove(&self.order, self.min_seq.to_be_bytes());
                    self.deletes += 2;
                    self.bytes_written += (pk.len() + 8) as u64;
                    self.min_seq += 1;
                }
            }
        }

        batch.commit().unwrap(); // durability None => journal-buffered
    }

    pub fn bytes_written(&self) -> u64 {
        self.bytes_written
    }

    /// On-disk footprint of the fjall directory (journal + tables).
    pub fn disk_bytes(&self) -> u64 {
        fn walk(p: &std::path::Path) -> u64 {
            let mut sum = 0;
            if let Ok(rd) = std::fs::read_dir(p) {
                for e in rd.flatten() {
                    let path = e.path();
                    if path.is_dir() {
                        sum += walk(&path);
                    } else if let Ok(m) = e.metadata() {
                        sum += m.len();
                    }
                }
            }
            sum
        }
        walk(&self.dir)
    }
}

impl Drop for FjallDedupe {
    fn drop(&mut self) {
        if self.sweep {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }
}

impl DedupeIndex for FjallDedupe {
    fn check(&mut self, scope: Scope, key: &[u8], w: u64, _arena: &Arena) -> Option<u64> {
        // Make any buffered group visible first (the harness checks between
        // groups; production checks against committed state too).
        self.commit_group();
        let lo = w.saturating_sub(self.span);
        match self.primary.get(scoped_key(scope, key)).unwrap() {
            Some(v) => {
                let pos = u64::from_le_bytes(v[..8].try_into().unwrap());
                (pos >= lo).then_some(pos)
            }
            None => None,
        }
    }

    fn insert(&mut self, scope: Scope, key: &[u8], position: u64, _ptr: u64) {
        self.pending.push((scoped_key(scope, key), position));
        if self.pending.len() >= self.group_size {
            self.commit_group();
        }
    }

    fn flush(&mut self) {
        self.commit_group();
    }

    fn deletes_issued(&self) -> u64 {
        self.deletes
    }

    fn resident_bytes(&self) -> u64 {
        // fjall's memtables/block-cache/index resident set is not cheaply
        // introspectable; the bench measures RSS deltas in a fresh process
        // instead. Return 0 here so nobody double-counts.
        0
    }

    fn serialized_bytes(&self) -> u64 {
        self.disk_bytes()
    }
}
