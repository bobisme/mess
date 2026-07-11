//! The head-table candidates behind one bench-facing trait.
//!
//! A0 fjall point table, A1 hashbrown HashMap (raw and RwLock-wrapped), and
//! the direct kinds from [`crate::direct`] (A2/A3/A4).

use crate::direct::{CellKind, DirectTable, Update};
use std::path::PathBuf;
use std::sync::RwLock;

/// Bench interface. `get` returns `(version, global)`, `(0, 0)` for absent.
/// `apply` is single-writer (one calling thread at a time).
pub trait Bench: Sync {
    fn name(&self) -> &'static str;
    fn get(&self, id: u64, retries: &mut u32) -> (u64, u64);
    fn apply(&self, updates: &mut [Update]);
}

// ---------------------------------------------------------------- A0 ----

/// A0: fjall point table. 8-byte BE key -> 16-byte LE (version, global).
/// Same API shape as `crates/mess-index/src/meta` (fjall 3.1.6:
/// `Database::builder(..).open()`, `db.keyspace(..)`).
pub struct FjallTable {
    // Options so Drop can tear the engine down BEFORE deleting its files
    // (struct fields normally drop after the Drop::drop body — deleting the
    // live database's directory wedges fjall's background threads).
    db: Option<fjall::Database>,
    ks: Option<fjall::Keyspace>,
    dir: PathBuf,
}

impl FjallTable {
    pub fn open(dir: PathBuf) -> Self {
        let _ = std::fs::remove_dir_all(&dir);
        let db = fjall::Database::builder(&dir).open().expect("fjall open");
        let ks = db
            .keyspace("heads", fjall::KeyspaceCreateOptions::default)
            .expect("fjall keyspace");
        FjallTable { db: Some(db), ks: Some(ks), dir }
    }

    #[inline]
    fn ks(&self) -> &fjall::Keyspace {
        self.ks.as_ref().unwrap()
    }
}

impl Drop for FjallTable {
    fn drop(&mut self) {
        drop(self.ks.take());
        drop(self.db.take());
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

impl Bench for FjallTable {
    fn name(&self) -> &'static str {
        "A0-fjall"
    }

    #[inline]
    fn get(&self, id: u64, _retries: &mut u32) -> (u64, u64) {
        match self.ks().get(id.to_be_bytes()).expect("fjall get") {
            Some(v) => {
                let b: &[u8] = &v;
                assert!(b.len() == 16, "bad fjall value len {}", b.len());
                (
                    u64::from_le_bytes(b[0..8].try_into().unwrap()),
                    u64::from_le_bytes(b[8..16].try_into().unwrap()),
                )
            }
            None => (0, 0),
        }
    }

    fn apply(&self, updates: &mut [Update]) {
        #[inline]
        fn enc(v: u64, g: u64) -> [u8; 16] {
            let mut out = [0u8; 16];
            out[0..8].copy_from_slice(&v.to_le_bytes());
            out[8..16].copy_from_slice(&g.to_le_bytes());
            out
        }
        if updates.len() == 1 {
            let (id, v, g) = updates[0];
            self.ks().insert(id.to_be_bytes(), enc(v, g)).expect("fjall insert");
            return;
        }
        // One atomic batch per commit group, like MetaStore::apply_group.
        let mut batch = self.db.as_ref().unwrap().batch();
        for &mut (id, v, g) in updates {
            batch.insert(self.ks(), id.to_be_bytes(), enc(v, g));
        }
        batch.commit().expect("fjall batch commit");
    }
}

// ---------------------------------------------------------------- A1 ----

pub type Map = hashbrown::HashMap<u64, (u64, u64)>;

/// A1 (raw): plain hashbrown map, default hasher (foldhash). Only valid with
/// no concurrent writer — used for the no-writer read-latency scenario.
pub struct RawHashTable(pub Map);

impl Bench for RawHashTable {
    fn name(&self) -> &'static str {
        "A1-hash-raw"
    }

    #[inline]
    fn get(&self, id: u64, _retries: &mut u32) -> (u64, u64) {
        self.0.get(&id).copied().unwrap_or((0, 0))
    }

    fn apply(&self, _updates: &mut [Update]) {
        unreachable!("RawHashTable is read-only; use LockedHashTable under writers");
    }
}

/// A1 (shared): the same map behind `std::sync::RwLock` — the minimum
/// synchronization a HashMap needs to serve readers concurrent with the
/// kernel writer.
pub struct LockedHashTable(pub RwLock<Map>);

impl LockedHashTable {
    pub fn with_capacity(n: usize) -> Self {
        LockedHashTable(RwLock::new(Map::with_capacity(n)))
    }
}

impl Bench for LockedHashTable {
    fn name(&self) -> &'static str {
        "A1-hash-rwlock"
    }

    #[inline]
    fn get(&self, id: u64, _retries: &mut u32) -> (u64, u64) {
        self.0.read().unwrap().get(&id).copied().unwrap_or((0, 0))
    }

    fn apply(&self, updates: &mut [Update]) {
        let mut m = self.0.write().unwrap();
        for &mut (id, v, g) in updates {
            m.insert(id, (v, g));
        }
    }
}

// ------------------------------------------------------- A2/A3/A4 -------

impl<C: CellKind> Bench for DirectTable<C> {
    fn name(&self) -> &'static str {
        C::NAME
    }

    #[inline]
    fn get(&self, id: u64, retries: &mut u32) -> (u64, u64) {
        DirectTable::get(self, id, retries)
    }

    fn apply(&self, updates: &mut [Update]) {
        DirectTable::apply(self, updates)
    }
}
