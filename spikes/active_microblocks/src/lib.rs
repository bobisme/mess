//! Spike F (bn-2gg): active pointer microblocks vs the incumbent
//! `ActiveIndex` (sharded `RwLock<HashMap<u64, Vec<StreamEntry>>>` + global
//! `Vec` + `applied_end` watermark).
//!
//! Candidates:
//! - **F0** — the real `mess_index::ActiveIndex` (path dependency; the
//!   incumbent, exactly as the post-Spike-C engine read path consumes it via
//!   `stream_entries_from` / `global_range` / `stream_head`).
//! - **F1** — microblock arena, linear within-block scan ([`micro`]).
//! - **F2** — F1 + binary search within blocks.
//! - **F3** — F1 + per-stream skip chain every 8 blocks.
//!
//! Plus the sparse global batch-offset array (design §8.4) vs F0's global
//! `Vec<GlobalEntry>` for position→batch seeks.

pub mod micro;
pub mod shim;
pub mod timing;
pub mod workload;

pub use mess_index::{ActiveIndex, BatchEntry, EventPtr, GlobalEntry, StreamEntry};

use crate::shim::{AtomicU64, Ordering::*};

const CANARY_ALIVE: u64 = 0xC0FF_EE00_C0FF_EE00;
const CANARY_DEAD: u64 = 0xDEAD_DEAD_DEAD_DEAD;

/// One benchable candidate: the common surface the driver monomorphizes
/// over (no dyn dispatch inside timed loops).
pub trait Index: Send + Sync + 'static {
    fn name(&self) -> &'static str;
    fn apply(&self, watermark: u64, batches: &[BatchEntry]);
    fn head(&self, sid: u64) -> Option<u64>;
    fn resolve(&self, sid: u64, version: u64) -> Option<EventPtr>;
    fn entries_from(&self, sid: u64, from: u64, max: usize) -> Vec<StreamEntry>;
    /// Global position -> (batch first_global_pos, byte offset). For F0 this
    /// is the production call shape (`global_range(pos, 1)`, which allocates
    /// its one-entry Vec exactly as `read_global` does); for F1+ it is the
    /// sparse checkpoint binary search.
    fn seek(&self, pos: u64) -> Option<(u64, u64)>;
    fn applied_end(&self) -> u64;
    /// Reclamation invariant: the generation's memory is still live.
    fn check_canary(&self);
}

/// F0: the incumbent, wrapped only to add the same canary/Drop-poison
/// discipline the microblock generation carries (so the reclaim comparison
/// is like-for-like).
pub struct F0 {
    pub idx: ActiveIndex,
    canary:  AtomicU64,
}

impl F0 {
    pub fn new() -> Self {
        F0 { idx: ActiveIndex::new(), canary: AtomicU64::new(CANARY_ALIVE) }
    }
}

impl Default for F0 {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for F0 {
    fn drop(&mut self) {
        self.canary.store(CANARY_DEAD, Release);
    }
}

impl Index for F0 {
    fn name(&self) -> &'static str {
        "F0-incumbent"
    }
    #[inline]
    fn apply(&self, watermark: u64, batches: &[BatchEntry]) {
        self.idx.apply_committed(watermark, batches)
    }
    #[inline]
    fn head(&self, sid: u64) -> Option<u64> {
        self.idx.stream_head(sid)
    }
    #[inline]
    fn resolve(&self, sid: u64, version: u64) -> Option<EventPtr> {
        self.idx.resolve(sid, version)
    }
    #[inline]
    fn entries_from(&self, sid: u64, from: u64, max: usize) -> Vec<StreamEntry> {
        self.idx.stream_entries_from(sid, from, max)
    }
    #[inline]
    fn seek(&self, pos: u64) -> Option<(u64, u64)> {
        self.idx
            .global_range(pos, 1)
            .first()
            .map(|e| (e.first_global_pos, e.ptr.offset))
    }
    #[inline]
    fn applied_end(&self) -> u64 {
        self.idx.applied_end()
    }
    #[inline]
    fn check_canary(&self) {
        assert_eq!(
            self.canary.load(Acquire),
            CANARY_ALIVE,
            "use-after-free: F0 generation read after retirement"
        );
    }
}

macro_rules! micro_candidate {
    ($name:ident, $label:literal, $binary:literal, $skip:literal, $resolve:ident) => {
        pub struct $name(pub micro::MicroIndex);

        impl $name {
            pub fn new(segment_id: u64, stride: u32) -> Self {
                $name(micro::MicroIndex::new(segment_id, stride))
            }
        }

        impl Index for $name {
            fn name(&self) -> &'static str {
                $label
            }
            #[inline]
            fn apply(&self, watermark: u64, batches: &[BatchEntry]) {
                self.0.apply_committed(watermark, batches)
            }
            #[inline]
            fn head(&self, sid: u64) -> Option<u64> {
                self.0.stream_head(sid)
            }
            #[inline]
            fn resolve(&self, sid: u64, version: u64) -> Option<EventPtr> {
                self.0.$resolve(sid, version)
            }
            #[inline]
            fn entries_from(&self, sid: u64, from: u64, max: usize) -> Vec<StreamEntry> {
                self.0.stream_entries_from::<$skip>(sid, from, max)
            }
            #[inline]
            fn seek(&self, pos: u64) -> Option<(u64, u64)> {
                self.0.global_seek(pos)
            }
            #[inline]
            fn applied_end(&self) -> u64 {
                self.0.applied_end()
            }
            #[inline]
            fn check_canary(&self) {
                self.0.check_canary()
            }
        }
    };
}

micro_candidate!(F1, "F1-micro-linear", false, false, resolve_f1);
micro_candidate!(F2, "F2-micro-binary", true, false, resolve_f2);
micro_candidate!(F3, "F3-micro-skip", false, true, resolve_f3);
