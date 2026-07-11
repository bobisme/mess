//! Direct dense-ID head tables (candidates A2, A3, A4).
//!
//! Shared shape (design.md §7.1): a grow-only two-level chunked array.
//!
//! ```text
//! page = stream_id >> PAGE_BITS      (PAGE_BITS = 12, 4096 cells/page)
//! slot = stream_id & (PAGE_SIZE - 1)
//! ```
//!
//! Pages are allocated when IDs enter their range and never move. The
//! top-level page directory is a boxed slice of `AtomicPtr<Page>`; growth
//! allocates a doubled directory, copies the page pointers, and publishes the
//! new directory with a `Release` store. Retired directories are kept alive
//! until table drop (readers may still hold them), which is cheap: 8 B/page,
//! and doubling bounds total retired size below the live directory size.
//!
//! Single-writer discipline: `apply` must only be called from one thread at a
//! time (the state-kernel owner). Readers are unrestricted.
//!
//! Cell kinds:
//! - [`SeqCell`] (A2): two `AtomicU64`s per cell, coherence via a per-page
//!   seqlock (design.md §7.2). 16 B/cell.
//! - [`DblCell`] (A3): double-buffered `[Head; 2]` plus an `AtomicU8` active
//!   selector. 40 B/cell. No reader retry, but a reader stalled between
//!   loading the selector and the fields can observe a torn pair if the
//!   writer updates the same cell twice in that window (research/03 §2.2
//!   flags exactly this).
//! - [`U128Cell`] (A4): one `portable_atomic::AtomicU128` per cell packing
//!   `(version << 64) | global`. 16 B/cell. Lock-freedom is
//!   machine-dependent; the driver reports `is_lock_free()`.

use crate::shim::{AtomicPtr, AtomicU8, AtomicU64, Ordering::*, fence, spin};
use std::sync::Mutex;

pub const PAGE_BITS: u32 = 12;
pub const PAGE_SIZE: usize = 1 << PAGE_BITS;
pub const SLOT_MASK: u64 = (PAGE_SIZE as u64) - 1;

/// Invariant encoding for the torn-pair check: every written head satisfies
/// `global == (version << 32) | stream_id`. Version 0 == never written
/// (`global` must then be 0). Any read that violates this is a torn pair.
#[inline]
pub fn encode_global(version: u64, id: u64) -> u64 {
    debug_assert!(version < (1 << 32) && id < (1 << 32));
    (version << 32) | id
}

/// Check the invariant for a read `(version, global)` of stream `id`.
#[inline]
pub fn check_pair(id: u64, version: u64, global: u64) -> bool {
    if version == 0 { global == 0 } else { global == encode_global(version, id) }
}

/// An update: `(stream_id, version, global)`.
pub type Update = (u64, u64, u64);

/// One head-table page: a seqlock word plus `len` cells. The seqlock word is
/// only used by [`SeqCell`]; the other kinds carry it unused (16 B per 4096
/// cells — noise).
pub struct Page<C> {
    pub seq: AtomicU64,
    pub cells: Box<[C]>,
}

impl<C: CellKind> Page<C> {
    pub fn with_len(len: usize) -> Self {
        let cells: Vec<C> = (0..len).map(|_| C::empty()).collect();
        Page { seq: AtomicU64::new(0), cells: cells.into_boxed_slice() }
    }
}

pub trait CellKind: Send + Sync + Sized + 'static {
    const NAME: &'static str;
    fn empty() -> Self;

    /// One bounded read attempt batch: try up to `max_tries` times, adding
    /// each failed attempt to `retries`. `None` only for the seqlock kind
    /// when every attempt raced the writer.
    fn try_read(
        page: &Page<Self>,
        slot: usize,
        max_tries: u32,
        retries: &mut u32,
    ) -> Option<(u64, u64)>;

    /// Blocking read: bounded spin, then yield-escalation. Terminates as long
    /// as the single writer's per-page critical section is finite.
    #[inline]
    fn read(page: &Page<Self>, slot: usize, retries: &mut u32) -> (u64, u64) {
        // Fast path: a handful of raw retries; then escalate to yielding so a
        // pathological writer cannot monopolize a shared core.
        if let Some(v) = Self::try_read(page, slot, 64, retries) {
            return v;
        }
        loop {
            std::thread::yield_now();
            if let Some(v) = Self::try_read(page, slot, 64, retries) {
                return v;
            }
        }
    }

    /// Single-writer publish of one page's updates (slot computed from id).
    fn write_batch(page: &Page<Self>, run: &[Update]);
}

// ---------------------------------------------------------------- A2 ----

/// A2: `[AtomicU64; 2]` per cell + per-page seqlock.
pub struct SeqCell {
    version: AtomicU64,
    global: AtomicU64,
}

impl CellKind for SeqCell {
    const NAME: &'static str = "A2-seqlock";

    fn empty() -> Self {
        SeqCell { version: AtomicU64::new(0), global: AtomicU64::new(0) }
    }

    #[inline]
    fn try_read(
        page: &Page<Self>,
        slot: usize,
        max_tries: u32,
        retries: &mut u32,
    ) -> Option<(u64, u64)> {
        for _ in 0..max_tries {
            let s1 = page.seq.load(Acquire);
            if s1 & 1 == 0 {
                let cell = &page.cells[slot];
                let v = cell.version.load(Relaxed);
                let g = cell.global.load(Relaxed);
                // Pairs with the writer's Release fence: if we saw any cell
                // value written after that fence, this Acquire fence makes
                // the odd seq store visible to the re-read below.
                fence(Acquire);
                let s2 = page.seq.load(Relaxed);
                if s1 == s2 {
                    return Some((v, g));
                }
            }
            *retries += 1;
            spin();
        }
        None
    }

    fn write_batch(page: &Page<Self>, run: &[Update]) {
        let s = page.seq.load(Relaxed);
        page.seq.store(s.wrapping_add(1), Relaxed); // odd: write in progress
        // Order the odd store before the cell stores (as observed by readers
        // doing a data-read + Acquire fence).
        fence(Release);
        for &(id, v, g) in run {
            let cell = &page.cells[(id & SLOT_MASK) as usize];
            cell.version.store(v, Relaxed);
            cell.global.store(g, Relaxed);
        }
        page.seq.store(s.wrapping_add(2), Release); // even: published
    }
}

// ---------------------------------------------------------------- A3 ----

/// A3: double-buffered cell + active-copy selector. 40 B/cell.
pub struct DblCell {
    copies: [[AtomicU64; 2]; 2],
    active: AtomicU8,
}

impl CellKind for DblCell {
    const NAME: &'static str = "A3-dblbuf";

    fn empty() -> Self {
        DblCell {
            copies: [
                [AtomicU64::new(0), AtomicU64::new(0)],
                [AtomicU64::new(0), AtomicU64::new(0)],
            ],
            active: AtomicU8::new(0),
        }
    }

    #[inline]
    fn try_read(
        page: &Page<Self>,
        slot: usize,
        _max_tries: u32,
        _retries: &mut u32,
    ) -> Option<(u64, u64)> {
        let cell = &page.cells[slot];
        let i = cell.active.load(Acquire) as usize;
        let copy = &cell.copies[i];
        Some((copy[0].load(Relaxed), copy[1].load(Relaxed)))
    }

    fn write_batch(page: &Page<Self>, run: &[Update]) {
        for &(id, v, g) in run {
            let cell = &page.cells[(id & SLOT_MASK) as usize];
            let next = (cell.active.load(Relaxed) ^ 1) as usize;
            let copy = &cell.copies[next];
            copy[0].store(v, Relaxed);
            copy[1].store(g, Relaxed);
            cell.active.store(next as u8, Release);
        }
    }
}

// ---------------------------------------------------------------- A4 ----

/// A4: packed 128-bit head, one atomic load/store per op.
pub struct U128Cell(portable_atomic::AtomicU128);

impl U128Cell {
    pub fn is_lock_free() -> bool {
        portable_atomic::AtomicU128::is_lock_free()
    }
}

impl CellKind for U128Cell {
    const NAME: &'static str = "A4-u128";

    fn empty() -> Self {
        U128Cell(portable_atomic::AtomicU128::new(0))
    }

    #[inline]
    fn try_read(
        page: &Page<Self>,
        slot: usize,
        _max_tries: u32,
        _retries: &mut u32,
    ) -> Option<(u64, u64)> {
        let x = page.cells[slot].0.load(std::sync::atomic::Ordering::Acquire);
        Some(((x >> 64) as u64, x as u64))
    }

    fn write_batch(page: &Page<Self>, run: &[Update]) {
        for &(id, v, g) in run {
            let x = ((v as u128) << 64) | g as u128;
            page.cells[(id & SLOT_MASK) as usize]
                .0
                .store(x, std::sync::atomic::Ordering::Release);
        }
    }
}

// --------------------------------------------------- directory table ----

struct Dir<C> {
    pages: Box<[AtomicPtr<Page<C>>]>,
}

impl<C> Dir<C> {
    fn with_len(len: usize) -> Box<Dir<C>> {
        let pages: Vec<AtomicPtr<Page<C>>> =
            (0..len).map(|_| AtomicPtr::new(std::ptr::null_mut())).collect();
        Box::new(Dir { pages: pages.into_boxed_slice() })
    }
}

/// Grow-only two-level table over any [`CellKind`].
pub struct DirectTable<C: CellKind> {
    dir: AtomicPtr<Dir<C>>,
    /// Growth lock + owner of every directory ever published (retired dirs
    /// stay alive for readers; freed on drop).
    grow: Mutex<Vec<*mut Dir<C>>>,
}

unsafe impl<C: CellKind> Send for DirectTable<C> {}
unsafe impl<C: CellKind> Sync for DirectTable<C> {}

impl<C: CellKind> DirectTable<C> {
    pub fn new(initial_pages: usize) -> Self {
        let dir = Box::into_raw(Dir::<C>::with_len(initial_pages.max(1)));
        DirectTable { dir: AtomicPtr::new(dir), grow: Mutex::new(vec![dir]) }
    }

    #[inline]
    fn load_dir(&self) -> &Dir<C> {
        // Safety: directories are never freed before drop.
        unsafe { &*self.dir.load(Acquire) }
    }

    /// Coherent point read. `(0, 0)` means "no stream".
    #[inline]
    pub fn get(&self, id: u64, retries: &mut u32) -> (u64, u64) {
        let dir = self.load_dir();
        let pidx = (id >> PAGE_BITS) as usize;
        if pidx >= dir.pages.len() {
            return (0, 0);
        }
        let page = dir.pages[pidx].load(Acquire);
        if page.is_null() {
            return (0, 0);
        }
        C::read(unsafe { &*page }, (id & SLOT_MASK) as usize, retries)
    }

    /// Ensure the page holding `id` exists (writer side; takes the grow lock
    /// only when a page or directory slot is actually missing).
    pub fn ensure(&self, id: u64) {
        let pidx = (id >> PAGE_BITS) as usize;
        let dir = self.load_dir();
        if pidx < dir.pages.len() && !dir.pages[pidx].load(Acquire).is_null() {
            return;
        }
        let mut owned = self.grow.lock().unwrap();
        let mut dir = self.load_dir();
        if pidx >= dir.pages.len() {
            let new_len = (pidx + 1).next_power_of_two().max(dir.pages.len() * 2);
            let new_dir = Dir::<C>::with_len(new_len);
            for (i, p) in dir.pages.iter().enumerate() {
                new_dir.pages[i].store(p.load(Acquire), Relaxed);
            }
            let raw = Box::into_raw(new_dir);
            owned.push(raw);
            self.dir.store(raw, Release); // publish grown directory
            dir = unsafe { &*raw };
        }
        if dir.pages[pidx].load(Acquire).is_null() {
            let page = Box::into_raw(Box::new(Page::<C>::with_len(PAGE_SIZE)));
            dir.pages[pidx].store(page, Release);
        }
    }

    /// Single-writer batch apply. Groups updates by page so the seqlock kind
    /// pays one odd/even cycle per touched page per batch. Sorting is part of
    /// the measured writer cost (the real kernel would group the same way).
    pub fn apply(&self, updates: &mut [Update]) {
        if updates.is_empty() {
            return;
        }
        if updates.len() > 1 {
            updates.sort_unstable_by_key(|u| u.0 >> PAGE_BITS);
        }
        let mut start = 0;
        while start < updates.len() {
            let pidx = updates[start].0 >> PAGE_BITS;
            let mut end = start + 1;
            while end < updates.len() && updates[end].0 >> PAGE_BITS == pidx {
                end += 1;
            }
            self.ensure(updates[start].0);
            let dir = self.load_dir();
            let page = dir.pages[pidx as usize].load(Acquire);
            debug_assert!(!page.is_null());
            C::write_batch(unsafe { &*page }, &updates[start..end]);
            start = end;
        }
    }

    /// Bytes actually allocated for cells + directory (analytic, for the
    /// memory gate alongside measured RSS).
    pub fn allocated_bytes(&self) -> usize {
        let owned = self.grow.lock().unwrap();
        let dir = self.load_dir();
        let mut pages = 0usize;
        for p in dir.pages.iter() {
            if !p.load(Acquire).is_null() {
                pages += 1;
            }
        }
        // Explicit borrow: rustc's `dangerous_implicit_autorefs` wants the
        // raw-pointer deref's autoref spelled out (clippy disagrees).
        #[allow(clippy::needless_borrow)]
        let dirs: usize = owned
            .iter()
            .map(|d| unsafe { (&(**d).pages).len() } * size_of::<AtomicPtr<Page<C>>>())
            .sum();
        pages * (size_of::<C>() * PAGE_SIZE + size_of::<AtomicU64>()) + dirs
    }
}

impl<C: CellKind> Drop for DirectTable<C> {
    fn drop(&mut self) {
        let owned = self.grow.get_mut().unwrap();
        // Free pages once, from the newest (superset) directory.
        let newest = *owned.last().unwrap();
        unsafe {
            for p in (*newest).pages.iter() {
                let p = p.load(Relaxed);
                if !p.is_null() {
                    drop(Box::from_raw(p));
                }
            }
            for d in owned.drain(..) {
                drop(Box::from_raw(d));
            }
        }
    }
}
