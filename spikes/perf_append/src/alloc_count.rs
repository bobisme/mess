//! Global allocation counter: counts every allocation (and allocated bytes)
//! process-wide via a wrapping GlobalAlloc. Used to report allocs/event per
//! load phase. Realloc counts as one allocation event (it may move memory).

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

static ALLOC_CALLS: AtomicU64 = AtomicU64::new(0);
static ALLOC_BYTES: AtomicU64 = AtomicU64::new(0);

pub struct CountingAlloc;

unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOC_CALLS.fetch_add(1, Relaxed);
        ALLOC_BYTES.fetch_add(layout.size() as u64, Relaxed);
        System.alloc(layout)
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        System.dealloc(ptr, layout)
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        ALLOC_CALLS.fetch_add(1, Relaxed);
        ALLOC_BYTES.fetch_add(layout.size() as u64, Relaxed);
        System.alloc_zeroed(layout)
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        ALLOC_CALLS.fetch_add(1, Relaxed);
        ALLOC_BYTES.fetch_add(new_size as u64, Relaxed);
        System.realloc(ptr, layout, new_size)
    }
}

#[derive(Clone, Copy, Debug)]
pub struct AllocSnapshot {
    pub calls: u64,
    pub bytes: u64,
}

pub fn snapshot() -> AllocSnapshot {
    AllocSnapshot {
        calls: ALLOC_CALLS.load(Relaxed),
        bytes: ALLOC_BYTES.load(Relaxed),
    }
}

pub fn delta(a: AllocSnapshot, b: AllocSnapshot) -> AllocSnapshot {
    AllocSnapshot { calls: b.calls - a.calls, bytes: b.bytes - a.bytes }
}
