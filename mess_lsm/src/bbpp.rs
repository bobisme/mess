use std::{
    marker::PhantomData,
    ptr::NonNull,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
};

use parking_lot::{Condvar, Mutex};

use crate::protector::{BorrowedProtector, ProtectorPool};

#[derive(Debug)]
#[repr(align(64))]
pub struct AtomicIndex(AtomicUsize);

impl Default for AtomicIndex {
    fn default() -> Self {
        Self::new(0)
    }
}

impl AtomicIndex {
    pub const fn new(index: usize) -> Self {
        Self(AtomicUsize::new(index))
    }

    pub fn get(&self) -> usize {
        self.0.load(Ordering::Acquire)
    }

    pub fn set(&self, index: usize) {
        self.0.store(index, Ordering::Release);
    }
}

#[derive(Debug)]
pub struct Range {
    head: AtomicIndex,
    tail: AtomicIndex,
}

pub struct Reader<'a> {
    protector: BorrowedProtector<'a, Arc<(Mutex<bool>, Condvar)>>,
}

pub struct BBPPIterator<'a> {
    reader: Reader<'a>,
}

pub struct Writer<'a> {
    // bbpp: &'a BBPP,
    bbpp: NonNull<BBPP>,
    _mark: PhantomData<&'a ()>,
}

pub struct BBPP {
    protectors: ProtectorPool<Arc<(Mutex<bool>, Condvar)>, 64>,
    is_writer_leased: AtomicBool,
}

impl BBPP {
    pub fn new() -> Self {
        let released = Arc::new((Mutex::new(false), Condvar::new()));
        Self {
            protectors: ProtectorPool::new(released),
            is_writer_leased: AtomicBool::new(false),
        }
    }

    fn new_reader(&self) -> Reader {
        let protector = self.protectors.blocking_get();

        Reader { protector }
    }

    fn try_get_writer(&self) -> Option<Writer> {
        let res = self.is_writer_leased.compare_exchange(
            false,
            true,
            Ordering::Release,
            Ordering::Acquire,
        );
        if res.is_err() {
            return None;
        }
        let bbpp =
            unsafe { NonNull::new_unchecked(self as *const _ as *mut _) };
        Some(Writer { bbpp, _mark: PhantomData })
    }

    pub fn iter(&self) -> BBPPIterator {
        let read = self.new_reader();
        BBPPIterator { reader: read }
    }
}

impl Default for BBPP {
    fn default() -> Self {
        Self::new()
    }
}
