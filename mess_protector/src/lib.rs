use core::ops;
use crossbeam_utils::CachePadded;
#[cfg(loom)]
use loom::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
#[cfg(not(loom))]
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use parking_lot::{Condvar, Mutex};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Protection {
    ProtectedFrom(usize),
    Unprotected,
}

impl Protection {
    pub const fn is_unprotected(&self) -> bool {
        matches!(self, Protection::Unprotected)
    }

    pub const fn is_protected(&self) -> bool {
        matches!(self, Protection::ProtectedFrom(_))
    }
}

impl From<usize> for Protection {
    fn from(value: usize) -> Self {
        match value {
            usize::MAX => Self::Unprotected,
            index => Self::ProtectedFrom(index),
        }
    }
}

#[derive(Debug)]
#[repr(transparent)]
pub struct Protector(CachePadded<AtomicUsize>);

impl Protector {
    // Hack so this can be used in array initialization.
    // There should be a test below to show this is sound.
    #[allow(clippy::declare_interior_mutable_const)]
    #[cfg(not(loom))]
    pub const NEW: Protector = Protector::new();

    #[cfg(not(loom))]
    pub const fn new() -> Self {
        Self(CachePadded::new(AtomicUsize::new(usize::MAX)))
    }

    #[cfg(loom)]
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Self(CachePadded::new(AtomicUsize::new(usize::MAX)))
    }

    pub fn protect(&self, index: usize) {
        self.0.store(index, Ordering::Release);
    }

    pub fn release(&self) {
        self.0.store(usize::MAX, Ordering::Release);
    }

    pub fn state(&self) -> Protection {
        Protection::from(self.0.load(Ordering::Acquire))
    }
}

#[cfg(all(test, not(loom)))]
mod test_protector {
    use super::*;
    use assert2::assert;

    #[test]
    fn new_const_doesnt_affect_other_protectors() {
        let p1 = Protector::NEW;
        let p2 = Protector::NEW;
        p1.protect(69);
        assert!(p1.state() == Protection::ProtectedFrom(69));
        assert!(p2.state() != Protection::ProtectedFrom(69));
    }
}

pub trait Release {
    fn release(&self);
}

impl Release for Arc<(Mutex<bool>, Condvar)> {
    fn release(&self) {
        let (lock, cvar) = &**self;
        *lock.lock() = true;
        cvar.notify_one();
    }
}

impl Release for () {
    fn release(&self) {}
}

#[derive(Debug)]
pub struct BorrowedProtector<'a, R: Release> {
    protector: &'a Protector,
    // released: Option<Arc<(Mutex<bool>, Condvar)>>,
    released: R,
}

impl<'a, R: Release> BorrowedProtector<'a, R> {
    pub const fn new(protector: &'a Protector, released: R) -> Self {
        // protector.protect(0);
        Self { protector, released }
    }
}

impl<'a, R: Release> ops::Deref for BorrowedProtector<'a, R> {
    type Target = Protector;

    fn deref(&self) -> &Self::Target {
        self.protector
    }
}

impl<'a, R: Release> Drop for BorrowedProtector<'a, R> {
    fn drop(&mut self) {
        self.protector.release();
        self.released.release();
    }
}

#[derive(Debug)]
pub struct ProtectorPool<R, const N: usize> {
    protectors: [Protector; N],
    released: R,
}

impl<R, const N: usize> ProtectorPool<R, N>
where
    R: Release + Clone,
{
    #[cfg(not(loom))]
    pub const fn new(released: R) -> Self {
        Self { protectors: [Protector::NEW; N], released }
    }
    #[cfg(loom)]
    pub fn new(released: R) -> Self {
        Self { protectors: std::array::from_fn(|_| Protector::new()), released }
    }

    /// Get the minimum Protection in the given range.
    /// Returns Protection::Unprotected if none in range.
    pub fn minimum_protected(&self, range: ops::Range<usize>) -> Protection {
        let min = self
            .protectors
            .iter()
            .map(|p| p.state())
            .filter_map(|s| match s {
                p @ Protection::ProtectedFrom(idx) => match idx {
                    i if range.contains(&i) => Some(p),
                    _ => None,
                },
                Protection::Unprotected => None,
            })
            .min();
        min.unwrap_or(Protection::Unprotected)
    }

    pub fn protected_range(
        &self,
        range: ops::Range<usize>,
    ) -> Option<ops::Range<usize>> {
        let min = self.minimum_protected(range.clone());
        match min {
            Protection::ProtectedFrom(idx) => Some(idx..range.end),
            Protection::Unprotected => None,
        }
    }

    pub fn try_get(&self) -> Option<BorrowedProtector<R>> {
        for p in self.protectors.iter() {
            let acquired = p.0.compare_exchange(
                usize::MAX,
                0,
                Ordering::Release,
                Ordering::Acquire,
            );
            if acquired.is_ok() {
                return Some(BorrowedProtector::new(p, self.released.clone()));
            }
        }
        None
    }
}

impl<const N: usize> ProtectorPool<Arc<(Mutex<bool>, Condvar)>, N> {
    pub fn blocking_get(
        &self,
    ) -> BorrowedProtector<Arc<(Mutex<bool>, Condvar)>> {
        let protector = self.try_get();
        if let Some(protector) = protector {
            return protector;
        }
        let (lock, cvar) = &*self.released;
        loop {
            let mut released = lock.lock();
            if !*released {
                cvar.wait(&mut released);
            }
            let protector = self.try_get();
            if let Some(protector) = protector {
                return protector;
            }
        }
        // let (lock, cvar) = &*self.released;
    }
}

#[cfg(all(test, not(loom)))]
mod test_protector_pool {
    use super::*;
    use assert2::assert;

    #[test]
    fn minimum_protected_picks_min_if_all_protected() {
        let pool = ProtectorPool::<(), 4>::new(());
        pool.protectors[0].protect(20);
        pool.protectors[1].protect(10);
        pool.protectors[2].protect(40);
        pool.protectors[3].protect(30);
        assert!(pool.minimum_protected(0..99) == Protection::ProtectedFrom(10));
    }

    #[test]
    fn minimum_protected_if_in_range() {
        let pool = ProtectorPool::<(), 4>::new(());
        pool.protectors[0].protect(20);
        pool.protectors[1].protect(10);
        pool.protectors[2].protect(40);
        pool.protectors[3].protect(30);
        assert!(
            pool.minimum_protected(25..99) == Protection::ProtectedFrom(30)
        );
    }

    #[test]
    fn minimum_protected_picks_min_if_some_protected() {
        let pool = ProtectorPool::<(), 4>::new(());
        pool.protectors[0].protect(20);
        pool.protectors[1].protect(10);
        assert!(pool.minimum_protected(0..99) == Protection::ProtectedFrom(10));
    }

    #[test]
    fn minimum_protected_returns_unprotected_if_none_protected_in_range() {
        let pool = ProtectorPool::<(), 4>::new(());
        pool.protectors[0].protect(20);
        assert!(pool.minimum_protected(0..25) == Protection::ProtectedFrom(20));
        assert!(pool.minimum_protected(25..99) == Protection::Unprotected);
    }

    #[test]
    fn minimum_protected_returns_unprotected_if_array_empty() {
        let pool = ProtectorPool::<(), 0>::new(());
        assert!(pool.minimum_protected(0..99) == Protection::Unprotected);
    }

    #[test]
    fn get_works_until_it_cant() {
        let pool = ProtectorPool::<(), 3>::new(());
        let p1 = pool.try_get();
        assert!(p1.is_some());
        let p2 = pool.try_get();
        assert!(p2.is_some());
        let p3 = pool.try_get();
        assert!(p3.is_some());
        let p4 = pool.try_get();
        assert!(p4.is_none());
    }

    #[test]
    fn releasing_and_getting_works() {
        let pool = ProtectorPool::<(), 3>::new(());
        let p1 = pool.try_get();
        let _p2 = pool.try_get();
        let _p3 = pool.try_get();
        let p4 = pool.try_get();
        assert!(p4.is_none());
        drop(p1);
        let p4 = pool.try_get();
        assert!(p4.is_some());
    }
}
