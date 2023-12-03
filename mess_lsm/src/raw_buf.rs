use core::slice;
use std::{
    alloc::{self, Layout, LayoutError},
    cmp,
    collections::TryReserveError,
    mem,
    ops::{Bound, Range, RangeBounds},
    ptr::NonNull,
};

use crate::error::{Error, Result};

const LEN_SIZE: usize = core::mem::size_of::<usize>();

/// Panics.
fn check_capacity(capacity: usize, max_capacity: usize) {
    if capacity > max_capacity
        || usize::BITS < 64 && capacity > isize::MAX as usize
    {
        capacity_overflow();
    }
}

#[cfg(not(no_global_oom_handling))]
#[cfg_attr(not(feature = "panic_immediate_abort"), inline(never))]
fn capacity_overflow() -> ! {
    panic!("capacity overflow");
}

#[inline(never)]
fn finish_grow(
    new_layout: core::result::Result<Layout, LayoutError>,
    current_memory: Option<(NonNull<u8>, Layout)>,
    max_capacity: usize,
) -> core::result::Result<NonNull<u8>, TryReserveError> {
    // Check for the error here to minimize the size of `RawVec::grow_*`.
    let new_layout = new_layout.unwrap_or_else(|_| capacity_overflow());
    let new_size = new_layout.size();

    check_capacity(new_size, max_capacity);

    let memory = if let Some((ptr, old_layout)) = current_memory {
        debug_assert_eq!(old_layout.align(), new_layout.align());
        unsafe {
            // The allocator checks for alignment equality
            // core::intrinsics::assume(old_layout.align() == new_layout.align());
            alloc::realloc(ptr.as_ptr(), new_layout, new_size)
        }
    } else {
        unsafe { alloc::alloc(new_layout) }
    };
    Ok(NonNull::new(memory.cast()).expect("allocation error"))
}

#[derive(Debug)]
pub struct RawBuf<const N: usize> {
    pub(crate) ptr: NonNull<u8>,
    cap: usize,
}

impl<const N: usize> RawBuf<N> {
    const MIN_NON_ZERO_CAP: usize = 8;

    pub const fn new() -> Self {
        Self { ptr: NonNull::dangling(), cap: 0 }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        dbg!("RawBuf::allocate");
        let cap = cmp::max(capacity, Self::MIN_NON_ZERO_CAP);
        let layout = match Layout::array::<u8>(cap) {
            Ok(layout) => layout,
            Err(_) => capacity_overflow(),
        };
        check_capacity(capacity, N);
        let new_ptr = unsafe { alloc::alloc(layout) };
        let ptr = match NonNull::new(new_ptr) {
            Some(p) => p,
            None => alloc::handle_alloc_error(layout),
        };
        Self { ptr, cap }
    }

    const fn current_memory(&self) -> Option<(NonNull<u8>, Layout)> {
        if self.cap == 0 {
            return None;
        }
        // We could use Layout::array here which ensures the absence of isize and usize overflows
        // and could hypothetically handle differences between stride and size, but this memory
        // has already been allocated so we know it can't overflow and currently rust does not
        // support such types. So we can do better by skipping some checks and avoid an unwrap.
        const _: () =
            assert!(mem::size_of::<u8>() % mem::align_of::<u8>() == 0);
        unsafe {
            let align = mem::align_of::<u8>();
            let size = mem::size_of::<u8>() * self.cap;
            let layout = Layout::from_size_align_unchecked(size, align);
            let nn = NonNull::new_unchecked(self.ptr.as_ptr());
            Some((nn, layout))
        }
    }

    pub(crate) fn grow_amortized(
        &mut self,
        len: usize,
        additional: usize,
    ) -> Result<()> {
        // This is ensured by the calling contexts.
        debug_assert!(additional > 0);

        // Nothing we can really do about these checks, sadly.
        let required_cap =
            len.checked_add(additional).ok_or_else(capacity_overflow).unwrap();
        if len > N {
            return Err(Error::CapacityOverLimit {
                cap: required_cap,
                limit: N,
            });
        }

        // This guarantees exponential growth. The doubling cannot overflow
        // because `cap <= isize::MAX` and the type of `cap` is `usize`.
        let cap = cmp::max(self.cap * 2, required_cap);
        let cap = cmp::max(Self::MIN_NON_ZERO_CAP, cap);
        let cap = cmp::min(N, cap);

        let new_layout = Layout::array::<u8>(cap);

        let ptr = finish_grow(new_layout, self.current_memory(), N)?;
        self.set_ptr_and_cap(ptr, cap);
        Ok(())
    }

    fn set_ptr_and_cap(&mut self, ptr: NonNull<u8>, cap: usize) {
        self.ptr = unsafe { NonNull::new_unchecked(ptr.cast().as_ptr()) };
        self.cap = cap;
    }

    pub const fn cap(&self) -> usize {
        self.cap
    }

    pub fn len_at(&self, index: usize) -> usize {
        usize::from_ne_bytes(self[index..index + LEN_SIZE].try_into().unwrap())
    }

    pub fn read(&self, index: usize) -> &[u8] {
        let len = self.len_at(index);
        let range = index + LEN_SIZE..index + len;
        self[range].try_into().unwrap()
    }
}

// impl<const N: usize> std::ops::Index<usize> for RawBuf<N> {
//     type Output = u8;
//
//     /// Panics if ptr is null.
//     fn index(&self, index: usize) -> &Self::Output {
//         if index > self.cap {
//             panic!("index out of bounds");
//         }
//         let new_ptr = unsafe { self.ptr.as_ptr().add(index) };
//         unsafe { new_ptr.as_ref().expect("buffer has not been initalized") }
//     }
// }
//
// impl<const N: usize> std::ops::IndexMut<usize> for RawBuf<N> {
//     fn index_mut(&mut self, index: usize) -> &mut Self::Output {
//         if index > self.cap {
//             panic!("index out of bounds");
//         }
//         let new_ptr = unsafe { self.ptr.as_ptr().add(index) };
//         unsafe { new_ptr.as_mut().expect("buffer has not been initalized") }
//     }
// }
fn bounds_to_range<R: RangeBounds<usize>>(
    bounds: R,
    max_exclusive: usize,
) -> Range<usize> {
    let start = match bounds.start_bound().cloned() {
        Bound::Included(x) => x,
        Bound::Excluded(x) => x + 1,
        Bound::Unbounded => 0,
    };
    let end = match bounds.end_bound().cloned() {
        Bound::Included(x) => x + 1,
        Bound::Excluded(x) => x,
        Bound::Unbounded => max_exclusive,
    };
    start..end
}

fn bounds_in_range<R: RangeBounds<usize>>(
    bounds: &R,
    range: Range<usize>,
) -> bool {
    let start = match bounds.start_bound().cloned() {
        Bound::Included(x) => x,
        Bound::Excluded(x) => x + 1,
        Bound::Unbounded => 0,
    };
    let end = match bounds.end_bound().cloned() {
        Bound::Included(x) => x,
        Bound::Excluded(x) => x - 1,
        Bound::Unbounded => 0,
    };
    !(range.contains(&start) && range.contains(&end))
}

// impl<const N: usize> std::ops::Index<Range<usize>> for RawBuf<N> {
impl<R: RangeBounds<usize>, const N: usize> std::ops::Index<R> for RawBuf<N> {
    type Output = [u8];

    /// Panics if ptr is null.
    fn index(&self, bounds: R) -> &Self::Output {
        if !bounds_in_range(&bounds, 0..self.cap) {
            panic!("index out of bounds");
        }
        let range = bounds_to_range(bounds, self.cap);
        let new_ptr = unsafe { self.ptr.as_ptr().add(range.start) };
        unsafe { slice::from_raw_parts(new_ptr, range.end - range.start) }
    }
}

impl<R: RangeBounds<usize>, const N: usize> std::ops::IndexMut<R>
    for RawBuf<N>
{
    /// Panics if ptr is null.
    fn index_mut(&mut self, bounds: R) -> &mut Self::Output {
        if !bounds_in_range(&bounds, 0..self.cap) {
            panic!("index out of bounds");
        }
        let range = bounds_to_range(bounds, self.cap);
        let new_ptr = unsafe { self.ptr.as_ptr().add(range.start) };
        unsafe { slice::from_raw_parts_mut(new_ptr, range.end - range.start) }
    }
}

#[cfg(all(test, not(loom), not(lincheck)))]
mod test_slice_tools {
    use super::*;
    use assert2::assert;

    const FIXTURE_CAP: usize = 12;

    fn buf_fixture() -> RawBuf<FIXTURE_CAP> {
        let mut buf = RawBuf::with_capacity(FIXTURE_CAP);
        buf[..LEN_SIZE].copy_from_slice(&usize::to_ne_bytes(4)[..]);
        // let buf_ptr = buf.cast::<u8>();
        buf[LEN_SIZE..12].copy_from_slice(&[42, 43, 44, 45][..]);
        // unsafe {
        //     bufllptr.as_ptr().add(8) = 42;
        //     bufllptr.as_ptr().add(9) = 43;
        //     bufllptr.as_ptr().add(10) = 44;
        //     bufllptr.as_ptr().add(11) = 45;
        // }
        buf
    }

    #[test]
    fn test_read_len_at() {
        let mut buf = RawBuf::<FIXTURE_CAP>::with_capacity(FIXTURE_CAP);
        buf[..LEN_SIZE].copy_from_slice(&usize::to_ne_bytes(42)[..]);
        assert!(buf.len_at(0) == 42);
    }

    #[test]
    fn test_read_at() {
        let buf = buf_fixture();
        assert!(buf.read(0) == &[42, 43, 44, 45]);
    }

    #[test]
    fn test_slice_buffer() {
        let buf = buf_fixture();
        assert!(buf[9..11] == [43, 44][..]);
    }

    #[test]
    fn test_slice_buffer_mut() {
        let mut buf = buf_fixture();
        buf[9..11].copy_from_slice(&[46, 47][..]);
        assert!(buf[9..11] == [46, 47][..]);
    }
}

#[cfg(all(test, kani))]
mod test {
    use super::*;
    use assert2::assert;

    #[test]
    fn it_works() {}
}

#[cfg(all(test, proptest))]
mod test_props {
    use super::*;
    use assert2::assert;
    use proptest::prelude::*;
    // (start in 0..=end, end in Just(end), size in Just(size))
    prop_compose! {
        fn size_and_end()
            (size in 1..=4096usize)
            (end in 0..size, size in Just(size))
            -> (usize, usize) {
            (end, size)
        }
    }
    prop_compose! {
        fn boundies()
            ((end, size) in size_and_end())
            (start in 0..end, end in Just(end), size in Just(size))
            -> (usize, usize, usize) {
            (start, end, size)
        }
    }

    proptest! {
        #[test]
        fn it_works((start, end, size) in boundies()) {
            let mut buf = RawBuf::<4096>::new();
            buf.grow_amortized(0, size).unwrap();
            assert!(buf.cap == size);
            buf[start..end].fill(69u8);
        }
    }
}
