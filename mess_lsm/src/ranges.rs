#![warn(clippy::missing_const_for_fn)]
#[cfg(loom)]
use loom::sync::atomic::{AtomicUsize, Ordering};
#[cfg(not(loom))]
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::error::{Error, Result};

#[derive(Debug)]
#[repr(align(128))]
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

    pub fn fetch_add(&self, val: usize) -> usize {
        self.0.fetch_add(val, Ordering::AcqRel)
    }
}

#[derive(Debug)]
pub struct Range {
    head: AtomicIndex,
    tail: AtomicIndex,
}

impl Range {
    pub const fn new(head: usize, tail: usize) -> Self {
        Self { head: AtomicIndex::new(head), tail: AtomicIndex::new(tail) }
    }

    pub fn range(&self) -> std::ops::Range<usize> {
        self.head.get()..self.tail.get()
    }

    pub fn is_empty(&self) -> bool {
        self.range().is_empty()
    }

    pub fn reset(&self) {
        self.head.set(0);
        self.tail.set(0);
    }
}

#[derive(Debug)]
pub enum RangeRefs<'a> {
    One(&'a Range),
    Two { read: &'a Range, write: &'a Range },
}

impl<'a> PartialEq for RangeRefs<'a> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (&Self::One(l0), &Self::One(r0)) => std::ptr::eq(l0, r0),
            (
                &Self::Two { read: l_read, write: l_write },
                &Self::Two { read: r_read, write: r_write },
            ) => std::ptr::eq(l_read, r_read) && std::ptr::eq(l_write, r_write),
            _ => false,
        }
    }
}

pub struct Ranges<const N: usize> {
    is_split: bool,
    inner: (Range, Range),
}

impl<const N: usize> Ranges<N> {
    pub const fn new() -> Self {
        Self { is_split: false, inner: (Range::new(0, 0), Range::new(0, 0)) }
    }
    pub fn refs(&self) -> RangeRefs {
        match self.is_split {
            false => RangeRefs::One(&self.inner.1),
            true => {
                RangeRefs::Two { read: &self.inner.1, write: &self.inner.0 }
            }
        }
    }

    pub fn read(&self) -> &Range {
        &self.inner.1
    }

    pub fn read_mut(&mut self) -> &mut Range {
        &mut self.inner.1
    }

    pub fn write(&self) -> &Range {
        match self.is_split {
            false => &self.inner.1,
            true => &self.inner.0,
        }
    }

    pub fn write_mut(&mut self) -> &mut Range {
        match self.is_split {
            false => &mut self.inner.1,
            true => &mut self.inner.0,
        }
    }

    pub fn split(&mut self) {
        if self.is_split {
            return;
        }
        self.is_split = true;
        self.inner.0.reset();
    }

    fn merge(&mut self) {
        if !self.is_split {
            return;
        }
        self.inner.1.head.set(self.inner.0.head.get());
        self.inner.1.tail.set(self.inner.0.tail.get());
        self.inner.0.reset();
        self.is_split = false;
    }

    pub fn grow(&mut self, len: usize) -> Result<()> {
        let write = self.write_mut();
        let range_end = write.tail.get();
        let bounds_idx = match self.is_split {
            false => N,
            true => self.read().head.get(),
        };
        if range_end + len > bounds_idx {
            return Err(Error::RangeFull);
        }
        self.write_mut().tail.fetch_add(len);
        Ok(())
    }

    pub fn shrink(&mut self, len: usize) -> Result<()> {
        let read_range = self.read_mut();
        let range_start = read_range.head.get();
        let range_end = read_range.tail.get();
        if range_start + len > range_end {
            return Err(Error::RangeEmpty);
        }
        read_range.head.fetch_add(len);
        if read_range.is_empty() {
            self.merge();
        }
        Ok(())
    }

    pub fn size(&self) -> usize {
        match self.is_split {
            false => self.inner.1.range().len(),
            true => self.inner.0.range().len() + self.inner.1.range().len(),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    mod size {
        use super::*;
        use assert2::assert;
        use rstest::*;
        #[rstest]
        fn it_only_uses_size_of_2nd_range_if_one() {
            let mut ranges: Ranges<10> = Ranges {
                is_split: false,
                inner: (Range::new(0, 4), Range::new(5, 10)),
            };

            assert!(ranges.size() == 5);
        }

        #[rstest]
        fn it_uses_size_of_both_if_2() {
            let mut ranges: Ranges<10> = Ranges {
                is_split: true,
                inner: (Range::new(0, 4), Range::new(5, 10)),
            };
            assert!(ranges.size() == 9);
        }
    }

    mod refs {
        use super::*;
        use assert2::assert;
        use rstest::*;

        #[rstest]
        fn it_return_one_if_not_split() {
            let mut ranges: Ranges<10> = Ranges {
                is_split: false,
                inner: (Range::new(0, 0), Range::new(0, 6)),
            };
            assert!(ranges.refs() == RangeRefs::One(&ranges.inner.1));
        }

        #[rstest]
        fn it_return_two_if_split() {
            let mut ranges: Ranges<10> = Ranges {
                is_split: true,
                inner: (Range::new(0, 3), Range::new(5, 6)),
            };
            assert!(
                ranges.refs()
                    == RangeRefs::Two {
                        read: &ranges.inner.1,
                        write: &ranges.inner.0,
                    }
            );
        }
    }

    mod grow {
        use super::*;
        use assert2::assert;
        use rstest::*;

        #[rstest]
        fn it_only_grows_2nd_if_one() {
            let mut ranges: Ranges<20> = Ranges {
                is_split: false,
                inner: (Range::new(0, 0), Range::new(5, 10)),
            };
            ranges.grow(5).unwrap();
            assert!(ranges.size() == 10);
            assert!(ranges.inner.0.range().start == 0);
            assert!(ranges.inner.0.range().end == 0);
            assert!(ranges.inner.1.range().start == 5);
            assert!(ranges.inner.1.range().end == 15);
        }

        #[rstest]
        fn it_only_grows_1st_if_two() {
            let mut ranges: Ranges<20> = Ranges {
                is_split: true,
                inner: (Range::new(0, 0), Range::new(5, 10)),
            };
            ranges.grow(5).unwrap();
            assert!(ranges.size() == 10);
            assert!(ranges.inner.0.range().start == 0);
            assert!(ranges.inner.0.range().end == 5);
            assert!(ranges.inner.1.range().start == 5);
            assert!(ranges.inner.1.range().end == 10);
        }

        #[rstest]
        fn it_errors_on_one_if_out_of_bounds() {
            let mut ranges: Ranges<10> = Ranges {
                is_split: false,
                inner: (Range::new(0, 0), Range::new(5, 9)),
            };
            assert!(matches!(ranges.grow(1), Ok(_)));
            assert!(matches!(ranges.grow(1), Err(_)));
        }

        #[rstest]
        fn it_errors_on_two_if_would_overlap() {
            let mut ranges: Ranges<10> = Ranges {
                is_split: true,
                inner: (Range::new(0, 4), Range::new(5, 10)),
            };
            assert!(ranges.grow(1) == Ok(()));
            assert!(matches!(ranges.grow(1), Err(_)));
        }
    }

    mod shrink {
        use super::*;
        use assert2::assert;
        use rstest::*;

        #[rstest]
        fn it_only_shrinks_2nd_if_one() {
            let mut ranges: Ranges<20> = Ranges {
                is_split: false,
                inner: (Range::new(0, 0), Range::new(5, 10)),
            };
            ranges.shrink(2).unwrap();
            assert!(ranges.size() == 3);
            assert!(ranges.inner.0.range().start == 0);
            assert!(ranges.inner.0.range().end == 0);
            assert!(ranges.inner.1.range().start == 7);
            assert!(ranges.inner.1.range().end == 10);
        }

        #[rstest]
        fn it_only_shrinks_2nd_if_two() {
            let mut ranges: Ranges<20> = Ranges {
                is_split: true,
                inner: (Range::new(0, 4), Range::new(5, 10)),
            };
            ranges.shrink(2).unwrap();
            assert!(ranges.size() == 7);
            assert!(ranges.inner.0.range().start == 0);
            assert!(ranges.inner.0.range().end == 4);
            assert!(ranges.inner.1.range().start == 7);
            assert!(ranges.inner.1.range().end == 10);
        }

        #[rstest]
        fn it_errors_if_range_empty() {
            let mut ranges: Ranges<10> = Ranges {
                is_split: false,
                inner: (Range::new(0, 0), Range::new(8, 9)),
            };
            assert!(matches!(ranges.grow(1), Ok(_)));
            assert!(matches!(ranges.grow(1), Err(_)));
        }

        #[rstest]
        fn it_merges_two_ranges_if_read_is_empty() {
            let mut ranges: Ranges<10> = Ranges {
                is_split: true,
                inner: (Range::new(0, 4), Range::new(9, 10)),
            };
            assert!(ranges.shrink(1) == Ok(()));
            assert!(ranges.is_split == false);
        }
    }

    mod merge {
        use super::*;
        use assert2::assert;
        use rstest::*;

        #[rstest]
        fn merging_resets_first_range() {
            let mut ranges: Ranges<10> = Ranges {
                is_split: true,
                inner: (Range::new(0, 4), Range::new(10, 10)),
            };
            ranges.merge();
            assert!(ranges.inner.0.head.get() == 0);
            assert!(ranges.inner.0.tail.get() == 0);
        }

        #[rstest]
        fn merging_sets_2nd_range_to_1st_indexes() {
            let mut ranges: Ranges<10> = Ranges {
                is_split: true,
                inner: (Range::new(0, 4), Range::new(10, 10)),
            };
            ranges.merge();
            assert!(ranges.inner.1.head.get() == 0);
            assert!(ranges.inner.1.tail.get() == 4);
        }
    }
}
