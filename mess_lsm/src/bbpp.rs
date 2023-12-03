use core::ops;
#[cfg(loom)]
use loom::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
#[cfg(not(loom))]
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
//
use std::{
    cell::UnsafeCell,
    io::Write,
    marker::PhantomData,
    mem::MaybeUninit,
    ops::IndexMut,
    os::fd::AsFd,
    ptr::NonNull,
    slice::{from_raw_parts, from_raw_parts_mut},
};

use parking_lot::{Condvar, Mutex};

use crate::{
    error::{Error, Result},
    protector::{BorrowedProtector, ProtectorPool, Release},
    ranges::{RangeRefs, Ranges},
    raw_buf::RawBuf,
};

const LEN_SIZE: usize = core::mem::size_of::<usize>();
const FREE_RATIO: f32 = 0.1;

type InnerBuf<const N: usize> = UnsafeCell<MaybeUninit<NonNull<u8>>>;
type ArcMutCond = Arc<(Mutex<bool>, Condvar)>;

#[inline(always)]
fn slice_buffer_mut<'a>(
    buf_start: *mut u8,
    offset: usize,
    len: usize,
    cap: usize,
) -> &'a mut [u8] {
    if offset + len > cap {
        panic!("tried to get a mut slice beyond allocated memory");
    }
    unsafe { from_raw_parts_mut(buf_start.add(offset), len) }
}

#[inline(always)]
fn slice_buffer<'a>(
    buf_start: *mut u8,
    offset: usize,
    len: usize,
    cap: usize,
) -> &'a [u8] {
    if offset + len > cap {
        panic!("tried to get a slice beyond allocated memory");
    }
    unsafe { from_raw_parts(buf_start.add(offset), len) }
}

#[inline(always)]
fn read_len_at(buf_start: *mut u8, offset: usize, cap: usize) -> usize {
    let len_bytes = slice_buffer(buf_start, offset, LEN_SIZE, cap);
    usize::from_ne_bytes(
        len_bytes.try_into().expect("did not slice enough to read len"),
    )
}

#[inline(always)]
fn read_at<'a>(buf_start: *mut u8, offset: usize, cap: usize) -> &'a [u8] {
    let len = read_len_at(buf_start, offset, cap);
    slice_buffer(buf_start, LEN_SIZE + offset, len, cap)
}

#[derive(Debug)]
pub struct BBPP<'a, const N: usize> {
    buf: RawBuf<N>,
    // protectors: ProtectorPool<ArcMutCond, 64>,
    protectors: ProtectorPool<(), 64>,
    is_writer_leased: AtomicBool,
    ranges: Ranges<N>,
    free_threshold: usize,
    _mark: PhantomData<&'a ()>,
}

impl<'a, const N: usize> BBPP<'a, N> {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        // eprintln!("DEBUG: BBPP::new");
        // eprintln!("DEBUG: buf");
        let buf = RawBuf::new();
        // eprintln!("DEBUG: protectors");
        let protectors = ProtectorPool::new(());
        // eprintln!("DEBUG: ranges");
        // let ranges = Ranges::new();
        // eprintln!("DEBUG: struct");
        Self {
            buf,
            // protectors: ProtectorPool::new(()),
            protectors,
            is_writer_leased: AtomicBool::new(false),
            // ranges,
            ranges: Ranges::new(),
            free_threshold: ((N as f32) * FREE_RATIO).round() as usize,
            _mark: PhantomData,
        }
    }

    pub fn with_capacity(cap: usize) -> Self {
        assert!(cap <= N);
        Self {
            buf: RawBuf::with_capacity(cap),
            protectors: ProtectorPool::new(()),
            is_writer_leased: AtomicBool::new(false),
            ranges: Ranges::new(),
            free_threshold: ((N as f32) * FREE_RATIO).round() as usize,
            _mark: PhantomData,
        }
    }

    pub fn with_full_capacity() -> Self {
        Self::with_capacity(N)
    }

    pub fn protected_ranges(
        &self,
    ) -> (Option<ops::Range<usize>>, Option<ops::Range<usize>>) {
        let ranges = self.ranges.ranges();
        (
            ranges.0.and_then(|r| self.protectors.protected_range(r)),
            self.protectors.protected_range(ranges.1),
        )
    }

    /// Try to get a new reader based on the available
    /// pool of protectors. It returns None if no reader available.
    /// use `new_reader_blocking` to block until a reader is available.
    pub fn new_reader(&'a self) -> Option<Reader<'a, (), N>> {
        let protector = self.protectors.try_get()?;
        let cached_ranges = self.ranges.ranges();
        let bbpp =
            unsafe { NonNull::new_unchecked(self as *const _ as *mut _) };
        Some(Reader { protector, bbpp, cached_ranges })
    }

    /// Try to get a new reader based on the available
    /// pool of protectors. It will block until one is available.
    // pub fn new_reader_blocking(&'a self) -> Reader<'a, ArcMutCond, N> {
    //     let protector = self.protectors.blocking_get();
    //     let cached_ranges = self.ranges.ranges();
    //     let bbpp =
    //         unsafe { NonNull::new_unchecked(self as *const _ as *mut _) };
    //     Reader { protector, bbpp, cached_ranges }
    // }

    /// Try to get a writer if one has not already been provisioned.
    /// Only 1 writer can exist at a time.
    /// `try_release_writer` _must_ be called to make future writers
    /// available.
    pub fn try_writer(&self) -> Option<Writer<'a, N>> {
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

    /// Give back ownership of the writer. This _must_ be called on so future
    /// writers can be created.
    pub fn release_writer(&self, writer: Writer<'a, N>) -> Result<()> {
        let is_own_writer = writer.bbpp.as_ptr() as *const Self == self;
        if !is_own_writer {
            return Err(Error::NotOwnWriter);
        }
        self.is_writer_leased.store(false, Ordering::Release);
        Ok(())
    }

    pub fn is_below_ratio(&self) -> bool {
        (N - self.ranges.size()) < self.free_threshold
    }

    pub const fn cap(&self) -> usize {
        self.buf.cap()
    }
}

unsafe impl<const N: usize> Sync for BBPP<'_, N> {}
unsafe impl<const N: usize> Send for BBPP<'_, N> {}

const _: () = {
    const fn is_send<T: Send>() {}
    const fn is_sync<T: Sync>() {}

    is_send::<BBPP<1>>();
    is_sync::<BBPP<1>>();
};

#[derive(Debug)]
pub struct Writer<'a, const N: usize> {
    bbpp: NonNull<BBPP<'a, N>>,
    _mark: PhantomData<&'a ()>,
}

impl<'a, const N: usize> Writer<'a, N> {
    pub const fn bbpp(&self) -> &BBPP<'a, N> {
        // SAFETY: Since `self.bbpp` is a NonNull pointer, it's guaranteed to be valid.
        unsafe { self.bbpp.as_ref() }
    }

    pub fn bbpp_mut(&mut self) -> &mut BBPP<'a, N> {
        // SAFETY: Since `self.bbpp` is a NonNull pointer, it's guaranteed to be valid.
        unsafe { self.bbpp.as_mut() }
    }

    fn try_push(&mut self, val: &[u8]) -> Result<()> {
        let buf = self.try_reserve(LEN_SIZE + val.len())?;
        let len_bytes = usize::to_ne_bytes(val.len());
        let (len_buf, val_buf) = buf.split_at_mut(LEN_SIZE);
        len_buf.copy_from_slice(&len_bytes);
        val_buf.copy_from_slice(val);
        self.bbpp_mut().ranges.grow(LEN_SIZE + val.len())?;
        Ok(())
    }

    fn try_reserve(&mut self, len: usize) -> Result<&mut [u8]> {
        if len > N {
            return Err(Error::EntryLargerThanBuffer);
        }
        let cap = self.bbpp().cap();
        let start = match self.bbpp().ranges.refs() {
            RangeRefs::One(reg) => {
                let range = reg.range();
                match range.end + len {
                    end if end <= N => Ok(range.end),
                    _ => Err(Error::ReserveFailed {
                        size: len,
                        index: range.end,
                    }),
                }
            }
            RangeRefs::Two { read, write } => {
                let write_range = write.range();
                let read_range = read.range();
                match write_range.end + len {
                    end if end <= read_range.start => Ok(write_range.end),
                    _ => Err(Error::ReserveFailed {
                        size: len,
                        index: write_range.end,
                    }),
                }
            }
        }?;
        if start + len > cap {
            let end = start + len;
            if end > N {
                return Err(Error::ReserveFailed { size: len, index: start });
            }
            // dbg!(start, len, cap);
            self.bbpp_mut().buf.grow(end - cap)?;
        }
        Ok(self.bbpp_mut().buf.index_mut(start..start + len))
    }

    fn push_one(&mut self, val: &[u8]) -> Result<Vec<usize>> {
        // Return on anythything _except_ region full.
        match self.try_push(val) {
            Ok(_) => return Ok(Vec::new()),
            Err(Error::ReserveFailed { size: _, index: _ }) => {}
            Err(err) => return Err(err),
        }
        self.bbpp_mut().ranges.split();
        let mut out_idxs = Vec::new();
        // pop enough stuff off to fit val
        while let Some(idx) = self.try_pop() {
            out_idxs.push(idx);
            match self.try_push(val) {
                Ok(_) => return Ok(out_idxs),
                Err(Error::ReserveFailed { size: _, index: _ }) => {}
                Err(err) => return Err(err),
            }
        }
        // Err(Error::Inconceivable)
        Ok(out_idxs)
    }

    #[tracing::instrument]
    pub fn push(&mut self, val: &[u8]) -> Result<Vec<usize>> {
        let mut popped = self.push_one(val)?;
        while self.bbpp().is_below_ratio() {
            if let Some(idx) = self.try_pop() {
                popped.push(idx);
            } else {
                return Ok(popped);
            }
        }
        Ok(popped)
    }

    /// Returns popped index.
    pub fn try_pop(&mut self) -> Option<usize> {
        let bip = self.bbpp();
        let reg = bip.ranges.read();
        let range = reg.range();
        if range.is_empty() {
            return None;
        }
        let len = bip.buf.len_at(range.start);
        self.bbpp_mut().ranges.shrink(LEN_SIZE + len).ok()?;
        Some(range.start)
    }

    pub fn pop_blocking(&mut self) -> Option<usize> {
        // let protected_ranges = bip.protected_ranges();
        let bip = self.bbpp();
        let reg = bip.ranges.read();
        let range = reg.range();
        if range.is_empty() {
            return None;
        }
        let len = self.bbpp().buf.len_at(range.start);
        let end_index = range.start + LEN_SIZE + len;
        self.bbpp_mut().ranges.shrink(LEN_SIZE + len).ok()?;
        while let Some(r) =
            self.bbpp().protectors.protected_range(range.clone())
        {
            if r.contains(&end_index) {
                core::hint::spin_loop();
                continue;
            }
            break;
        }
        Some(range.start)
    }
}

pub struct Reader<'a, R, const N: usize>
where
    R: Release,
{
    // protector: BorrowedProtector<'a, ArcMutCond>,
    protector: BorrowedProtector<'a, R>,
    bbpp: NonNull<BBPP<'a, N>>,
    cached_ranges: (Option<std::ops::Range<usize>>, std::ops::Range<usize>),
}

impl<'a, R, const N: usize> Reader<'a, R, N>
where
    R: Release,
{
    pub const fn bbpp(&self) -> &BBPP<N> {
        // SAFETY: Since `self.bip_buffer` is a NonNull pointer, it's guaranteed to be valid.
        unsafe { self.bbpp.as_ref() }
    }

    const fn cap(&self) -> usize {
        self.bbpp().buf.cap()
    }

    pub fn check_index(&self, index: usize) -> Result<usize> {
        let idx_len_end_idx = index + LEN_SIZE;
        let cap = self.cap();
        if idx_len_end_idx > cap {
            return Err(Error::LengthBeyondCap { cap, idx: index });
        }
        let range_1 = &self.cached_ranges.1;
        if range_1.contains(&index) && range_1.contains(&idx_len_end_idx) {
            return Ok(index);
        }
        let Some(range) = &self.cached_ranges.0 else {
            return Err(Error::Inconceivable);
        };
        if range.contains(&index) && range.contains(&idx_len_end_idx) {
            Ok(index)
        } else {
            Err(Error::IndexOutOfRange { idx: index })
        }
    }

    pub const fn range_refs(&'a self) -> RangeRefs<'a> {
        self.bbpp().ranges.refs()
    }

    pub fn read_at(&'a self, offset: usize) -> Option<&'a [u8]> {
        self.check_index(offset).ok()?;
        let bbpp = self.bbpp();
        Some(bbpp.buf.read(offset))
    }

    pub const fn iter(&'a self) -> BBPPIterator<'a, R, N> {
        // let head = self.bbpp().ranges.read().head.get();
        BBPPIterator { reader: self, idx: Some(self.cached_ranges.1.start) }
    }
}

pub struct BBPPIterator<'a, R, const N: usize>
where
    R: Release,
{
    reader: &'a Reader<'a, R, N>,
    idx: Option<usize>,
}

impl<'a, R, const N: usize> BBPPIterator<'a, R, N>
where
    R: Release,
{
    const UPDATE_INTERVAL: usize = 8;
    /// Convenience function that sets internal index to None
    /// and always returns None.
    fn end(&mut self) -> Option<<Self as Iterator>::Item> {
        self.idx = None;
        None
    }
}

impl<'a, R, const N: usize> Iterator for BBPPIterator<'a, R, N>
where
    R: Release,
{
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        // std::thread::sleep(core::time::Duration::from_millis(1));
        let idx = self.idx?;
        let item;
        let (write_range, read_range) = &self.reader.cached_ranges;
        self.idx = match (write_range, read_range) {
            (None, range) => {
                let Some(data) = self.reader.read_at(idx) else {
                    return self.end();
                };
                let next = idx + LEN_SIZE + data.len();
                item = Some(data);
                match next {
                    n if range.contains(&n) => {
                        self.reader.protector.protect(n);
                        Some(n)
                    }
                    _ => None,
                }
            }
            (Some(write_range), read_range) => {
                let Some(data) = self.reader.read_at(idx) else {
                    self.idx = None;
                    return None;
                };
                let next = idx + LEN_SIZE + data.len();
                item = Some(data);
                match idx {
                    i if read_range.contains(&i) => match next {
                        n if n >= read_range.end => {
                            self.reader.protector.protect(n);
                            Some(write_range.start)
                        }
                        n => Some(n),
                    },
                    i if write_range.contains(&i) => match next {
                        n if n >= write_range.end => None,
                        n => Some(n),
                    },
                    _ => None,
                }
            }
        };
        item
    }
}

#[cfg(all(test, not(loom), not(lincheck)))]
mod test_bbpp {
    use super::*;
    use assert2::assert;

    #[test]
    fn test_new() {
        let bbpp: BBPP<1024> = BBPP::new();
        assert!(!bbpp.is_writer_leased.load(Ordering::Acquire));
    }

    #[test]
    fn test_protected_ranges() {
        let bbpp: BBPP<1024> = BBPP::new();
        let (range1, range2) = bbpp.protected_ranges();
        assert!(range1.is_none());
        assert!(range2.is_none());
    }

    #[test]
    fn test_new_reader() {
        let mut bbpp: BBPP<1024> = BBPP::new();
        bbpp.ranges = Ranges::new();
        bbpp.ranges.grow(20).unwrap();
        bbpp.ranges.shrink(10).unwrap();
        bbpp.ranges.split();
        bbpp.ranges.grow(10).unwrap();
        let reader = bbpp.new_reader().unwrap();
        assert!(reader.cached_ranges == (Some(0..10), 10..20));
    }

    #[test]
    fn test_multiple_readers() {
        let bbpp: BBPP<1024> = BBPP::new();
        let reader_1 = bbpp.new_reader();
        let reader_2 = bbpp.new_reader();
        assert!(reader_1.is_some());
        assert!(reader_2.is_some());
    }

    // #[test]
    // fn test_new_reader_blocking() {
    //     let bbpp: BBPP<1024> = BBPP::new();
    //     let reader = bbpp.new_reader_blocking();
    //     assert!(reader.is_some());
    // }

    #[test]
    fn test_try_writer() {
        let bbpp: BBPP<1024> = BBPP::new();
        let writer = bbpp.try_writer();
        assert!(writer.is_some());
        assert!(bbpp.is_writer_leased.load(Ordering::Acquire));
    }

    #[test]
    fn test_release_writer() {
        let bbpp: BBPP<1024> = BBPP::new();
        let writer = bbpp.try_writer().unwrap();
        let result = bbpp.release_writer(writer);
        assert!(result.is_ok());
        assert!(!bbpp.is_writer_leased.load(Ordering::Acquire));
    }

    mod is_below_ratio {
        use super::*;
        use assert2::assert;
        use rstest::*;

        #[rstest]
        fn it_should_be_false_on_new() {
            let bbpp = BBPP::<1_000>::new();
            assert!(bbpp.ranges.size() == 0);
            assert!(bbpp.is_below_ratio() == false);
        }
    }
}

#[cfg(all(test, not(loom), not(lincheck)))]
mod test_writer {
    use super::*;
    use assert2::assert;

    #[test]
    fn test_push_and_pop() {
        let bbpp = BBPP::<100>::new();
        let writer = bbpp.try_writer();
        assert!(writer.is_some());
        let mut writer = writer.unwrap();

        let data = vec![1, 2, 3, 4, 5];
        assert!(writer.push(&data).is_ok());

        let popped_index = writer.pop_blocking();
        assert!(popped_index.is_some());
        assert!(popped_index.unwrap() == 0);
    }

    #[test]
    fn test_push_large_data() {
        const N: usize = 100;
        let bbpp = BBPP::<N>::new();
        let mut writer = bbpp.try_writer().unwrap();

        let data = vec![1; N + 1];
        assert!(writer.push(&data) == Err(Error::EntryLargerThanBuffer));
    }

    #[test]
    fn test_push_and_pop_multiple() {
        let bbpp = BBPP::<100>::new();
        let mut writer = bbpp.try_writer().unwrap();

        let data1 = vec![1, 2, 3, 4, 5];
        let data2 = vec![6, 7, 8, 9, 10];
        assert!(writer.push(&data1).is_ok());
        assert!(writer.push(&data2).is_ok());

        let popped_index1 = writer.pop_blocking();
        assert!(popped_index1.is_some());
        assert!(popped_index1.unwrap() == 0);

        let popped_index2 = writer.pop_blocking();
        assert!(popped_index2.is_some());
        assert!(popped_index2.unwrap() == (8 + 5));
    }

    #[test]
    fn test_push_until_full() {
        const N: usize = (8 + 5) * 5;
        let bbpp = BBPP::<N>::new();
        let mut writer = bbpp.try_writer().unwrap();

        let data = vec![1, 2, 3, 4, 5];
        assert!(writer.try_push(&data) == Ok(()));
        assert!(writer.try_push(&data) == Ok(()));
        assert!(writer.try_push(&data) == Ok(()));
        assert!(writer.try_push(&data) == Ok(()));
        assert!(writer.try_push(&data) == Ok(()));
        let last_push = writer.try_push(&data);

        assert!(last_push == Err(Error::ReserveFailed { size: 13, index: 65 }));
    }
}

#[cfg(all(test, not(loom), not(lincheck)))]
mod test_reader {
    use super::*;
    use assert2::assert;

    fn preloaded_bbpp<'a>() -> BBPP<'a, 100> {
        let mut bbpp = BBPP::new();
        bbpp.buf.grow(100);
        bbpp.buf[0..20].copy_from_slice(&[
            2, 0, 0, 0, 0, 0, 0, 0, 3, 4, // 2 bytes = [3, 4]
            2, 0, 0, 0, 0, 0, 0, 0, 1, 2, // 2 bytes = [1, 2]
        ]);
        bbpp.ranges = Ranges::new();
        bbpp.ranges.grow(20).unwrap();
        bbpp.ranges.shrink(10).unwrap();
        bbpp.ranges.split();
        bbpp.ranges.grow(10).unwrap();
        bbpp
    }

    #[cfg(test)]
    mod check_index {
        use super::*;
        use assert2::assert;
        use rstest::*;

        #[rstest]
        fn it_is_valid_if_within_ranges() {
            let mut bbpp = BBPP::<100>::new();
            bbpp.buf.grow(100);
            let mut reader = bbpp.new_reader().unwrap();
            reader.cached_ranges = (Some(0..20), 30..60);
            assert!(reader.check_index(10) == Ok(10));
        }

        #[rstest]
        fn the_index_and_len_bytes_must_be_in_range() {
            let mut bbpp = BBPP::<100>::new();
            bbpp.buf.grow(100);
            let mut reader = bbpp.new_reader().unwrap();
            reader.cached_ranges = (Some(0..20), 30..60);
            assert!(reader.check_index(11) == Ok(11));
            assert!(
                reader.check_index(12)
                    == Err(Error::IndexOutOfRange { idx: 12 })
            );
            assert!(
                reader.check_index(29)
                    == Err(Error::IndexOutOfRange { idx: 29 })
            );
        }

        #[rstest]
        fn it_is_invalid_if_len_larger_than_cap() {
            let mut bbpp = BBPP::<100>::new();
            bbpp.buf.grow(50);
            let mut reader = bbpp.new_reader().unwrap();
            reader.cached_ranges = (Some(0..10), 10..51);
            assert!(reader.check_index(42) == Ok(42));
            assert!(
                reader.check_index(50)
                    == Err(Error::LengthBeyondCap { cap: 50, idx: 50 })
            );
        }
    }

    #[test]
    fn test_read_at() {
        let bbpp = preloaded_bbpp();
        let reader = bbpp.new_reader().unwrap();
        assert!(reader.read_at(10) == Some(&[1, 2][..]));
        assert!(reader.read_at(0) == Some(&[3, 4][..]));
        assert!(reader.read_at(25) == None);
    }

    #[test]
    fn test_iter() {
        let bbpp = preloaded_bbpp();
        let reader = bbpp.new_reader().unwrap();
        let iterator = reader.iter();
        assert!(iterator.idx == Some(10));
    }
}

#[cfg(all(test, not(loom), not(lincheck)))]
mod test_iterator {
    use super::*;

    fn preloaded_bbpp<'a>() -> BBPP<'a, 100> {
        let mut bbpp = BBPP::new();
        bbpp.buf.grow(20).unwrap();
        bbpp.buf[0..20].copy_from_slice(&[
            2, 0, 0, 0, 0, 0, 0, 0, 3, 4, // 2 bytes = [3, 4]
            2, 0, 0, 0, 0, 0, 0, 0, 1, 2, // 2 bytes = [1, 2]
        ]);
        bbpp.ranges = Ranges::new();
        bbpp.ranges.grow(20).unwrap();
        bbpp.ranges.shrink(10).unwrap();
        bbpp.ranges.split();
        bbpp.ranges.grow(10).unwrap();
        bbpp
    }

    #[test]
    fn test_bbpp_iterator() {
        let bbpp = preloaded_bbpp();
        let reader = bbpp.new_reader().unwrap();
        let mut iterator = reader.iter();

        assert_eq!(iterator.next(), Some(&[1, 2][..]));
        assert_eq!(iterator.next(), Some(&[3, 4][..]));
        assert_eq!(iterator.next(), None);
    }

    #[test]
    fn test_bbpp_iterator_end() {
        let bbpp = preloaded_bbpp();
        let reader = bbpp.new_reader().unwrap();
        let mut iterator = reader.iter();

        assert_eq!(iterator.next(), Some(&[1, 2][..]));
        iterator.end();
        assert_eq!(iterator.next(), None);
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use assert2::assert;
    use rstest::*;

    #[rstest]
    fn check_free_threshold() {
        let bbpp = BBPP::<1_000>::new();
        assert!(bbpp.free_threshold == 100);
    }
}

#[cfg(all(test, loom))]
mod test_loom {
    use super::*;

    // This takes over 4,500 seconds to run.
    #[test]
    fn it_works() {
        loom::model(|| {
            let bbpp: BBPP<1024> = BBPP::new();
            let abbpp = Arc::new(bbpp);
            let ths: Vec<_> = (0..2)
                .map(|_| {
                    let bbpp = Arc::clone(&abbpp);
                    loom::thread::spawn(move || {
                        let r = bbpp.new_reader().unwrap();
                        let mut res: Vec<usize> = vec![];
                        // for _ in 0..2 {
                        let count = r.iter().count();
                        res.push(count);
                        // }
                        res
                    })
                })
                .collect();
            let w_bbpp = Arc::clone(&abbpp);
            let th = loom::thread::spawn(move || {
                let mut w = w_bbpp.try_writer().unwrap();
                w.push(&[1, 2, 3]).unwrap();
                w_bbpp.release_writer(w).unwrap();
            });
            th.join().unwrap();
            ths.into_iter().for_each(|th| {
                let _ = th.join().unwrap();
            });
        });
    }
}

#[cfg(all(test, lincheck))]
mod test_lincheck {
    use super::*;

    use assert2::assert;
    use lincheck::{ConcurrentSpec, Lincheck, SequentialSpec};
    use loom::sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    };
    use proptest::prelude::*;
    use rstest::*;
    use tracing::debug;
    use tracing_subscriber::{fmt::format::FmtSpan, prelude::*};

    fn subscriber() -> impl tracing::subscriber::Subscriber {
        tracing_subscriber::registry()
            .with(tracing_subscriber::EnvFilter::from_default_env())
            .with(
                tracing_subscriber::fmt::layer()
                    .compact()
                    .with_span_events(FmtSpan::CLOSE),
            )
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    enum Op {
        Push(String),
        Read { count: usize },
    }

    impl Arbitrary for Op {
        type Parameters = ();
        type Strategy = BoxedStrategy<Self>;

        fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
            prop_oneof![
                prop::string::string_regex("\\PC*").unwrap().prop_map(Op::Push),
                prop::num::usize::ANY.prop_map(|x| Op::Read { count: x }),
                // Just(Op::WriteX),
                // Just(Op::WriteY),
                // Just(Op::ReadX),
                // Just(Op::ReadY),
            ]
            .boxed()
        }
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    enum RetErr {
        GetWriter,
        GetReader,
        Push,
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    enum Ret {
        Pushed { popped: Vec<usize> },
        DidRead { count: usize },
        Error(RetErr),
    }

    #[derive(Debug, Default)]
    struct SeqVec {
        head: usize,
        tail: usize,
        data: Vec<Vec<u8>>,
    }

    impl SeqVec {
        fn new() -> Self {
            Self { head: 0, tail: 0, data: Default::default() }
        }

        fn size(&self) -> usize {
            self.data.iter().map(|v| v.len() + LEN_SIZE).sum()
        }

        fn is_empty(&self) -> bool {
            self.data.is_empty()
        }

        pub fn push(&mut self, value: Vec<u8>) {
            self.data.push(value)
        }

        pub fn pop(&mut self) -> Option<usize> {
            let popped = self.data.pop()?;
            let head = self.head;
            self.head = popped.len() + LEN_SIZE;
            Some(head)
        }
    }

    #[derive(Default, Debug)]
    struct SeqBbpp {
        used_bytes: usize,
        is_split: bool,
        data: (SeqVec, SeqVec),
        cap: usize,
    }

    impl SeqBbpp {
        #[tracing::instrument]
        fn push(&mut self, bytes: &[u8]) -> Vec<usize> {
            let vec = bytes.to_vec();
            let bytes_len = bytes.len();
            let mut popped = Vec::new();
            if self.is_split {
            } else {
                while self.data.0.tail + bytes_len + LEN_SIZE > self.data.1.head
                {
                    let pop = self.data.1.pop();
                    if pop.is_none() {
                        break;
                    }
                    popped.push(pop.unwrap());
                }
                self.data.0.push(vec);
            }
            popped
        }

        #[tracing::instrument]
        pub fn iter(&self) -> impl Iterator<Item = &Vec<u8>> {
            self.data.1.data.iter().chain(self.data.0.data.iter())
        }
    }

    impl SequentialSpec for SeqBbpp {
        type Op = Op;
        type Ret = Ret;

        fn exec(&mut self, op: Op) -> Self::Ret {
            match op {
                Op::Push(s) => {
                    let popped = self.push(s.as_bytes());
                    Ret::Pushed { popped }
                }
                Op::Read { count } => {
                    Ret::DidRead { count: self.iter().take(count).count() }
                }
            }
        }
    }

    #[derive(Debug)]
    struct ParBbpp<'a>(BBPP<'a, 4096>);

    impl Default for ParBbpp<'_> {
        fn default() -> Self {
            eprintln!("\nDEBUG: construct default for ParBbpp");
            debug!("hi");
            let slf = Self(BBPP::new());
            eprintln!("DEBUG: made it");
            slf
        }
    }

    impl ConcurrentSpec for ParBbpp<'_> {
        type Seq = SeqBbpp;

        fn exec(&self, op: Op) -> <Self::Seq as SequentialSpec>::Ret {
            dbg!("parallel exec");
            match op {
                Op::Push(s) => {
                    dbg!("parallel push op");
                    let mut w = self.0.try_writer().unwrap();
                    let popped = match w.push(s.as_bytes()) {
                        Ok(x) => x,
                        Err(_) => return Ret::Error(RetErr::Push),
                    };
                    self.0.release_writer(w).unwrap();
                    Ret::Pushed { popped }
                }
                Op::Read { count } => {
                    dbg!("parallel read op");
                    let reader = self.0.new_reader();
                    let iter = reader.iter().take(count);
                    Ret::DidRead { count: iter.count() }
                }
            }
        }
    }

    #[derive(Debug, Default)]
    struct NothingS;
    impl SequentialSpec for NothingS {
        type Op = Op;
        type Ret = Ret;

        fn exec(&mut self, op: Op) -> Self::Ret {
            match op {
                Op::Push(s) => {
                    debug!("seq push");
                    Ret::Pushed { popped: vec![] }
                }
                Op::Read { count } => {
                    debug!("seq push");
                    Ret::DidRead { count: std::hint::black_box(count + 1) }
                }
            }
        }
    }
    #[derive(Debug)]
    struct NothingP;

    impl Default for NothingP {
        fn default() -> Self {
            // let _ = tracing::subscriber::set_global_default(subscriber());
            debug!("default constructed");
            Self
        }
    }
    impl ConcurrentSpec for NothingP {
        type Seq = NothingS;

        fn exec(&self, op: Op) -> <Self::Seq as SequentialSpec>::Ret {
            // let _ = tracing::subscriber::set_global_default(subscriber());
            match op {
                Op::Push(s) => Ret::Pushed { popped: vec![] },
                Op::Read { count } => Ret::DidRead { count: count + 1 },
            }
        }
    }

    #[rstest]
    fn check_bbpp() {
        // tracing::subscriber::set_global_default(subscriber()).unwrap();
        // Lincheck { num_threads: 2, num_ops: 5 }.verify_or_panic::<NothingP>();
        Lincheck { num_threads: 2, num_ops: 5 }.verify_or_panic::<ParBbpp>();
    }
}
