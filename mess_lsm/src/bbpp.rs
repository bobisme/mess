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
    marker::PhantomData,
    mem::MaybeUninit,
    ptr::NonNull,
    slice::{from_raw_parts, from_raw_parts_mut},
};

use parking_lot::{Condvar, Mutex};

use crate::{
    error::{Error, Result},
    protector::{BorrowedProtector, ProtectorPool},
    ranges::{RangeRefs, Ranges},
};

const LEN_SIZE: usize = core::mem::size_of::<usize>();

type InnerBuf<const N: usize> = UnsafeCell<MaybeUninit<[u8; N]>>;

fn buf_start<const N: usize>(buf: &InnerBuf<N>) -> *mut u8 {
    buf.get().cast::<u8>()
}

fn slice_buffer_mut<const N: usize>(
    buf: &mut InnerBuf<N>,
    offset: usize,
    len: usize,
) -> &mut [u8] {
    unsafe { from_raw_parts_mut(buf_start(buf).add(offset), len) }
}

fn slice_buffer<const N: usize>(
    buf: &InnerBuf<N>,
    offset: usize,
    len: usize,
) -> &[u8] {
    unsafe { from_raw_parts(buf_start(buf).add(offset), len) }
}

pub fn read_len_at<const N: usize>(buf: &InnerBuf<N>, offset: usize) -> usize {
    let len_bytes = slice_buffer(buf, offset, LEN_SIZE);
    usize::from_ne_bytes(
        len_bytes.try_into().expect("did not slice enough to read len"),
    )
}

pub fn read_at<const N: usize>(buf: &InnerBuf<N>, offset: usize) -> &[u8] {
    let len = read_len_at(buf, offset);
    slice_buffer(buf, LEN_SIZE + offset, len)
}

pub struct BBPP<'a, const N: usize> {
    buf: UnsafeCell<MaybeUninit<[u8; N]>>,
    protectors: ProtectorPool<Arc<(Mutex<bool>, Condvar)>, 64>,
    is_writer_leased: AtomicBool,
    ranges: Ranges<N>,
    free_ratio: f32,
    initialized: bool,
    _mark: PhantomData<&'a ()>,
}

impl<'a, const N: usize> BBPP<'a, N> {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        let released = Arc::new((Mutex::new(false), Condvar::new()));
        Self {
            buf: UnsafeCell::new(MaybeUninit::uninit()),
            protectors: ProtectorPool::new(released),
            is_writer_leased: AtomicBool::new(false),
            ranges: Ranges::new(),
            free_ratio: 0.1,
            initialized: false,
            _mark: PhantomData,
        }
    }

    fn init(&mut self) {
        if self.initialized {
            return;
        }
        // Write a zero byte to avoid undefined behavior when
        // handing out NonNull references.
        let buf_start = self.buf.get().cast::<u8>();
        unsafe {
            buf_start.write_bytes(0u8, 1);
        };
        self.initialized = true;
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
    pub fn new_reader(&'a self) -> Option<Reader<'a, N>> {
        if !self.initialized {
            return None;
        }
        // self.init();
        let protector = self.protectors.try_get()?;
        let cached_ranges = self.ranges.ranges();
        let bbpp =
            unsafe { NonNull::new_unchecked(self as *const _ as *mut _) };
        Some(Reader { protector, bbpp, cached_ranges })
    }

    /// Try to get a new reader based on the available
    /// pool of protectors. It will block until one is available.
    pub fn new_reader_blocking(&'a self) -> Reader<'a, N> {
        let protector = self.protectors.blocking_get();
        let cached_ranges = self.ranges.ranges();
        let bbpp =
            unsafe { NonNull::new_unchecked(self as *const _ as *mut _) };
        Reader { protector, bbpp, cached_ranges }
    }

    /// Try to get a writer if one has not already been provisioned.
    /// Only 1 writer can exist at a time.
    /// `try_release_writer` _must_ be called to make future writers
    /// available.
    pub fn try_writer(&mut self) -> Option<Writer<'a, N>> {
        self.init();
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

    fn is_below_ratio(&self) -> bool {
        let size = self.ranges.size();
        1.0 - ((size as f32) / (N as f32)) < self.free_ratio
    }
}

unsafe impl<const N: usize> Sync for BBPP<'_, N> {}

const _: () = {
    const fn is_send<T: Send>() {}
    const fn is_sync<T: Sync>() {}

    is_send::<BBPP<1>>();
    is_sync::<BBPP<1>>();
};

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
        if len >= N {
            return Err(Error::EntryLargerThanBuffer);
        }
        match self.bbpp().ranges.refs() {
            RangeRefs::One(reg) => {
                let range = reg.range();
                match range.end + len {
                    end if end <= N => Ok(slice_buffer_mut(
                        &mut self.bbpp_mut().buf,
                        range.end,
                        len,
                    )),
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
                    end if end <= read_range.start => Ok(slice_buffer_mut(
                        &mut self.bbpp_mut().buf,
                        write_range.end,
                        len,
                    )),
                    _ => Err(Error::ReserveFailed {
                        size: len,
                        index: write_range.end,
                    }),
                }
            }
        }
    }

    fn push_one(&mut self, val: &[u8]) -> Result<Vec<usize>> {
        // Return on anythything _except_ region full.
        match self.try_push(val) {
            Ok(_) => return Ok(Vec::new()),
            Err(Error::RangeFull) => {}
            Err(err) => return Err(err),
        }
        self.bbpp_mut().ranges.split();
        let mut out_idxs = Vec::new();
        // pop enough stuff off to fit val
        while let Some(idx) = self.try_pop() {
            out_idxs.push(idx);
            match self.try_push(val) {
                Ok(_) => return Ok(out_idxs),
                Err(Error::RangeFull) => {}
                Err(err) => return Err(err),
            }
        }
        Err(Error::Inconceivable)
    }

    pub fn push(&mut self, val: &[u8]) -> Result<Vec<usize>> {
        let mut popped = Vec::new();
        popped.extend(&self.push_one(val)?);
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
        let len = read_len_at(&bip.buf, range.start);
        // let data = self.slice_buffer(range.start + LEN_SIZE, len);
        self.bbpp_mut().ranges.shrink(LEN_SIZE + len).ok()?;
        Some(range.start)
    }

    pub fn pop_blocking(&mut self) -> Option<usize> {
        // let protected_ranges = bip.protected_ranges();
        let reg = self.bbpp().ranges.read();
        let range = reg.range();
        if range.is_empty() {
            return None;
        }
        let len = read_len_at(&self.bbpp().buf, range.start);
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

pub struct Reader<'a, const N: usize> {
    protector: BorrowedProtector<'a, Arc<(Mutex<bool>, Condvar)>>,
    bbpp: NonNull<BBPP<'a, N>>,
    cached_ranges: (Option<std::ops::Range<usize>>, std::ops::Range<usize>),
}

impl<'a, const N: usize> Reader<'a, N> {
    pub const fn bbpp(&self) -> &BBPP<N> {
        // SAFETY: Since `self.bip_buffer` is a NonNull pointer, it's guaranteed to be valid.
        unsafe { self.bbpp.as_ref() }
    }

    pub fn is_index_valid(&self, index: usize) -> bool {
        if self.cached_ranges.1.contains(&index) {
            return true;
        }
        let Some(range) = &self.cached_ranges.0 else {
            return false;
        };
        range.contains(&index)
    }

    pub const fn range_refs(&'a self) -> RangeRefs<'a> {
        self.bbpp().ranges.refs()
    }

    pub fn read_at(&'a self, offset: usize) -> Option<&'a [u8]> {
        if !self.is_index_valid(offset) {
            return None;
        }
        Some(read_at(&self.bbpp().buf, offset))
    }

    pub fn iter(&'a self) -> BBPPIterator<'a, N> {
        let head = self.bbpp().ranges.read().head.get();
        BBPPIterator { reader: self, idx: Some(head) }
    }
}

pub struct BBPPIterator<'a, const N: usize> {
    reader: &'a Reader<'a, N>,
    idx: Option<usize>,
}

impl<'a, const N: usize> BBPPIterator<'a, N> {
    /// Convenience function that sets internal index to None
    /// and always returns None.
    fn end(&mut self) -> Option<<Self as Iterator>::Item> {
        self.idx = None;
        None
    }
}

impl<'a, const N: usize> Iterator for BBPPIterator<'a, N> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
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

#[cfg(test)]
mod test_slice_tools {
    use super::*;
    use assert2::assert;

    #[test]
    fn test_read_len_at() {
        let buf = UnsafeCell::new(MaybeUninit::<[u8; 8]>::uninit());
        let buf_ptr = buf.get().cast::<usize>();
        unsafe {
            *buf_ptr.add(0) = 1;
        }
        assert!(read_len_at(&buf, 0) == 1);
    }

    #[test]
    fn test_read_at() {
        let buf = UnsafeCell::new(MaybeUninit::<[u8; 8]>::uninit());
        let buf_ptr = buf.get().cast::<u64>();
        unsafe {
            *buf_ptr.add(0) = 1;
        }
        let buf_ptr = buf.get().cast::<u8>();
        unsafe {
            *buf_ptr.add(8) = 42;
        }
        assert!(read_at(&buf, 0) == &[42]);
    }

    // #[test]
    // #[should_panic(expected = "did not slice enough to read len")]
    // fn test_read_len_at_panic() {
    //     let buf = UnsafeCell::new(MaybeUninit::<[u8; 2]>::uninit());
    //     read_len_at(&buf, 0);
    // }

    #[test]
    fn test_slice_buffer() {
        let buf = UnsafeCell::new(MaybeUninit::<[u8; 8]>::uninit());
        let buf_ptr = buf.get().cast::<u8>();
        unsafe {
            *buf_ptr.add(0) = 42;
            *buf_ptr.add(1) = 43;
            *buf_ptr.add(2) = 44;
            *buf_ptr.add(3) = 45;
        }
        assert!(slice_buffer(&buf, 1, 2) == &[43, 44]);
    }

    #[test]
    fn test_slice_buffer_mut() {
        let mut buf = UnsafeCell::new(MaybeUninit::<[u8; 8]>::uninit());
        let buf_ptr = buf.get().cast::<u8>();
        unsafe {
            *buf_ptr.add(0) = 42;
            *buf_ptr.add(1) = 43;
            *buf_ptr.add(2) = 44;
            *buf_ptr.add(3) = 45;
        }
        let slice = slice_buffer_mut(&mut buf, 1, 2);
        slice[0] = 46;
        slice[1] = 47;
        assert!(slice == &[46, 47]);
    }
}

#[cfg(test)]
mod test_bbpp {
    use super::*;
    use assert2::assert;

    #[test]
    fn test_new() {
        let bbpp: BBPP<1024> = BBPP::new();
        assert!(!bbpp.is_writer_leased.load(Ordering::Acquire));
        assert!(!bbpp.initialized);
    }

    #[test]
    fn test_init() {
        let mut bbpp: BBPP<1024> = BBPP::new();
        bbpp.init();
        assert!(bbpp.initialized);
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
        bbpp.init();
        bbpp.ranges = Ranges::new();
        bbpp.ranges.grow(20).unwrap();
        bbpp.ranges.shrink(10).unwrap();
        bbpp.ranges.split();
        bbpp.ranges.grow(10).unwrap();
        let reader = bbpp.new_reader().unwrap();
        assert!(reader.cached_ranges == (Some(0..10), 10..20));
    }

    // #[test]
    // fn test_new_reader_blocking() {
    //     let bbpp: BBPP<1024> = BBPP::new();
    //     let reader = bbpp.new_reader_blocking();
    //     assert!(reader.is_some());
    // }

    #[test]
    fn test_try_writer() {
        let mut bbpp: BBPP<1024> = BBPP::new();
        let writer = bbpp.try_writer();
        assert!(writer.is_some());
        assert!(bbpp.is_writer_leased.load(Ordering::Acquire));
    }

    #[test]
    fn test_release_writer() {
        let mut bbpp: BBPP<1024> = BBPP::new();
        let writer = bbpp.try_writer().unwrap();
        let result = bbpp.release_writer(writer);
        assert!(result.is_ok());
        assert!(!bbpp.is_writer_leased.load(Ordering::Acquire));
    }

    #[test]
    fn test_is_below_ratio() {
        let mut bbpp: BBPP<30> = BBPP::new();
        bbpp.free_ratio = 0.35;
        assert!(!bbpp.is_below_ratio());
        bbpp.ranges.grow(20).unwrap();
        assert!(bbpp.is_below_ratio());
    }
}

#[cfg(test)]
mod test_writer {
    use super::*;
    use assert2::assert;

    #[test]
    fn test_push_and_pop() {
        let mut bbpp = BBPP::<100>::new();
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
        let mut bbpp = BBPP::<N>::new();
        let mut writer = bbpp.try_writer().unwrap();

        let data = vec![1; N + 1];
        assert!(writer.push(&data) == Err(Error::EntryLargerThanBuffer));
    }

    #[test]
    fn test_push_and_pop_multiple() {
        let mut bbpp = BBPP::<100>::new();
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
        let mut bbpp = BBPP::<N>::new();
        let mut writer = bbpp.try_writer().unwrap();

        let data = vec![1, 2, 3, 4, 5];
        assert!(writer.try_push(&data) == Ok(()));
        assert!(writer.try_push(&data) == Ok(()));
        assert!(writer.try_push(&data) == Ok(()));
        assert!(writer.try_push(&data) == Ok(()));
        assert!(writer.try_push(&data) == Ok(()));

        assert!(
            writer.push(&data)
                == Err(Error::ReserveFailed { size: 13, index: 65 })
        );
    }
}

#[cfg(test)]
mod test_reader {
    use super::*;
    use assert2::assert;

    #[test]
    fn test_is_index_valid() {
        let mut bbpp = BBPP::<100>::new();
        bbpp.init();
        let mut reader = bbpp.new_reader().unwrap();
        reader.cached_ranges = (Some(0..10), 10..20);
        assert!(reader.is_index_valid(5));
        assert!(reader.is_index_valid(15));
        assert!(!reader.is_index_valid(25));
    }

    #[test]
    fn test_read_at() {
        let mut bbpp = BBPP::<100>::new();
        bbpp.init();
        let slice = slice_buffer_mut(&mut bbpp.buf, 0, 20);
        slice.copy_from_slice(&[
            2, 0, 0, 0, 0, 0, 0, 0, 3, 4, 2, 0, 0, 0, 0, 0, 0, 0, 1, 2,
        ]);
        let mut reader = bbpp.new_reader().unwrap();
        reader.cached_ranges = (Some(0..10), 10..20);
        assert!(reader.read_at(10) == Some(&[1, 2][..]));
        assert!(reader.read_at(0) == Some(&[3, 4][..]));
        assert!(reader.read_at(25) == None);
    }

    #[test]
    fn test_iter() {
        let mut bbpp = BBPP::<100>::new();
        bbpp.init();
        let slice = slice_buffer_mut(&mut bbpp.buf, 0, 20);
        slice.copy_from_slice(&[
            2, 0, 0, 0, 0, 0, 0, 0, 3, 4, // 2 bytes = [3, 4]
            2, 0, 0, 0, 0, 0, 0, 0, 1, 2, // 2 bytes = [1, 2]
        ]);
        bbpp.ranges = Ranges::new();
        bbpp.ranges.grow(20).unwrap();
        bbpp.ranges.shrink(10).unwrap();
        bbpp.ranges.split();
        bbpp.ranges.grow(10).unwrap();
        let reader = bbpp.new_reader().unwrap();
        let iterator = reader.iter();
        assert!(iterator.idx == Some(10));
    }
}

#[cfg(test)]
mod test_iter {
    use super::*;

    #[test]
    fn test_bbp_iterator() {
        let mut bbpp = BBPP::<100>::new();
        bbpp.init();
        let slice = slice_buffer_mut(&mut bbpp.buf, 0, 20);
        slice.copy_from_slice(&[
            2, 0, 0, 0, 0, 0, 0, 0, 3, 4, // 2 bytes = [3, 4]
            2, 0, 0, 0, 0, 0, 0, 0, 1, 2, // 2 bytes = [1, 2]
        ]);
        bbpp.ranges.grow(20).unwrap();
        bbpp.ranges.shrink(10).unwrap();
        bbpp.ranges.split();
        bbpp.ranges.grow(10).unwrap();
        let reader = bbpp.new_reader().unwrap();
        let mut iterator = reader.iter();

        assert_eq!(iterator.next(), Some(&[1, 2][..]));
        assert_eq!(iterator.next(), Some(&[3, 4][..]));
        assert_eq!(iterator.next(), None);
    }

    #[test]
    fn test_bbp_iterator_end() {
        let mut bbpp = BBPP::<100>::new();
        bbpp.init();
        let slice = slice_buffer_mut(&mut bbpp.buf, 0, 20);
        bbpp.ranges.grow(20).unwrap();
        bbpp.ranges.shrink(10).unwrap();
        bbpp.ranges.split();
        bbpp.ranges.grow(10).unwrap();
        slice.copy_from_slice(&[
            2, 0, 0, 0, 0, 0, 0, 0, 3, 4, // 2 bytes = [3, 4]
            2, 0, 0, 0, 0, 0, 0, 0, 1, 2, // 2 bytes = [1, 2]
        ]);
        let reader = bbpp.new_reader().unwrap();
        let mut iterator = reader.iter();

        assert_eq!(iterator.next(), Some(&[1, 2][..]));
        iterator.end();
        assert_eq!(iterator.next(), None);
    }
}

#[cfg(test)]
mod tests {}

// #[cfg(all(test, lincheck))]
// mod test_lincheck {
//     use super::*;
//
//     use assert2::assert;
//     use lincheck::{ConcurrentSpec, Lincheck, SequentialSpec};
//     use loom::sync::{
//         atomic::{AtomicBool, Ordering},
//         Arc,
//     };
//     use proptest::prelude::*;
//     use rstest::*;
//
//     #[derive(Debug, Clone, PartialEq, Eq)]
//     enum Op {
//         Push(String),
//         Read { count: usize },
//     }
//
//     impl Arbitrary for Op {
//         type Parameters = ();
//         type Strategy = BoxedStrategy<Self>;
//
//         fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
//             prop_oneof![
//                 prop::string::string_regex("\\PC*").unwrap().prop_map(Op::Push),
//                 prop::num::usize::ANY.prop_map(|x| Op::Read { count: x }),
//                 // Just(Op::WriteX),
//                 // Just(Op::WriteY),
//                 // Just(Op::ReadX),
//                 // Just(Op::ReadY),
//             ]
//             .boxed()
//         }
//     }
//
//     #[derive(Debug, Clone, PartialEq, Eq)]
//     enum RetErr {
//         GetWriter,
//         GetReader,
//         Push,
//     }
//
//     #[derive(Debug, Clone, PartialEq, Eq)]
//     enum Ret {
//         Pushed { popped: Vec<usize> },
//         Read { count: usize },
//         Error(RetErr),
//     }
//
//     #[derive(Debug, Default)]
//     struct SeqVec {
//         head: usize,
//         tail: usize,
//         data: Vec<Vec<u8>>,
//     }
//
//     impl SeqVec {
//         fn new() -> Self {
//             Self { head: 0, tail: 0, data: Default::default() }
//         }
//
//         fn size(&self) -> usize {
//             self.data.iter().map(|v| v.len() + LEN_SIZE).sum()
//         }
//
//         fn is_empty(&self) -> bool {
//             self.data.is_empty()
//         }
//
//         pub fn push(&mut self, value: Vec<u8>) {
//             self.data.push(value)
//         }
//
//         pub fn pop(&mut self) -> Option<usize> {
//             let popped = self.data.pop()?;
//             let head = self.head;
//             self.head = popped.len() + LEN_SIZE;
//             Some(head)
//         }
//     }
//
//     #[derive(Default)]
//     struct SeqBbpp {
//         used_bytes: usize,
//         is_split: bool,
//         data: (SeqVec, SeqVec),
//         cap: usize,
//     }
//
//     impl SeqBbpp {
//         fn push(&mut self, bytes: &[u8]) -> Vec<usize> {
//             let vec = bytes.to_vec();
//             let bytes_len = bytes.len();
//             let mut popped = Vec::new();
//             if self.is_split {
//             } else {
//                 while self.data.0.tail + bytes_len + LEN_SIZE > self.data.1.head
//                 {
//                     let pop = self.data.1.pop();
//                     if pop.is_none() {
//                         break;
//                     }
//                     popped.push(pop.unwrap());
//                 }
//                 self.data.0.push(vec);
//             }
//             popped
//         }
//
//         pub fn iter(&self) -> impl Iterator<Item = &Vec<u8>> {
//             self.data.1.data.iter().chain(self.data.0.data.iter())
//         }
//     }
//
//     impl SequentialSpec for SeqBbpp {
//         type Op = Op;
//         type Ret = Ret;
//
//         fn exec(&mut self, op: Op) -> Self::Ret {
//             match op {
//                 Op::Push(s) => {
//                     let popped = self.push(s.as_bytes());
//                     Ret::Pushed { popped }
//                 }
//                 Op::Read { count } => Ret::Read { count: self.iter().count() },
//             }
//         }
//     }
//
//     struct ParBbpp<'a>(BBPP<'a, 4096>);
//
//     impl Default for ParBbpp<'_> {
//         fn default() -> Self {
//             Self(BBPP::new())
//         }
//     }
//
//     impl ConcurrentSpec for ParBbpp<'_> {
//         type Seq = SeqBbpp;
//
//         fn exec(&self, op: Op) -> <Self::Seq as SequentialSpec>::Ret {
//             match op {
//                 Op::Push(s) => {
//                     let mut w = self.0.try_writer().unwrap();
//                     let popped = match w.push(s.as_bytes()) {
//                         Ok(x) => x,
//                         Err(_) => return Ret::Error(RetErr::Push),
//                     };
//                     self.0.try_release_writer(w).unwrap();
//                     Ret::Pushed { popped }
//                 }
//                 Op::Read { count } => {
//                     let reader = self.0.new_reader();
//                     let iter = reader.iter();
//                     Ret::Read { count: iter.count() }
//                 }
//             }
//         }
//     }
//     #[rstest]
//     fn check_bbpp() {
//         Lincheck { num_threads: 2, num_ops: 5 }
//             .verify_or_panic::<ParBbpp<'_>>();
//     }
// }
