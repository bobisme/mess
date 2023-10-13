#[cfg(loom)]
use loom::{
    hint,
    sync::atomic::{
        AtomicBool, AtomicUsize,
        Ordering::{AcqRel, Acquire, Release},
    },
};
use std::{
    cell::UnsafeCell,
    marker::PhantomData,
    mem::MaybeUninit,
    ops::Range,
    ptr::NonNull,
    slice::{from_raw_parts, from_raw_parts_mut},
};
#[cfg(not(loom))]
use std::{
    hint,
    sync::atomic::{
        AtomicBool, AtomicUsize,
        Ordering::{AcqRel, Acquire, Release},
    },
};

use crate::error::{Error, Result};

const LEN_SIZE: usize = core::mem::size_of::<usize>();

#[derive(Debug)]
pub struct Region<const N: usize> {
    head: AtomicUsize,
    tail: AtomicUsize,
}

impl<const N: usize> Region<N> {
    pub fn new() -> Self {
        Self { head: AtomicUsize::new(0), tail: AtomicUsize::new(0) }
    }

    pub fn head(&self) -> usize {
        self.head.load(Acquire)
    }
    pub fn tail(&self) -> usize {
        self.tail.load(Acquire)
    }
    pub fn set_head(&self, val: usize) {
        self.head.store(val, Release)
    }
    pub fn set_tail(&self, val: usize) {
        self.tail.store(val, Release)
    }
    pub fn range(&self) -> Range<usize> {
        self.head()..self.tail()
    }
    pub fn reset(&self) {
        self.head.store(0, Release);
        self.tail.store(0, Release);
    }
}

impl<const N: usize> Default for Region<N> {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub enum Count {
    One,
    Two,
}

#[repr(transparent)]
pub struct RegionCount(AtomicBool);

impl RegionCount {
    pub fn new() -> Self {
        Self(AtomicBool::new(false))
    }

    pub fn one() -> Self {
        Self(AtomicBool::new(false))
    }

    pub fn two() -> Self {
        Self(AtomicBool::new(true))
    }

    pub fn get(&self) -> Count {
        match self.0.load(Acquire) {
            false => Count::One,
            true => Count::Two,
        }
    }

    pub fn set(&self, count: Count) {
        match count {
            Count::One => self.0.store(false, Release),
            Count::Two => self.0.store(true, Release),
        }
    }
}

impl Default for RegionCount {
    fn default() -> Self {
        Self::new()
    }
}

pub enum RegionRefs<'a, const N: usize> {
    One(&'a Region<N>),
    Two { read: &'a Region<N>, write: &'a Region<N> },
}

pub struct Regions<const N: usize> {
    regions: (Region<N>, Region<N>),
    count: RegionCount,
}

impl<const N: usize> Regions<N> {
    pub fn new() -> Self {
        Self {
            regions: (Region::new(), Region::new()),
            count: RegionCount::one(),
        }
    }

    pub fn is_index_valid(&self, index: usize) -> bool {
        match self.count.get() {
            Count::One => self.regions.1.range().contains(&index),
            Count::Two => {
                self.regions.1.range().contains(&index)
                    && self.regions.0.range().contains(&index)
            }
        }
    }
}
impl<const N: usize> Default for Regions<N> {
    fn default() -> Self {
        Self::new()
    }
}

impl<const N: usize> Regions<N> {
    pub fn refs(&self) -> RegionRefs<N> {
        match self.count.get() {
            Count::One => RegionRefs::One(&self.regions.1),
            Count::Two => RegionRefs::Two {
                read: &self.regions.1,
                write: &self.regions.0,
            },
        }
    }

    pub fn read(&self) -> &Region<N> {
        &self.regions.1
    }

    pub fn read_mut(&mut self) -> &mut Region<N> {
        &mut self.regions.1
    }

    pub fn write(&self) -> &Region<N> {
        match self.count.get() {
            Count::One => &self.regions.1,
            Count::Two => &self.regions.0,
        }
    }

    pub fn write_mut(&mut self) -> &Region<N> {
        match self.count.get() {
            Count::One => &mut self.regions.1,
            Count::Two => &mut self.regions.0,
        }
    }

    pub fn split(&mut self) {
        if self.count.get() != Count::One {
            return;
        }
        self.count.set(Count::Two);
        self.regions.0.reset();
    }

    pub fn merge(&mut self) {
        if self.count.get() != Count::Two {
            return;
        }
        self.count.set(Count::One);
        self.regions.1.set_head(self.regions.0.head());
        self.regions.1.set_tail(self.regions.0.tail());
    }

    /// NOTE: caller must check that the range is valid
    pub fn grow(&mut self, len: usize) {
        let write = self.write_mut();
        let range = write.range();
        assert!(range.end + len <= N);
        self.write_mut().tail.fetch_add(len, AcqRel);
    }

    /// NOTE: caller must check that the range is valid
    pub fn shrink(&mut self, len: usize) {
        let read = self.read_mut();
        let range = read.range();
        assert!(range.start + len <= range.end);
        read.head.fetch_add(len, AcqRel);
        if read.range().is_empty() {
            self.merge();
        }
    }

    pub fn size(&self) -> usize {
        match self.count.get() {
            Count::One => self.regions.1.range().len(),
            Count::Two => {
                self.regions.0.range().len() + self.regions.1.range().len()
            }
        }
    }
}

pub const PROTECT_N: usize = 8;

pub struct Protector(AtomicUsize);

impl Protector {
    pub fn protect(&self) -> bool {
        let mut val = self.0.load(Acquire);
        while val != usize::MAX {
            let cex = self.0.compare_exchange(val, val + 1, AcqRel, Acquire);
            if cex.is_ok() {
                return true;
            }
            val = cex.unwrap_err();
            std::thread::yield_now();
        }
        false
    }
}

pub struct BipBuffer<const N: usize> {
    regions: Regions<N>,
    buf: UnsafeCell<MaybeUninit<[u8; N]>>,
    protectors: AtomicUsize,
    free_ratio: f32,
}

// #[allow(clippy::declare_interior_mutable_const)]
// const ATOMIC_0: AtomicUsize = AtomicUsize::new(0);

impl<'a, const N: usize> BipBuffer<N> {
    pub fn new() -> Self {
        let slf = Self {
            buf: UnsafeCell::new(MaybeUninit::uninit()),
            regions: Regions::new(),
            protectors: AtomicUsize::new(0),
            free_ratio: 0.1,
        };
        // Write a zero byte to avoid undefined behavior when
        // handing out NonNull references.
        unsafe {
            slf.buf_start().write_bytes(0u8, 1);
        };
        slf
    }

    fn is_below_ratio(&self) -> bool {
        let size = self.regions.size();
        1.0 - ((size as f32) / (N as f32)) < self.free_ratio
    }

    fn buf_start(&self) -> *mut u8 {
        self.buf.get().cast::<u8>()
    }

    fn slice_buffer_mut(&mut self, offset: usize, len: usize) -> &mut [u8] {
        unsafe { from_raw_parts_mut(self.buf_start().add(offset), len) }
    }

    fn slice_buffer(&self, offset: usize, len: usize) -> &[u8] {
        unsafe { from_raw_parts(self.buf_start().add(offset), len) }
    }

    pub fn read_len_at(&self, offset: usize) -> usize {
        let len_bytes = self.slice_buffer(offset, LEN_SIZE);
        usize::from_ne_bytes(
            len_bytes.try_into().expect("did not slice enough to read len"),
        )
    }

    pub fn read_at(&self, offset: usize) -> &[u8] {
        let len = self.read_len_at(offset);
        self.slice_buffer(LEN_SIZE + offset, len)
    }

    pub fn try_reader(&'a self) -> Result<BipReader<'a, N>> {
        self.protectors
            .fetch_update(Release, Acquire, |protectors| match protectors {
                usize::MAX => None,
                x => Some(x + 1),
            })
            .map(|_| {
                let bip_buffer = unsafe {
                    NonNull::new_unchecked(self as *const _ as *mut _)
                };
                BipReader { bip_buffer, _mark: PhantomData }
            })
            .map_err(|_| Error::ReaderBlocked)
    }

    pub async fn reader(&'a self) -> BipReader<'a, N> {
        loop {
            match self.try_reader() {
                Err(_) => hint::spin_loop(),
                Ok(reader) => {
                    return reader;
                }
            }
        }
    }

    pub fn try_writer(&'a self) -> Result<BipWriter<'a, N>> {
        self.protectors
            .fetch_update(Release, Acquire, |protectors| match protectors {
                0 => Some(usize::MAX),
                _ => None,
            })
            .map(|_| {
                let bip_buffer = unsafe {
                    NonNull::new_unchecked(self as *const _ as *mut _)
                };
                BipWriter { bip_buffer, _mark: PhantomData }
            })
            .map_err(|_| Error::WriterBlocked)
    }

    pub async fn writer(&'a self) -> BipWriter<'a, N> {
        loop {
            match self.try_writer() {
                Err(_) => hint::spin_loop(),
                Ok(writer) => {
                    return writer;
                }
            }
        }
    }
}

impl<const N: usize> Default for BipBuffer<N> {
    fn default() -> Self {
        Self::new()
    }
}

unsafe impl<const N: usize> Sync for BipBuffer<N> {}

const _: () = {
    const fn is_send<T: Send>() {}
    const fn is_sync<T: Sync>() {}

    is_send::<BipBuffer<1>>();
    is_sync::<BipBuffer<1>>();
};

pub struct Iter<'a, const N: usize> {
    reader: &'a BipReader<'a, N>,
    idx: Option<usize>,
}

impl<'a, const N: usize> Iterator for Iter<'a, N> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        let idx = self.idx?;
        let item;
        self.idx = match self.reader.region_refs() {
            RegionRefs::One(reg) => {
                let range = reg.range();
                if !range.contains(&idx) {
                    self.idx = None;
                    return None;
                }
                let data = self.reader.read_at(idx);
                let next = idx + LEN_SIZE + data.len();
                item = Some(data);
                match next {
                    n if range.contains(&n) => Some(n),
                    _ => None,
                }
            }
            RegionRefs::Two { read, write } => {
                let read_range = read.range();
                let write_range = write.range();
                let is_valid =
                    read_range.contains(&idx) || write_range.contains(&idx);
                if !is_valid {
                    self.idx = None;
                    return None;
                }
                let data = self.reader.read_at(idx);
                let next = idx + LEN_SIZE + data.len();
                item = Some(data);
                match idx {
                    i if read_range.contains(&i) => match next {
                        n if n >= read_range.end => Some(write_range.start),
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

pub struct BipReader<'a, const N: usize> {
    bip_buffer: NonNull<BipBuffer<N>>,
    _mark: PhantomData<&'a ()>,
}

impl<'a, const N: usize> BipReader<'a, N> {
    pub fn bip(&self) -> &BipBuffer<N> {
        // SAFETY: Since `self.bip_buffer` is a NonNull pointer, it's guaranteed to be valid.
        unsafe { self.bip_buffer.as_ref() }
    }

    pub fn region_refs(&self) -> RegionRefs<'_, N> {
        self.bip().regions.refs()
    }

    pub fn read_at(&self, offset: usize) -> &[u8] {
        self.bip().read_at(offset)
    }

    pub fn iter(&self) -> Iter<N> {
        Iter { reader: self, idx: Some(self.bip().regions.read().head()) }
    }

    pub fn iter_from(&self, start_index: usize) -> Iter<N> {
        Iter { reader: self, idx: Some(start_index) }
    }
}

impl<const N: usize> Drop for BipReader<'_, N> {
    fn drop(&mut self) {
        // SAFETY: Since `self.bip_buffer` is a NonNull pointer, it's guaranteed to be valid.
        // However, proper synchronization is required to ensure that the `BipBuffer` instance is
        // not dropped while this `BipReader` is still in use. This synchronization should be
        // ensured by the rest of your program logic.
        unsafe {
            let bip_buffer = self.bip_buffer.as_ref();
            bip_buffer.protectors.fetch_sub(1, AcqRel);
        }
    }
}

pub struct BipWriter<'a, const N: usize> {
    bip_buffer: NonNull<BipBuffer<N>>,
    _mark: PhantomData<&'a ()>,
}

impl<'a, const N: usize> BipWriter<'a, N> {
    pub fn bip(&self) -> &BipBuffer<N> {
        // SAFETY: Since `self.bip_buffer` is a NonNull pointer, it's guaranteed to be valid.
        unsafe { self.bip_buffer.as_ref() }
    }

    pub fn bip_mut(&mut self) -> &mut BipBuffer<N> {
        // SAFETY: Since `self.bip_buffer` is a NonNull pointer, it's guaranteed to be valid.
        unsafe { self.bip_buffer.as_mut() }
    }

    fn try_push(&mut self, val: &[u8]) -> Result<()> {
        let buf = self.try_reserve(LEN_SIZE + val.len())?;
        let len_bytes = usize::to_ne_bytes(val.len());
        let (len_buf, val_buf) = buf.split_at_mut(LEN_SIZE);
        len_buf.copy_from_slice(&len_bytes);
        val_buf.copy_from_slice(val);
        self.bip_mut().regions.grow(LEN_SIZE + val.len());
        Ok(())
    }

    fn try_reserve(&mut self, len: usize) -> Result<&mut [u8]> {
        if len >= N {
            return Err(Error::EntryTooBig);
        }
        match self.bip().regions.refs() {
            RegionRefs::One(reg) => {
                let range = reg.range();
                if range.end + len <= N {
                    Ok(self.bip_mut().slice_buffer_mut(range.end, len))
                } else {
                    Err(Error::RegionFull)
                }
            }
            RegionRefs::Two { read, write } => {
                let write_range = write.range();
                let read_range = read.range();
                if write_range.end + len <= read_range.start {
                    Ok(self.bip_mut().slice_buffer_mut(write_range.end, len))
                } else {
                    Err(Error::RegionFull)
                }
            }
        }
    }

    fn push_one(&mut self, val: &[u8]) -> Result<Vec<usize>> {
        // Return on anythything _except_ region full.
        match self.try_push(val) {
            Ok(_) => return Ok(Vec::new()),
            Err(Error::RegionFull) => {}
            Err(err) => return Err(err),
        }
        self.bip_mut().regions.split();
        let mut out_idxs = Vec::new();
        // pop enough stuff off to fit val
        while let Some(idx) = self.try_pop() {
            out_idxs.push(idx);
            match self.try_push(val) {
                Ok(_) => return Ok(out_idxs),
                Err(Error::RegionFull) => {}
                Err(err) => return Err(err),
            }
        }
        Err(Error::Inconceivable)
    }

    pub fn push(&mut self, val: &[u8]) -> Result<Vec<usize>> {
        let mut popped = Vec::new();
        popped.extend(&self.push_one(val)?);
        while self.bip().is_below_ratio() {
            if let Some(idx) = self.try_pop() {
                popped.push(idx);
            } else {
                return Ok(popped);
            }
        }
        Ok(popped)
    }

    pub fn push_batch<'iter>(
        &mut self,
        vals: impl Iterator<Item = &'iter [u8]>,
    ) -> Result<Vec<usize>> {
        let mut popped = Vec::new();
        for val in vals {
            popped.extend(&self.push_one(val)?);
        }
        while self.bip().is_below_ratio() {
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
        let bip = self.bip();
        let reg = bip.regions.read();
        let range = reg.range();
        if range.is_empty() {
            return None;
        }
        let len = bip.read_len_at(range.start);
        // let data = self.slice_buffer(range.start + LEN_SIZE, len);
        self.bip_mut().regions.shrink(LEN_SIZE + len);
        Some(range.start)
    }
}

impl<const N: usize> Drop for BipWriter<'_, N> {
    fn drop(&mut self) {
        // SAFETY: Since `self.bip_buffer` is a NonNull pointer, it's guaranteed to be valid.
        // However, proper synchronization is required to ensure that the `BipBuffer` instance is
        // not dropped while this `BipWriter` is still in use. This synchronization should be
        // ensured by the rest of your program logic.
        unsafe {
            let bip_buffer = self.bip_buffer.as_ref();
            bip_buffer.protectors.store(0, Release);
        }
    }
}

#[cfg(test)]
mod test_bip_buffer {
    use super::*;
    use assert2::assert;
    use proptest::proptest;
    use rstest::*;

    #[rstest]
    fn read_len_at_works() {
        let mut bip = BipBuffer::<1024>::new();
        let buf = bip.slice_buffer_mut(0, 20);
        buf[..LEN_SIZE].copy_from_slice(&usize::to_ne_bytes(69));
        let buf = bip.slice_buffer_mut(20, 30);
        buf[..LEN_SIZE].copy_from_slice(&usize::to_ne_bytes(43));
        assert!(bip.read_len_at(0) == 69);
        assert!(bip.read_len_at(20) == 43);
    }

    #[rstest]
    fn try_push_does_not_change_read_head() {
        let bip = BipBuffer::<1024>::new();
        let mut buf = bip.try_writer().unwrap();
        assert!(buf.try_push(b"hey now!").is_ok());
        let read_region = bip.regions.read();
        assert!(read_region.head() == 0);
    }

    #[rstest]
    fn try_push_moves_read_tail_forward() {
        let bip = BipBuffer::<1024>::new();
        let mut buf = bip.try_writer().unwrap();
        assert!(buf.try_push(b"hey now!").is_ok());
        let read_region = bip.regions.read();
        assert!(read_region.tail() == LEN_SIZE + 8 /* hey now! */);
    }

    #[rstest]
    fn try_push_errors_if_full() {
        let bip = BipBuffer::<60>::new();
        let mut buf = bip.try_writer().unwrap();
        for _ in 0..3 {
            assert!(buf.try_push(b"hey now!").is_ok());
        }
        assert!(buf.try_push(b"hey now!") == Err(Error::RegionFull));
        let read_region = bip.regions.read();
        assert!(read_region.tail() == 3 * (LEN_SIZE + 8));
    }

    #[rstest]
    fn push_on_full_splits_the_regions() {
        let bip = BipBuffer::<60>::new();
        let mut buf = bip.try_writer().unwrap();
        for _ in 0..3 {
            assert!(buf.try_push(b"hey now!").is_ok());
        }
        assert!(bip.regions.count.get() == Count::One);
        let _ = buf.push(b"hey now!");
        assert!(bip.regions.count.get() == Count::Two);
    }

    #[rstest]
    fn push_on_full_pops_from_read_region() {
        let bip = BipBuffer::<60>::new();
        let mut buf = bip.try_writer().unwrap();
        for _ in 0..3 {
            assert!(buf.try_push(b"hey now!").is_ok());
        }
        let _ = buf.push(b"hey now!");
        assert!(bip.regions.read().head() == LEN_SIZE + 8);
    }

    #[rstest]
    fn push_on_full_pops_more_than_once_if_needed() {
        let bip = BipBuffer::<60>::new();
        let mut buf = bip.try_writer().unwrap();
        for _ in 0..3 {
            assert!(buf.try_push(b"hey now!").is_ok());
        }
        let _ = buf.push(b"hey now now!");
        assert!(bip.regions.read().head() == 2 * (LEN_SIZE + 8));
    }

    #[rstest]
    fn push_on_full_appends_to_write_region() {
        let bip = BipBuffer::<60>::new();
        let mut buf = bip.try_writer().unwrap();
        for _ in 0..3 {
            assert!(buf.try_push(b"hey now!").is_ok());
        }
        assert!(buf.push(b"hey now!") == Ok(vec![0]));
        assert!(bip.regions.write().tail() == (LEN_SIZE + 8));
    }

    #[rstest]
    fn is_below_ratio_works() {
        let mut bip = BipBuffer::<100>::new();
        bip.free_ratio = 0.1;
        let mut buf = bip.try_writer().unwrap();
        for _ in 0..8 {
            assert!(buf.try_push(b"xo").is_ok());
        }
        assert!(bip.regions.size() == 80);
        assert!(bip.is_below_ratio() == false);
        let _ = buf.try_push(b"xox");
        assert!(bip.regions.size() == 91);
        assert!(bip.is_below_ratio() == true);
    }

    #[rstest]
    fn push_will_automatically_free_if_below_ratio() {
        let mut bip = BipBuffer::<100>::new();
        bip.free_ratio = 0.1;
        let mut buf = bip.try_writer().unwrap();
        for _ in 0..8 {
            assert!(buf.try_push(b"xo").is_ok());
        }
        assert!(bip.regions.size() == 80);
        assert!(bip.regions.read().range().eq(0..80));
        assert!(buf.push(b"xox") == Ok(vec![0]));
        assert!(bip.regions.size() == 81);
        assert!(bip.regions.read().range().eq(10..91));
    }

    #[rstest]
    fn it_pushes_until_regions_merge() {
        let bip = BipBuffer::<60>::new();
        let mut buf = bip.try_writer().unwrap();
        for _ in 0..3 {
            assert!(buf.try_push(b"hey now!").is_ok());
        }
        assert!(buf.push(b"hey now!").is_ok());
        assert!(bip.regions.count.get() == Count::Two);
        assert!(buf.push(b"hey now!") == Ok(vec![LEN_SIZE + 8]));
        assert!(buf.push(b"hey now!") == Ok(vec![2 * (LEN_SIZE + 8)]));
        assert!(bip.regions.count.get() == Count::One);
    }

    #[rstest]
    fn try_pop_works() {
        let bip = BipBuffer::<1024>::new();
        let mut buf = bip.try_writer().unwrap();
        assert!(buf.try_push(b"hey now!").is_ok());
        assert!(buf.try_push(b"hey now?").is_ok());
        assert!(buf.try_pop() == Some(0));
        assert!(buf.try_pop() == Some(LEN_SIZE + 8));
        assert!(buf.try_pop() == None);
    }

    #[cfg(all(proptest))]
    proptest! {
        #[rstest]
        fn try_push_proptest(s in "\\PC*") {
            let bip = BipBuffer::<1024>::new();
            let mut w = bip.try_writer().unwrap();
            assert!(w.try_push(s.as_bytes()) == Ok(()));
            drop(w);
            let r = bip.try_reader().unwrap();
            let data = r.iter().next();
            assert!(data == Some(s.as_bytes()));
        }
    }

    #[cfg(all(proptest))]
    proptest! {
        #[rstest]
        fn push_one_proptest(s in "\\PC*") {
            let bip = BipBuffer::<1024>::new();
            let mut w = bip.try_writer().unwrap();
            assert!(w.push_one(s.as_bytes()) == Ok(Vec::new()));
            drop(w);
            let r = bip.try_reader().unwrap();
            let data = r.iter().next();
            assert!(data == Some(s.as_bytes()));
        }
    }

    #[cfg(all(proptest))]
    proptest! {
        #[rstest]
        fn push_proptest(s in "\\PC*") {
            let bip = BipBuffer::<1024>::new();
            let mut w = bip.try_writer().unwrap();
            assert!(w.push(s.as_bytes()) == Ok(Vec::new()));
            drop(w);
            let r = bip.try_reader().unwrap();
            let data = r.iter().next();
            assert!(data == Some(s.as_bytes()));
        }
    }
}

#[cfg(test)]
mod test_iter {
    use super::*;
    use assert2::assert;
    use rstest::*;

    #[rstest]
    fn it_works() {
        let buf = BipBuffer::<60>::new();
        let mut w = buf.try_writer().unwrap();
        assert!(w.try_push(b"ab") == Ok(()));
        assert!(w.try_push(b"cd") == Ok(()));
        assert!(w.try_push(b"ef") == Ok(()));
        drop(w);
        let reader = buf.try_reader().unwrap();
        let mut iter = reader.iter();
        assert!(iter.next() == Some(b"ab".as_slice()));
        assert!(iter.next() == Some(b"cd".as_slice()));
        assert!(iter.next() == Some(b"ef".as_slice()));
        assert!(iter.next() == None);
    }

    #[rstest]
    fn it_wraps() {
        let mut buf = BipBuffer::<30>::new();
        buf.free_ratio = 0.0;
        let mut w = buf.try_writer().unwrap();
        assert!(w.push(b"ab") == Ok(vec![]));
        assert!(w.push(b"cd") == Ok(vec![]));
        assert!(w.push(b"ef") == Ok(vec![]));
        assert!(w.push(b"gh") == Ok(vec![0]));
        drop(w);
        let reader = buf.try_reader().unwrap();
        let mut iter = reader.iter();
        assert!(iter.next() == Some(b"cd".as_slice()));
        assert!(iter.next() == Some(b"ef".as_slice()));
        assert!(iter.next() == Some(b"gh".as_slice()));
        assert!(iter.next() == None);
    }
}

#[cfg(all(test, loom))]
mod test_loom_bip {
    use super::*;
    use assert2::assert;
    use loom::sync::Arc;
    // use loom::thread;
    use rstest::*;

    #[rstest]
    fn multiple_readers_can_be_acquired() {
        loom::model(|| {
            let buf = BipBuffer::<60>::new();
            let buf = Arc::new(buf);

            assert!(buf.protectors.load(Acquire) == 0);
            let reader1 = buf.try_reader();
            assert!(reader1.is_ok());
            assert!(buf.protectors.load(Acquire) == 1);
            let reader2 = buf.try_reader();
            assert!(reader2.is_ok());
            assert!(buf.protectors.load(Acquire) == 2);
            drop(reader2);
            assert!(buf.protectors.load(Acquire) == 1);
            drop(reader1);
            assert!(buf.protectors.load(Acquire) == 0);
        });
    }

    #[rstest]
    fn readers_block_writers() {
        loom::model(|| {
            let buf = BipBuffer::<60>::new();
            let buf = Arc::new(buf);

            let reader = buf.try_reader();
            assert!(reader.is_ok());
            let writer = buf.try_writer();
            assert!(writer.is_err());
            drop(reader);
            let writer = buf.try_writer();
            assert!(writer.is_ok());
        });
    }

    #[rstest]
    fn writers_block_readers() {
        loom::model(|| {
            let buf = BipBuffer::<60>::new();
            let buf = Arc::new(buf);

            let writer = buf.try_writer();
            assert!(writer.is_ok());
            let reader = buf.try_reader();
            assert!(reader.is_err());
            drop(writer);
            let reader = buf.try_reader();
            assert!(reader.is_ok());
        });
    }

    #[rstest]
    fn writers_block_writers() {
        loom::model(|| {
            let buf = BipBuffer::<60>::new();
            let buf = Arc::new(buf);

            let writer = buf.try_writer();
            assert!(writer.is_ok());
            let writer2 = buf.try_writer();
            assert!(writer2.is_err());
            drop(writer);
            let writer2 = buf.try_writer();
            assert!(writer2.is_ok());
        });
    }

    #[rstest]
    fn do_something() {
        let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();
        loom::model(move || {
            rt.block_on(async {
                let buf = BipBuffer::<60>::new();
                let buf = Arc::new(buf);
                let buf_1 = Arc::clone(&buf);
                let wtask = tokio::task::spawn(async move {
                    let mut writer = buf_1.writer().await;
                    writer.push(b"fart1").unwrap();
                    writer.push(b"fart2").unwrap();
                    writer.push(b"fart3").unwrap();
                });
                let rtasks = (0..3).map(|_| {
                    let buf = Arc::clone(&buf);
                    tokio::task::spawn(async move {
                        let reader = buf.reader().await;
                        let items: Vec<_> = reader.iter().collect();
                        assert!(items == vec![b"fart1", b"fart2", b"fart3"]);
                    })
                });
                tokio::try_join!(wtask).unwrap();
                for rtask in rtasks {
                    tokio::try_join!(rtask).unwrap();
                }
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
        Read { count: usize },
        Error(RetErr),
    }

    #[derive(Default)]
    struct SeqBip {
        used_bytes: usize,
        data: Vec<Vec<u8>>,
    }

    impl SequentialSpec for SeqBip {
        type Op = Op;
        type Ret = Ret;

        fn exec(&mut self, op: Op) -> Self::Ret {
            match op {
                Op::Push(s) => {
                    let bytes = s.as_bytes().to_vec();
                    let bytes_len = bytes.len();
                    self.data.push(bytes);
                    self.used_bytes = bytes_len + 8;
                    Ret::Pushed { popped: vec![] }
                }
                Op::Read { count } => {
                    Ret::Read { count: self.data.iter().take(count).count() }
                }
            }
        }
    }

    #[derive(Default)]
    struct ParBip(BipBuffer<4096>);

    impl ConcurrentSpec for ParBip {
        type Seq = SeqBip;

        fn exec(&self, op: Op) -> <Self::Seq as SequentialSpec>::Ret {
            // let rt_handle = tokio::runtime::Handle::current();
            // let _ = rt_handle.enter();
            ::futures::executor::block_on(async {
                match op {
                    Op::Push(s) => {
                        let mut w = self.0.writer().await;
                        // let popped = match w.push(s.as_bytes()) {
                        //     Ok(x) => x,
                        //     Err(_) => return Ret::Error(RetErr::CantPush),
                        // };
                        // Ret::Pushed { popped }
                        Ret::Pushed { popped: vec![] }
                    }
                    Op::Read { count } => {
                        let r = self.0.reader().await;
                        let iter = r.iter();
                        Ret::Read { count: iter.count() }
                    }
                }
            })
        }
    }
    #[rstest]
    fn two_slots() {
        tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(async {
                Lincheck { num_threads: 2, num_ops: 5 }
                    .verify_or_panic::<ParBip>()
            });
    }
}
