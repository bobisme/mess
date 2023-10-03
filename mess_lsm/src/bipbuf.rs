use std::{
    cell::UnsafeCell,
    mem::MaybeUninit,
    ops::Range,
    slice::{from_raw_parts, from_raw_parts_mut},
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
};

use crate::error::{Error, Result};

const LEN_SIZE: usize = core::mem::size_of::<usize>();

#[derive(Debug)]
pub struct Region<const N: usize> {
    head: AtomicUsize,
    tail: AtomicUsize,
}

impl<const N: usize> Region<N> {
    pub const fn new() -> Self {
        Self { head: AtomicUsize::new(0), tail: AtomicUsize::new(0) }
    }

    pub fn head(&self) -> usize {
        self.head.load(Ordering::Acquire)
    }
    pub fn tail(&self) -> usize {
        self.tail.load(Ordering::Acquire)
    }
    pub fn set_head(&self, val: usize) {
        self.head.store(val, Ordering::Release)
    }
    pub fn set_tail(&self, val: usize) {
        self.tail.store(val, Ordering::Release)
    }
    pub fn range(&self) -> Range<usize> {
        self.head()..self.tail()
    }
    pub fn reset(&self) {
        self.head.store(0, Ordering::Release);
        self.tail.store(0, Ordering::Release);
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
    pub const fn new() -> Self {
        Self(AtomicBool::new(false))
    }

    pub const fn one() -> Self {
        Self(AtomicBool::new(false))
    }

    pub const fn two() -> Self {
        Self(AtomicBool::new(true))
    }

    pub fn get(&self) -> Count {
        match self.0.load(Ordering::Acquire) {
            false => Count::One,
            true => Count::Two,
        }
    }

    pub fn set(&self, count: Count) {
        match count {
            Count::One => self.0.store(false, Ordering::Release),
            Count::Two => self.0.store(true, Ordering::Release),
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
    pub const fn new() -> Self {
        Self {
            regions: (Region::new(), Region::new()),
            count: RegionCount::one(),
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
        self.write_mut().tail.fetch_add(len, Ordering::AcqRel);
    }

    /// NOTE: caller must check that the range is valid
    pub fn shrink(&mut self, len: usize) {
        let read = self.read_mut();
        let range = read.range();
        assert!(range.start + len <= range.end);
        read.head.fetch_add(len, Ordering::AcqRel);
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
        let mut val = self.0.load(Ordering::Acquire);
        while val != usize::MAX {
            let cex = self.0.compare_exchange(
                val,
                val + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            );
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

impl<const N: usize> BipBuffer<N> {
    pub const fn new() -> Self {
        Self {
            buf: UnsafeCell::new(MaybeUninit::uninit()),
            regions: Regions::new(),
            protectors: AtomicUsize::new(0),
            free_ratio: 0.1,
        }
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

    fn try_push(&mut self, val: &[u8]) -> Result<()> {
        let buf = self.try_reserve(LEN_SIZE + val.len())?;
        let len_bytes = usize::to_ne_bytes(val.len());
        let (len_buf, val_buf) = buf.split_at_mut(LEN_SIZE);
        len_buf.copy_from_slice(&len_bytes);
        val_buf.copy_from_slice(val);
        self.regions.grow(LEN_SIZE + val.len());
        Ok(())
    }

    fn try_reserve(&mut self, len: usize) -> Result<&mut [u8]> {
        if len >= N {
            return Err(Error::EntryTooBig);
        }
        match self.regions.refs() {
            RegionRefs::One(reg) => {
                let range = reg.range();
                if range.end + len <= N {
                    Ok(self.slice_buffer_mut(range.end, len))
                } else {
                    Err(Error::RegionFull)
                }
            }
            RegionRefs::Two { read, write } => {
                let write_range = write.range();
                let read_range = read.range();
                if write_range.end + len <= read_range.start {
                    Ok(self.slice_buffer_mut(write_range.end, len))
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
        self.regions.split();
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

    fn is_below_ratio(&self) -> bool {
        let size = self.regions.size();
        1.0 - ((size as f32) / (N as f32)) < self.free_ratio
    }

    pub fn push(&mut self, val: &[u8]) -> Result<Vec<usize>> {
        let mut popped = Vec::new();
        popped.extend(&self.push_one(val)?);
        while self.is_below_ratio() {
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
        while self.is_below_ratio() {
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
        let reg = self.regions.read();
        let range = reg.range();
        if range.is_empty() {
            return None;
        }
        let len = self.read_len_at(range.start);
        // let data = self.slice_buffer(range.start + LEN_SIZE, len);
        self.regions.shrink(LEN_SIZE + len);
        Some(range.start)
    }

    pub fn iter(&self) -> Iter<N> {
        Iter { bip: self, idx: Some(self.regions.read().head()) }
    }
}

pub struct Iter<'a, const N: usize> {
    bip: &'a BipBuffer<N>,
    idx: Option<usize>,
}
impl<'a, const N: usize> Iterator for Iter<'a, N> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        let idx = self.idx?;
        let item = self.bip.read_at(idx);
        if item.is_empty() {
            self.idx = None;
            return None;
        }
        let next = idx + LEN_SIZE + item.len();
        self.idx = match self.bip.regions.refs() {
            RegionRefs::One(reg) => {
                let range = reg.range();
                match next {
                    n if range.contains(&n) => Some(n),
                    _ => None,
                }
            }
            RegionRefs::Two { read, write } => {
                let read_range = read.range();
                let write_range = write.range();
                match next {
                    n if read_range.contains(&n) => Some(n),
                    n if write_range.contains(&n) => Some(n),
                    _ => None,
                }
            }
        };
        Some(item)
    }
}

#[cfg(test)]
mod test_bip_buffer {
    use super::*;
    use assert2::assert;
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
        let mut buf = BipBuffer::<1024>::new();
        assert!(buf.try_push(b"hey now!").is_ok());
        let read_region = buf.regions.read();
        assert!(read_region.head() == 0);
    }

    #[rstest]
    fn try_push_moves_read_tail_forward() {
        let mut buf = BipBuffer::<1024>::new();
        assert!(buf.try_push(b"hey now!").is_ok());
        let read_region = buf.regions.read();
        assert!(read_region.tail() == LEN_SIZE + 8 /* hey now! */);
    }

    #[rstest]
    fn try_push_errors_if_full() {
        let mut buf = BipBuffer::<60>::new();
        for _ in 0..3 {
            assert!(buf.try_push(b"hey now!").is_ok());
        }
        assert!(buf.try_push(b"hey now!") == Err(Error::RegionFull));
        let read_region = buf.regions.read();
        assert!(read_region.tail() == 3 * (LEN_SIZE + 8));
    }

    #[rstest]
    fn push_on_full_splits_the_regions() {
        let mut buf = BipBuffer::<60>::new();
        for _ in 0..3 {
            assert!(buf.try_push(b"hey now!").is_ok());
        }
        assert!(buf.regions.count.get() == Count::One);
        let _ = buf.push(b"hey now!");
        assert!(buf.regions.count.get() == Count::Two);
    }

    #[rstest]
    fn push_on_full_pops_from_read_region() {
        let mut buf = BipBuffer::<60>::new();
        for _ in 0..3 {
            assert!(buf.try_push(b"hey now!").is_ok());
        }
        let _ = buf.push(b"hey now!");
        assert!(buf.regions.read().head() == LEN_SIZE + 8);
    }

    #[rstest]
    fn push_on_full_pops_more_than_once_if_needed() {
        let mut buf = BipBuffer::<60>::new();
        for _ in 0..3 {
            assert!(buf.try_push(b"hey now!").is_ok());
        }
        let _ = buf.push(b"hey now now!");
        assert!(buf.regions.read().head() == 2 * (LEN_SIZE + 8));
    }

    #[rstest]
    fn push_on_full_appends_to_write_region() {
        let mut buf = BipBuffer::<60>::new();
        for _ in 0..3 {
            assert!(buf.try_push(b"hey now!").is_ok());
        }
        assert!(buf.push(b"hey now!") == Ok(vec![0]));
        assert!(buf.regions.write().tail() == (LEN_SIZE + 8));
    }

    #[rstest]
    fn is_below_ratio_works() {
        let mut buf = BipBuffer::<100>::new();
        buf.free_ratio = 0.1;
        for _ in 0..8 {
            assert!(buf.try_push(b"xo").is_ok());
        }
        assert!(buf.regions.size() == 80);
        assert!(buf.is_below_ratio() == false);
        let _ = buf.try_push(b"xox");
        assert!(buf.regions.size() == 91);
        assert!(buf.is_below_ratio() == true);
    }

    #[rstest]
    fn push_will_automatically_free_if_below_ratio() {
        let mut buf = BipBuffer::<100>::new();
        buf.free_ratio = 0.1;
        for _ in 0..8 {
            assert!(buf.try_push(b"xo").is_ok());
        }
        assert!(buf.regions.size() == 80);
        assert!(buf.regions.read().range().eq(0..80));
        assert!(buf.push(b"xox") == Ok(vec![0]));
        assert!(buf.regions.size() == 81);
        assert!(buf.regions.read().range().eq(10..91));
    }

    #[rstest]
    fn it_pushes_until_regions_merge() {
        let mut buf = BipBuffer::<60>::new();
        for _ in 0..3 {
            assert!(buf.try_push(b"hey now!").is_ok());
        }
        assert!(buf.push(b"hey now!").is_ok());
        assert!(buf.regions.count.get() == Count::Two);
        assert!(buf.push(b"hey now!") == Ok(vec![LEN_SIZE + 8]));
        assert!(buf.push(b"hey now!") == Ok(vec![2 * (LEN_SIZE + 8)]));
        assert!(buf.regions.count.get() == Count::One);
    }

    #[rstest]
    fn try_pop_works() {
        let mut buf = BipBuffer::<1024>::new();
        assert!(buf.try_push(b"hey now!").is_ok());
        assert!(buf.try_push(b"hey now?").is_ok());
        assert!(buf.try_pop() == Some(0));
        assert!(buf.try_pop() == Some(LEN_SIZE + 8));
        assert!(buf.try_pop() == None);
    }
}
