use std::{borrow::Cow};

use crate::{
    error::{Error},
    ringlist::{RingIter, RingList},
};

const HEADER_SIZE: usize = 4 + 8;
const ALIGN_SIZE: u8 = 1;

pub fn crosses_head(
    head: usize,
    tail: usize,
    start: usize,
    end: usize,
) -> bool {
    match (head, tail, start, end) {
        (h, _, s, e) if s <= h && h < e => true,
        // (h, _, s, e) if h < t && h < e => true,
        // (h, t, s, _) if s <= h && 0 < t && s == 0 => true,
        // (h, t, s, _) if h < t && 0 < t && s == 0 => true,
        _ => false,
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct EntryHeader {
    offset: u64,
    len: u32,
}

fn align_idx(idx: usize) -> usize {
    if ALIGN_SIZE == 1 {
        return idx;
    }
    const ALIGN: f64 = ALIGN_SIZE as f64;
    ((idx as f64 / ALIGN).ceil() * ALIGN) as usize
}

impl EntryHeader {
    pub fn data_len(&self) -> usize {
        self.len as usize
    }

    pub fn full_entry_len(&self) -> usize {
        HEADER_SIZE + self.len as usize
    }

    pub fn is_blank(&self) -> bool {
        self.offset == 0 && self.len == 0
    }

    pub fn as_bytes(&self) -> [u8; 12] {
        let mut out = [0u8; 12];
        out[0..8].copy_from_slice(&self.offset.to_le_bytes());
        out[8..12].copy_from_slice(&self.len.to_le_bytes());
        out
    }

    pub fn copy_to(&self, bytes: &mut [u8]) {
        bytes.copy_from_slice(&self.as_bytes());
    }
}

impl TryFrom<&[u8]> for EntryHeader {
    type Error = Error;

    fn try_from(bytes: &[u8]) -> std::result::Result<Self, Self::Error> {
        let Some(slice) = &bytes.get(..HEADER_SIZE) else {
            return Err(Error::InvalidHeader)
        };
        let err = |_| Error::InvalidHeader;
        let offset = u64::from_le_bytes(slice[0..8].try_into().map_err(err)?);
        let len = u32::from_le_bytes(slice[8..12].try_into().map_err(err)?);
        let header = Self { offset, len };
        if header.is_blank() {
            return Err(Error::InvalidHeader);
        }
        Ok(header)
    }
}

impl TryFrom<Option<&[u8]>> for EntryHeader {
    type Error = Error;

    fn try_from(
        bytes: Option<&[u8]>,
    ) -> std::result::Result<Self, Self::Error> {
        let Some(bytes) = bytes else {
            return Err(Error::InvalidHeader);
        };
        Self::try_from(bytes)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Entry<'a> {
    header: EntryHeader,
    data: Cow<'a, [u8]>,
}

impl<'a> Entry<'a> {
    pub fn copy_to(&self, bytes: &mut [u8]) {
        bytes[0..HEADER_SIZE].copy_from_slice(&self.header.as_bytes());
        bytes[HEADER_SIZE..HEADER_SIZE + self.data.len()]
            .copy_from_slice(&self.data);
    }
}

impl<'a> TryFrom<&'a [u8]> for Entry<'a> {
    type Error = Error;

    fn try_from(bytes: &'a [u8]) -> std::result::Result<Self, Self::Error> {
        let header = EntryHeader::try_from(bytes)?;
        let Some(data) = bytes.get(HEADER_SIZE..HEADER_SIZE + header.data_len()) else {
            return Err(Error::InvalidEntry { index: None });
        };
        Ok(Entry { header, data: data.into() })
    }
}

impl<'a> TryFrom<Option<&'a [u8]>> for Entry<'a> {
    type Error = Error;

    fn try_from(
        bytes: Option<&'a [u8]>,
    ) -> std::result::Result<Self, Self::Error> {
        let Some(bytes) = bytes else {
            return Err(Error::InvalidEntry { index: None });
        };
        Self::try_from(bytes)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OwnedEntry {
    header: EntryHeader,
    data: Vec<u8>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct IndexItem {
    global_position: u64,
    entry_idx: u32,
    /// The index of the next free spot. It may include uninitialized space
    /// for the sake of cache alignment.
    next_idx: u32,
}

impl IndexItem {
    pub fn new(global_position: u64, entry_idx: u32, next_idx: u32) -> Self {
        Self { global_position, entry_idx, next_idx }
    }

    pub fn index(&self) -> usize {
        self.entry_idx as usize
    }

    pub fn next_index(&self) -> usize {
        self.next_idx as usize
    }
}

pub struct MemLog<const I: usize> {
    entry_bytes: Vec<u8>,
    entry_cap: usize,
    idxs: RingList<IndexItem, I>,
    tail: usize,
}

// impl<const I: usize> MemLog<I> {
//     pub fn with_capacities(data_cap: usize, idx_cap: usize) -> Self {
//         Self {
//             entry_bytes: vec![0; data_cap],
//             entry_cap: data_cap,
//             idxs: RingList::new(),
//             tail: 0,
//         }
//     }
//
//     // pub fn head(&self) -> Option<usize> {
//     //     self.idxs.head()
//     // }
//
//     // pub fn tail(&self) -> Option<usize> {
//     //     self.idxs.tail()
//     // }
//
//     pub fn is_empty(&self) -> bool {
//         // self.idxs.is_empty()
//         true
//     }
//
//     pub fn get_header(&self, idx: usize) -> Result<EntryHeader> {
//         EntryHeader::try_from(self.entry_bytes.get(idx..))
//             .map_err(|_| Error::InvalidEntry { index: Some(idx) })
//     }
//
//     pub fn get(&self, idx: usize) -> Result<Entry<'_>> {
//         Entry::try_from(self.entry_bytes.get(idx..))
//             .map_err(|_| Error::InvalidEntry { index: Some(idx) })
//     }
//
//     pub fn get_owned(&self, idx: usize) -> Result<OwnedEntry> {
//         let entry = Entry::try_from(self.entry_bytes.get(idx..))
//             .map_err(|_| Error::InvalidEntry { index: Some(idx) })?;
//         Ok(OwnedEntry {
//             header: entry.header,
//             data: entry.data.as_ref().to_vec(),
//         })
//     }
//
//     pub fn pop(&mut self) -> Result<Option<OwnedEntry>> {
//         let Some(indexed) = self.idxs.pop() else {return Ok(None)};
//         let index = indexed.index();
//         let popped_entry = self.get_owned(index)?;
//         self.entry_bytes[index..index + popped_entry.header.full_entry_len()]
//             .fill(0);
//         Ok(Some(popped_entry))
//     }
//
//     pub fn occupied_space(&self) -> (Range<usize>, Range<usize>) {
//         if self.is_empty() {
//             return (0..0, 0..0);
//         }
//         match (self.head(), self.tail()) {
//             (Some(h), Some(t)) if h < t => (h..t, 0..0),
//             (Some(h), Some(t)) => (0..t, h..self.entry_cap),
//             _ => (0..0, 0..0),
//         }
//     }
//
//     pub fn find_spot(&self, num_bytes: usize) -> Range<usize> {
//         let tail = self.tail();
//         let start = match (tail, num_bytes) {
//             (Some(tail), len) if tail + len >= self.entry_cap => 0,
//             (Some(tail), _) => tail,
//             _ => 0,
//         };
//         let end = start + num_bytes;
//         start..end
//     }
//
//     /// Append treats entry_bytes as a circular buffer.
//     pub fn push(&mut self, entry: Entry<'_>) -> Result<()> {
//         let entry_len = entry.header.full_entry_len();
//         let entry_range = self.find_spot(entry_len);
//         if !self.idxs.is_empty() {
//             loop {
//                 let head = self.head().unwrap();
//                 if entry_range.contains(&head) {
//                     self.pop()?;
//                 } else {
//                     break;
//                 }
//             }
//         }
//
//         let next_idx = match align_idx(entry_range.end) {
//             x if x >= self.entry_cap => 0,
//             x => x,
//         };
//         self.idxs.push(IndexItem {
//             global_position: entry.header.offset,
//             entry_idx: 0 as u32, // TODO: WHAT
//             next_idx: next_idx as u32,
//         })?;
//         entry.copy_to(&mut self.entry_bytes[entry_range]);
//         Ok(())
//     }
//
//     pub fn iter(&self) -> Iter<'_, I> {
//         Iter { log: self, idx: self.head() }
//     }
//
//     pub fn iter_headers(&self) -> HeaderIter<'_, I> {
//         HeaderIter { log: self, idx_iter: self.idxs.iter() }
//     }
// }

#[derive(Debug, PartialEq, Eq)]
pub struct IndexedHeader(usize, EntryHeader);

impl IndexedHeader {
    pub fn span(&self) -> std::ops::Range<usize> {
        let end = self.0 + self.1.full_entry_len();
        self.0..end
    }
}

pub struct HeaderIter<'a, const I: usize> {
    log: &'a MemLog<I>,
    idx_iter: RingIter<'a, IndexItem, I>,
}

impl<'a, const I: usize> Iterator for HeaderIter<'a, I> {
    type Item = IndexedHeader;

    fn next(&mut self) -> Option<Self::Item> {
        let idx = self.idx_iter.next()?.index();
        let header =
            EntryHeader::try_from(self.log.entry_bytes.get(idx..)).ok()?;
        Some(IndexedHeader(idx, header))
    }
}

pub struct Iter<'a, const I: usize> {
    log: &'a MemLog<I>,
    idx: Option<usize>,
}

// impl<'a, const I: usize> Iterator for Iter<'a, I> {
//     type Item = Entry<'a>;
//
//     fn next(&mut self) -> Option<Self::Item> {
//         let idx = self.idx?;
//         let entry = self.log.get(idx);
//         if let Ok(entry) = entry {
//             self.idx = Some(idx + entry.header.full_entry_len());
//             Some(entry)
//         } else {
//             let tail = self.log.tail()?;
//             let head = self.log.head()?;
//             if tail < head && idx > head {
//                 self.idx = Some(0);
//                 self.next()
//             } else {
//                 None
//             }
//         }
//     }
// }

// #[cfg(test)]
// mod test_header_iter {
//     use super::*;
//     use assert2::assert;
//     use rstest::*;
//
//     const DATA: [u8; 128] = {
//         let mut data = [0u8; 128];
//         let mut i = 0;
//         while i < 128 {
//             data[i] = i as u8;
//             i += 1;
//         }
//         data
//     };
//
//     fn make_entry(i: u64, len: usize) -> Entry<'static> {
//         let data = Cow::Borrowed(&DATA[..len]);
//         Entry { header: EntryHeader { offset: i, len: len as u32 }, data }
//     }
//
//     #[rstest]
//     fn it_returns_nothing_if_log_is_empty() {
//         let log = MemLog::with_capacities(150, 20);
//         let headers = log.iter_headers().collect::<Vec<_>>();
//         assert!(headers == vec![]);
//     }
//
//     #[rstest]
//     fn it_returns_non_wrapping_headers() {
//         let mut log = MemLog::with_capacities(150, 20);
//         let entry = make_entry(0, 5);
//         log.push(entry.clone()).unwrap();
//         let entry = make_entry(1, 4);
//         log.push(entry.clone()).unwrap();
//         let headers: Vec<_> = log.iter_headers().collect();
//         let expected = vec![
//             IndexedHeader(0, EntryHeader { offset: 0, len: 5 }),
//             IndexedHeader(17, EntryHeader { offset: 1, len: 4 }),
//         ];
//         assert!(headers == expected);
//     }
//
//     #[rstest]
//     fn it_returns_wrapping_headers() {
//         let mut log = MemLog::with_capacities(150, 20);
//         let entries: Vec<_> =
//             (0..15).map(|i| make_entry(i, i as usize + 1)).collect();
//         for entry in entries.iter().take(10) {
//             log.push(entry.clone()).unwrap();
//         }
//
//         let idxs = log.iter_headers().map(|x| x.0).collect::<Vec<_>>();
//         assert!(idxs == vec![58, 75, 93, 112, 0, 21]);
//     }
// }

// #[cfg(test)]
// mod test {
//     use super::*;
//     use assert2::assert;
//     use rstest::*;
//
//     const DATA: [u8; 128] = {
//         let mut data = [0u8; 128];
//         let mut i = 0;
//         while i < 128 {
//             data[i] = i as u8;
//             i += 1;
//         }
//         data
//     };
//
//     fn make_entry(i: u64, len: usize) -> Entry<'static> {
//         let data = Cow::Borrowed(&DATA[..len]);
//         Entry { header: EntryHeader { offset: i, len: len as u32 }, data }
//     }
//
//     #[rstest]
//     fn it_leaves_head_at_0_in_the_beginning() {
//         let mut log = MemLog::with_capacities(256, 20);
//         let entry = Entry {
//             header: EntryHeader { offset: 4567, len: 4 },
//             data: Cow::Borrowed(&[1, 2, 3, 4]),
//         };
//         log.push(entry).unwrap();
//         assert!(log.head() == Some(0));
//     }
//
//     #[rstest]
//     fn it_writes_the_header_to_the_first_12_bytes() {
//         let mut log = MemLog::with_capacities(256, 20);
//         let entry = Entry {
//             header: EntryHeader { offset: 4567, len: 4 },
//             data: Cow::Borrowed(&[1, 2, 3, 4]),
//         };
//         log.push(entry).unwrap();
//         assert!(
//             &log.entry_bytes[0..HEADER_SIZE]
//                 == &[215, 17, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0]
//         );
//     }
//
//     #[rstest]
//     fn it_writes_the_data_to_the_next_bytes() {
//         let mut log = MemLog::with_capacities(256, 20);
//         let entry = Entry {
//             header: EntryHeader { offset: 4567, len: 4 },
//             data: Cow::Borrowed(&[1, 2, 3, 4]),
//         };
//         let expected = entry.clone();
//         log.push(entry).unwrap();
//         assert!(
//             &log.entry_bytes[HEADER_SIZE..HEADER_SIZE + 4] == &[1, 2, 3, 4]
//         );
//         let entry = log.get(0).unwrap();
//         assert!(entry == expected);
//     }
//
//     #[rstest]
//     fn it_writes_multiple_entries() {
//         let mut log = MemLog::with_capacities(256, 20);
//         let entry1 = Entry {
//             header: EntryHeader { offset: 4567, len: 4 },
//             data: Cow::Borrowed(&[1, 2, 3, 4]),
//         };
//         let entry2 = Entry {
//             header: EntryHeader { offset: 1234, len: 8 },
//             data: Cow::Borrowed(&[5, 6, 7, 8, 9, 10, 11, 12]),
//         };
//         let expected1 = entry1.clone();
//         let expected2 = entry2.clone();
//         log.push(entry1).unwrap();
//         log.push(entry2).unwrap();
//         let entry1 = log.get(0).unwrap();
//         let entry2 = log.get(16).unwrap();
//         assert!(entry1 == expected1);
//         assert!(entry2 == expected2);
//     }
//
//     #[rstest]
//     fn pop_moves_the_head_forward() {
//         let mut log = MemLog::with_capacities(150, 20);
//         let entries: Vec<_> = (0..15).map(|i| make_entry(i, 4)).collect();
//         for entry in entries.iter().take(5) {
//             log.push(entry.clone()).unwrap();
//         }
//         assert!(log.head() == Some(0));
//         log.pop().unwrap();
//         assert!(log.head() == Some(16));
//         log.pop().unwrap();
//         assert!(log.head() == Some(32));
//     }
//
//     #[rstest]
//     fn it_wraps_around() {
//         let mut log = MemLog::with_capacities(150, 20);
//         let entries: Vec<_> =
//             (0..15).map(|i| make_entry(i, i as usize)).collect();
//         for entry in entries.iter().take(10) {
//             // dbg!(entry);
//             let res = log.push(entry.clone());
//             assert!(res != Err(Error::InvalidEntry { index: Some(0) }))
//         }
//         let entry = log.get(0).unwrap();
//         assert!(entry == entries[8]);
//
//         let entry = log.get(entries[8].header.full_entry_len()).unwrap();
//         assert!(entry == entries[9]);
//         // assert!(
//         //     log.get(entries[8].full_size() + entries[9].full_size()).unwrap()
//         //         == entries[2]
//         // );
//         // assert!(log.head() == entries[8].full_size());
//         assert!(log.idxs.head() == Some(69));
//         assert!(log.get(log.head().unwrap()).unwrap() == entries[2]);
//     }
//
//     #[rstest]
//     fn it_iterates_over_entries() {
//         let mut log = MemLog::with_capacities(150, 20);
//         let entries: Vec<_> =
//             (0..15).map(|i| make_entry(i, i as usize)).collect();
//         for entry in entries.iter().take(10) {
//             log.push(entry.clone()).unwrap();
//         }
//         let collected: Vec<_> = log.iter().collect();
//         assert!(collected.as_slice() == &entries[..10]);
//     }
// }
