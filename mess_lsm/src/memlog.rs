use std::{
    borrow::Cow,
    num::NonZeroU32,
    sync::atomic::{AtomicUsize, Ordering},
};

use crate::error::{Error, Result};

const HEADER_SIZE: usize = 4 + 8;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct EntryHeader {
    offset: u64,
    len: u32,
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
    pub fn full_size(&self) -> usize {
        self.data.len() + HEADER_SIZE
    }
}

impl<'a> TryFrom<&'a [u8]> for Entry<'a> {
    type Error = Error;

    fn try_from(bytes: &'a [u8]) -> std::result::Result<Self, Self::Error> {
        let header = EntryHeader::try_from(bytes)?;
        let Some(data) = bytes.get(HEADER_SIZE..HEADER_SIZE + header.data_len()) else {
            return Err(Error::InvalidEntry { index: 0 });
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
            return Err(Error::InvalidEntry { index: 0 });
        };
        Self::try_from(bytes)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OwnedEntry {
    header: EntryHeader,
    data: Vec<u8>,
}

pub struct MemLog {
    entry_bytes: Vec<u8>,
    entry_cap: usize,
    idxs: Vec<Option<NonZeroU32>>,
    idx_cap: usize,
    idx_head: usize,
    idx_tail: usize,

    head: AtomicUsize,
    tail: AtomicUsize,
}

impl MemLog {
    pub fn with_capacities(data_cap: usize, idx_cap: usize) -> Self {
        let entry_bytes = vec![0; data_cap];
        let idxs = vec![None; idx_cap];
        // idxs[0] = NonZeroU32::new(1);
        Self {
            entry_bytes,
            entry_cap: data_cap,
            head: AtomicUsize::new(0),
            tail: AtomicUsize::new(0),
            idxs,
            idx_cap,
            idx_head: 0,
            idx_tail: 0,
        }
    }

    pub fn head(&self) -> usize {
        // self.head.load(Ordering::Acquire)
        let Some(Some(data_idx)) = self.idxs.get(self.idx_head) else {
            return 0;
        };
        data_idx.get() as usize - 1
    }

    pub fn tail(&self) -> usize {
        // self.tail.load(Ordering::Acquire)
        let Some(Some(data_idx)) = self.idxs.get(self.idx_tail) else {
            return 0;
        };
        data_idx.get() as usize - 1
    }

    pub fn get_header(&self, idx: usize) -> Result<EntryHeader> {
        EntryHeader::try_from(self.entry_bytes.get(idx..))
            .map_err(|_| Error::InvalidEntry { index: idx })
    }

    pub fn get(&self, idx: usize) -> Result<Entry<'_>> {
        Entry::try_from(self.entry_bytes.get(idx..))
            .map_err(|_| Error::InvalidEntry { index: idx })
    }

    pub fn get_owned(&self, idx: usize) -> Result<OwnedEntry> {
        let entry = Entry::try_from(self.entry_bytes.get(idx..))
            .map_err(|_| Error::InvalidEntry { index: idx })?;
        Ok(OwnedEntry {
            header: entry.header,
            data: entry.data.as_ref().to_vec(),
        })
    }

    pub fn pop(&mut self) -> Result<Option<OwnedEntry>> {
        let Some(Some(data_idx)) = self.idxs.get(self.idx_head) else {
            return Ok(None);
        };
        let data_idx = data_idx.get() as usize - 1;
        let entry = self.get_owned(data_idx)?;
        self.remove_head()?;
        Ok(Some(entry))
    }

    pub fn remove_head(&mut self) -> Result<()> {
        let idx_head = self.idx_head;
        let Some(Some(data_idx)) = self.idxs.get(idx_head) else {
            return Ok(());
        };
        let data_idx = data_idx.get() as usize - 1;
        let header = self.get_header(data_idx)?;
        self.entry_bytes[data_idx..data_idx + header.full_entry_len()].fill(0);
        self.idxs[idx_head] = None;
        self.idx_head += 1;
        if self.idx_head >= self.idx_cap {
            self.idx_head = 0;
        }
        Ok(())
    }

    /// Append treats entry_bytes as a circular buffer.
    pub fn append(&mut self, entry: Entry<'_>) -> Result<()> {
        let mut set_head = false;
        if entry.full_size() > self.entry_cap {
            return Err(Error::EntryTooBig);
        }
        let head = self.head();
        let tail = self.tail();
        let write_idx = if tail + entry.full_size() >= self.entry_cap {
            set_head = true;
            0
        } else {
            tail
        };
        let new_tail = write_idx + entry.full_size();

        set_head = set_head || (write_idx <= head && new_tail > head);
        if set_head {
            // let mut del_count = 0;
            let mut end = new_tail;
            for header in self.iter_headers() {
                if header.span().contains(&new_tail) {
                    // del_count += 1;
                    end = header.span().end;
                } else {
                    break;
                }
            }
            self.entry_bytes[head..end].fill(0);
            let next_head = if end >= self.entry_cap { 0 } else { end };
            self.head.store(next_head, Ordering::Release);
        }

        self.entry_bytes[write_idx..write_idx + HEADER_SIZE]
            .copy_from_slice(&entry.header.as_bytes());
        self.entry_bytes
            [write_idx + HEADER_SIZE..write_idx + entry.full_size()]
            .copy_from_slice(&entry.data);
        self.tail.store(new_tail, Ordering::Release);
        Ok(())
    }

    pub fn iter(&self) -> Iter<'_> {
        Iter { log: self, idx: self.head() }
    }

    pub fn iter_headers(&self) -> HeaderIter<'_> {
        HeaderIter { log: self, idx: self.head() }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct IndexedHeader(usize, EntryHeader);

impl IndexedHeader {
    pub fn span(&self) -> std::ops::Range<usize> {
        let end = self.0 + self.1.full_entry_len();
        self.0..end
    }
}

pub struct HeaderIter<'a> {
    log: &'a MemLog,
    idx: usize,
}

impl<'a> Iterator for HeaderIter<'a> {
    type Item = IndexedHeader;

    fn next(&mut self) -> Option<Self::Item> {
        let idx = self.idx;
        let head = self.log.head();
        let tail = self.log.tail();
        if tail > head && idx > tail {
            return None;
        }
        if tail <= head && idx > tail && idx >= head {
            return None;
        }
        let header =
            EntryHeader::try_from(self.log.entry_bytes.get(idx..)).ok()?;
        self.idx += HEADER_SIZE + header.data_len();
        Some(IndexedHeader(idx, header))
    }
}

pub struct Iter<'a> {
    log: &'a MemLog,
    idx: usize,
}

impl<'a> Iterator for Iter<'a> {
    type Item = Entry<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        let entry = self.log.get(self.idx);
        if let Ok(entry) = entry {
            self.idx += entry.full_size();
            Some(entry)
        } else {
            let tail = self.log.tail.load(Ordering::Acquire);
            let head = self.log.head.load(Ordering::Acquire);
            if tail < head && self.idx > head {
                self.idx = 0;
                self.next()
            } else {
                None
            }
        }
    }
}

#[cfg(test)]
mod test_header_iter {
    use super::*;
    use assert2::assert;
    use rstest::*;

    const DATA: [u8; 128] = {
        let mut data = [0u8; 128];
        let mut i = 0;
        while i < 128 {
            data[i] = i as u8;
            i += 1;
        }
        data
    };

    fn make_entry(i: u64, len: usize) -> Entry<'static> {
        let data = Cow::Borrowed(&DATA[..len]);
        Entry { header: EntryHeader { offset: i, len: len as u32 }, data }
    }

    #[rstest]
    fn it_returns_nothing_if_log_is_empty() {
        let log = MemLog::with_capacities(150, 20);
        let headers = log.iter_headers().collect::<Vec<_>>();
        assert!(headers == vec![]);
    }

    // #[rstest]
    fn it_returns_non_wrapping_headers() {
        let mut log = MemLog::with_capacities(150, 20);
        let entry = make_entry(0, 5);
        log.append(entry.clone()).unwrap();
        let entry = make_entry(1, 4);
        log.append(entry.clone()).unwrap();
        let headers: Vec<_> = log.iter_headers().collect();
        let expected = vec![
            IndexedHeader(0, EntryHeader { offset: 0, len: 5 }),
            IndexedHeader(17, EntryHeader { offset: 1, len: 4 }),
        ];
        assert!(headers == expected);
    }

    #[rstest]
    fn it_returns_wrapping_headers() {
        let mut log = MemLog::with_capacities(150, 20);
        let entries: Vec<_> =
            (0..15).map(|i| make_entry(i, i as usize + 1)).collect();
        for entry in entries.iter().take(10) {
            log.append(entry.clone()).unwrap();
        }

        let headers = log.iter_headers().collect::<Vec<_>>();
        assert!(headers != vec![]);
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use assert2::assert;
    use rstest::*;

    #[rstest]
    fn it_leaves_head_at_0_in_the_beginning() {
        let mut log = MemLog::with_capacities(256, 20);
        let entry = Entry {
            header: EntryHeader { offset: 4567, len: 4 },
            data: Cow::Borrowed(&[1, 2, 3, 4]),
        };
        log.append(entry).unwrap();
        assert!(log.head() == 0);
    }

    #[rstest]
    fn it_writes_the_header_to_the_first_12_bytes() {
        let mut log = MemLog::with_capacities(256, 20);
        let entry = Entry {
            header: EntryHeader { offset: 4567, len: 4 },
            data: Cow::Borrowed(&[1, 2, 3, 4]),
        };
        log.append(entry).unwrap();
        assert!(
            &log.entry_bytes[0..HEADER_SIZE]
                == &[215, 17, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0]
        );
    }

    #[rstest]
    fn it_writes_the_data_to_the_next_bytes() {
        let mut log = MemLog::with_capacities(256, 20);
        let entry = Entry {
            header: EntryHeader { offset: 4567, len: 4 },
            data: Cow::Borrowed(&[1, 2, 3, 4]),
        };
        let expected = entry.clone();
        log.append(entry).unwrap();
        assert!(
            &log.entry_bytes[HEADER_SIZE..HEADER_SIZE + 4] == &[1, 2, 3, 4]
        );
        let entry = log.get(0).unwrap();
        assert!(entry == expected);
    }

    // #[rstest]
    fn it_writes_multiple_entries() {
        let mut log = MemLog::with_capacities(256, 20);
        let entry1 = Entry {
            header: EntryHeader { offset: 4567, len: 4 },
            data: Cow::Borrowed(&[1, 2, 3, 4]),
        };
        let entry2 = Entry {
            header: EntryHeader { offset: 1234, len: 8 },
            data: Cow::Borrowed(&[5, 6, 7, 8, 9, 10, 11, 12]),
        };
        let expected1 = entry1.clone();
        let expected2 = entry2.clone();
        log.append(entry1).unwrap();
        log.append(entry2).unwrap();
        let entry1 = log.get(0).unwrap();
        let entry2 = log.get(16).unwrap();
        assert!(entry1 == expected1);
        assert!(entry2 == expected2);
    }

    // #[rstest]
    fn it_wraps_around() {
        let data = (0u8..128).collect::<Vec<_>>();
        let mut log = MemLog::with_capacities(150, 20);
        let make_entry = |i: u64| {
            let len = i as u32 + 1;
            let data = Cow::Borrowed(&data[..len as usize]);
            Entry { header: EntryHeader { offset: i, len }, data }
        };
        let entries = (0..15).map(make_entry).collect::<Vec<_>>();
        for entry in entries.iter().take(10) {
            log.append(entry.clone()).unwrap();
        }
        let entry = log.get(0).unwrap();
        assert!(entry != entries[0]);
        assert!(entry == entries[8]);
        assert!(log.get(entries[8].full_size()).unwrap() == entries[9]);
        // assert!(
        //     log.get(entries[8].full_size() + entries[9].full_size()).unwrap()
        //         == entries[2]
        // );
        // assert!(log.head() == entries[8].full_size());
        assert!(log.get(log.head()).unwrap() == entries[2]);
    }

    #[rstest]
    fn it_iterates_over_entries() {
        let mut log = MemLog::with_capacities(256, 20);
        let entries = [
            Entry {
                header: EntryHeader { offset: 4567, len: 4 },
                data: Cow::Borrowed(&[1, 2, 3, 4]),
            },
            Entry {
                header: EntryHeader { offset: 1234, len: 8 },
                data: Cow::Borrowed(&[5, 6, 7, 8, 9, 10, 11, 12]),
            },
            Entry {
                header: EntryHeader { offset: 5678, len: 4 },
                data: Cow::Borrowed(&[13, 14, 15, 16]),
            },
        ];
        log.append(entries[0].clone()).unwrap();
        log.append(entries[1].clone()).unwrap();
        log.append(entries[2].clone()).unwrap();
        let collected_entries: Vec<Entry> = log.iter().collect();
        // assert!(collected_entries.as_slice() == &entries[..]);
    }
}
