use std::{borrow::Cow, fs::File, os::unix::prelude::FileExt, path::Path};

use zerovec::{
    ule::{RawBytesULE, ULE},
    ZeroSlice,
};

use crate::error::{Error, Result};

const BLOCK_SIZE: usize = 4_096;
const HEADER_SIZE: usize = 3;

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Eq, Hash, Debug)]
pub enum Op<'a> {
    Put(Cow<'a, [u8]>),
}

#[derive(PartialEq, Eq, Hash, Debug)]
pub struct FetchedOp<'a> {
    op: Op<'a>,
    block_offset: u16,
    file_offset: u64,
}

#[derive(Copy, Clone, Default, Debug)]
pub enum BlockType {
    #[default]
    V1,
    Unknown,
}

impl From<u8> for BlockType {
    fn from(value: u8) -> Self {
        match value {
            0 => BlockType::V1,
            _ => BlockType::Unknown,
        }
    }
}
impl From<BlockType> for u8 {
    fn from(value: BlockType) -> Self {
        match value {
            BlockType::V1 => 0,
            BlockType::Unknown => u8::MAX,
        }
    }
}

#[derive(Copy, Clone, Debug)]
pub struct BlockHeader {
    typ: BlockType,
    block_size: u16,
}

impl Default for BlockHeader {
    fn default() -> Self {
        Self { typ: Default::default(), block_size: BLOCK_SIZE as u16 }
    }
}

pub struct Block {
    fd: File,
    initial_offset: u64,
    buf: [u8; BLOCK_SIZE],
    next_index: usize,
}

impl Block {
    pub fn load_or_create(
        path: impl AsRef<Path>,
        initial_offset: u64,
    ) -> Result<Self> {
        let fd = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .open(path.as_ref())?;
        Ok(Self {
            fd,
            initial_offset,
            buf: [0; BLOCK_SIZE],
            next_index: HEADER_SIZE,
        })
    }

    pub fn get_header(&self) -> BlockHeader {
        BlockHeader {
            typ: BlockType::from(self.buf[0]),
            block_size: u16::from_le_bytes(self.buf[1..=2].try_into().unwrap()),
        }
    }

    pub fn set_header(&mut self, header: BlockHeader) {
        self.buf[0] = header.typ.into();
        let bytes = header.block_size.to_le_bytes();
        self.buf[1] = bytes[0];
        self.buf[2] = bytes[1];
    }

    pub fn item_count(&self) -> u16 {
        // let x: [u8; 2] = self.buf.iter().skip(BLOCK_SIZE - 2).take(2).cloned().collect();
        u16::from_le_bytes(self.buf[BLOCK_SIZE - 2..].try_into().unwrap())
    }

    pub fn set_item_count(&mut self, x: u16) {
        let bytes = x.to_le_bytes();
        self.buf[BLOCK_SIZE - 2] = bytes[0];
        self.buf[BLOCK_SIZE - 1] = bytes[1];
    }

    pub fn footer_size(&self) -> usize {
        self.item_count() as usize * 2 + 2
    }

    pub fn footer_start(&self) -> usize {
        let size = self.footer_size();
        BLOCK_SIZE - size
    }

    pub fn offsets(&self) -> Result<&ZeroSlice<u16>> {
        let start = self.footer_start();
        let range = start..BLOCK_SIZE - 2;
        assert!(range.len() % 2 == 0);
        let slice = RawBytesULE::<2>::parse_byte_slice(&self.buf[range])?;
        Ok(ZeroSlice::from_ule_slice(slice))
    }

    pub fn ops(&self) -> Result<impl '_ + Iterator<Item = Result<FetchedOp>>> {
        let offsets = self.offsets()?;
        Ok(offsets.iter().rev().map(|offset| {
            let range = offset as usize..self.footer_start();
            let op: Op = postcard::from_bytes(&self.buf[range])
                .map_err(Error::PostcardError)?;
            Ok(FetchedOp {
                op,
                block_offset: offset,
                file_offset: self.initial_offset + offset as u64,
            })
        }))
    }

    /// Returns next offset
    pub fn put_single(&mut self, op: Op) -> Result<usize> {
        let start = self.next_index;
        let end = self.footer_start() - 2;
        let Some(buf) = &mut self.buf.get_mut(start..end) else {
            return Err(Error::BlockFull)
        };
        let ser = postcard::to_slice(&op, buf)?;
        self.next_index += ser.len();

        let bytes = (start as u16).to_le_bytes();
        self.buf[end] = bytes[0];
        self.buf[end + 1] = bytes[1];

        let count = self.item_count();
        self.set_item_count(count + 1);

        Ok(self.next_index)
    }
    //
    // pub fn put(&mut self, ops: impl Iterator<Item = Op>) -> Result<usize> {
    //     for op in ops {
    //         self.put_single(op)?
    //     }
    // }

    pub fn sync(&mut self) -> Result<usize> {
        let header = BlockHeader::default();
        self.set_header(header);
        let n = self.fd.write_at(&self.buf, self.initial_offset)?;
        Ok(n)
    }
}

#[cfg(test)]
mod test {
    use std::path::PathBuf;

    use assert2::assert;
    use rstest::*;

    use super::*;
    fn dir() -> PathBuf {
        let tmp = std::env::temp_dir();
        let rand: String = (0..10).map(|_| fastrand::alphanumeric()).collect();
        let dir = tmp.join(rand);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn build_block() -> Block {
        let path = dir().join("data.0");
        dbg!(&path);
        let mut block = Block::load_or_create(&path, 0).unwrap();
        for _ in 0..20 {
            block.put_single(Op::Put(b"hello there"[..].into())).unwrap();
            block.put_single(Op::Put(b"what's up?"[..].into())).unwrap();
        }
        block
    }

    #[rstest]
    fn it_records_the_item_count() {
        let block = build_block();
        assert!(block.item_count() == 40);
    }

    #[rstest]
    fn it_records_the_offsets() {
        let block = build_block();
        let offsets = block.offsets().unwrap();
        let offsets = offsets.iter().rev().collect::<Vec<_>>();
        assert!(offsets.len() == 40);
        assert!(offsets[0] == 3);
        assert!(offsets[1] == 16);
        assert!(offsets[39] == (13 * 20 + 12 * 19 + HEADER_SIZE) as u16);
    }

    #[rstest]
    fn it_records_the_ops() {
        let block = build_block();
        let ops = block.ops().unwrap().collect::<Vec<_>>();
        assert!(ops.len() == 40);
        for (i, op) in ops.iter().enumerate() {
            let op = op.as_ref().unwrap();
            if i % 2 == 0 {
                assert!(op.op == Op::Put(b"hello there"[..].into()));
            } else {
                assert!(op.op == Op::Put(b"what's up?"[..].into()));
            }
        }
    }

    #[rstest]
    fn put_errors_when_write_would_overflow_block() {
        let mut block = build_block();
        for _ in 0..117 {
            block
                .put_single(Op::Put(b"let's overflow this sucker"[..].into()))
                .unwrap();
        }
        let res = block.put_single(Op::Put(
            b"this one should actually overflow"[..].into(),
        ));
        assert!(res.unwrap_err() == Error::BlockFull)
    }
}
