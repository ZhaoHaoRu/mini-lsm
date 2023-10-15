#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

mod builder;
mod iterator;

pub use builder::BlockBuilder;
/// You may want to check `bytes::BufMut` out when manipulating continuous chunks of memory
use bytes::{Buf, BufMut, Bytes, BytesMut};
pub use iterator::BlockIterator;
use std::io::{Cursor, Seek, SeekFrom};

/// A block is the smallest unit of read and caching in LSM tree.
/// It is a collection of sorted key-value pairs.
/// The `actual` storage format is as below (After `Block::encode`):
///
/// ----------------------------------------------------------------------------------------------------
/// |             Data Section             |              Offset Section             |      Extra      |
/// ----------------------------------------------------------------------------------------------------
/// | Entry #1 | Entry #2 | ... | Entry #N | Offset #1 | Offset #2 | ... | Offset #N | num_of_elements |
/// ----------------------------------------------------------------------------------------------------
pub struct Block {
    data: Vec<u8>,
    offsets: Vec<u16>,
}

impl Default for Block {
    fn default() -> Self {
        Block::new()
    }
}

impl Block {
    pub fn new() -> Self {
        Self {
            data: vec![],
            offsets: vec![],
        }
    }
    /// Encode the internal data to the data layout illustrated in the tutorial
    /// Note: You may want to recheck if any of the expected field is missing from your output
    pub fn encode(&self) -> Bytes {
        let mut result = BytesMut::new();
        for elem in &self.data {
            result.put_u8(*elem)
        }
        for offset in &self.offsets {
            result.put_u16(*offset)
        }
        let num_of_elements: u16 = self.offsets.len() as u16;
        result.put_u16(num_of_elements);
        result.freeze()
    }

    /// Decode from the data layout, transform the input `data` to a single `Block`
    pub fn decode(data: &[u8]) -> Self {
        let size = data.len();
        if size < 2 {
            return Self {
                data: Vec::new(),
                offsets: Vec::new(),
            };
        }

        // read the number of elements
        let mut cursor = Cursor::new(data);
        cursor
            .seek(SeekFrom::Start((size - 2) as u64))
            .expect("[Block::decode] unable to seek to the expected position");
        let num_of_elements = cursor.get_u16() as usize;
        if size < num_of_elements * 2 + 2 {
            return Self {
                data: Vec::new(),
                offsets: Vec::new(),
            };
        }

        // read the offsets
        let mut begin_pos = size - num_of_elements * 2 - 2;
        let mut offsets: Vec<u16> = Vec::new();
        while begin_pos != size - 2 {
            cursor.seek(SeekFrom::Start(begin_pos as u64)).expect(
                "[Block::decode] unable to seek to the expected position when parse the offset",
            );
            let offset = cursor.get_u16();
            offsets.push(offset);
            begin_pos += 2;
        }

        // read the data
        let end_pos = size - num_of_elements * 2 - 2;
        let begin_pos = offsets[0] as usize;

        // check the range
        if begin_pos > end_pos {
            return Self {
                data: Vec::new(),
                offsets: Vec::new(),
            };
        }

        let data = data[begin_pos..end_pos].to_vec();
        Self { data, offsets }
    }
}

#[cfg(test)]
mod tests;
