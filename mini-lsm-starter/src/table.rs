#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

mod builder;
mod iterator;

use anyhow::Error;
use std::fs::File;
use std::io::{Cursor, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
pub use builder::SsTableBuilder;
use bytes::{Buf, BufMut, Bytes};
pub use iterator::SsTableIterator;
use log::{log, Level};

use crate::block::filter::Filter;
use crate::block::Block;
use crate::lsm_storage::BlockCache;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockMeta {
    /// Offset of this data block.
    pub offset: usize,
    /// The first key of the data block, mainly used for index purpose.
    pub first_key: Bytes,
}

impl BlockMeta {
    /// Encode block meta to a buffer.
    /// You may add extra fields to the buffer,
    /// in order to help keep track of `first_key` when decoding from the same buffer in the future.
    pub fn encode_block_meta(
        block_meta: &[BlockMeta],
        #[allow(clippy::ptr_arg)] // remove this allow after you finish
        buf: &mut Vec<u8>,
    ) {
        for elem in block_meta {
            let offset_slice: [u8; 8] = (elem.offset).to_be_bytes();
            buf.put_slice(&offset_slice);
            let key_len_slice: [u8; 8] = (elem.first_key.len()).to_be_bytes();
            buf.put_slice(&key_len_slice);
            buf.put_slice(&elem.first_key);
        }
    }

    /// Decode block meta from a buffer.
    pub fn decode_block_meta(buf: impl Buf) -> Vec<BlockMeta> {
        let mut result = Vec::new();
        let mut binding = Cursor::new(buf);
        let cursor = binding.get_mut();

        while cursor.remaining() > 0 {
            // read the offset
            let elem_offset = cursor.get_u64() as usize;
            // read the key length
            let elem_key_len = cursor.get_u64() as usize;
            let mut elem_key = vec![0; elem_key_len];
            cursor.copy_to_slice(&mut elem_key);
            result.push(BlockMeta {
                offset: elem_offset,
                first_key: Bytes::from(elem_key),
            })
        }
        result
    }
}

pub struct FilterIndex {}

impl FilterIndex {
    pub fn encode_filter_index(data: &Vec<u32>, buf: &mut Vec<u8>) {
        buf.reserve(buf.len() + data.len() * 4);
        for elem in data {
            let elem_slice: [u8; 4] = elem.to_be_bytes();
            buf.put_slice(&elem_slice);
        }
    }

    pub fn decode_filter_index(buf: Vec<u8>) -> Vec<u32> {
        let buf_len = buf.len();
        let mut index = 0;
        let mut result = Vec::new();
        result.reserve(buf_len / 4);
        while index + 4 <= buf_len {
            result.push(
                (u32::from(buf[index]) << 24)
                    | (u32::from(buf[index + 1]) << 16)
                    | (u32::from(buf[index + 2]) << 8)
                    | (u32::from(buf[index + 3])),
            );
            index += 4;
        }
        result
    }
}

/// A file object.
pub struct FileObject(Bytes);

impl FileObject {
    pub fn read(&self, offset: u64, len: u64) -> Result<Vec<u8>> {
        Ok(self.0[offset as usize..(offset + len) as usize].to_vec())
    }

    pub fn size(&self) -> u64 {
        self.0.len() as u64
    }

    /// Create a new file object (day 2) and write the file to the disk (day 4).
    pub fn create(path: &Path, data: Vec<u8>) -> Result<Self> {
        // assert the parent directory exists
        let parent_dir = path.parent().unwrap();
        if !parent_dir.exists() {
            std::fs::create_dir_all(parent_dir).expect("[FileObject::create] create dir fail");
        }
        // create a new file and write the data
        let mut file = File::create(path).expect("[FileObject::create] create file fail");
        file.write_all(&data[..])
            .expect("[FileObject::create] write file fail");
        file.flush().expect("[FileObject::create] flush file fail");
        Ok(Self(Bytes::from(data)))
    }

    pub fn open(path: &Path) -> Result<Self> {
        let mut file = File::open(path).unwrap_or_else(|_| {
            panic!(
                "{}",
                &format!(
                    "[FileObject::open] open file with path {} fail",
                    path.to_str().unwrap()
                )
            )
        });
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)?;
        Ok(Self(Bytes::from(buffer)))
    }
}

/// ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
/// |              Data Block              |              Filter Block              |             Meta Block              |                    Filter index Block             |                          Extra                       |                  |
/// ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
/// | Data Block #1 | ... | Data Block #N | Filter Block #1 | ... |Filter Block #N | Meta Block #1 | ... | Meta Block #N |Filter index Block #1 | ... | Filter index Block #N |  Meta Block Offset (u32) | Filter index Block (u32) | Block Size (u32)  |
/// ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
pub struct SsTable {
    /// The actual storage unit of SsTable, the format is as above.
    file: FileObject,
    /// The meta blocks that hold info for data blocks.
    block_metas: Vec<BlockMeta>,
    /// The bloom filters
    filters: Vec<Filter>,
    /// The offset that indicates the start point of meta blocks in `file`.
    block_meta_offset: u32,
    /// The offset that indicates the start point of the filter index blocks in `file`
    filter_index_offset: u32,
    /// The offset that indicates the start point of the filter block, also the end point of the data blocks
    filter_block_offset: u32,
    /// The block cache
    block_cache: Option<Arc<BlockCache>>,
    /// The sst id
    sst_id: usize,
}

impl SsTable {
    #[cfg(test)]
    pub(crate) fn open_for_test(file: FileObject) -> Result<Self> {
        Self::open(0, None, file)
    }

    /// Open SSTable from a file.
    pub fn open(id: usize, block_cache: Option<Arc<BlockCache>>, file: FileObject) -> Result<Self> {
        // get the meta block offset and filter offset
        let file_size = file.0.len();
        let file_ref = &file.0;
        let mut cursor = Cursor::new(file_ref);
        cursor
            .seek(SeekFrom::Start((file_size - 12) as u64))
            .expect("[SsTable::open] the file is too small");
        let meta_offset = cursor.get_u32();
        let filter_index_offset = cursor.get_u32();
        let block_size = cursor.get_u32();

        // get the block meta and decode
        cursor
            .seek(SeekFrom::Start(meta_offset as u64))
            .expect("[SsTable::open] fail to seek meta block, the file is too small");
        // let block_meta_cursor = cursor.by_ref().take(file_size - 8 - meta_offset as usize);
        let mut block_meta_buffer: Vec<u8> = Vec::new();
        block_meta_buffer.resize(filter_index_offset as usize - meta_offset as usize, 0);
        cursor
            .read_exact(&mut block_meta_buffer)
            .expect("[SsTable::open] fail to read the block meta from the file");
        let block_metas = BlockMeta::decode_block_meta(Bytes::from(block_meta_buffer));

        // get the filter index and decode
        cursor
            .seek(SeekFrom::Start(filter_index_offset as u64))
            .expect("[SsTable::open] fail to seek the filter index block");
        let mut filter_index_buffer: Vec<u8> = Vec::new();
        filter_index_buffer.resize(file_size - 12 - filter_index_offset as usize, 0);
        cursor
            .read_exact(&mut filter_index_buffer)
            .expect("[SsTable::open] fail to read the filter index block from file");
        let mut filter_index_blocks = FilterIndex::decode_filter_index(filter_index_buffer);

        // decode the filter according to the filter index blocks
        let mut filters = Vec::new();
        let mut filter_block_offset = meta_offset;
        filters.reserve(filter_index_blocks.len());
        filter_index_blocks.push((file_size - 12) as u32);

        for (id, offset) in filter_index_blocks.iter().enumerate() {
            if id == 0 {
                filter_block_offset = *offset;
                continue;
            }
            let mut filter_block_buffer: Vec<u8> = Vec::new();
            let prev_offset = filter_index_blocks[id - 1];
            filter_block_buffer.reserve((offset - filter_index_blocks[id - 1]) as usize);
            cursor
                .seek(SeekFrom::Start(prev_offset as u64))
                .expect("[SsTable::open] fail to seek the filter block from the file");
            cursor
                .read_exact(&mut filter_block_buffer)
                .expect("[SsTable::open] fail to read the filter index block from the file");
            filters.push(Filter::decode(
                &filter_block_buffer,
                (block_size / 32) as usize,
            ));
        }

        Ok(Self {
            file,
            block_metas,
            filters,
            block_meta_offset: meta_offset,
            filter_index_offset,
            filter_block_offset,
            block_cache,
            sst_id: id,
        })
    }

    /// Read a block from the disk.
    pub fn read_block(&self, block_idx: usize) -> Result<Arc<Block>> {
        // check whether the block_idx is valid
        if block_idx >= self.block_metas.len() {
            return Err(Error::msg(
                "[SsTable::read_block] the block index is out of range",
            ));
        }

        let block_offset = self.block_metas[block_idx].offset;
        let block_first_key = &self.block_metas[block_idx].first_key;

        let mut block_size =
            (self.filter_block_offset as usize - self.block_metas[block_idx].offset) as u64;
        if block_idx < self.block_metas.len() - 1 {
            block_size = (self.block_metas[block_idx + 1].offset
                - self.block_metas[block_idx].offset) as u64;
        }
        let block_raw_data = self
            .file
            .read(block_offset as u64, block_size)
            .expect("[SsTable::read_block] unable to read block from SSTable file");
        let block = Block::decode(&block_raw_data[..]);
        Ok(Arc::new(block))
    }

    /// Read a block from disk, with block cache. (Day 4)
    pub fn read_block_cached(&self, block_idx: usize) -> Result<Arc<Block>> {
        if let Some(cache) = self.block_cache.as_ref() {
            let wanna_block =
                cache.try_get_with((self.sst_id, block_idx), || self.read_block(block_idx));
            if let Ok(block) = wanna_block {
                return Ok(block);
            }
        }

        log!(
            Level::Info,
            "[SsTable::read_block_cached] the block cache is disabled or populate the cache fail"
        );
        self.read_block(block_idx)
    }

    /// Find the block that may contain `key`.
    /// Note: You may want to make use of the `first_key` stored in `BlockMeta`.
    /// You may also assume the key-value pairs stored in each consecutive block are sorted.
    pub fn find_block_idx(&self, key: &[u8]) -> usize {
        // find the
        let mut left: usize = 0;
        let mut right: usize = self.block_metas.len() - 1;
        let mut result: usize = 0;
        while left < right {
            let mid = left + (right - left) / 2;
            if self.block_metas[mid].first_key >= key {
                result = mid;
                right = mid;
            } else {
                left = mid + 1;
            }
        }
        result
    }

    /// Get number of data blocks.
    pub fn num_of_blocks(&self) -> usize {
        self.block_metas.len()
    }

    pub fn min_key(&self) -> Vec<u8> {
        if !self.block_metas.is_empty() {
            return Vec::from(self.block_metas[0].first_key.clone());
        }
        vec![]
    }

    pub fn get_sst_id(&self) -> usize {
        self.sst_id
    }
}

#[cfg(test)]
mod tests;
