use std::mem;
use std::path::Path;
use std::sync::Arc;

use crate::block::{Block, BlockBuilder};
use anyhow::Result;
use bytes::{BufMut, Bytes};

use super::{BlockMeta, SsTable};
use crate::lsm_storage::BlockCache;
use crate::table::FileObject;

/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
    pub(super) meta: Vec<BlockMeta>,
    // Add other fields you need.
    block_builder: BlockBuilder,
    block_section: Vec<Block>,
    data_block_offset: usize,
    block_size: usize,
}

impl SsTableBuilder {
    /// Create a builder based on target block size.
    pub fn new(block_size: usize) -> Self {
        Self {
            data_block_offset: 0,
            meta: vec![],
            block_builder: BlockBuilder::new(block_size),
            block_section: vec![],
            block_size,
        }
    }

    /// Serialize the current block
    fn serialize_block(&mut self, prev_block_builder: BlockBuilder) {
        let block_size = prev_block_builder.get_size();
        self.block_section.push(prev_block_builder.build());
        let block_count = self.block_section.len();
        self.data_block_offset += block_size + 2;
    }

    /// Adds a key-value pair to SSTable.
    /// Note: You should split a new block when the current block is full.(`std::mem::replace` may be of help here)
    pub fn add(&mut self, key: &[u8], value: &[u8]) {
        // if the block is empty, generate the block meta first
        if self.block_builder.get_size() == 0 {
            self.meta.push(BlockMeta {
                offset: self.data_block_offset,
                first_key: Bytes::copy_from_slice(key),
            })
        }

        if !self.block_builder.add(key, value) {
            // replace with a new block to process incoming kv
            let prev_block_builder =
                mem::replace(&mut self.block_builder, BlockBuilder::new(self.block_size));
            // serialize the old block
            self.serialize_block(prev_block_builder);
            self.add(key, value);
        }
    }

    /// Get the estimated size of the SSTable.
    /// Since the data blocks contain much more data than meta blocks, just return the size of data blocks here.
    pub fn estimated_size(&self) -> usize {
        // XXX: whether need to include the block in building?
        // current solution: include
        self.data_block_offset + self.block_builder.get_size()
    }

    /// Builds the SSTable and writes it to the given path. No need to actually write to disk until
    /// chapter 4 block cache.
    pub fn build(
        self,
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
    ) -> Result<SsTable> {
        // generate the file data to write to disk
        let mut data: Vec<u8> = Vec::new();
        // data section
        data.reserve(self.data_block_offset);
        for block in &self.block_section {
            data.extend_from_slice(&block.encode());
        }
        // if the last data block is not serialized, not forget to handle it
        if self.block_builder.get_size() > 0 {
            data.extend_from_slice(&self.block_builder.build().encode());
        }

        let meta_offset = data.len();
        // meta block
        BlockMeta::encode_block_meta(&self.meta, &mut data);
        // the offset of meta block
        let offset_slices: [u8; 8] = meta_offset.to_be_bytes();
        data.put_slice(&offset_slices);

        // make the data persistent
        let file = FileObject::create(path.as_ref(), data)?;

        // generate the SsTable
        Ok(SsTable {
            file,
            block_metas: self.meta,
            block_meta_offset: meta_offset,
            block_cache,
            sst_id: id,
        })
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }
}
