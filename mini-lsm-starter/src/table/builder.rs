use std::mem;
use std::path::Path;
use std::sync::Arc;

use crate::block::filter::Filter;
use crate::block::{Block, BlockBuilder};
use anyhow::Result;
use bytes::{BufMut, Bytes};

use super::{BlockMeta, FilterIndex, SsTable};
use crate::lsm_storage::BlockCache;
use crate::table::FileObject;

/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
    pub(super) meta: Vec<BlockMeta>,
    // Add other fields you need.
    block_builder: BlockBuilder,
    block_section: Vec<Block>,
    filter_section: Vec<Filter>,
    data_block_offset: usize,
    filter_block_offset: usize,
    block_size: usize,
}

impl SsTableBuilder {
    /// Create a builder based on target block size.
    pub fn new(block_size: usize) -> Self {
        Self {
            data_block_offset: 0,
            filter_block_offset: 0,
            meta: vec![],
            block_builder: BlockBuilder::new(block_size),
            block_section: vec![],
            filter_section: vec![],
            block_size,
        }
    }

    /// Serialize the current block
    fn serialize_block(&mut self, prev_block_builder: BlockBuilder) {
        let block_size = prev_block_builder.get_size();
        let (block, filter) = prev_block_builder.build();
        self.block_section.push(block);
        self.filter_section.push(filter);
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
        let mut last_filter: Option<Filter> = None;
        // data section
        data.reserve(self.data_block_offset);
        for block in &self.block_section {
            data.extend_from_slice(&block.encode());
        }
        // if the last data block is not serialized, not forget to handle it
        if self.block_builder.get_size() > 0 {
            let (block, filter) = self.block_builder.build();
            data.extend_from_slice(&block.encode());
            last_filter = Some(filter)
        }

        // filter section
        let mut filer_index_block: Vec<u32> = Vec::new();
        let filter_block_offset = data.len() as u32;
        filer_index_block.reserve(self.filter_section.len());
        let mut new_filters = self.filter_section;
        for filter in &new_filters {
            filer_index_block.push(data.len() as u32);
            data.extend_from_slice(&filter.encode());
        }
        if let Some(filter) = last_filter {
            filer_index_block.push(data.len() as u32);
            data.extend_from_slice(&filter.encode());
            new_filters.push(filter);
        }

        let meta_offset = data.len() as u32;
        // meta block
        BlockMeta::encode_block_meta(&self.meta, &mut data);

        let filter_offset = data.len() as u32;
        // filter index block
        FilterIndex::encode_filter_index(&filer_index_block, &mut data);

        // put the offset of meta block
        let offset_slices: [u8; 4] = meta_offset.to_be_bytes();
        data.put_slice(&offset_slices);

        // put the offset of filter index block
        let filter_offset_slices: [u8; 4] = filter_offset.to_be_bytes();
        data.put_slice(&filter_offset_slices);

        // put the block size
        let block_size_slice: [u8; 4] = (self.block_size as u32).to_be_bytes();
        data.put_slice(&block_size_slice);

        // make the data persistent
        let file = FileObject::create(path.as_ref(), data)?;

        // generate the SsTable
        Ok(SsTable {
            file,
            block_metas: self.meta,
            block_meta_offset: meta_offset,
            filter_index_offset: filter_offset,
            filter_block_offset,
            block_cache,
            sst_id: id,
            filters: new_filters,
        })
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }
}
