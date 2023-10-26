#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::sync::Arc;

use crate::block::{Block, BlockIterator};
use anyhow::{Error, Result};
use bytes::Bytes;
use log::warn;

use super::SsTable;
use crate::iterators::StorageIterator;

/// An iterator over the contents of an SSTable.
pub struct SsTableIterator {
    table: Arc<SsTable>,
    /// The current block
    block_iterator: BlockIterator,
    /// The block index traverse currently
    idx: usize,
}

impl SsTableIterator {
    /// Create a new iterator and seek to the first key-value pair in the first data block.
    pub fn create_and_seek_to_first(table: Arc<SsTable>) -> Result<Self> {
        let mut iterator = Self {
            table,
            block_iterator: BlockIterator::new(Arc::new(Block::new())),
            idx: 0,
        };
        match iterator.seek_to_first() {
            Ok(()) => Ok(iterator),
            Err(err) => Err(err),
        }
    }

    /// Seek to the first key-value pair in the first data block.
    pub fn seek_to_first(&mut self) -> Result<()> {
        if self.table.block_metas.is_empty() {
            return Err(Error::msg(
                "[SsTableIterator::seek_to_first] the SsTable size is too small",
            ));
        }

        self.idx = 0;
        // find the wanted data block
        let source_block = self
            .table
            .read_block_cached(self.idx)
            .expect("[SsTableIterator::seek_to_first] read block fail");
        self.block_iterator = BlockIterator::create_and_seek_to_first(source_block);
        Ok(())
    }

    /// Create a new iterator and seek to the first key-value pair which >= `key`.
    pub fn create_and_seek_to_key(table: Arc<SsTable>, key: &[u8]) -> Result<Self> {
        let mut iterator = Self {
            table,
            block_iterator: BlockIterator::new(Arc::new(Block::new())),
            idx: 0,
        };

        match iterator.seek_to_key(key, false) {
            Ok(()) => Ok(iterator),
            Err(err) => Err(err),
        }
    }

    /// Create a new iterator and seek for a particular key that equal to `key`
    pub fn create_and_find_target_key(table: Arc<SsTable>, key: &[u8]) -> Result<Self> {
        let mut iterator = Self {
            table,
            block_iterator: BlockIterator::new(Arc::new(Block::new())),
            idx: 0,
        };

        match iterator.seek_to_key(key, true) {
            Ok(()) => Ok(iterator),
            Err(err) => Err(err),
        }
    }

    /// Seek to the first key-value pair equal to `key`.

    /// Find the block that probably has the target key
    fn locate_block_by_key(&mut self, key: &Bytes) -> usize {
        match self
            .table
            .block_metas
            .binary_search_by(|meta| meta.first_key.cmp(key))
        {
            Ok(index) => index,
            Err(to_insert_index) => {
                if to_insert_index == 0 {
                    0
                } else {
                    to_insert_index - 1
                }
            }
        }
    }

    /// Seek to the first key-value pair which >= `key` or equal to `key`
    /// Note: You probably want to review the handout for detailed explanation when implementing this function.
    pub fn seek_to_key(&mut self, key: &[u8], is_precise: bool) -> Result<()> {
        let block_index = self.locate_block_by_key(&Bytes::copy_from_slice(key));
        if block_index >= self.table.block_metas.len() {
            self.block_iterator = BlockIterator::new(Arc::new(Block::new()));
            warn!("[SsTableIterator::seek_to_key] no available block, the key is too big");
            return Ok(());
        }

        let candidate_block = self
            .table
            .read_block_cached(block_index)
            .expect("[SsTableIterator::seek_to_key] read block fail");

        // use filter block to check
        if !self.table.filters[block_index].is_exist(key) {
            // the target is to find the particular key, and the key is not exist
            if is_precise {
                self.block_iterator = BlockIterator::new(Arc::new(Block::new()));
                return Ok(());
            }
        }

        self.block_iterator = BlockIterator::create_and_seek_to_key(candidate_block, key);
        // make sure there is actually some key bigger than the target key
        if self.block_iterator.is_valid() && self.block_iterator.key() >= key {
            self.idx = block_index;
            return Ok(());
        }

        // switch to the next block
        if block_index == self.table.block_metas.len() - 1 {
            warn!("[SsTableIterator::seek_to_key] no available key bigger than or equal to the target key");
            return Ok(());
        }
        let candidate_block = self
            .table
            .read_block_cached(block_index + 1)
            .expect("[SsTableIterator::seek_to_key] read block fail");
        self.block_iterator = BlockIterator::create_and_seek_to_first(candidate_block);
        if self.block_iterator.is_valid() {
            self.idx = block_index + 1;
            return Ok(());
        }

        Err(Error::msg(
            "[SsTableIterator::seek_to_key] unable to seek to target key",
        ))
    }
}

impl StorageIterator for SsTableIterator {
    /// Return the `value` that's held by the underlying block iterator.
    fn value(&self) -> &[u8] {
        self.block_iterator.value()
    }

    /// Return the `key` that's held by the underlying block iterator.
    fn key(&self) -> &[u8] {
        self.block_iterator.key()
    }

    /// Return whether the current block iterator is valid or not.
    fn is_valid(&self) -> bool {
        self.block_iterator.is_valid()
    }

    /// Move to the next `key` in the block.
    /// Note: You may want to check if the current block iterator is valid after the move.
    fn next(&mut self) -> Result<()> {
        self.block_iterator.next();
        if !self.block_iterator.is_valid() {
            self.idx += 1;
            if self.idx > self.table.block_metas.len() {
                return Ok(());
            }
            if self.idx == self.table.block_metas.len() {
                self.block_iterator = BlockIterator::new(Arc::new(Block::new()));
                return Ok(());
            }
            let candidate_block = self
                .table
                .read_block_cached(self.idx)
                .expect("[SsTableIterator::next] read block fail");
            self.block_iterator = BlockIterator::create_and_seek_to_first(candidate_block);
        }
        Ok(())
    }
}
