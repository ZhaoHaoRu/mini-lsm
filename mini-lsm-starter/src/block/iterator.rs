#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::sync::Arc;

use super::Block;

/// Iterates on a block.
pub struct BlockIterator {
    /// The internal `Block`, wrapped by an `Arc`
    block: Arc<Block>,
    /// The current key, empty represents the iterator is invalid
    key: Vec<u8>,
    /// The corresponding value, can be empty
    value: Vec<u8>,
    /// Current index of the key-value pair, should be in range of [0, num_of_elements)
    idx: usize,
}

impl BlockIterator {
    fn new(block: Arc<Block>) -> Self {
        Self {
            block,
            key: Vec::new(),
            value: Vec::new(),
            idx: 0,
        }
    }

    /// Creates a block iterator and seek to the first entry.
    pub fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        let mut iterator = Self {
            block,
            key: Vec::new(),
            value: Vec::new(),
            idx: 0,
        };
        iterator.seek_to_first();
        iterator
    }

    /// Creates a block iterator and seek to the first key that >= `key`.
    pub fn create_and_seek_to_key(block: Arc<Block>, key: &[u8]) -> Self {
        let mut iterator = Self {
            block,
            key: Vec::new(),
            value: Vec::new(),
            idx: 0,
        };
        iterator.seek_to_key(key);
        iterator
    }

    /// Returns the key of the current entry.
    pub fn key(&self) -> &[u8] {
        &self.key[..]
    }

    /// Returns the value of the current entry.
    pub fn value(&self) -> &[u8] {
        &self.value[..]
    }

    /// Returns true if the iterator is valid.
    /// Note: You may want to make use of `key`
    pub fn is_valid(&self) -> bool {
        self.key.len() > 0
    }

    fn read_elem(&self, offset: usize) -> Vec<u8> {
        let data_len = self.block.data.len();
        if data_len - offset >= 2 {
            let elem_len = (((self.block.data[offset] as u16) << 8) | self.block.data[offset + 1] as u16) as usize;
            if data_len - 2 >= elem_len {
                let elem_slice = &self.block.data[offset + 2..offset + 2 + elem_len];
                let result = Vec::from(elem_slice);
                return result;
            }
        }
        Vec::new()
    }

    fn seek_to_target_index(&mut self) {
        if self.block.offsets.len() < self.idx {
            self.key = Vec::new();
            return;
        }
        let mut offset = self.block.offsets[self.idx - 1] as usize;
        let data_len = self.block.data.len();
        self.key.clear();
        self.value.clear();

        // read key
        if data_len - offset >= 2 {
            let key_len   = (((self.block.data[offset] as u16) << 8) | self.block.data[offset + 1] as u16) as usize;
            offset += 2;
            if data_len - offset >= key_len {
                let key_slice = &self.block.data[offset..offset + key_len];
                self.key.extend_from_slice(key_slice);
                offset += key_len;

                // read value
                if data_len - offset >= 2 {
                    let value_len = (((self.block.data[offset] as u16) << 8) | self.block.data[offset + 1] as u16) as usize;
                    offset += 2;
                    if data_len - offset >= value_len {
                        let value_slice = &self.block.data[offset..offset+value_len];
                        self.value.extend_from_slice(value_slice);
                        return
                    }
                }
            }
        }

        // seek fail, empty the key
        self.key = Vec::new()
    }

    /// Seeks to the first key in the block.
    pub fn seek_to_first(&mut self) {
        self.idx = 1;
        self.seek_to_target_index()
    }

    /// Move to the next key in the block.
    pub fn next(&mut self) {
        self.idx += 1;
        self.seek_to_target_index()
    }

    /// Seek to the first key that >= `key`.
    /// Note: You should assume the key-value pairs in the block are sorted when being added by callers.
    pub fn seek_to_key(&mut self, key: &[u8]) {
        let mut left : usize  = 0;
        let mut right : usize = self.block.offsets.len();
        let mut result : usize = 0;

        while left < right {
            let mid = left + (right - left) / 2;
            let mid_key = self.read_elem(self.block.offsets[mid] as usize);
            if &mid_key[..] >= key {
                result = mid;
                right = mid;
            } else {
                left = mid + 1;
            }
        }

        // update the target key and value
        self.key = self.read_elem(self.block.offsets[result] as usize);
        self.idx = result + 1;
        self.value = self.read_elem(self.block.offsets[result] as usize + 2 + self.key.len());
    }
}
