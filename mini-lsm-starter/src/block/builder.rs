use super::Block;
use bytes::BufMut;

/// Builds a block.
pub struct BlockBuilder {
    block: Block,
    target_size: usize,
    cur_size: usize,
    last_key: Vec<u8>,
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        // the size that can use is the block_size
        let target_size = block_size;
        Self {
            block: Block {
                data: Vec::new(),
                offsets: Vec::new(),
            },
            target_size,
            cur_size: 0,
            last_key: Vec::new(),
        }
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    #[must_use]
    pub fn add(&mut self, key: &[u8], value: &[u8]) -> bool {
        let needed: usize = 4 + key.len() + value.len();

        if self.cur_size + needed + 2 >= self.target_size {
            return false;
        }

        // check whether the key is bigger than previous
        if self.cur_size != 0 && self.last_key[..] > *key {
            return false;
        }

        // update the block
        // -----------------------------------------------------------------------
        // |                           Entry #1                            | ... |
        // -----------------------------------------------------------------------
        // | key_len (2B) | key (keylen) | value_len (2B) | value (varlen) | ... |
        // -----------------------------------------------------------------------
        self.block.offsets.push(self.cur_size as u16);
        let key_len: [u8; 2] = (key.len() as u16).to_be_bytes();
        self.block.data.put_slice(&key_len);
        self.block.data.put_slice(key);
        let value_len: [u8; 2] = (value.len() as u16).to_be_bytes();
        self.block.data.put_slice(&value_len);
        self.block.data.put_slice(value);
        self.cur_size += needed;

        true
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.cur_size == 0
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        self.block
    }

    pub fn get_size(&self) -> usize {
        self.cur_size + self.block.offsets.len() * 2
    }
}
