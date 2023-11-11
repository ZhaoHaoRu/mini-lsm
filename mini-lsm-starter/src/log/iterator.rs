#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use super::{LogBlock, LogRecord};
use crate::iterators::StorageIterator;
use anyhow::Result;
use std::fs::File;
use std::io::Read;
use std::path::Path;
use std::sync::Arc;

/// An iterator over the log file
pub struct LogIterator {
    logs: Vec<Arc<LogRecord>>,
    idx: usize,
}

impl LogIterator {
    pub fn create(path: &Path) -> Result<Self> {
        // open the file and write the content
        let mut file = File::open(path)?;
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)
            .expect("[LogIterator::create] read file fail");
        let logs = LogBlock::decode(&buffer[..]);
        Ok(Self { logs, idx: 0 })
    }
}

impl StorageIterator for LogIterator {
    fn value(&self) -> &[u8] {
        if self.idx >= self.logs.len() {
            return &[];
        }

        &self.logs[self.idx].value
    }

    fn key(&self) -> &[u8] {
        if self.idx >= self.logs.len() {
            return &[];
        }
        &self.logs[self.idx].key
    }

    fn is_valid(&self) -> bool {
        self.idx < self.logs.len()
    }

    fn next(&mut self) -> Result<()> {
        self.idx += 1;
        Ok(())
    }
}
