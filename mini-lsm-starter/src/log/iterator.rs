#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use anyhow::Result;
use std::fs::File;
use std::io::Read;
use std::path::Path;
use std::sync::Arc;
use crate::iterators::StorageIterator;
use super::{LogBlock, LogRecord};

/// An iterator over the log file
pub struct LogIterator {
    logs: Vec<Arc<LogRecord>>,
    idx: usize,
}

impl LogIterator {
    pub fn create(path: &Path) -> Self {
        // open the file and write the content
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
        file.read_to_end(&mut buffer).expect("[LogIterator::create] read file fail");
        let logs = LogBlock::decode(&buffer[..]);
        Self{
            logs,
            idx: 0
        }
    }
}

impl StorageIterator for LogIterator {
    fn value(&self) -> &[u8] {
        if self.idx >= self.logs.len() {
            return  &[];
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

