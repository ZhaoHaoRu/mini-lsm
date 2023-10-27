#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use anyhow::Result;
use std::fs::File;
use std::io::Read;
use std::path::Path;
use std::sync::Arc;
use log::warn;
use crate::iterators::StorageIterator;
use crate::table::FileObject;
use super::{LogBlock, LogRecord, RecordData};

// /// An iterator over a log block
// pub struct LogBlockIterator {
//     block: Arc<LogBlock>,
//     idx: usize,
//     key: Vec<u8>,
//     value: Vec<u8>
// }
//
// impl LogBlockIterator {
//     /// seek to the key specified by the index
//     fn seek_to_key(&mut self)  {
//         if self.block.records.len() <= self.idx {
//             return
//         }
//         if let Ok((key, value)) = RecordData::decode(&self.block.records[self.idx].data) {
//             self.key = key;
//             self.value = value
//         } else {
//             warn!("[LogBlockIterator::seek_to_first] unable to decode log block data");
//         }
//     }
//
//     /// create an iterator for target block
//     pub fn create(block: Arc<LogBlock>) -> Self {
//         let mut instance = LogBlockIterator {
//             block: block.clone(),
//             idx: 0,
//             key: vec![],
//             value: vec![],
//         };
//         instance.seek_to_key();
//         instance
//     }
// }
//
// impl StorageIterator for LogBlockIterator {
//     fn value(&self) -> &[u8] {
//         &self.value[..]
//     }
//
//     fn key(&self) -> &[u8] {
//         &self.key[..]
//     }
//
//     fn is_valid(&self) -> bool {
//         !self.key.is_empty()
//     }
//
//     fn next(&mut self) -> anyhow::Result<()> {
//         self.idx += 1;
//         self.seek_to_key();
//         Ok(())
//     }
// }

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
        file.read_to_end(&mut buffer)?;
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
