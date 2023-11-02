use std::path::{Path, PathBuf};

use anyhow::{Error, Result};
use crate::log::iterator::LogIterator;

pub struct LogReader {
    cur_log_file_id: usize,
    dir_path: PathBuf,
}

impl LogReader {
    /// Create a new log reader
    pub fn new(log_file_id: usize, path: &Path) -> Self {
        Self {
            cur_log_file_id: log_file_id,
            dir_path: path.to_path_buf()
        }
    }

    /// Read all records from a file
    pub fn read_records(&mut self) -> Result<LogIterator> {
        // NOTE: assume all log files id are consequent
        let file_path = self.dir_path.to_path_buf().join(self.cur_log_file_id.to_string() + ".log");
        match LogIterator::create(&file_path) {
            Ok(iter) => {
                self.cur_log_file_id += 1;
                Ok(iter)
            }
            Err(_) => {
                return Err(Error::msg("[LogReader::read_records] finished"));
            }
        }
    }
}