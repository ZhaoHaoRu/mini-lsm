use std::fs;
use std::fs::File;
use std::io::{Write};
use std::path::{Path, PathBuf};

use anyhow::{Error, Result};
use log::debug;
use tempfile::NamedTempFile;
use crate::log::LogBlock;

/// A file abstraction for sequential writing
pub struct WritableFile {
    file: File,
    is_active: bool
}

impl WritableFile {
    // XXX: maybe we can have a better method to generate the default file
    pub fn default() -> Self {
        Self {
            file: NamedTempFile::new().unwrap().into_file(),
            is_active: false
        }
    }

    pub fn new(path: &Path) -> Self {
        // assert the parent directory exist
        let parent_dir = path.parent().unwrap();
        if !parent_dir.exists() {
            std::fs::create_dir_all(parent_dir).expect("[WritableFile::new] create dir fail");
        }
        // create a new file and write the data
        let file = File::create(path).expect("[WritableFile::new] create file fail");
        Self {
            file,
            is_active: true
        }
    }

    pub fn append(&mut self, data: &[u8]) -> Result<usize> {
        if !self.is_active {
            return Err(Error::msg("[WritableFile::append] the file is already close"));
        }
        match self.file.write(data) {
            Ok(size) => {
                Ok(size)
            }
            Err(err) => {
                self.is_active = false;
                Err(Error::from(err))
            }
        }
    }

    /// Flush this output stream, ensuring that all intermediately buffered contents reach their destination
    pub fn flush(&mut self) -> Result<()> {
        if !self.is_active {
            return Err(Error::msg("[WritableFile::flush] the file is already close"));
        }
        match self.file.flush() {
            Ok(()) => Ok(()),
            Err(err) => {
                self.is_active = false;
                Err(Error::from(err))
            }
        }
    }

    /// Ensure that all in-memory data reaches the filesystem before returning.
    pub fn sync(&mut self) -> Result<()> {
        if !self.is_active {
            return Err(Error::msg("[WritableFile::sync] the file is already close"));
        }
        self.file.sync_all().expect("[WritableFile::sync] sync all data fail");
        Ok(())
    }
}

/// Write information to log file
pub struct LogBuilder {
    log_file_id: usize,
    dest_file: WritableFile,
    dir_path: PathBuf,
    cur_log_block: LogBlock,
    log_block_max_size: usize,
}

impl LogBuilder {
    /// clear current log block and sync the log file
    // NOTE: make public for test
    pub fn sync_cur_log_file(&mut self) -> Result<()> {
        // replace current log block with a new log block
        let mut old_block = std::mem::replace(&mut self.cur_log_block, LogBlock::new(self.log_block_max_size));
        // add the log block to file and flush
        self.dest_file.append(&old_block.encode())?;
        self.dest_file.sync()
    }

    /// Generate the log file path
    fn generate_file_path(&self) -> PathBuf {
        let log_file_name = self.log_file_id.to_string() + ".log";
        self.dir_path.to_path_buf().join(log_file_name)
    }

    /// Create a log builder
    pub fn new(log_file_id: usize, path: &Path, log_block_max_size: usize) -> Self {
        let mut instance = Self {
            log_file_id,
            dest_file: WritableFile::default(),
            dir_path: path.to_path_buf(),
            cur_log_block: LogBlock::new(log_block_max_size),
            log_block_max_size
        };
        let file_path = instance.generate_file_path();
        instance.dest_file = WritableFile::new(&file_path);
        instance
    }

    /// Replace the dest file with a new file
    pub fn replace_dest(&mut self) -> Result<()> {
        self.log_file_id += 1;
        let file_path = self.generate_file_path();
        // clear current file
        self.sync_cur_log_file()?;
        let _old_file = std::mem::replace(&mut self.dest_file, WritableFile::new(&file_path));
        Ok(())
    }

    /// Add a new record according the kv pair
    pub fn add_record(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        match self.cur_log_block.add(key, value) {
            Ok(is_full) => {
                if is_full {
                   self.sync_cur_log_file()?;
                }
            }
            Err(err) => {
                return Err(err);
            }
        }
        Ok(())
    }

    /// Get current log file id
    pub fn get_cur_log_file_id(&self) -> usize {
        self.log_file_id
    }

    /// Remove the stale log file
    pub fn remove_stale_log_file(path: &Path, file_id: usize) {
        let log_file_path = path.join(file_id.to_string() + ".log");
        if !log_file_path.exists() {
            debug!("[LogBuilder::remove_stale_log_file] the log file not exist");
        }
        fs::remove_file(log_file_path).expect("[LogBuilder::remove_stale_log_file] remove file fail");
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;
    use crate::log::builder::WritableFile;
    use crate::tests::utils;

    #[test]
    fn test_writable_file() {
        let dir = tempdir().unwrap().path().to_path_buf();
        let file_name = dir.to_path_buf().join("writable.txt");
        let mut writable_file_instance = WritableFile::new(&file_name);
        let append_result = writable_file_instance.append(b"aabbcc");
        assert!(append_result.is_ok());
        assert_eq!(append_result.unwrap(), 6);
        let flush_result = writable_file_instance.flush();
        assert!(flush_result.is_ok());
        let sync_result = writable_file_instance.sync();
        assert!(sync_result.is_ok());
    }

    #[test]
    fn test_log_builder() {
        let dir = tempdir().unwrap().path().to_path_buf();
        let mut log_builder_instance = super::LogBuilder::new(0, &dir, 1024);
        for i in 1..=100 {
            let key = utils::key_of(i);
            let value = utils::value_of(i);
            let add_record_result = log_builder_instance.add_record(&key, &value);
            assert!(add_record_result.is_ok());
        }
        let replace_dest_result = log_builder_instance.replace_dest();
        assert!(replace_dest_result.is_ok());
    }
}