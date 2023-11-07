mod iterator;
pub(crate) mod builder;
mod reader;
mod tests;

pub use iterator::LogIterator;
pub use builder::LogBuilder;
pub use reader::LogReader;

use std::mem;
use std::sync::Arc;
use anyhow::{Result, Error};
use bytes::BufMut;

/**
 * the log record format:
 * -----------------------------------------------------------
 * ｜ record length (u16) ｜ key length (u16) ｜ key ｜ value ｜
 * -----------------------------------------------------------
 */
#[derive(Clone)]
pub struct LogRecord {
    pub key: Vec<u8>,
    pub value: Vec<u8>
}

impl LogRecord {
    pub fn new(key: Vec<u8>, value: Vec<u8>) -> Self {
        Self {
            key,
            value
        }
    }

    /// encode log record to raw data
    pub fn encode(key: &[u8], value: &[u8]) -> Vec<u8> {
        let mut result = Vec::new();
        let key_len = key.len();
        let value_len = value.len();
        result.reserve(4 + key_len + value_len);
        let length_slices : [u8; 2] = ((key_len + value_len + 2) as u16).to_be_bytes();
        result.extend_from_slice(&length_slices);
        let key_len_slice: [u8; 2] = (key_len as u16).to_be_bytes();
        result.extend_from_slice(&key_len_slice);
        result.extend_from_slice(key);
        result.extend_from_slice(value);
        assert_eq!(4 + key_len + value_len, result.len());
        result
    }

    /// decode log record from raw data
    pub fn decode(data: &[u8]) -> Result<(Self, usize)> {
        let data_len = data.len();
        if data_len < 4 {
            return Err(Error::msg("[LogRecord::decode] the data for decoding is to short, data_len is too short"));
        }
        let record_len = (((data[0] as u16) << 8) | (data[1] as u16)) as usize;
        if 2 + record_len > data_len {
            return Err(Error::msg("[LogRecord::decode] the data for decoding is to short, record_len is too long"));
        }
        let key_len = ((data[2] as u16) << 8) | (data[3] as u16);
        if ((4 + key_len) as usize) > data_len {
            return Err(Error::msg("[LogRecord::decode] the data for decoding is to short, key_len is too long"));
        }

        let key : Vec<u8> = Vec::from(&data[4..4+(key_len as usize)]);
        let value : Vec<u8> = Vec::from(&data[(4+key_len as usize)..(record_len + 2)]);
        assert_eq!(record_len, 2 + key_len as usize + value.len());
        Ok((Self {
            key,
            value,
        }, record_len + 2))
    }

}

#[derive(Clone)]
pub struct LogBlock {
    record_data: Vec<u8>,
    max_size: usize,
    cur_size: usize,
}

impl LogBlock {
    pub fn new(max_size: usize) -> Self {
        Self {
            record_data: vec![],
            max_size,
            cur_size: 0
        }
    }

    /// Add kv pair to current log block, return true if need flush after inserting
    pub fn add(&mut self, key: &[u8], value: &[u8]) -> Result<bool> {
        if self.cur_size >= self.max_size {
            return Err(Error::msg("[LogBlock::add] no space left"));
        }
        self.cur_size += 4 + key.len() + value.len();
        self.record_data.put_slice(&*LogRecord::encode(key, value));
        Ok(self.cur_size > self.max_size)
    }

    pub fn encode(&mut self) -> Vec<u8> {
        mem::replace(&mut self.record_data, Vec::new())
    }

    pub fn decode(data: &[u8]) -> Vec<Arc<LogRecord>> {
        let mut result = Vec::new();
        let mut input = data;
        while let Ok((record, offset)) = LogRecord::decode(input) {
            result.push(Arc::new(record));
            input = &input[offset..];
        }
        result
    }
}

#[cfg(test)]
mod log_tests {
    use bytes::BufMut;
    use crate::log::{LogBlock, LogRecord};
    use crate::tests::utils::{key_of, value_of};

    #[test]
    fn test_log_record() {
        let mut middle_storage = vec![];
        for i in 1..=100 {
            let key = key_of(i);
            let value = value_of(i);
            let record = LogRecord::encode(&key, &value);
            middle_storage.push(record);
        }
        for i in 1..=100 {
            let key = key_of(i);
            let value = value_of(i);
            let (record, offset) = LogRecord::decode(&middle_storage[i - 1]).unwrap();
            assert_eq!(offset, 4 + key.len() + value.len());
            assert_eq!(String::from_utf8(record.key).unwrap(), String::from_utf8(key).unwrap());
            assert_eq!(String::from_utf8(record.value).unwrap(), String::from_utf8(value).unwrap());
        }
    }

    #[test]
    fn test_log_block() {
        let mut middle_storage = Vec::new();
        let mut log_block = LogBlock::new(100);
        for i in 1..=100 {
            let key = key_of(i);
            let value = value_of(i);
            let need_flush = log_block.add(&key, &value).unwrap();
            if need_flush {
                let data = log_block.encode();
                middle_storage.put_slice(&data);
                log_block = LogBlock::new(100);
            }
        }
        let data = log_block.encode();
        middle_storage.put_slice(&data);
        let records = LogBlock::decode(&middle_storage);

        for i in 1..=100 {
            let key = key_of(i);
            let value = value_of(i);
            assert_eq!(String::from_utf8(records[i - 1].key.clone()).unwrap(), String::from_utf8(key).unwrap());
            assert_eq!(String::from_utf8(records[i - 1].value.clone()).unwrap(), String::from_utf8(value).unwrap());
        }
    }
}