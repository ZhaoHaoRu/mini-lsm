mod iterator;
mod builder;
mod reader;

use std::mem;
use std::sync::Arc;
use anyhow::{Result, Error};
use bytes::BufMut;

pub struct RecordData {}

impl RecordData {
    /// encode the log record data from command content
    pub fn encode(key: &[u8], value: &[u8]) -> Vec<u8> {
        let key_len = key.len();
        let value_len = value.len();
        let mut result = Vec::new();
        let data_len = 4 + key_len + value_len;
        result.reserve(data_len);
        let key_len_slice: [u8; 2] = (key_len as u16).to_be_bytes();
        result.extend_from_slice(&key_len_slice);
        result.copy_from_slice(key);
        result.copy_from_slice(value);
        assert_eq!(data_len, result.len());
        result
    }

    /// decode the log record data from raw
    pub fn decode(data: &[u8]) -> Result<(Vec<u8>, Vec<u8>)> {
        let data_len = data.len();
        if data_len < 4 {
            return Err(Error::msg("[RecordData::decode] the data for decoding is to short"));
        }
        let key_len = ((data[0] as u16) << 8) | (data[1] as u16);
        let value_len = ((data[2] as u16) << 8) | (data[3] as u16);
        if key_len + value_len + 4 < data_len as u16 {
            return Err(Error::msg("[RecordData::decode] the data for decoding is to short"));
        }

        let key : Vec<u8> = Vec::from(&data[4..4+(key_len as usize)]);
        let value : Vec<u8> = Vec::from(&data[(4+key_len as usize)..((4+key_len+value_len) as usize)]);
        Ok((key, value))
    }
}

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
        result.copy_from_slice(key);
        result.copy_from_slice(value);
        assert_eq!(4 + key_len + value_len, result.len());
        result
    }

    /// decode log record from raw data
    pub fn decode(data: &[u8]) -> Result<(Self, usize)> {
        let data_len = data.len();
        if data_len < 4 {
            return Err(Error::msg("[LogRecord::decode] the data for decoding is to short"));
        }
        let record_len = (((data[0] as u16) << 8) | (data[1] as u16)) as usize;
        if 2 + record_len > data_len {
            return Err(Error::msg("[LogRecord::decode] the data for decoding is to short"));
        }
        let key_len = ((data[2] as u16) << 8) | (data[3] as u16);
        if ((4 + key_len) as usize) < data_len {
            return Err(Error::msg("[LogRecord::decode] the data for decoding is to short"));
        }

        let key : Vec<u8> = Vec::from(&data[4..4+(key_len as usize)]);
        let value : Vec<u8> = Vec::from(&data[(4+key_len as usize)..(record_len as usize)]);
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
