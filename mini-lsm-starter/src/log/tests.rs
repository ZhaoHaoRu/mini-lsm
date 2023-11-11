#![allow(unused_variables)]
#![allow(dead_code)]
use crate::iterators::StorageIterator;
use crate::log::reader::LogReader;

use tempfile::tempdir;

use super::builder::LogBuilder;

pub fn key_of(idx: usize) -> Vec<u8> {
    format!("key_{:05}", idx).into_bytes()
}

pub fn value_of(idx: usize) -> Vec<u8> {
    format!("value_{:010}", idx).into_bytes()
}

#[test]
fn test_log_reader_and_log_iterator() {
    // generate a log by builder
    let dir = tempdir().unwrap().path().to_path_buf();
    let mut log_builder_instance = LogBuilder::new(0, &dir, 1024);
    for i in 1..=100 {
        let key = key_of(i);
        let value = value_of(i);
        let add_record_result = log_builder_instance.add_record(&key, &value);
        assert!(add_record_result.is_ok());
    }

    log_builder_instance.sync_cur_log_file().unwrap();

    // read the log by reader
    let mut log_reader_instance = LogReader::new(0, &dir);
    while let Ok(mut iter) = log_reader_instance.read_records() {
        for i in 1..=100 {
            assert!(iter.is_valid());
            let k = key_of(i);
            let v = value_of(i);
            assert!(iter.is_valid());
            assert_eq!(
                k,
                iter.key(),
                "expected key: {:?}, actual key: {:?}",
                k,
                iter.key(),
            );
            assert_eq!(
                v,
                iter.value(),
                "expected value: {:?}, actual value: {:?}",
                v,
                iter.value(),
            );
            iter.next().unwrap();
        }
        assert!(!iter.is_valid());
    }
}
