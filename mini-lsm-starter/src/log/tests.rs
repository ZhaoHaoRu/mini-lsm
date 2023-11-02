use tempfile::tempdir;
use crate::iterators::StorageIterator;
use crate::log::reader::LogReader;
use crate::tests::utils;

use super::builder::LogBuilder;

#[test]
fn test_log_reader_and_log_iterator() {
    // generate a log by builder
    let dir = tempdir().unwrap().path().to_path_buf();
    let mut log_builder_instance = LogBuilder::new(0, &dir, 1024);
    for i in 1..=100 {
        let key = utils::key_of(i);
        let value = utils::value_of(i);
        let add_record_result = log_builder_instance.add_record(&key, &value);
        assert!(add_record_result.is_ok());
    }

    log_builder_instance.sync_cur_log_file().unwrap();

    // read the log by reader
    let mut log_reader_instance = LogReader::new(0, &dir);
    loop {
        match log_reader_instance.read_records() {
            Ok(mut iter) => {
                for i in 1..=100 {
                    assert!(iter.is_valid());
                    let k = utils::key_of(i);
                    let v = utils::value_of(i);
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
            Err(_) => {
                break;
            }
        }
    }
}