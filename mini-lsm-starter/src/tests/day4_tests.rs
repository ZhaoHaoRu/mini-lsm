use std::ops::Bound;
use std::path::Path;

use bytes::Bytes;
use log::debug;
use tempfile::tempdir;

use crate::iterators::StorageIterator;
use crate::tests::utils::{key_of, new_value_of, value_of};

fn as_bytes(x: &[u8]) -> Bytes {
    Bytes::copy_from_slice(x)
}

fn check_iter_result(iter: impl StorageIterator, expected: Vec<(Bytes, Bytes)>) {
    debug!("[check_iter_result] begin check");
    let mut iter = iter;
    for (k, v) in expected {
        assert!(iter.is_valid());
        assert_eq!(
            k,
            iter.key(),
            "expected key: {:?}, actual key: {:?}",
            k,
            as_bytes(iter.key()),
        );
        assert_eq!(
            v,
            iter.value(),
            "expected value: {:?}, actual value: {:?}",
            v,
            as_bytes(iter.value()),
        );
        iter.next().unwrap();
    }
    assert!(!iter.is_valid());
}

fn wal_log_test_setup(dir: &Path) {
    let log_dir_path = dir.join("test_data");
    if log_dir_path.exists() {
        let entries = std::fs::read_dir(&log_dir_path).unwrap();
        for entry in entries {
            let entry = entry.unwrap();
            let path = entry.path();
            if path.is_file() {
                std::fs::remove_file(&path).unwrap();
            }
        }
    } else {
        std::fs::create_dir(&log_dir_path).unwrap();
    }
}

#[test]
fn test_storage_get() {
    use crate::lsm_storage::LsmStorage;
    let dir = tempdir().unwrap();
    let storage = LsmStorage::open(&dir).unwrap();
    storage.put(b"1", b"233").unwrap();
    storage.put(b"2", b"2333").unwrap();
    storage.put(b"3", b"23333").unwrap();
    assert_eq!(&storage.get(b"1").unwrap().unwrap()[..], b"233");
    assert_eq!(&storage.get(b"2").unwrap().unwrap()[..], b"2333");
    assert_eq!(&storage.get(b"3").unwrap().unwrap()[..], b"23333");
    storage.delete(b"2").unwrap();
    assert!(storage.get(b"2").unwrap().is_none());
}

#[test]
fn test_storage_scan_memtable_1() {
    use crate::lsm_storage::LsmStorage;
    let dir = tempdir().unwrap();
    let storage = LsmStorage::open(&dir).unwrap();
    storage.put(b"1", b"233").unwrap();
    storage.put(b"2", b"2333").unwrap();
    storage.put(b"3", b"23333").unwrap();
    storage.delete(b"2").unwrap();
    check_iter_result(
        storage.scan(Bound::Unbounded, Bound::Unbounded).unwrap(),
        vec![
            (Bytes::from("1"), Bytes::from("233")),
            (Bytes::from("3"), Bytes::from("23333")),
        ],
    );
    check_iter_result(
        storage
            .scan(Bound::Included(b"1"), Bound::Included(b"2"))
            .unwrap(),
        vec![(Bytes::from("1"), Bytes::from("233"))],
    );
    check_iter_result(
        storage
            .scan(Bound::Excluded(b"1"), Bound::Excluded(b"3"))
            .unwrap(),
        vec![],
    );
}

#[test]
fn test_storage_scan_memtable_2() {
    use crate::lsm_storage::LsmStorage;
    let dir = tempdir().unwrap();
    let storage = LsmStorage::open(&dir).unwrap();
    storage.put(b"1", b"233").unwrap();
    storage.put(b"2", b"2333").unwrap();
    storage.put(b"3", b"23333").unwrap();
    storage.delete(b"1").unwrap();
    check_iter_result(
        storage.scan(Bound::Unbounded, Bound::Unbounded).unwrap(),
        vec![
            (Bytes::from("2"), Bytes::from("2333")),
            (Bytes::from("3"), Bytes::from("23333")),
        ],
    );
    check_iter_result(
        storage
            .scan(Bound::Included(b"1"), Bound::Included(b"2"))
            .unwrap(),
        vec![(Bytes::from("2"), Bytes::from("2333"))],
    );
    check_iter_result(
        storage
            .scan(Bound::Excluded(b"1"), Bound::Excluded(b"3"))
            .unwrap(),
        vec![(Bytes::from("2"), Bytes::from("2333"))],
    );
}

#[test]
fn test_storage_get_after_sync() {
    use crate::lsm_storage::LsmStorage;
    let dir = tempdir().unwrap();
    let storage = LsmStorage::open(&dir).unwrap();
    storage.put(b"1", b"233").unwrap();
    storage.put(b"2", b"2333").unwrap();
    storage.sync().unwrap();
    storage.put(b"3", b"23333").unwrap();
    assert_eq!(&storage.get(b"1").unwrap().unwrap()[..], b"233");
    assert_eq!(&storage.get(b"2").unwrap().unwrap()[..], b"2333");
    assert_eq!(&storage.get(b"3").unwrap().unwrap()[..], b"23333");
    storage.delete(b"2").unwrap();
    assert!(storage.get(b"2").unwrap().is_none());
}

#[test]
fn test_storage_scan_memtable_1_after_sync() {
    use crate::lsm_storage::LsmStorage;
    let dir = tempdir().unwrap();
    let storage = LsmStorage::open(&dir).unwrap();
    storage.put(b"1", b"233").unwrap();
    storage.put(b"2", b"2333").unwrap();
    storage.sync().unwrap();
    storage.put(b"3", b"23333").unwrap();
    storage.delete(b"2").unwrap();
    check_iter_result(
        storage.scan(Bound::Unbounded, Bound::Unbounded).unwrap(),
        vec![
            (Bytes::from("1"), Bytes::from("233")),
            (Bytes::from("3"), Bytes::from("23333")),
        ],
    );
    check_iter_result(
        storage
            .scan(Bound::Included(b"1"), Bound::Included(b"2"))
            .unwrap(),
        vec![(Bytes::from("1"), Bytes::from("233"))],
    );
    check_iter_result(
        storage
            .scan(Bound::Excluded(b"1"), Bound::Excluded(b"3"))
            .unwrap(),
        vec![],
    );
}

#[test]
fn test_storage_scan_memtable_2_after_sync() {
    use crate::lsm_storage::LsmStorage;
    let dir = tempdir().unwrap();
    let storage = LsmStorage::open(&dir).unwrap();
    storage.put(b"1", b"233").unwrap();
    storage.put(b"2", b"2333").unwrap();
    storage.sync().unwrap();
    storage.put(b"3", b"23333").unwrap();
    storage.sync().unwrap();
    storage.delete(b"1").unwrap();
    check_iter_result(
        storage.scan(Bound::Unbounded, Bound::Unbounded).unwrap(),
        vec![
            (Bytes::from("2"), Bytes::from("2333")),
            (Bytes::from("3"), Bytes::from("23333")),
        ],
    );
    check_iter_result(
        storage
            .scan(Bound::Included(b"1"), Bound::Included(b"2"))
            .unwrap(),
        vec![(Bytes::from("2"), Bytes::from("2333"))],
    );
    check_iter_result(
        storage
            .scan(Bound::Excluded(b"1"), Bound::Excluded(b"3"))
            .unwrap(),
        vec![(Bytes::from("2"), Bytes::from("2333"))],
    );
}

#[test]
/// too much file writing, need to run for a long time
fn test_storage_scan_after_compaction() {
    use crate::lsm_storage::LsmStorage;
    let dir = tempdir().unwrap();
    let storage = LsmStorage::open(&dir).unwrap();
    for i in 0..10 {
        for j in 1..=1000 {
            storage
                .put(
                    key_of(i * 1000 + j).as_slice(),
                    value_of(i * 1000 + j).as_slice(),
                )
                .unwrap();
        }
        storage.sync().unwrap();
    }
    storage
        .compaction(0)
        .expect("[test_storage_scan_after_compaction] compaction fail");
    // check scan result
    check_iter_result(
        storage.scan(Bound::Unbounded, Bound::Unbounded).unwrap(),
        (1..=10000)
            .map(|i| {
                let key_slice = key_of(i);
                let value_slice = value_of(i);
                (Bytes::from(key_slice), Bytes::from(value_slice))
            })
            .collect::<Vec<_>>(),
    );

    // check scan after update and delete
    for i in 0..2 {
        for j in 1..=1000 {
            storage.delete(key_of(i * 1000 + j).as_slice()).unwrap();
        }
        storage.sync().unwrap();
    }
    for i in 2..4 {
        for j in 1..=1000 {
            storage
                .put(
                    key_of(i * 1000 + j).as_slice(),
                    new_value_of(i * 1000 + j).as_slice(),
                )
                .unwrap();
        }
        storage.sync().unwrap();
    }
    storage
        .compaction(0)
        .expect("[test_storage_scan_after_compaction] compaction fail");
    check_iter_result(
        storage
            .scan(
                Bound::Included(&*key_of(0)),
                Bound::Included(&*key_of(4000)),
            )
            .unwrap(),
        (2001..=4000)
            .map(|i| {
                let key_slice = key_of(i);
                let value_slice = new_value_of(i);
                (Bytes::from(key_slice), Bytes::from(value_slice))
            })
            .collect::<Vec<_>>(),
    );
}

#[test]
fn test_wal_log_with_mem_table() {
    use crate::lsm_storage::LsmStorage;

    // let log_dir_path = env::current_dir().unwrap().join("test_data");
    let log_dir_path = tempdir().unwrap().into_path();
    wal_log_test_setup(&log_dir_path);

    // test log write
    {
        let storage = LsmStorage::open(&log_dir_path).unwrap();
        for i in 0..10 {
            for j in 1..=1000 {
                storage
                    .put(
                        key_of(i * 1000 + j).as_slice(),
                        value_of(i * 1000 + j).as_slice(),
                    )
                    .unwrap();
            }
        }
        storage.sync_log_builder().unwrap();
    }

    // test log recovery
    {
        let storage = LsmStorage::open(&log_dir_path).unwrap();
        // check scan result
        check_iter_result(
            storage.scan(Bound::Unbounded, Bound::Unbounded).unwrap(),
            (1..=10000)
                .map(|i| {
                    let key_slice = key_of(i);
                    let value_slice = value_of(i);
                    (Bytes::from(key_slice), Bytes::from(value_slice))
                })
                .collect::<Vec<_>>(),
        );
    }
}

#[test]
fn test_wal_log_with_mem_table_and_sst() {
    use crate::lsm_storage::LsmStorage;

    // let log_dir_path = env::current_dir().unwrap().join("test_data");
    let log_dir_path = tempdir().unwrap().into_path();
    wal_log_test_setup(&log_dir_path);

    // test log write
    {
        let storage = LsmStorage::open(&log_dir_path).unwrap();
        for i in 0..10 {
            for j in 1..=1000 {
                storage
                    .put(
                        key_of(i * 1000 + j).as_slice(),
                        value_of(i * 1000 + j).as_slice(),
                    )
                    .unwrap();
            }
            if i < 8 {
                storage.sync().unwrap();
            }
        }
        storage
            .compaction(0)
            .expect("[test_wal_log_with_mem_table_and_sst] compaction fail");
        storage.sync_log_builder().unwrap();
    }

    // test log and sst recovery
    {
        let storage = LsmStorage::open(&log_dir_path).unwrap();
        // check scan result
        check_iter_result(
            storage.scan(Bound::Unbounded, Bound::Unbounded).unwrap(),
            (1..=10000)
                .map(|i| {
                    let key_slice = key_of(i);
                    let value_slice = value_of(i);
                    (Bytes::from(key_slice), Bytes::from(value_slice))
                })
                .collect::<Vec<_>>(),
        );
    }
}
