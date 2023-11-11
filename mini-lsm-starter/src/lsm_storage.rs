#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::fs::File;
use std::io::{BufRead, BufReader, Write};
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use anyhow::{Error, Result};
use bytes::Bytes;
use log::{debug, warn};
use moka::sync::Cache;
use parking_lot::RwLock;

use crate::block::Block;
use crate::compaction::Compaction;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::iterators::StorageIterator;
use crate::log::builder::LogBuilder;
use crate::log::LogReader;
use crate::lsm_iterator::{FusedIterator, LsmIterator};
use crate::mem_table::MemTable;
use crate::table::{FileObject, SsTable, SsTableBuilder, SsTableIterator};
use crate::utils;

pub type BlockCache = Cache<(usize, usize), Arc<Block>>;

#[derive(Clone)]
pub struct LsmStorageInner {
    /// The current memtable.
    memtable: Arc<MemTable>,
    /// Immutable memTables, from earliest to latest.
    imm_memtables: Vec<Arc<MemTable>>,
    /// L0 SsTables, from earliest to latest.
    l0_sstables: Vec<Arc<SsTable>>,
    /// L1 - L6 SsTables, sorted by key range.
    #[allow(dead_code)]
    levels: Vec<Vec<Arc<SsTable>>>,
    /// The next SSTable ID.
    next_sst_id: Arc<Mutex<usize>>,
    /// The block cache
    block_cache: Arc<BlockCache>,
    /// The relation between memtable/imm_memtables and log id
    log_number_records: [Vec<usize>; 2],
    /// The log builder
    log_builder: Arc<Mutex<LogBuilder>>,
}

impl LsmStorageInner {
    fn create(path: &Path) -> Self {
        Self {
            memtable: Arc::new(MemTable::create()),
            imm_memtables: vec![],
            l0_sstables: vec![],
            levels: vec![],
            next_sst_id: Arc::new(Mutex::new(1)),
            //NOTE: the default cache size is 4GB
            block_cache: Arc::new(Cache::new(4 * 1024 * 1024 * 1024)),
            log_number_records: [vec![1], Vec::new()],
            log_builder: Arc::new(Mutex::new(LogBuilder::new(1, path, 128))),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn create_after_crash(
        next_sst_id: usize,
        mem_table: Arc<MemTable>,
        imm_mem_tables: Vec<Arc<MemTable>>,
        l0_sstables: Vec<Arc<SsTable>>,
        levels: Vec<Vec<Arc<SsTable>>>,
        log_number_records: [Vec<usize>; 2],
        block_cache: Arc<BlockCache>,
        log_builder: LogBuilder,
    ) -> Self {
        Self {
            memtable: mem_table,
            imm_memtables: imm_mem_tables,
            l0_sstables,
            levels,
            next_sst_id: Arc::new(Mutex::new(next_sst_id)),
            block_cache,
            log_number_records,
            log_builder: Arc::new(Mutex::new(log_builder)),
        }
    }

    fn sync_manifest(&self, path: &Path) {
        // generate the data for manifest file
        let mut buf = Vec::new();
        for row in &self.log_number_records {
            for elem in row {
                buf.extend(elem.to_string().as_bytes());
                buf.push(b' ');
            }
            buf.push(b'\n');
        }
        for elem in &self.l0_sstables {
            buf.extend(elem.get_sst_id().to_string().as_bytes());
            buf.push(b' ');
        }
        buf.push(b'\n');
        for level in &self.levels {
            for elem in level {
                buf.extend(elem.get_sst_id().to_string().as_bytes());
                // buf.append(&mut elem.get_sst_id().to_string().to_vec());
                buf.push(b' ');
            }
        }
        buf.push(b'\n');
        let manifest_path = path.join("manifest.txt");
        let mut manifest_file = File::create(manifest_path)
            .expect("[LsmStorageInner::sync_manifest] open manifest file fail");
        manifest_file
            .write_all(&buf)
            .expect("[LsmStorageInner::sync_manifest] write to manifest file fail");
        manifest_file
            .sync_data()
            .expect("[LsmStorageInner::sync_manifest] sync manifest file fail");
    }
}

/// The storage interface of the LSM tree.
pub struct LsmStorage {
    inner: Arc<RwLock<Arc<LsmStorageInner>>>,
    /// The mutex to hold when change the lsm structure, such as sync and compaction
    structure_mutex: Mutex<i32>,
    /// The path of the storage
    path: PathBuf,
}

impl LsmStorage {
    /// restore the mem_table or imm_mem_tables from the log file
    fn restore_memtable(log_file_id: usize, path: &Path) -> Result<Arc<MemTable>> {
        let mut log_reader = LogReader::new(log_file_id, path);
        match log_reader.read_records() {
            Ok(mut iter) => {
                let mem_table = MemTable::create();
                while iter.is_valid() {
                    let key = iter.key();
                    let value = iter.value();
                    mem_table.put(key, value);
                    iter.next().unwrap();
                }
                Ok(Arc::new(mem_table))
            }
            Err(error) => Err(error),
        }
    }

    /// recover from the previous ss-tables and WAL log
    fn recovery(path: &Path) -> Arc<LsmStorageInner> {
        let manifest_path = path.join("manifest.txt");
        if !manifest_path.exists() {
            // the manifest file not exist, maybe the first time to create the storage
            return Arc::new(LsmStorageInner::create(path));
        }

        // open the manifest file and read the earliest and latest log file id
        let manifest_file = std::fs::File::open(manifest_path).unwrap();
        let manifest_reader = BufReader::new(manifest_file);
        let block_cache = Arc::new(Cache::new(4 * 1024 * 1024 * 1024));
        let mut memtable = Arc::new(MemTable::create());
        let mut imm_memtables = vec![];
        let mut l0_sstables = vec![];
        let mut levels = vec![];
        let mut next_sst_id = 1;
        let mut log_number_records = [Vec::new(), Vec::new()];

        // read line by line and restore the lsm tree structure
        for (id, line) in manifest_reader.lines().enumerate() {
            if let Ok(data) = line {
                if data.is_empty() {
                    continue;
                }
                let file_ids = data.trim().split(' ').collect::<Vec<&str>>();
                if id == 0 {
                    // mem_table
                    assert_eq!(file_ids.len(), 1);
                    let mem_table_id = file_ids[0].parse::<usize>().unwrap();
                    next_sst_id = mem_table_id;
                    memtable = LsmStorage::restore_memtable(mem_table_id, path)
                        .expect("[LsmStorage::recovery] recover mem_table fail");
                    log_number_records[0].push(mem_table_id);
                } else if id == 1 {
                    // imm_mem_tables
                    imm_memtables.reserve(file_ids.len());
                    log_number_records[1].reserve(file_ids.len());
                    for file_id in file_ids.into_iter() {
                        let mem_table_id = file_id.parse::<usize>().unwrap();
                        imm_memtables.push(
                            LsmStorage::restore_memtable(mem_table_id, path)
                                .expect("[LsmStorage::recovery] recover immutable mem_table fail"),
                        );
                    }
                } else if id == 2 {
                    // l0 ss-table
                    l0_sstables.reserve(file_ids.len());
                    for file_id in file_ids.into_iter() {
                        let sst_id = file_id.parse::<usize>().unwrap();
                        let ss_table_path = path.join(file_id.to_owned() + ".sst");
                        // XXX: here file object read all data from ss-table to cache, absolutely need optimization
                        l0_sstables.push(Arc::new(
                            SsTable::open(
                                sst_id,
                                Some(block_cache.clone()),
                                FileObject::open(&ss_table_path).unwrap(),
                            )
                            .unwrap(),
                        ));
                    }
                } else {
                    // other ss-table
                    let mut level = Vec::new();
                    level.reserve(file_ids.len());
                    for file_id in file_ids.into_iter() {
                        let sst_id = file_id.parse::<usize>().unwrap();
                        let ss_table_path = path.join(sst_id.to_string() + ".sst");
                        level.push(Arc::new(
                            SsTable::open(
                                sst_id,
                                Some(block_cache.clone()),
                                FileObject::open(&ss_table_path).unwrap(),
                            )
                            .unwrap(),
                        ));
                    }
                    levels.push(level);
                }
            } else {
                warn!("[LsmStorage::recovery] invalid line in manifest file");
                break;
            }
        }

        // init the LsmStorageInner
        let log_builder = LogBuilder::new(next_sst_id, path, 128);
        log_number_records[0][0] = next_sst_id;
        Arc::new(LsmStorageInner::create_after_crash(
            next_sst_id,
            memtable.clone(),
            imm_memtables,
            l0_sstables,
            levels,
            log_number_records,
            block_cache,
            log_builder,
        ))
    }

    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        // get the lsm storage inner and log builder
        let storage_inner = LsmStorage::recovery(path.as_ref());
        LsmStorageInner::sync_manifest(&storage_inner, path.as_ref());
        Ok(Self {
            inner: Arc::new(RwLock::new(storage_inner)),
            structure_mutex: Mutex::new(0),
            path: path.as_ref().to_path_buf(),
        })
    }

    /// Get a key from the storage. In day 7, this can be further optimized by using a bloom filter.
    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        // optimize the logic of copying all pointers
        let snapshot = {
            let r = self.inner.read();
            r.as_ref().clone()
        };

        // step1: search the mem-table
        if let Some(mem_table_result) = snapshot.memtable.get(key) {
            if mem_table_result.is_empty() {
                return Ok(None);
            }
            return Ok(Some(mem_table_result));
        }

        // step2: search the immutable mem-tables from the latest to the earliest
        for imm_mem_table in snapshot.imm_memtables {
            if let Some(imm_mem_table_result) = imm_mem_table.get(key) {
                if imm_mem_table_result.is_empty() {
                    return Ok(None);
                }
                return Ok(Some(imm_mem_table_result));
            }
        }

        let search_in_sst = |table: Arc<SsTable>| -> Result<Option<Bytes>> {
            if let Ok(ss_table_iter) = SsTableIterator::create_and_find_target_key(table, key) {
                if ss_table_iter.is_valid() && ss_table_iter.key() == key {
                    if ss_table_iter.value().is_empty() {
                        return Ok(None);
                    }
                    return Ok(Some(Bytes::copy_from_slice(ss_table_iter.value())));
                }
            }
            Err(Error::msg("[search_in_sst] nothing found"))
        };

        // step3: search the level0 file from the latest to the earliest
        for ss_table in snapshot.l0_sstables {
            if let Ok(result) = search_in_sst(ss_table) {
                return Ok(result);
            }
        }

        // step4: search from level 1 to n
        for level in snapshot.levels {
            for ss_table in level {
                if let Ok(result) = search_in_sst(ss_table) {
                    return Ok(result);
                }
            }
        }

        Ok(None)
    }

    /// Put a key-value pair into the storage by writing into the current memtable.
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let r = self.inner.read();
        let mut log_builder = r
            .log_builder
            .lock()
            .expect("[LsmStorage::put] get log builder fail");
        log_builder
            .add_record(key, value)
            .expect("[LsmStorage::put] write log file fail");
        r.memtable.put(key, value);
        Ok(())
    }

    /// Remove a key from the storage by writing an empty value.
    pub fn delete(&self, _key: &[u8]) -> Result<()> {
        let r = self.inner.read();
        let mut log_builder = r
            .log_builder
            .lock()
            .expect("[LsmStorage::delete] get log builder fail");
        log_builder
            .add_record(_key, &[])
            .expect("[LsmStorage::delete] write log file fail");
        r.memtable.put(_key, &[]);
        Ok(())
    }

    /// Persist data to disk.
    ///
    /// In day 3: flush the current memtable to disk as L0 SST.
    /// In day 6: call `fsync` on WAL.
    pub fn sync(&self) -> Result<()> {
        let _unused = self
            .structure_mutex
            .lock()
            .expect("[LsmStorage::sync] encounter an error when acquire lock");

        // Firstly, move the current mutable mem-table to immutable mem-table list
        {
            // NOTE: the operation on write guard is copy-on-write
            let mut w = self.inner.write();
            let mut object = w.as_ref().clone();
            let replaced_mem_table = object.memtable.clone();
            object.imm_memtables.insert(0, replaced_mem_table);
            object.memtable = Arc::new(MemTable::create());

            // generate a new log file for the new mem-table
            assert!(!object.log_number_records[0].is_empty());
            object.log_number_records[1].insert(0, object.log_number_records[0][0]);
            {
                let mut log_builder = object
                    .log_builder
                    .lock()
                    .expect("[LsmStorage::sync] get log builder lock fail");
                log_builder
                    .replace_dest()
                    .expect("[LsmStorage::sync] replace being used log file fail");
                object.log_number_records[0][0] = log_builder.get_cur_log_file_id();
            }
            *w = Arc::new(object);
        }

        // Secondly, flush the immutable mem-tables to ss-tables without taking any lock
        let mut ss_table_builders = vec![];
        {
            let r = self.inner.read();
            for mem_table in &r.imm_memtables {
                let mut ss_table_builder = SsTableBuilder::new(4096);
                mem_table
                    .flush(&mut ss_table_builder)
                    .expect("[LsmStorage::sync] flush mem-table fail");
                ss_table_builders.push(ss_table_builder);
            }
        }

        // Thirdly, remove the mem-table and put the SST into l0_tables in a critical section
        {
            let mut w = self.inner.write();
            let mut object = w.as_ref().clone();
            {
                let mut next_sst_id = object.next_sst_id.lock().unwrap();
                for (_, ss_table_builder) in ss_table_builders.into_iter().enumerate() {
                    object.l0_sstables.insert(
                        0,
                        Arc::new(
                            ss_table_builder
                                .build(
                                    *next_sst_id,
                                    Some(object.block_cache.clone()),
                                    utils::generate_sst_name(Some(&self.path), *next_sst_id),
                                )
                                .unwrap(),
                        ),
                    );
                    *next_sst_id += 1;
                }
            }

            // remove the immutable mem-tables
            object.imm_memtables.clear();

            // delete the stale log files
            for imm_memtable_id in &object.log_number_records[1] {
                LogBuilder::remove_stale_log_file(&self.path, *imm_memtable_id);
            }
            object.log_number_records[1].clear();

            // persist the lsm structure meta-data
            object.sync_manifest(&self.path);

            *w = Arc::new(object);
        }

        Ok(())
    }

    /// Create an iterator over a range of keys.
    pub fn scan(
        &self,
        _lower: Bound<&[u8]>,
        _upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        let snapshot = {
            let r = self.inner.read();
            r.as_ref().clone()
        };

        // create scan iterators
        let mut mem_table_iterators = vec![];
        mem_table_iterators.push(Box::new(snapshot.memtable.scan(_lower, _upper)));
        for mem_table in snapshot.imm_memtables {
            mem_table_iterators.push(Box::new(mem_table.scan(_lower, _upper)));
        }

        // generate the boundary key for SsTable
        let mut boundary = vec![];
        match _lower {
            Bound::Excluded(value) => {
                boundary = Vec::from(value);
                let len = boundary.len();
                if len > 0 {
                    if boundary[len - 1] < 255 {
                        boundary[len - 1] += 1;
                    } else {
                        boundary.push(0);
                    }
                }
            }
            Bound::Included(value) => {
                boundary = Vec::from(value);
            }
            Bound::Unbounded => {}
        }

        let mut ss_table_iterators = vec![];
        for ss_table in snapshot.l0_sstables {
            ss_table_iterators.push(Box::new(SsTableIterator::create_and_seek_to_key(
                ss_table,
                &boundary[..],
            )?));
        }
        for level in snapshot.levels.into_iter() {
            for ss_table in level {
                ss_table_iterators.push(Box::new(SsTableIterator::create_and_seek_to_key(
                    ss_table, &boundary,
                )?));
            }
        }

        debug!("[LSMStorage::scan] generate the iterator");
        Ok(FusedIterator::new(LsmIterator::new(
            TwoMergeIterator::create(
                MergeIterator::create(mem_table_iterators),
                MergeIterator::create(ss_table_iterators),
            )
            .unwrap(),
            _upper,
        )))
    }

    /// compaction from level 0 to level n until satisfy the condition
    pub fn compaction(&self, start_level: usize) -> Result<()> {
        let _structure_lock = self
            .structure_mutex
            .lock()
            .expect("[LsmStorage::compaction] encounter an error when acquire lock");
        let mut level = start_level;

        // get some metadata
        let dir = self.path.clone();
        let block_cache;
        let next_sst_id;
        {
            let object = self.inner.read();
            block_cache = Some(object.block_cache.clone());
            next_sst_id = object.next_sst_id.clone();
        }

        loop {
            // get the ss-table candidate for compaction
            let level_n;
            let mut level_n_1 = vec![];
            {
                let object = self.inner.read();
                if level > object.levels.len() {
                    break;
                }
                if level == 0 {
                    level_n = object.l0_sstables.clone();
                    if !object.levels.is_empty() {
                        level_n_1 = object.levels[0].clone();
                    }
                } else {
                    level_n = object.levels[level - 1].clone();
                    if level < object.levels.len() {
                        level_n_1 = object.levels[level].clone();
                    }
                }
            }

            // generate the compaction instance
            let mut compaction_instance = Compaction::new(
                level,
                [level_n, level_n_1],
                next_sst_id.clone(),
                block_cache.clone(),
                1024 * 1024 * 256,
                dir.clone(),
            );
            // do compaction work
            compaction_instance
                .compact_two_levels()
                // .unwrap_or_else(|_| {
                //     panic!(
                //         "{}",
                //         &format!(
                //             "[LsmStorage::compaction] compaction level {} and level {} fail",
                //             level,
                //             level + 1
                //         )
                //     )
                // });
                .unwrap();

            // install the compaction result
            let new_levels = compaction_instance.generate_output();
            assert_eq!(new_levels.len(), 2);
            {
                let mut object = self.inner.write();
                let mut snapshot = object.as_ref().clone();
                if level == 0 {
                    snapshot.l0_sstables = new_levels[0].clone();
                    if snapshot.levels.is_empty() {
                        snapshot.levels.push(new_levels[1].clone());
                    } else {
                        snapshot.levels[0] = new_levels[1].clone();
                    }
                } else {
                    snapshot.levels[level - 1] = new_levels[0].clone();
                    if level >= snapshot.levels.len() {
                        snapshot.levels.push(new_levels[1].clone());
                    } else {
                        snapshot.levels[level] = new_levels[1].clone();
                    }
                }
                *object = Arc::new(snapshot);
                // persist the lsm structure
                object.sync_manifest(&self.path);
            }

            // judge whether need to stop compaction
            if !compaction_instance.need_continue() {
                break;
            }

            level += 1;
        }
        Ok(())
    }

    /// Sync the log builder, ensure all log data is written to disk
    // NOTE: this function is only used for test
    pub fn sync_log_builder(&self) -> Result<()> {
        let r = self.inner.read();
        let mut log_builder = r
            .log_builder
            .lock()
            .expect("[LsmStorage::sync_log_builder] get log builder lock fail");
        log_builder.sync_cur_log_file()
    }
}
