#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use anyhow::{Error, Result};
use bytes::Bytes;
use moka::sync::Cache;
use parking_lot::RwLock;

use crate::block::Block;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::iterators::StorageIterator;
use crate::lsm_iterator::{FusedIterator, LsmIterator};
use crate::mem_table::MemTable;
use crate::table::{SsTable, SsTableBuilder, SsTableIterator};

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
    next_sst_id: usize,
    /// The block cache
    block_cache: Arc<BlockCache>,
}

impl LsmStorageInner {
    fn create() -> Self {
        Self {
            memtable: Arc::new(MemTable::create()),
            imm_memtables: vec![],
            l0_sstables: vec![],
            levels: vec![],
            next_sst_id: 1,
            //NOTE: the default cache size is 4GB
            block_cache: Arc::new(Cache::new(4 * 1024 * 1024 * 1024)),
        }
    }
}

/// The storage interface of the LSM tree.
pub struct LsmStorage {
    inner: Arc<RwLock<Arc<LsmStorageInner>>>,
    mutex: Mutex<i32>,
    path: PathBuf,
}

fn generate_sst_name(sst_id: usize) -> String {
    sst_id.to_string() + ".sst"
}

impl LsmStorage {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self {
            inner: Arc::new(RwLock::new(Arc::new(LsmStorageInner::create()))),
            mutex: Mutex::new(0),
            path: path.as_ref().to_path_buf(),
        })
    }

    /// Get a key from the storage. In day 7, this can be further optimized by using a bloom filter.
    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        let mut memtable_copy: Arc<MemTable> = Arc::new(MemTable::create());
        let mut imm_memtables_copy = vec![];
        let mut l0_sstables_copy = vec![];
        let mut levels_copy = vec![];

        // critic area: hold the read lock to copy all the pointer to mem-tables and sst out of the inner structure
        {
            let r1 = self.inner.read();
            memtable_copy = r1.memtable.clone();
            imm_memtables_copy = r1.imm_memtables.clone();
            for elem in r1.l0_sstables.clone().into_iter() {
                l0_sstables_copy.push(elem.clone());
            }
            for level in r1.levels.clone().into_iter() {
                let mut level_copy = vec![];
                for elem in level {
                    level_copy.push(elem);
                }
                levels_copy.push(level_copy);
            }
        }

        // step1: search the mem-table
        if let Some(mem_table_result) = memtable_copy.get(key) {
            if mem_table_result.is_empty() {
                return Ok(None);
            }
            return Ok(Some(mem_table_result));
        }

        // step2: search the immutable mem-tables from the latest to the earliest
        for imm_mem_table in imm_memtables_copy {
            if let Some(imm_mem_table_result) = imm_mem_table.get(key) {
                if imm_mem_table_result.is_empty() {
                    return Ok(None);
                }
                return Ok(Some(imm_mem_table_result));
            }
        }

        let search_in_sst = |table: Arc<SsTable>| -> Result<Option<Bytes>> {
            if let Ok(ss_table_iter) = SsTableIterator::create_and_seek_to_key(table, key) {
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
        for ss_table in l0_sstables_copy {
            if let Ok(result) = search_in_sst(ss_table) {
                return Ok(result);
            }
        }

        // step4: search from level 1 to n
        for level in levels_copy {
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
        assert!(!value.is_empty(), "value cannot be empty");
        assert!(!key.is_empty(), "key cannot be empty");
        let w = self.inner.write();
        w.memtable.put(key, value);
        Ok(())
    }

    /// Remove a key from the storage by writing an empty value.
    pub fn delete(&self, _key: &[u8]) -> Result<()> {
        let w = self.inner.write();
        w.memtable.put(_key, &[]);
        Ok(())
    }

    /// Persist data to disk.
    ///
    /// In day 3: flush the current memtable to disk as L0 SST.
    /// In day 6: call `fsync` on WAL.
    pub fn sync(&self) -> Result<()> {
        let _unused = self
            .mutex
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
            for (_, ss_table_builder) in ss_table_builders.into_iter().enumerate() {
                ss_table_builder
                    .build(
                        object.next_sst_id,
                        Some(object.block_cache.clone()),
                        self.path.join(generate_sst_name(object.next_sst_id)),
                    )
                    .unwrap();
                object.next_sst_id += 1;
            }
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
        let mut memtable_copy: Arc<MemTable> = Arc::new(MemTable::create());
        let mut imm_memtables_copy = vec![];
        let mut l0_sstables_copy = vec![];
        let mut levels_copy = vec![];

        // critic area: hold the read lock to copy all the pointer to mem-tables and sst out of the inner structure
        {
            let r1 = self.inner.read();
            memtable_copy = r1.memtable.clone();
            imm_memtables_copy = r1.imm_memtables.clone();
            for elem in &r1.l0_sstables {
                l0_sstables_copy.push(elem.clone());
            }
            for level in r1.levels.clone().into_iter() {
                let mut level_copy = vec![];
                for elem in level {
                    level_copy.push(elem);
                }
                levels_copy.push(level_copy);
            }
        }

        // create scan iterators
        let mut mem_table_iterators = vec![];
        mem_table_iterators.push(Box::new(memtable_copy.scan(_lower, _upper)));
        for mem_table in imm_memtables_copy {
            mem_table_iterators.push(Box::new(mem_table.scan(_lower, _upper)));
        }

        // generate the boundary key for SsTable
        let mut boundary = vec![];
        match _lower {
            Bound::Excluded(value) => {
                boundary = Vec::from(value);
                let len = boundary.len();
                if len > 0 {
                    boundary[len - 1] = 255;
                }
            }
            Bound::Included(value) => {
                boundary = Vec::from(value);
            }
            Bound::Unbounded => {}
        }

        let mut ss_table_iterators = vec![];
        for ss_table in l0_sstables_copy {
            ss_table_iterators.push(Box::new(SsTableIterator::create_and_seek_to_key(
                ss_table,
                &boundary[..],
            )?));
        }
        for level in levels_copy.clone().into_iter() {
            for ss_table in level {
                ss_table_iterators.push(Box::new(SsTableIterator::create_and_seek_to_key(
                    ss_table, &boundary,
                )?));
            }
        }

        Ok(FusedIterator::new(LsmIterator::new(
            TwoMergeIterator::create(
                MergeIterator::create(mem_table_iterators),
                MergeIterator::create(ss_table_iterators),
            )
            .unwrap(),
        )))
    }
}
