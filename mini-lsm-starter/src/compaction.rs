
use std::cmp::max;
use std::collections::HashSet;
use std::fs;
use std::sync::Arc;
use anyhow::{Error, Result};
use log::warn;
use parking_lot::Mutex;
use crate::iterators::{ StorageIterator};
use crate::iterators::merge_iterator::MergeIterator;
use crate::lsm_storage::BlockCache;
use crate::table::{SsTable, SsTableBuilder, SsTableIterator};
use crate::utils;


// TODO(zhr): need to be configured by Option file in the future
const L0_SIZE: usize = 2;
const AMPLIFICATION_FACTOR: usize = 2;

/// A Compaction encapsulates information about a compaction.
pub struct Compaction {
    level: usize,
    input_ss_tables: [Vec<Arc<SsTable>>; 2],
    // TODO(zhr): maybe need to be configured by Option file in the future, current use the default size(256MB)
    max_file_size: usize,
    /// The lower and upper bound for compaction
    left_key_bound: Vec<u8>,
    right_key_bound: Vec<u8>,
    upper_left_bound: usize,
    upper_right_bound: usize,
    lower_left_bound: usize,
    lower_right_bound: usize,
    next_sst_id: Arc<Mutex<usize>>,
    block_cache: Option<Arc<BlockCache>>,
}

impl Compaction {

    fn level_needs_compaction(level: usize, level_size: usize) -> bool {
        return if level == 0 {
            level_size >= L0_SIZE
        } else {
            level_size >= AMPLIFICATION_FACTOR.pow(level as u32)
        }
    }
    pub fn needs_compaction(l0: &Vec<Arc<SsTable>>, levels: &Vec<Vec<Arc<SsTable>>>) -> bool {
        if Compaction::level_needs_compaction(0, l0.len()) {
            return true;
        }
        for (level_id, level) in levels.iter().enumerate() {
            if Compaction::level_needs_compaction(level_id + 1, level.len()) {
                return true;
            }
        }
        false
    }

    /// pick the sstable in upper level for compaction
    /// rule: try to pick the older tables, the boundary is left-close and right-open
    fn pick_compaction(&mut self) {
        let level_size = self.input_ss_tables[0].len();
        // if level 0, pick all files
        if self.level == 0 {
            (self.upper_left_bound, self.upper_right_bound) = (0, level_size);
            return;
        }

        let excess_size = level_size as i32 - AMPLIFICATION_FACTOR.pow(self.level as i32 as u32);
        assert!(excess_size >= 0);
        if excess_size == 0 {
            self.upper_left_bound = 0;
            self.upper_right_bound = 0;
            return;
        }

        // find the oldest file
        let mut oldest_idx = 0;
        let mut oldest_value = usize::MAX;
        for (id, table) in self.input_ss_tables[0].iter().enumerate() {
            if table.get_sst_id() < oldest_value {
                oldest_value = table.get_sst_id();
                oldest_idx = id;
            }
        }

        // find the appropriate merge candidates around the oldest the ss-table
        if excess_size == 1 {
            (self.upper_left_bound, self.upper_right_bound) = (oldest_idx, oldest_idx + 1);
            return;
        }

        let mut left_bound = oldest_idx;
        let mut right_bound = oldest_idx + 1;
        while right_bound - left_bound < excess_size as usize {
            if left_bound > 0 && right_bound < level_size - 1 {
                if self.input_ss_tables[0][left_bound - 1].get_sst_id() < self.input_ss_tables[0][right_bound + 1].get_sst_id() {
                    left_bound -= 1;
                } else {
                    right_bound += 1;
                }
            } else if left_bound > 0 {
                left_bound -= 1;
            } else {
                right_bound += 1;
            }
        }

        (self.upper_left_bound, self.upper_right_bound) = (left_bound, right_bound);
    }


    /// get the key range for compaction in the upper level
    fn get_required_key_range(&mut self) {
        // if level is 0, all files should be evicted to the next level
        if self.level == 0 {
            for metadata in &self.input_ss_tables[0] {
                if self.left_key_bound.is_empty() {
                    self.left_key_bound = metadata.min_key();
                } else {
                    let candidate = metadata.min_key();
                    if candidate < self.left_key_bound {
                        self.left_key_bound = candidate;
                    }
                }
            }
            self.right_key_bound = vec![];
        }

        // if level is bigger than 0, the The latest n (excess count) of sstables
        //  need to be merged into the lower layer
        if !self.input_ss_tables[0].is_empty()  {
            self.left_key_bound = self.input_ss_tables[0][self.upper_left_bound].min_key();
            if self.upper_right_bound + 1 < self.input_ss_tables[0].len() {
                self.right_key_bound = self.input_ss_tables[0][self.upper_right_bound + 1].min_key();
            } else {
                self.right_key_bound = vec![];
            }
        }
        warn!("[Compaction::get_required_key_range] invalid key range");
    }

    /// get the overlapped lower range, and the bound is left-close, right-open
    fn get_overlapped_range(&mut self) {
        if self.input_ss_tables[1].is_empty() {
            (self.lower_left_bound, self.lower_right_bound) = (0, 0);
            return;
        }
        let mut left = 0;
        let mut id  = 0;
        let mut right = -1;
        let mut first = true;
        for table in &self.input_ss_tables[1] {
            if table.min_key() < self.left_key_bound {
                left += 1;
            } else if first && table.min_key() >= self.left_key_bound {
                left = max(id - 1, 0);
                first = false;
                if self.right_key_bound.is_empty() {
                    right = self.input_ss_tables[1].len() as i32;
                    break;
                }
            } else if table.min_key() > self.right_key_bound {
                right = id;
                break;
            }
            id += 1;
        }
        if right < 0 {
            right = self.input_ss_tables[1].len() as i32;
        }

        (self.lower_left_bound, self.lower_right_bound) = (left as usize, right as usize);
    }

    /// merge SsTables candidates in these two levels
    pub fn merge(&self) -> Result<Vec<Arc<SsTable>>> {
        let mut ss_table_iterators = vec![];
        let upper_ss_tables_slices = &self.input_ss_tables[0][self.upper_left_bound..self.upper_right_bound];
        for ss_table in upper_ss_tables_slices {
            ss_table_iterators.push(Box::new(SsTableIterator::create_and_seek_to_first(ss_table.to_owned()).unwrap()));
        }


        if self.lower_left_bound < self.lower_right_bound {
            let lower_ss_tables_slices = &self.input_ss_tables[1][self.lower_left_bound..self.lower_right_bound];
            for ss_table in lower_ss_tables_slices {
                ss_table_iterators.push(Box::new(SsTableIterator::create_and_seek_to_first(ss_table.to_owned()).unwrap()));
            }
        }

        // merge the ss-table candidates
        let mut merge_iterator = MergeIterator::create(ss_table_iterators);
        let mut deleted_elements = HashSet::new();
        let mut duplicated_elements = HashSet::new();
        let mut ss_table_builders = vec![];
        // XXX: for test. change from 4096 to 128
        let mut current_ss_table_builder = SsTableBuilder::new(128);
        let mut result = vec![];

        loop {
            if !merge_iterator.is_valid() {
                break;
            }

            let mut key = Vec::with_capacity(merge_iterator.key().len());
            key.extend_from_slice(merge_iterator.key());

            if merge_iterator.value().is_empty() {
                deleted_elements.insert(key);
                continue;
            }
            if deleted_elements.contains(merge_iterator.key()) || duplicated_elements.contains(merge_iterator.key()) {
                continue;
            }

            duplicated_elements.insert(key);

            current_ss_table_builder.add(merge_iterator.key(), merge_iterator.value());
            if current_ss_table_builder.estimated_size() >= self.max_file_size {
                let replaced_ss_table_builder = std::mem::replace(&mut current_ss_table_builder, SsTableBuilder::new(4096));
                ss_table_builders.push(replaced_ss_table_builder);
            }

            merge_iterator.next().unwrap();
        }

        // generate the new ss-tables
        result.reserve(ss_table_builders.len());
        {
            let mut next_sst_id = self.next_sst_id.lock();
            for (_, ss_table_builder) in ss_table_builders.into_iter().enumerate() {
                result.push(Arc::new(
                    ss_table_builder.build(*next_sst_id,
                                           self.block_cache.clone(),
                                           utils::generate_sst_name(None, *next_sst_id)
                    ).expect("[compaction::merge_sort] build new ss-table fail")));
                *next_sst_id += 1;
            }
        }


        Ok(result)
    }


    pub fn compact_two_levels(&mut self) -> Result<()> {
        // find the file to be compacted in the upper level
        self.pick_compaction();
        if self.upper_right_bound - self.upper_left_bound == 0 {
            return Err(Error::msg("[Compaction::compact_two_levels] no need for compaction"));
        }

        // find the upper key range for compaction
        self.get_required_key_range();

        // find the suitable range of the lower level
        self.get_overlapped_range();

        // merge the ss-table candidates
        if let Ok(merge_result) = self.merge() {
            self.uninstall_and_install_file(merge_result)?;
            return Ok(());
        }

        Err(Error::msg("[Compaction::compact_two_levels] compact two level fail"))
    }

    /// delete the expired files after compaction and generate the new level
    pub fn uninstall_and_install_file(&mut self, new_ss_tables: Vec<Arc<SsTable>>) -> Result<()> {
        let delete_file_func = |file_name| -> Result<()> {
            if let Err(err) = fs::remove_file(file_name) {
                return Err(Error::from(err));
            }
            Ok(())
        };

        // deleted the expired files
        if self.upper_right_bound > self.upper_left_bound {
            let upper_table_slices = &self.input_ss_tables[0][self.upper_left_bound..self.upper_right_bound];
            for table in upper_table_slices {
                delete_file_func(utils::generate_sst_name(None, table.get_sst_id())).expect("[Compression::delete_file_func] delete expired file fail");
            }
        }
        if self.lower_right_bound > self.lower_left_bound {
            let lower_table_slices = &self.input_ss_tables[1][self.lower_left_bound..self.lower_right_bound];
            for table in lower_table_slices {
                delete_file_func(utils::generate_sst_name(None, table.get_sst_id())).expect("[Compression::delete_file_func] delete expired file fail");
            }
        }

        // generate the new levels with the PARAM new_ss_tables
        self.input_ss_tables[0].drain(self.upper_left_bound..self.upper_right_bound);
        self.input_ss_tables[1] = self.input_ss_tables[1].splice(self.lower_left_bound..self.lower_right_bound, new_ss_tables).collect();
        Ok(())
    }

    /// judge whether the leveled compaction is stop after merging these two levels
    pub fn is_finished(&self) -> bool {
        Compaction::level_needs_compaction(self.level + 1, self.input_ss_tables[1].len())
    }

    pub fn generate_output(&self) -> [Vec<Arc<SsTable>>; 2] {
        self.input_ss_tables.clone()
    }
}


#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use tempfile::tempdir;
    use parking_lot::Mutex;
    use crate::compaction::Compaction;
    use crate::table::{SsTable, SsTableBuilder};


    fn key_of(idx: usize) -> Vec<u8> {
        format!("key_{:03}", idx * 5).into_bytes()
    }

    fn value_of(idx: usize) -> Vec<u8> {
        format!("value_{:010}", idx).into_bytes()
    }

    fn generate_sst(lower_bound: usize, upper_bound: usize, sst_id: usize) -> SsTable {
        let mut builder = SsTableBuilder::new(128);
        for idx in lower_bound..upper_bound {
            let key = key_of(idx);
            let value = value_of(idx);
            builder.add(&key[..], &value[..]);
        }
        let dir = tempdir().unwrap();
        let path = dir.path().join(sst_id.to_string() + ".sst");
        builder.build_for_test(path).unwrap()
    }

    #[test]
    fn test_compile() {
        // generate ss-table first
        let level0 = vec![Arc::new(generate_sst(0, 100, 6)), Arc::new(generate_sst(50, 150, 5)), Arc::new(generate_sst(100, 200, 4))];
        let level1 = vec![Arc::new(generate_sst(100, 200, 3)), Arc::new(generate_sst(200, 300, 2)), Arc::new(generate_sst(300, 400, 1))];


        let _compaction = Compaction {
            level: 0,
            input_ss_tables: [level0, level1],
            max_file_size: 0,
            left_key_bound: vec![],
            right_key_bound: vec![],
            upper_left_bound: 0,
            upper_right_bound: 0,
            lower_left_bound: 0,
            lower_right_bound: 0,
            next_sst_id: Arc::new(Mutex::new(1)),
            block_cache: None,
        };

        println!("compile success");
    }

    #[test]
    fn test_pick_compaction() {

    }
}
