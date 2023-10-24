use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::StorageIterator;
use crate::lsm_storage::BlockCache;
use crate::table::{SsTable, SsTableBuilder, SsTableIterator};
use crate::utils;
use anyhow::{Error, Result};
use log::{debug, warn};
use std::cmp::max;
use std::collections::HashSet;
use std::fs;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

// TODO(zhr): need to be configured by Option file in the future
const L0_SIZE: usize = 2;
const AMPLIFICATION_FACTOR: usize = 2;

const BLOCK_SIZE: usize = 4096;

/// A Compaction encapsulates information about a compaction.
pub struct Compaction {
    level: usize,
    input_ss_tables: [Vec<Arc<SsTable>>; 2],
    // TODO(zhr): maybe need to be configured by Option file in the future, current use the default size(256MB)
    max_file_size: usize,
    /// The lower and upper key bound for compaction
    left_key_bound: Vec<u8>,
    right_key_bound: Vec<u8>,
    /// The lower and upper table bound for compaction
    upper_left_bound: usize,
    upper_right_bound: usize,
    lower_left_bound: usize,
    lower_right_bound: usize,
    /// The next SSTable ID.
    next_sst_id: Arc<Mutex<usize>>,
    /// The block cache
    block_cache: Option<Arc<BlockCache>>,
    /// The path of the storage
    dir: PathBuf,
}

impl Compaction {
    pub fn new(
        level: usize,
        input: [Vec<Arc<SsTable>>; 2],
        sst_id: Arc<Mutex<usize>>,
        block_cache: Option<Arc<BlockCache>>,
        dir: PathBuf,
    ) -> Self {
        Compaction {
            level,
            input_ss_tables: input,
            max_file_size: 0,
            left_key_bound: vec![],
            right_key_bound: vec![],
            upper_left_bound: 0,
            upper_right_bound: 0,
            lower_left_bound: 0,
            lower_right_bound: 0,
            next_sst_id: sst_id,
            block_cache,
            dir,
        }
    }

    fn level_needs_compaction(level: usize, level_size: usize) -> bool {
        if level == 0 {
            level_size >= L0_SIZE
        } else {
            level_size >= AMPLIFICATION_FACTOR.pow(level as u32)
        }
    }

    pub fn needs_compaction(l0: &Vec<Arc<SsTable>>, levels: &[Vec<Arc<SsTable>>]) -> (bool, usize) {
        if Compaction::level_needs_compaction(0, l0.len()) {
            return (true, 0);
        }
        for (level_id, level) in levels.iter().enumerate() {
            if Compaction::level_needs_compaction(level_id + 1, level.len()) {
                return (true, level_id + 1);
            }
        }
        (false, 0)
    }

    /// pick the ss-table in upper level for compaction
    /// rule: try to pick the older tables, the boundary is left-close and right-open
    fn pick_compaction(&mut self) {
        let level_size = self.input_ss_tables[0].len();
        // if level 0, pick all files
        if self.level == 0 {
            (self.upper_left_bound, self.upper_right_bound) = (0, level_size);
            return;
        }

        let excess_size =
            level_size as i32 - (AMPLIFICATION_FACTOR as i32).pow(self.level as i32 as u32);
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
                if self.input_ss_tables[0][left_bound - 1].get_sst_id()
                    < self.input_ss_tables[0][right_bound + 1].get_sst_id()
                {
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
        if !self.input_ss_tables[0].is_empty() {
            self.left_key_bound = self.input_ss_tables[0][self.upper_left_bound].min_key();
            if self.upper_right_bound < self.input_ss_tables[0].len() {
                self.right_key_bound = self.input_ss_tables[0][self.upper_right_bound].min_key();
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
        let mut right = -1;
        let mut first = true;
        for (id, table) in self.input_ss_tables[1].iter().enumerate() {
            if table.min_key() < self.left_key_bound {
                left += 1;
            } else if first && table.min_key() >= self.left_key_bound {
                left = max(id as i32 - 1, 0);
                first = false;
                if self.right_key_bound.is_empty() {
                    right = self.input_ss_tables[1].len() as i32;
                    break;
                }
            } else if table.min_key() > self.right_key_bound {
                right = id as i32;
                break;
            }
        }
        if right < 0 {
            right = self.input_ss_tables[1].len() as i32;
        }

        (self.lower_left_bound, self.lower_right_bound) = (left as usize, right as usize);
    }

    /// merge SsTables candidates in these two levels
    pub fn merge(&self) -> Result<Vec<Arc<SsTable>>> {
        let mut ss_table_iterators = vec![];
        let upper_ss_tables_slices =
            &self.input_ss_tables[0][self.upper_left_bound..self.upper_right_bound];
        for ss_table in upper_ss_tables_slices {
            ss_table_iterators.push(Box::new(
                SsTableIterator::create_and_seek_to_first(ss_table.to_owned()).unwrap(),
            ));
        }

        if self.lower_left_bound < self.lower_right_bound {
            let lower_ss_tables_slices =
                &self.input_ss_tables[1][self.lower_left_bound..self.lower_right_bound];
            for ss_table in lower_ss_tables_slices {
                ss_table_iterators.push(Box::new(
                    SsTableIterator::create_and_seek_to_first(ss_table.to_owned()).unwrap(),
                ));
            }
        }

        // merge the ss-table candidates
        let mut merge_iterator = MergeIterator::create(ss_table_iterators);
        let mut deleted_elements = HashSet::new();
        let mut duplicated_elements = HashSet::new();
        let mut ss_table_builders = vec![];
        // XXX: for test. change from 4096 to 128
        let mut current_ss_table_builder = SsTableBuilder::new(BLOCK_SIZE);
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
            if deleted_elements.contains(merge_iterator.key())
                || duplicated_elements.contains(merge_iterator.key())
            {
                continue;
            }

            debug!(
                "[Compaction::merge] current key: {:?}",
                String::from_utf8(key.clone())
            );
            duplicated_elements.insert(key);

            current_ss_table_builder.add(merge_iterator.key(), merge_iterator.value());
            debug!(
                "[Compaction::merge] the estimated size: {}, file's max size: {}",
                current_ss_table_builder.estimated_size(),
                self.max_file_size
            );
            if current_ss_table_builder.estimated_size() >= self.max_file_size {
                let replaced_ss_table_builder =
                    std::mem::replace(&mut current_ss_table_builder, SsTableBuilder::new(4096));
                ss_table_builders.push(replaced_ss_table_builder);
            }

            merge_iterator.next().unwrap();
        }

        if current_ss_table_builder.estimated_size() > 0 {
            ss_table_builders.push(current_ss_table_builder);
        }

        // generate the new ss-tables
        result.reserve(ss_table_builders.len());
        {
            let mut next_sst_id = self.next_sst_id.lock().unwrap();
            for (_, ss_table_builder) in ss_table_builders.into_iter().enumerate() {
                result.push(Arc::new(
                    ss_table_builder
                        .build(
                            *next_sst_id,
                            self.block_cache.clone(),
                            utils::generate_sst_name(Some(&self.dir), *next_sst_id),
                        )
                        .expect("[compaction::merge_sort] build new ss-table fail"),
                ));
                *next_sst_id += 1;
            }
        }

        Ok(result)
    }

    pub fn compact_two_levels(&mut self) -> Result<()> {
        // find the file to be compacted in the upper level
        self.pick_compaction();
        if self.upper_right_bound - self.upper_left_bound == 0 {
            return Err(Error::msg(
                "[Compaction::compact_two_levels] no need for compaction",
            ));
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

        Err(Error::msg(
            "[Compaction::compact_two_levels] compact two level fail",
        ))
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
            let upper_table_slices =
                &self.input_ss_tables[0][self.upper_left_bound..self.upper_right_bound];
            for table in upper_table_slices {
                delete_file_func(utils::generate_sst_name(
                    Some(&self.dir),
                    table.get_sst_id(),
                ))
                .expect("[Compression::delete_file_func] delete expired file fail");
            }
        }
        if self.lower_right_bound > self.lower_left_bound {
            let lower_table_slices =
                &self.input_ss_tables[1][self.lower_left_bound..self.lower_right_bound];
            for table in lower_table_slices {
                delete_file_func(utils::generate_sst_name(
                    Some(&self.dir),
                    table.get_sst_id(),
                ))
                .expect("[Compression::delete_file_func] delete expired file fail");
            }
        }

        // generate the new levels with the PARAM new_ss_tables
        self.input_ss_tables[0].drain(self.upper_left_bound..self.upper_right_bound);

        // XXX(zhr): the splice method is not effective, has to delete first and then insert by iterating
        self.input_ss_tables[1].drain(self.lower_left_bound..self.lower_right_bound);
        for (i, new_table) in new_ss_tables.into_iter().enumerate() {
            self.input_ss_tables[1].insert(self.lower_left_bound + i, new_table);
        }
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
    use crate::compaction::Compaction;
    use crate::iterators::StorageIterator;
    use crate::table::{SsTable, SsTableIterator};
    use crate::tests::utils::{generate_sst, key_of, new_value_of, value_of};
    use log::LevelFilter;
    use std::env;
    use std::path::Path;
    use std::sync::{Arc, Mutex};
    use tempfile::tempdir;

    fn basic_setup(dir: &Path) -> Compaction {
        let level0 = vec![
            Arc::new(generate_sst(0, 100, 6, false, dir)),
            Arc::new(generate_sst(50, 150, 5, false, dir)),
            Arc::new(generate_sst(100, 200, 4, false, dir)),
        ];
        let level1 = vec![
            Arc::new(generate_sst(100, 200, 3, false, dir)),
            Arc::new(generate_sst(200, 300, 2, false, dir)),
            Arc::new(generate_sst(300, 400, 1, false, dir)),
        ];
        Compaction {
            level: 0,
            input_ss_tables: [level0, level1],
            max_file_size: 4 * 1024 * 1024,
            left_key_bound: vec![],
            right_key_bound: vec![],
            upper_left_bound: 0,
            upper_right_bound: 0,
            lower_left_bound: 0,
            lower_right_bound: 0,
            next_sst_id: Arc::new(Mutex::new(10)),
            block_cache: None,
            dir: dir.to_path_buf(),
        }
    }

    fn basic_setup2(dir: &Path) -> Compaction {
        let level1 = vec![
            Arc::new(generate_sst(100, 200, 7, false, dir)),
            Arc::new(generate_sst(200, 300, 6, false, dir)),
            Arc::new(generate_sst(300, 400, 8, false, dir)),
        ];
        let level2 = vec![
            Arc::new(generate_sst(100, 200, 3, false, dir)),
            Arc::new(generate_sst(300, 400, 2, false, dir)),
            Arc::new(generate_sst(400, 500, 1, false, dir)),
        ];
        Compaction {
            level: 1,
            input_ss_tables: [level1, level2],
            max_file_size: 4 * 1024 * 1024,
            left_key_bound: vec![],
            right_key_bound: vec![],
            upper_left_bound: 0,
            upper_right_bound: 0,
            lower_left_bound: 0,
            lower_right_bound: 0,
            next_sst_id: Arc::new(Mutex::new(10)),
            block_cache: None,
            dir: dir.to_path_buf(),
        }
    }

    fn complicated_setup1(dir: &Path) -> Compaction {
        let level1 = vec![
            Arc::new(generate_sst(100, 200, 7, true, dir)),
            Arc::new(generate_sst(200, 300, 6, true, dir)),
            Arc::new(generate_sst(400, 500, 8, true, dir)),
        ];
        let level2 = vec![
            Arc::new(generate_sst(150, 250, 3, false, dir)),
            Arc::new(generate_sst(250, 450, 2, false, dir)),
            Arc::new(generate_sst(450, 600, 1, false, dir)),
        ];

        Compaction {
            level: 1,
            input_ss_tables: [level1, level2],
            max_file_size: 4 * 1024 * 1024,
            left_key_bound: vec![],
            right_key_bound: vec![],
            upper_left_bound: 0,
            upper_right_bound: 0,
            lower_left_bound: 0,
            lower_right_bound: 0,
            next_sst_id: Arc::new(Mutex::new(11)),
            block_cache: None,
            dir: dir.to_path_buf(),
        }
    }

    fn check_sst_table_content(tables: &Vec<Arc<SsTable>>, start: usize) {
        if tables.is_empty() {
            return;
        }
        let mut counter = start;
        for table in tables {
            let mut iter = SsTableIterator::create_and_seek_to_first(table.clone()).unwrap();
            loop {
                if !iter.is_valid() {
                    break;
                }
                let key = iter.key();
                let value = iter.value();
                assert_eq!(
                    key,
                    key_of(counter),
                    "expected key: {:?}, actual key: {:?}",
                    String::from_utf8(key_of(counter)),
                    String::from_utf8(Vec::from(key))
                );
                assert_eq!(
                    value,
                    value_of(counter),
                    "expected value: {:?}, actual value: {:?}",
                    String::from_utf8(value_of(counter)),
                    String::from_utf8(Vec::from(value))
                );
                iter.next().unwrap();
                counter += 1;
            }
        }
    }

    #[test]
    fn test_compile() {
        // generate ss-table first
        let dir = tempdir().unwrap().path().to_path_buf();
        basic_setup(&dir);
        println!("compile success");
    }

    #[test]
    fn test_pick_compaction() {
        // generate ss-table first
        let dir = tempdir().unwrap().path().to_path_buf();
        let mut compaction = basic_setup(&dir);

        compaction.pick_compaction();
        assert_eq!(compaction.upper_left_bound, 0);
        assert_eq!(compaction.upper_right_bound, 3);

        compaction.level = 1;
        compaction.pick_compaction();
        assert_eq!(compaction.upper_left_bound, 2);
        assert_eq!(compaction.upper_right_bound, 3);
    }

    #[test]
    fn test_get_required_key_range() {
        // generate ss-table first
        let dir = tempdir().unwrap().path().to_path_buf();
        let mut compaction = basic_setup(&dir);

        compaction.pick_compaction();
        compaction.get_required_key_range();
        assert_eq!(compaction.left_key_bound, key_of(0));
        assert_eq!(compaction.right_key_bound, &[]);

        compaction.level = 1;
        compaction.pick_compaction();
        compaction.get_required_key_range();
        assert_eq!(compaction.left_key_bound, key_of(100));
        assert_eq!(compaction.right_key_bound, &[]);

        compaction = basic_setup2(&dir);
        compaction.pick_compaction();
        assert_eq!(compaction.upper_left_bound, 1);
        assert_eq!(compaction.upper_right_bound, 2);
        compaction.get_required_key_range();
        assert_eq!(compaction.left_key_bound, key_of(200));
        assert_eq!(compaction.right_key_bound, key_of(300));
    }

    #[test]
    fn test_get_overlapped_range() {
        // generate ss-table first
        let dir = tempdir().unwrap().path().to_path_buf();
        let mut compaction = basic_setup(&dir);

        compaction.pick_compaction();
        compaction.get_required_key_range();
        compaction.get_overlapped_range();
        assert_eq!(compaction.lower_left_bound, 0);
        assert_eq!(compaction.lower_right_bound, 3);

        compaction = basic_setup2(&dir);
        compaction.level = 0;
        compaction.pick_compaction();
        compaction.get_required_key_range();
        compaction.get_overlapped_range();
        assert_eq!(compaction.lower_left_bound, 0);
        assert_eq!(compaction.lower_right_bound, 3);

        compaction.level = 1;
        compaction.pick_compaction();
        compaction.get_required_key_range();
        compaction.get_overlapped_range();
        assert_eq!(compaction.lower_left_bound, 0);
        assert_eq!(compaction.lower_right_bound, 2);
    }

    #[test]
    fn test_merge_micro() {
        // generate ss-table first
        let dir = tempdir().unwrap().path().to_path_buf();
        let mut compaction = basic_setup(&dir);

        log::set_max_level(LevelFilter::Debug);

        compaction.pick_compaction();
        compaction.get_required_key_range();
        compaction.get_overlapped_range();
        let result = compaction.merge().unwrap();
        println!("the merge result: {}", result.len());
        check_sst_table_content(&result, 0);

        compaction = basic_setup2(&dir);
        compaction.level = 0;
        compaction.pick_compaction();
        compaction.get_required_key_range();
        compaction.get_overlapped_range();
        let result = compaction.merge().unwrap();
        check_sst_table_content(&result, 100);

        compaction.level = 1;
        compaction.pick_compaction();
        compaction.get_required_key_range();
        compaction.get_overlapped_range();
        let result = compaction.merge().unwrap();
        assert!(!result.is_empty());
        assert_eq!(result[0].min_key(), key_of(100));
        check_sst_table_content(&result, 100);
    }

    #[test]
    fn test_merge_more_complicated() {
        let dir = tempdir().unwrap().path().to_path_buf();
        let mut compaction = complicated_setup1(&dir);
        compaction.level = 0;
        log::set_max_level(LevelFilter::Debug);

        compaction.pick_compaction();
        assert_eq!(compaction.upper_left_bound, 0);
        assert_eq!(compaction.upper_right_bound, 3);

        compaction.get_required_key_range();
        assert_eq!(compaction.left_key_bound, key_of(100));
        assert_eq!(compaction.right_key_bound, &[]);

        compaction.get_overlapped_range();
        assert_eq!(compaction.lower_left_bound, 0);
        assert_eq!(compaction.lower_right_bound, 3);

        let result = compaction.merge().unwrap();
        assert!(!result.is_empty());
        assert_eq!(result[0].min_key(), key_of(100));

        let check_updated_content =
            |iter: &mut SsTableIterator, lower_bound: usize, upper_bound: usize, is_new: bool| {
                for i in lower_bound..upper_bound {
                    if !iter.is_valid() {
                        break;
                    }
                    let key = iter.key();
                    let value = iter.value();
                    assert_eq!(
                        key,
                        key_of(i),
                        "expected key: {:?}, actual key: {:?}",
                        String::from_utf8(key_of(i)),
                        String::from_utf8(Vec::from(key))
                    );
                    if is_new {
                        assert_eq!(
                            value,
                            new_value_of(i),
                            "expected value: {:?}, actual value: {:?}",
                            String::from_utf8(new_value_of(i)),
                            String::from_utf8(Vec::from(value))
                        );
                    } else {
                        assert_eq!(
                            value,
                            value_of(i),
                            "expected value: {:?}, actual value: {:?}",
                            String::from_utf8(value_of(i)),
                            String::from_utf8(Vec::from(value))
                        );
                    }
                    iter.next().unwrap();
                }
            };

        let mut iter = SsTableIterator::create_and_seek_to_first(result[0].clone()).unwrap();
        check_updated_content(&mut iter, 100, 300, true);
        check_updated_content(&mut iter, 300, 400, false);
        check_updated_content(&mut iter, 400, 500, true);
        check_updated_content(&mut iter, 500, 600, false);
    }

    #[test]
    fn test_uninstall_and_install_file() {
        let dir = env::current_dir().unwrap();
        let mut compaction = basic_setup(&dir);
        compaction.pick_compaction();
        compaction.get_required_key_range();
        compaction.get_overlapped_range();
        let result = compaction.merge().unwrap();
        compaction.uninstall_and_install_file(result).unwrap();
        assert_eq!(compaction.input_ss_tables[0].len(), 0);
        assert_eq!(compaction.input_ss_tables[1].len(), 1);
        assert_eq!(compaction.input_ss_tables[1][0].get_sst_id(), 10);
        {
            let next_sst_id = compaction.next_sst_id.lock().unwrap();
            assert_eq!(*next_sst_id, 11);
        }

        compaction = basic_setup2(&dir);
        compaction.pick_compaction();
        compaction.get_required_key_range();
        compaction.get_overlapped_range();
        let result = compaction.merge().unwrap();
        compaction.uninstall_and_install_file(result).unwrap();
        assert_eq!(compaction.input_ss_tables[0].len(), 2);
        assert_eq!(compaction.input_ss_tables[1].len(), 2);
        assert_eq!(compaction.input_ss_tables[1][0].get_sst_id(), 10);
        {
            let next_sst_id = compaction.next_sst_id.lock().unwrap();
            assert_eq!(*next_sst_id, 11);
        }

        compaction = complicated_setup1(&dir);
        compaction.pick_compaction();
        compaction.get_required_key_range();
        compaction.get_overlapped_range();
        let result = compaction.merge().unwrap();
        compaction.uninstall_and_install_file(result).unwrap();
        assert_eq!(compaction.input_ss_tables[0].len(), 2);
        assert_eq!(compaction.input_ss_tables[1].len(), 2);
        assert_eq!(compaction.input_ss_tables[1][0].get_sst_id(), 11);
        assert_eq!(compaction.input_ss_tables[1][1].get_sst_id(), 1);
    }
}
