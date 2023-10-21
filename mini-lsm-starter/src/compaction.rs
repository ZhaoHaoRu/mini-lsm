
use std::cmp::max;
use std::collections::HashSet;
use std::sync::Arc;
use anyhow::Error;
use log::warn;
use crate::iterators::{ StorageIterator};
use crate::iterators::merge_iterator::MergeIterator;
use crate::lsm_storage::BlockCache;
use crate::table::{SsTable, SsTableBuilder, SsTableIterator};
use crate::utils;


/// A Compaction encapsulates information about a compaction.
pub struct Compaction {
    level: usize,
    input_ss_tables: [Vec<Arc<SsTable>>; 2],
    grandparents: Option<Vec<Arc<SsTable>>>,
    // TODO(zhr): maybe need to be configured by Option file in the future, current use the default size(256MB)
    max_file_size: usize,
    /// The lower and upper bound for compaction
    lower: Vec<u8>,
    upper: Vec<u8>,
    next_sst_id: Arc<usize>,
    block_cache: Arc<BlockCache>,
}

impl Compaction {
    /// pick the sstable in upper level for compaction
    /// rule: try to pick the older tables, the boundary is left-close and right-open
    fn pick_compaction(&self) -> (usize, usize) {
        let level_size = self.input_ss_tables[0].len();
        let excess_size = level_size as i32 - 10i32.pow(self.level as i32 as u32);
        assert!(excess_size >= 0);
        if excess_size == 0 {
            return (0, 0);
        }

        // find the oldest file
        let mut oldest_idx = 0;
        let mut oldest_value = *self.next_sst_id;
        for (id, table) in self.input_ss_tables[0].iter().enumerate() {
            if table.get_sst_id() < oldest_value {
                oldest_value = table.get_sst_id();
                oldest_idx = id;
            }
        }

        // find the appropriate merge candidates around the oldest the ss-table
        if excess_size == 1 {
            return (oldest_idx, oldest_idx + 1);
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

        (left_bound, right_bound)
    }


    /// get the key range for compaction in the upper level
    fn get_required_key_range(&mut self) {
        // if level is 0, all files should be evicted to the next level
        if self.level == 0 {
            for metadata in self.input_ss_tables[0] {
                if self.lower.is_empty() {
                    self.lower = metadata.min_key();
                } else {
                    let candidate = metadata.min_key();
                    if candidate < self.lower {
                        self.lower = candidate;
                    }
                }
            }
            self.upper = vec![];
        }

        // if level is bigger than 0, the The latest n (excess count) of sstables
        //  need to be merged into the lower layer
        if !self.input_ss_tables[0].is_empty()  {
            self.lower = self.input_ss_tables[0][0].min_key();
        }
        warn!("[Compaction::get_required_key_range] invalid key range");
    }

    /// get the overlapped lower range, and the bound is left-close, right-open
    fn get_overlapped_range(&self) -> (usize, usize) {
        if self.input_ss_tables[1].is_empty() {
            return (0, 0);
        }
        let mut left = 0;
        let mut id  = 0;
        let mut right = -1;
        let mut first = true;
        for table in self.input_ss_tables[1] {
            if table.min_key() < self.lower {
                left += 1;
            } else if first && table.min_key() >= self.lower {
                left = max(id - 1, 0);
                first = false;
                if self.upper.is_empty() {
                    right = self.input_ss_tables[1].len() as i32;
                    break;
                }
            } else if table.min_key() > self.upper {
                right = id;
                break;
            }
            id += 1;
        }
        if right < 0 {
            right = self.input_ss_tables[1].len() as i32;
        }

        (left as usize, right as usize)
    }

    /// merge SsTables candidates in these two levels
    pub fn compact_two_level(&self) -> Result<Vec<Arc<SsTable>>, anyhow> {
        let mut ss_table_iterators = vec![];
        for ss_table in self.input_ss_tables[0] {
            ss_table_iterators.push(Box::new(SsTableIterator::create_and_seek_to_first(ss_table).unwrap()));
        }

        let (left_bound, right_bound) = self.get_overlapped_range();
        if left_bound < right_bound {
            let lower_ss_tables_slices = &self.input_ss_tables[1][left_bound..right_bound];
            for ss_table in lower_ss_tables_slices {
                ss_table_iterators.push(Box::new(SsTableIterator::create_and_seek_to_first(ss_table.to_owned()).unwrap()));
            }
        }

        // merge the ss-table candidates
        let mut merge_iterator = MergeIterator::create(ss_table_iterators);
        let mut deleted_elements = HashSet::new();
        let mut duplicated_elements = HashSet::new();
        let mut ss_table_builders = vec![];
        let mut current_ss_table_builder = SsTableBuilder::new(4096);
        let mut result = vec![];

        while let Ok(result) = merge_iterator.next() {
            if merge_iterator.value().is_empty() {
                deleted_elements.insert(merge_iterator.key());
                continue;
            }
            if deleted_elements.contains(merge_iterator.key()) || duplicated_elements.contains(merge_iterator.key()) {
                continue;
            }
            duplicated_elements.insert(merge_iterator.key());

            current_ss_table_builder.add(merge_iterator.key(), merge_iterator.value());
            if current_ss_table_builder.estimated_size() >= self.max_file_size {
                let replaced_ss_table_builder = std::mem::replace(&mut current_ss_table_builder, SsTableBuilder::new(4096));
                ss_table_builders.push(replaced_ss_table_builder);
            }
        }

        // generate the new ss-tables
        result.reserve(ss_table_builders.len());
        for (_, ss_table_builder) in ss_table_builders.into_iter().enumerate() {
            result.push(Arc::new(
                ss_table_builder.build(*self.next_sst_id,
                                       Some(self.block_cache.clone()),
                                       utils::generate_sst_name(None, *self.next_sst_id)
                ).expect("[compaction::merge_sort] build new ss-table fail")));
        }

        Ok(result)
    }

    pub fn uninstall_deleted_file(&self) {

    }
}





#[cfg(test)]
mod tests {

    use super::Compaction;
    // fn compile_test() {
    //     _ = Compaction{
    //         level: 0,
    //         input_file_metadata: [],
    //         grandparents: None,
    //         max_file_size: 0,
    //     };
    //     println!("hello world");
    // }

}