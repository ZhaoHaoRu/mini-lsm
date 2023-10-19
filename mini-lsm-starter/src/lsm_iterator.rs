#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use crate::iterators::merge_iterator::MergeIterator;
use anyhow::Result;
use bytes::Bytes;
use log::warn;
use std::collections::HashSet;

use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::iterators::StorageIterator;
use crate::mem_table::MemTableIterator;
use crate::table::SsTableIterator;

pub struct LsmIterator {
    inner_iterator:
        TwoMergeIterator<MergeIterator<MemTableIterator>, MergeIterator<SsTableIterator>>,
    deleted_elements: HashSet<Bytes>,
}

impl LsmIterator {
    pub fn new(
        iter: TwoMergeIterator<MergeIterator<MemTableIterator>, MergeIterator<SsTableIterator>>,
    ) -> Self {
        let mut lsm_iterator = Self {
            inner_iterator: iter,
            deleted_elements: HashSet::new(),
        };
        lsm_iterator
            .skip_deleted()
            .expect("[LsmIterator::new] skip deleted fail when initializing");
        lsm_iterator
    }

    /// skip the deleted entries indicated by the empty entry
    fn skip_deleted(&mut self) -> Result<()> {
        if !self.is_valid()
            || (!self.deleted_elements.contains(self.key()) && !self.value().is_empty())
        {
            return Ok(());
        }
        while self.is_valid()
            && (self.deleted_elements.contains(self.key()) || self.value().is_empty())
        {
            if self.value().is_empty() {
                self.deleted_elements
                    .insert(Bytes::copy_from_slice(self.key()));
            }
            if let Err(error) = self.next() {
                return Err(error);
            }
        }
        Ok(())
    }
}

impl StorageIterator for LsmIterator {
    fn is_valid(&self) -> bool {
        self.inner_iterator.is_valid()
    }

    fn key(&self) -> &[u8] {
        self.inner_iterator.key()
    }

    fn value(&self) -> &[u8] {
        self.inner_iterator.value()
    }

    fn next(&mut self) -> Result<()> {
        self.inner_iterator
            .next()
            .expect("[LsmIterator::next] unable to call current iterator `next` method");
        self.skip_deleted()
    }
}

/// A wrapper around existing iterator, will prevent users from calling `next` when the iterator is
/// invalid.
pub struct FusedIterator<I: StorageIterator> {
    iter: I,
}

impl<I: StorageIterator> FusedIterator<I> {
    pub fn new(iter: I) -> Self {
        Self { iter }
    }
}

impl<I: StorageIterator> StorageIterator for FusedIterator<I> {
    fn is_valid(&self) -> bool {
        self.iter.is_valid()
    }

    fn key(&self) -> &[u8] {
        // filter empty
        self.iter.key()
    }

    fn value(&self) -> &[u8] {
        self.iter.value()
    }

    fn next(&mut self) -> Result<()> {
        match self.iter.next() {
            Ok(result) => Ok(result),
            Err(_) => {
                warn!("[FusedIterator::next] no available next element");
                Ok(())
            }
        }
    }
}
