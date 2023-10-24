#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use crate::iterators::merge_iterator::MergeIterator;
use anyhow::Result;
use bytes::Bytes;
use log::{debug, warn};
use std::collections::HashSet;
use std::ops::Bound;

use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::iterators::StorageIterator;
use crate::mem_table::MemTableIterator;
use crate::table::SsTableIterator;
use crate::utils::from_slice_to_bytes;

pub struct LsmIterator {
    inner_iterator:
        TwoMergeIterator<MergeIterator<MemTableIterator>, MergeIterator<SsTableIterator>>,
    deleted_elements: HashSet<Bytes>,
    duplicated_elements: HashSet<Bytes>,
    upper_bound: Bound<Bytes>, // indicate the upper bound for sst iterator
}

impl LsmIterator {
    pub fn new(
        iter: TwoMergeIterator<MergeIterator<MemTableIterator>, MergeIterator<SsTableIterator>>,
        raw_upper_bound: Bound<&[u8]>,
    ) -> Self {
        let mut lsm_iterator = Self {
            inner_iterator: iter,
            deleted_elements: HashSet::new(),
            duplicated_elements: HashSet::new(),
            upper_bound: from_slice_to_bytes(raw_upper_bound),
        };
        lsm_iterator
            .skip_deleted_duplicated()
            .expect("[LsmIterator::new] skip deleted fail when initializing");
        debug!("[LsmIterator::new] init finished");
        lsm_iterator
    }

    /// skip the deleted and duplicated entries indicated by the empty entry
    fn skip_deleted_duplicated(&mut self) -> Result<()> {
        if !self.is_valid()
            || ((!self.deleted_elements.contains(self.key()) && !self.value().is_empty())
                && !self.duplicated_elements.contains(self.key()))
        {
            return Ok(());
        }
        while self.is_valid()
            && (self.deleted_elements.contains(self.key())
                || self.value().is_empty()
                || self.duplicated_elements.contains(self.key()))
        {
            if self.value().is_empty() {
                self.deleted_elements
                    .insert(Bytes::copy_from_slice(self.key()));
            }
            self.inner_iterator
                .next()
                .expect("[LsmIterator::skip_deleted_duplicated] unable to call current iterator `next` method");
        }
        if self.is_valid() {
            self.duplicated_elements
                .insert(Bytes::copy_from_slice(self.key()));
        }
        debug!(
            "[LsmIterator::skip_deleted_duplicated] current key: {:?}, value: {:?}",
            String::from_utf8(self.key().to_vec()).unwrap(),
            String::from_utf8(self.value().to_vec()).unwrap()
        );
        Ok(())
    }
}

impl StorageIterator for LsmIterator {
    fn is_valid(&self) -> bool {
        self.inner_iterator.is_valid()
            && match self.upper_bound {
                Bound::Included(ref upper_bound) => self.key() <= upper_bound,
                Bound::Excluded(ref upper_bound) => self.key() < upper_bound,
                Bound::Unbounded => true,
            }
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
        self.skip_deleted_duplicated()
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
