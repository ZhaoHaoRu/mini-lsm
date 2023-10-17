use std::ops::Bound;
use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;
use crossbeam_skiplist::map::Entry;
use crossbeam_skiplist::SkipMap;
use log::warn;
use ouroboros::self_referencing;

use crate::iterators::StorageIterator;
use crate::table::SsTableBuilder;

/// A basic mem-table based on crossbeam-skiplist
pub struct MemTable {
    map: Arc<SkipMap<Bytes, Bytes>>,
}

impl MemTableIterator {
    /// copy from Chi's code
    fn entry_to_item(entry: Option<Entry<'_, Bytes, Bytes>>) -> (Bytes, Bytes) {
        entry
            .map(|x| (x.key().clone(), x.value().clone()))
            .unwrap_or_else(|| (Bytes::from_static(&[]), Bytes::from_static(&[])))
    }
}

impl MemTable {
    fn from_slice_to_bytes(former: Bound<&[u8]>) -> Bound<Bytes> {
        match former {
            Bound::Excluded(data) => Bound::Excluded(Bytes::copy_from_slice(data)),
            Bound::Included(data) => Bound::Included(Bytes::copy_from_slice(data)),
            Bound::Unbounded => Bound::Unbounded,
        }
    }

    /// Create a new mem-table.
    pub fn create() -> Self {
        Self {
            map: Arc::new(SkipMap::new()),
        }
    }

    /// Get a value by key.
    pub fn get(&self, key: &[u8]) -> Option<Bytes> {
        match self.map.get(key) {
            None => Option::None,
            Some(pair) => Some(Bytes::copy_from_slice(pair.value())),
        }
    }

    /// Put a key-value pair into the mem-table.
    pub fn put(&self, key: &[u8], value: &[u8]) {
        self.map
            .insert(Bytes::copy_from_slice(key), Bytes::copy_from_slice(value));
    }

    /// Get an iterator over a range of keys.
    pub fn scan(&self, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> MemTableIterator {
        // MemTableIterator::new(Arc::clone(&self.map), self.map.range(MemTable::from_slice_to_bytes(lower)..MemTable::from_slice_to_bytes(upper)), (item.key().to_owned(), item.value().to_owned()))
        let lower_bound = MemTable::from_slice_to_bytes(lower);
        let upper_bound = MemTable::from_slice_to_bytes(upper);
        let mut mem_table_iterator = MemTableIteratorBuilder {
            map: self.map.clone(),
            iter_builder: |map| map.range((lower_bound, upper_bound)),
            item: (Bytes::from_static(&[]), Bytes::from_static(&[])),
        }
        .build();
        let entry =
            mem_table_iterator.with_iter_mut(|iter| MemTableIterator::entry_to_item(iter.next()));
        mem_table_iterator.with_mut(|x| *x.item = entry);
        mem_table_iterator
    }

    /// Flush the mem-table to SSTable.
    pub fn flush(&self, builder: &mut SsTableBuilder) -> Result<()> {
        for entry in self.map.iter() {
            builder.add(entry.key(), entry.value());
        }
        Ok(())
    }
}

type SkipMapRangeIter<'a> =
    crossbeam_skiplist::map::Range<'a, Bytes, (Bound<Bytes>, Bound<Bytes>), Bytes, Bytes>;

/// An iterator over a range of `SkipMap`.
#[self_referencing]
pub struct MemTableIterator {
    map: Arc<SkipMap<Bytes, Bytes>>,
    #[borrows(map)]
    #[not_covariant]
    iter: SkipMapRangeIter<'this>,
    item: (Bytes, Bytes),
}

impl StorageIterator for MemTableIterator {
    fn value(&self) -> &[u8] {
        &self.borrow_item().1
    }

    fn key(&self) -> &[u8] {
        &self.borrow_item().0
    }

    fn is_valid(&self) -> bool {
        !self.borrow_item().0.is_empty()
    }

    fn next(&mut self) -> Result<()> {
        let mut source = (Bytes::new(), Bytes::new());
        let mut flag = false;
        self.with_iter_mut(|iter| match iter.next() {
            None => {
                warn!("[MemTableIterator::next] no available next element");
            }
            Some(pair) => {
                source = (pair.key().to_owned(), pair.value().to_owned());
                flag = true;
            }
        });
        self.with_item_mut(|entry| {
            if flag {
                entry.0 = source.0;
                entry.1 = source.1;
            } else {
                entry.0 = Bytes::new();
            }
        });
        Ok(())
    }
}

#[cfg(test)]
mod tests;
