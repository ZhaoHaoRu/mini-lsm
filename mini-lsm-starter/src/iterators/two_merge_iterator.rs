use anyhow::Result;

use super::StorageIterator;

/// Merges two iterators of different types into one. If the two iterators have the same key, only
/// produce the key once and prefer the entry from A.
pub struct TwoMergeIterator<A: StorageIterator, B: StorageIterator> {
    a: A,
    b: B,
    // Add fields as need
}

impl<A: StorageIterator, B: StorageIterator> TwoMergeIterator<A, B> {
    pub fn create(a: A, b: B) -> Result<Self> {
        Ok(Self { a, b })
    }
}

impl<A: StorageIterator, B: StorageIterator> StorageIterator for TwoMergeIterator<A, B> {
    fn key(&self) -> &[u8] {
        if !self.a.is_valid() && !self.b.is_valid() {
            return &[];
        }
        if !self.a.is_valid() {
            return self.b.key();
        }
        if !self.b.is_valid() {
            return self.a.key();
        }
        if self.a.key() <= self.b.key() {
            return self.a.key();
        }
        self.b.key()
    }

    fn value(&self) -> &[u8] {
        if !self.a.is_valid() && !self.b.is_valid() {
            return &[];
        }
        if !self.a.is_valid() {
            return self.b.value();
        }
        if !self.b.is_valid() {
            return self.a.value();
        }
        if self.a.key() <= self.b.key() {
            return self.a.value();
        }
        self.b.value()
    }

    fn is_valid(&self) -> bool {
        self.a.is_valid() || self.b.is_valid()
    }

    fn next(&mut self) -> Result<()> {
        if !self.a.is_valid() && !self.b.is_valid() {
            return Ok(());
        }
        if !self.a.is_valid() {
            return self.b.next();
        }
        if !self.b.is_valid() {
            return self.a.next();
        }
        if self.a.key() == self.b.key() {
            self.b.next()?;
            return self.a.next();
        }
        if self.a.key() < self.b.key() {
            return self.a.next();
        }
        self.b.next()
    }
}
