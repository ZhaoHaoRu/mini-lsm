use std::cmp::{self, Ordering};
use std::collections::binary_heap::PeekMut;
use std::collections::BinaryHeap;

use anyhow::Result;

use super::StorageIterator;

#[derive(Clone)]
struct HeapWrapper<I: StorageIterator>(pub usize, pub Box<I>);

impl<I: StorageIterator> PartialEq for HeapWrapper<I> {
    fn eq(&self, other: &Self) -> bool {
        self.partial_cmp(other).unwrap() == cmp::Ordering::Equal
    }
}

impl<I: StorageIterator> Eq for HeapWrapper<I> {}

impl<I: StorageIterator> PartialOrd for HeapWrapper<I> {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        match self.1.key().cmp(other.1.key()) {
            cmp::Ordering::Greater => Some(cmp::Ordering::Greater),
            cmp::Ordering::Less => Some(cmp::Ordering::Less),
            cmp::Ordering::Equal => self.0.partial_cmp(&other.0),
        }
        .map(|x| x.reverse())
    }
}

impl<I: StorageIterator> Ord for HeapWrapper<I> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.partial_cmp(other).unwrap()
    }
}

/// Merge multiple iterators of the same type. If the same key occurs multiple times in some
/// iterators, perfer the one with smaller index.
pub struct MergeIterator<I: StorageIterator> {
    iters: BinaryHeap<HeapWrapper<I>>,
    current: Option<HeapWrapper<I>>,
}

impl<I: StorageIterator> MergeIterator<I> {
    pub fn create(iters: Vec<Box<I>>) -> Self {
        let mut heap = BinaryHeap::new();
        for (index, elem) in iters.into_iter().enumerate() {
            if elem.is_valid() {
                heap.push(HeapWrapper(index, elem));
            }
        }
        if let Some(current) = heap.pop() {
            Self {
                iters: heap,
                current: Some(current),
            }
        } else {
            Self {
                iters: BinaryHeap::new(),
                current: None,
            }
        }
    }
}

impl<I: StorageIterator> StorageIterator for MergeIterator<I> {
    fn key(&self) -> &[u8] {
        self.current.as_ref().unwrap().1.key()
    }

    fn value(&self) -> &[u8] {
        self.current.as_ref().unwrap().1.value()
    }

    fn is_valid(&self) -> bool {
        self.current.is_some() && self.current.as_ref().unwrap().1.is_valid()
    }

    fn next(&mut self) -> Result<()> {
        if self.current.is_none() {
            return Ok(());
        }
        let current = self.current.as_mut().unwrap();
        while let Some(mut candidate) = self.iters.peek_mut() {
            if candidate.1.key() == current.1.key() {
                match candidate.1.next() {
                    Ok(_) => {}
                    Err(e) => {
                        PeekMut::pop(candidate);
                        return Err(e);
                    }
                }
                // the iter is no longer valid, just pop it
                if !candidate.1.is_valid() {
                    PeekMut::pop(candidate);
                }
            } else {
                break;
            }
        }

        current
            .1
            .next()
            .expect("[MergeIterator::next] unable to call current iterator `next` method");
        // if the current element is invalid, replace with the top element in the heap
        if !current.1.is_valid() {
            if let Some(current) = self.iters.pop() {
                self.current = Some(current);
            } else {
                return Ok(());
            }
        } else if let Some(mut candidate) = self.iters.peek_mut() {
            if Ord::cmp(current, &candidate) == Ordering::Less {
                std::mem::swap(current, &mut *candidate);
            }
        }

        Ok(())
    }
}
