use crate::heap_page::HeapPage;
use crate::heap_page::HeapPageIntoIter;
use crate::heapfile::HeapFile;
use crate::page;
use crate::page::Page;
use common::prelude::*;
use serde::de::value;
use std::sync::Arc;

#[allow(dead_code)]
/// The struct for a HeapFileIterator.
/// We use a slightly different approach for HeapFileIterator than
/// standard way of Rust's IntoIter for simplicity (avoiding lifetime issues).
/// This should store the state/metadata required to iterate through the file.
///
/// HINT: This will need an Arc<HeapFile>
pub struct HeapFileIterator {
    //TODO milestone hs
    page_start: u16,
    tid: TransactionId,
    hf: Arc<HeapFile>,
    iter: Option<HeapPageIntoIter>,
    slot_start: Option<u16>,
    container_id: ContainerId
}

/// Required HeapFileIterator functions
impl HeapFileIterator {
    /// Create a new HeapFileIterator that stores the tid, and heapFile pointer.
    /// This should initialize the state required to iterate through the heap file.
    pub(crate) fn new(tid: TransactionId, hf: Arc<HeapFile>) -> Self {
        let container_id = hf.container_id.clone();
        HeapFileIterator {
            page_start: 0,
            tid,
            hf,
            iter: None,
            slot_start: None,
            container_id
        }
    }

    pub(crate) fn new_from(tid: TransactionId, hf: Arc<HeapFile>, value_id: ValueId) -> Self {
        let page_start = match value_id.page_id {
            Some(page_id) => page_id,
            None => panic!("page_id is None"),
        };

        let slot_start = value_id.slot_id;
        let container_id = hf.container_id.clone();

        HeapFileIterator {
            page_start,
            tid,
            hf,
            iter: None,
            slot_start,
            container_id
        }
    }
}

/// Trait implementation for heap file iterator.
/// Note this will need to iterate through the pages and their respective iterators.
impl Iterator for HeapFileIterator {
    type Item = (Vec<u8>, ValueId);
    fn next(&mut self) -> Option<Self::Item> {
        let page_id = self.page_start;
        let container_id = self.container_id;

        if self.page_start >= self.hf.num_pages() {
            return None;
        }

        if let Some(page_iter) = self.iter.as_mut() {
            if let Some((bytes, slot)) = page_iter.next() {
                let val_id = ValueId {
                    container_id,
                    segment_id: None,
                    page_id: Some(page_id),
                    slot_id: Some(slot)
                };
                return Some((bytes, val_id));
            } else {
                self.page_start += 1;
                self.iter = None;
                return self.next();
            }
        } else {
            let p = self.hf.read_page_from_file(self.page_start).unwrap();
            let mut page_iter = p.into_iter();

            if let Some(slot) = self.slot_start {
                let idx = page_iter.slot_ids.iter().position(|&x| x == slot).unwrap();
                page_iter.slot_ids.drain(..idx);
                self.slot_start = None;
            }
            return self.next();
        }
    }
}
