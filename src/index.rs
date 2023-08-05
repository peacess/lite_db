use bytes::Bytes;

use crate::{DbResult, LogRecordPos};

pub trait Indexer: Sync + Send {
    fn put(&self, key: Vec<u8>, pos: LogRecordPos) -> Option<LogRecordPos>;

    fn get(&self, key: Vec<u8>) -> Option<LogRecordPos>;

    fn delete(&self, key: Vec<u8>) -> Option<LogRecordPos>;

    fn list_keys(&self) -> DbResult<Vec<Bytes>>;

    fn iterator(&self, options: IteratorOptions) -> Box<dyn IndexIterator>;
}

pub trait IndexIterator: Sync + Send {
    fn rewind(&mut self);

    fn seek(&mut self, key: Vec<u8>);

    fn next(&mut self) -> Option<(&Vec<u8>, &LogRecordPos)>;
}


pub struct IteratorOptions {
    pub prefix: Vec<u8>,
    pub reverse: bool,
}

impl Default for IteratorOptions {
    fn default() -> Self {
        Self {
            prefix: Default::default(),
            reverse: false,
        }
    }
}