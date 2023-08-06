use bytes::Bytes;

use crate::db::LogDbPos;
use crate::db::ResultDb;

pub trait Indexer: Sync + Send {
    fn put(&self, key: Vec<u8>, pos: LogDbPos) -> Option<LogDbPos>;

    fn get(&self, key: Vec<u8>) -> Option<LogDbPos>;

    fn delete(&self, key: Vec<u8>) -> Option<LogDbPos>;

    fn list_keys(&self) -> ResultDb<Vec<Bytes>>;

    fn iterator(&self, options: IteratorOptions) -> Box<dyn IndexIterator>;
}

pub trait IndexIterator: Sync + Send {
    fn rewind(&mut self);

    fn seek(&mut self, key: Vec<u8>);

    fn next(&mut self) -> Option<(&Vec<u8>, &LogDbPos)>;
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
