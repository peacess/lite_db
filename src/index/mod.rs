use std::path::PathBuf;

use crate::db::{IndexType, Indexer, ResultDb};

mod bptree;
mod btree;

pub fn new_indexer(index_type: IndexType, _dir_path: PathBuf) -> ResultDb<Box<dyn Indexer>> {
    match index_type {
        IndexType::BTree => Ok(Box::new(btree::BTree::new())),
        IndexType::BPlusTree => Ok(Box::new(bptree::BPlusTree::new(_dir_path)?)),
    }
}
