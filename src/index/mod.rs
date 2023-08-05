use std::path::PathBuf;

use crate::db::{Indexer, IndexType};

mod bptree;

pub fn new_indexer(index_type: IndexType, dir_path: PathBuf) -> Box<dyn Indexer> {
    match index_type {
        IndexType::BPlusTree => Box::new(bptree::BPlusTree::new(dir_path)),
    }
}