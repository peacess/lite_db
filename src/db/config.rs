use std::path::PathBuf;

use crate::db::ErrDb;

#[derive(Clone, PartialEq, Debug)]
pub enum IndexType {
    BTree,
    BPlusTree,
}

pub struct WriteBatchOptions {
    // 一个批次当中的最大数据量
    pub max_batch_num: usize,

    // 提交时候是否进行 sync 持久化
    pub sync_writes: bool,
}

impl Default for WriteBatchOptions {
    fn default() -> Self {
        Self {
            max_batch_num: 10000,
            sync_writes: true,
        }
    }
}

#[derive(Clone, PartialEq, Debug)]
pub enum IoType {
    StdIo,
    MemoryMap,
}

#[derive(Clone, Debug)]
pub struct Config {
    pub path_db: PathBuf,
    pub file_size_db: u64,
    pub sync_writes: bool,
    pub bytes_per_sync: usize,
    pub index_type: IndexType,
    pub mmap_at_startup: bool,
    pub merge_ratio: f32,
}

impl Config {
    pub(crate) fn check(&self) -> Option<ErrDb> {
        let mut err = ErrDb::None;
        let dir_path = self.path_db.to_str();
        if dir_path.is_none() || dir_path.unwrap().is_empty() {
            err = ErrDb::Err("the db config path is none".to_owned());
        } else if self.file_size_db == 0 {
            err = ErrDb::Err("the db config file size  <= 0".to_owned());
        } else if self.merge_ratio < 0.0 || self.merge_ratio > 1.0 {
            err = ErrDb::Err("the db config merge ratio < 0 or > 1".to_owned());
        }
        if err.is_not_none() {
            Some(err)
        } else {
            None
        }
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            path_db: std::env::temp_dir().join("lite_db"),
            file_size_db: 128 * 1024 * 1024,
            sync_writes: false,
            bytes_per_sync: 0,
            index_type: IndexType::BTree,
            mmap_at_startup: true,
            merge_ratio: 0.5,
        }
    }
}
