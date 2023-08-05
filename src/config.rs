use std::path::PathBuf;

use crate::DbErr;

#[derive(Clone, PartialEq, Debug)]
pub enum IndexType {
    BTree,
    SkipList,
    BPlusTree,
}

#[derive(Clone, PartialEq, Debug)]
pub enum IOType {
    StdFIO,
    MMap,
}

#[derive(Clone, Debug)]
pub struct DbConfig {
    pub db_path: PathBuf,
    pub db_file_size: u64,
    pub index_type: IndexType,
    // merge ratio
    pub merge_ratio: f32,
}

impl DbConfig {
    pub(crate) fn check(&self) -> Option<DbErr> {
        let mut err = DbErr::None;
        let dir_path = self.db_path.to_str();
        if dir_path.is_none() || dir_path.unwrap().len() == 0 {
            err = DbErr::Err("the db config path is none".to_owned());
        } else if self.db_file_size <= 0 {
            err = DbErr::Err("the db config file size  <= 0".to_owned());
        } else if self.merge_ratio < 0 as f32 || self.merge_ratio > 1 as f32 {
            err = DbErr::Err("the db config merge ratio < 0 or > 1".to_owned());
        }
        if err.is_not_none() {
            Some(err)
        } else {
            return None;
        }
    }
}

impl Default for DbConfig {
    fn default() -> Self {
        Self {
            db_path: std::env::temp_dir().join("lite_db"),
            db_file_size: 128 * 1024 * 1024,
            index_type: IndexType::BTree,
            merge_ratio: 0.5,
        }
    }
}
