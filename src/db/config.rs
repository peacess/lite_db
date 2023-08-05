use std::path::PathBuf;

use crate::db::ErrDb;

#[derive(Clone, PartialEq, Debug)]
pub enum IndexType {
    BPlusTree,
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
    pub index_type: IndexType,
    // merge ratio
    pub merge_ratio: f32,
}

impl Config {
    pub(crate) fn check(&self) -> Option<ErrDb> {
        let mut err = ErrDb::None;
        let dir_path = self.path_db.to_str();
        if dir_path.is_none() || dir_path.unwrap().len() == 0 {
            err = ErrDb::Err("the db config path is none".to_owned());
        } else if self.file_size_db <= 0 {
            err = ErrDb::Err("the db config file size  <= 0".to_owned());
        } else if self.merge_ratio < 0 as f32 || self.merge_ratio > 1 as f32 {
            err = ErrDb::Err("the db config merge ratio < 0 or > 1".to_owned());
        }
        if err.is_not_none() {
            Some(err)
        } else {
            return None;
        }
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            path_db: std::env::temp_dir().join("lite_db"),
            file_size_db: 128 * 1024 * 1024,
            index_type: IndexType::BPlusTree,
            merge_ratio: 0.5,
        }
    }
}
