use std::fs;

use fs2::FileExt;

use crate::{Adder, Closer, Db, DbConfig, DbErr, DbResult, Editor, Getter, Key, Remover, Value};
use crate::lite::Table;

pub(crate) const FILE_LOCK_NAME: &str = "___lite_db_file_lock_name___";

pub struct LiteDb {}

impl LiteDb {
    pub fn open(cfg: &DbConfig) -> DbResult<LiteDb> {
        if let Some(e) = cfg.check() {
            log::error!("{}", e.to_string());
            return Err(e);
        }
        let db_path = &cfg.db_path;
        if !db_path.is_dir() {
            if let Err(e) = fs::create_dir_all(db_path.clone()) {
                log::error!("{}", e.to_string());
                return Err(DbErr::IoErr(e));
            }
        }
        // 检查是否已经打开一个数据库
        let lock_file = {
            match fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(db_path.join(FILE_LOCK_NAME)) {
                Ok(f) => f,
                Err(e) => {
                    log::error!("{}", e.to_string());
                    return Err(DbErr::IoErr(e));
                }
            }
        };
        if let Err(e) = lock_file.try_lock_exclusive() {
            log::error!("{}", e.to_string());
            return Err(DbErr::IoErr(e));
        }
        todo!()
    }

    pub fn open_table(&self) -> DbResult<Table> {
        todo!()
    }
}

impl Getter for LiteDb {
    fn get(&self, key: &Key) -> DbResult<Value> {
        todo!()
    }
}

impl Adder for LiteDb {
    fn add(&self, key: &Key, v: &Value) -> DbResult<()> {
        todo!()
    }
}

impl Remover for LiteDb {
    fn remove(&self, key: &Key) -> DbResult<Value> {
        todo!()
    }
}

impl Closer for LiteDb {
    fn close(&self) -> DbResult<()> {
        todo!()
    }
}

impl Editor for LiteDb {}

impl Db for LiteDb {}