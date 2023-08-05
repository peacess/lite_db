use std::fs;

use fs2::FileExt;

use crate::{Adder, Closer, Db, DbConfig, DbErr, DbResult, Editor, Getter, Key, Remover, Value};
use crate::lite::Table;

pub(crate) const FILE_LOCK_NAME: &str = "___lite_db_file_lock_name___";

pub struct LiteDb {
    config: DbConfig,
    lock_file: fs::File,
}

impl LiteDb {
    pub fn open(config: DbConfig) -> DbResult<LiteDb> {
        if let Some(e) = config.check() {
            log::error!("{}", e.to_string());
            return Err(e);
        }
        let db_path = &config.db_path;
        if !db_path.is_dir() {
            if let Err(e) = fs::create_dir_all(db_path.clone()) {
                log::error!("{}", e.to_string());
                return Err(DbErr::IoErr(e));
            }
        }
        // check whether the file opened
        let lock_file = {
            match fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(db_path.join(FILE_LOCK_NAME))
            {
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
        Ok(LiteDb {
            config,
            lock_file,
        })
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
