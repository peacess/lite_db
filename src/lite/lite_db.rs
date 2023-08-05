use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;

use bytes::Bytes;
use fs2::FileExt;
use parking_lot::RwLock;

use crate::db::{Adder, Closer, Config, DATA_FILE_NAME_SUFFIX, Db, Editor, ErrDb, Getter, Indexer, IoType, Key, Remover, ResultDb, Value};
use crate::db::{LogRecordPos, LogRecordType};
use crate::db::FileDb;
use crate::index::new_indexer;
use crate::lite::Table;

pub(crate) const FILE_LOCK_NAME: &str = "___lite_db_file_lock_name___";
const INITIAL_FILE_ID: u32 = 0;

pub struct LiteDb {
    config: Config,
    active_file: Arc<RwLock<FileDb>>,
    // 当前活跃数据文件
    older_files: Arc<RwLock<HashMap<u32, FileDb>>>,
    // 旧的数据文件
    index: Box<dyn Indexer>,
    lock_file: fs::File,
}

impl LiteDb {
    pub fn open(config: Config) -> ResultDb<LiteDb> {
        if let Some(e) = config.check() {
            log::error!("{}", e.to_string());
            return Err(e);
        }
        let path_db = &config.path_db;
        if !path_db.is_dir() {
            if let Err(e) = fs::create_dir_all(path_db.clone()) {
                log::error!("{}", e.to_string());
                return Err(ErrDb::IoErr(e));
            }
        }
        // check whether the file opened
        let lock_file = {
            match fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(path_db.join(FILE_LOCK_NAME))
            {
                Ok(f) => {
                    if let Err(e) = f.try_lock_exclusive() {
                        log::error!("{}", e.to_string());
                        return Err(ErrDb::IoErr(e));
                    }
                    f
                }
                Err(e) => {
                    log::error!("{}", e.to_string());
                    return Err(ErrDb::IoErr(e));
                }
            }
        };

        let mut data_files = load_data_files(path_db.clone(), false)?;
        {
            let mut file_ids = Vec::new();
            for v in data_files.iter() {
                file_ids.push(v.get_file_id());
            }
        }

        let active_file = match data_files.pop() {
            Some(v) => v,
            None => FileDb::new(path_db.clone(), INITIAL_FILE_ID, IoType::StdIo)?,
        };

        let older_files = {
            if data_files.len() > 0 {
                // 将旧的数据文件放到后面，新的数据文件在第一个位置
                data_files.into_iter().rev().map(|f| (f.get_file_id(), f)).collect()
            } else {
                HashMap::new()
            }
        };

        let index = new_indexer(config.index_type.clone(), config.path_db.clone());
        Ok(LiteDb {
            config,
            active_file: Arc::new(RwLock::new(active_file)),
            older_files: Arc::new(RwLock::new(older_files)),
            index,
            lock_file,
        })
    }

    pub fn open_table(&self) -> ResultDb<Table> {
        todo!()
    }

    fn get_value_by_pos(&self, log_record_pos: &LogRecordPos) -> ResultDb<Bytes> {
        let active_file = self.active_file.read();
        let log_record = {
            if active_file.get_file_id() == log_record_pos.file_id {
                active_file.read_log_record(log_record_pos.offset)?.record
            } else {
                match self.older_files.read().get(&log_record_pos.file_id) {
                    None => return Err(ErrDb::new_io_file_not_find("")),
                    Some(data_file) => {
                        data_file.read_log_record(log_record_pos.offset)?.record
                    }
                }
            }
        };

        if log_record.rec_type == LogRecordType::DELETED {
            return Err(ErrDb::NotFindKey);
        }

        Ok(log_record.value.into())
    }
}

impl Getter for LiteDb {
    fn get(&self, key: &Key) -> ResultDb<Value> {
        let p = {
            match self.index.get(key.to_vec()) {
                Some(p) => p,
                None => return Err(ErrDb::NotFindKey)
            }
        };
        self.get_value_by_pos(&p)
    }
}

impl Adder for LiteDb {
    fn add(&self, key: &Key, v: &Value) -> ResultDb<()> {
        todo!()
    }
}

impl Remover for LiteDb {
    fn remove(&self, key: &Key) -> ResultDb<Value> {
        todo!()
    }
}

impl Closer for LiteDb {
    fn close(&self) -> ResultDb<()> {
        if !self.config.path_db.is_dir() {
            return Ok(());
        }
        //todo
        self.lock_file.unlock()?;
        Ok(())
    }
}

impl Editor for LiteDb {}

impl Db for LiteDb {}


fn load_data_files(dir_path: PathBuf, use_mmap: bool) -> ResultDb<Vec<FileDb>> {
    // 读取数据目录
    let dir = fs::read_dir(dir_path.clone())?;

    let mut file_ids: Vec<u32> = Vec::new();
    let mut data_files: Vec<FileDb> = Vec::new();
    for file in dir {
        if let Ok(entry) = file {
            // 拿到文件名
            let file_os_str = entry.file_name();
            let file_name = file_os_str.to_str().unwrap();

            // 判断文件名称是否是以 .data 结尾
            if file_name.ends_with(DATA_FILE_NAME_SUFFIX) {
                let split_names: Vec<&str> = file_name.split(".").collect();
                let file_id = split_names[0].parse::<u32>()?;

                file_ids.push(file_id);
            }
        }
    }

    // 如果没有数据文件，则直接返回
    if file_ids.is_empty() {
        return Ok(data_files);
    }

    // 对文件 id 进行排序，从小到大进行加载
    file_ids.sort();
    // 遍历所有的文件id，依次打开对应的数据文件
    for file_id in file_ids.iter() {
        let mut io_type = IoType::StdIo;
        if use_mmap {
            io_type = IoType::MemoryMap;
        }
        let data_file = FileDb::new(dir_path.clone(), *file_id, io_type)?;
        data_files.push(data_file);
    }

    Ok(data_files)
}