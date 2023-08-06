use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use bytes::Bytes;
use fs2::FileExt;
use parking_lot::{Mutex, RwLock};

use crate::db::{FileDb, LogDb, WriteBatchOptions};
use crate::db::{
    Adder, Closer, Config, DATA_FILE_NAME_SUFFIX, Db, Editor, ErrDb, Getter, Indexer, IoType, Key, Remover, ResultDb, Value,
};
use crate::db::{LogDbPos, LogDbType};
use crate::db::IndexType::BPlusTree;
use crate::index::new_indexer;
use crate::lite::batch::{log_db_key_with_seq, NON_TRANSACTION_SEQ_NO, WriteBatch};
use crate::lite::Table;

pub(crate) const FILE_LOCK_NAME: &str = "___lite_db_file_lock_name___";
const SEQ_NO_KEY: &str = "___seq_no___";
const INITIAL_FILE_ID: u32 = 0;

pub struct LiteDb {
    pub(crate) config: Config,
    pub(crate) active_file: RwLock<FileDb>,
    pub(crate) older_files: RwLock<HashMap<u32, FileDb>>,
    pub(crate) index: Box<dyn Indexer>,
    pub(crate) batch_commit_lock: Mutex<()>,
    // 事务序列号，全局递增
    pub(crate) seq_no: AtomicUsize,
    // 防止多个线程同时 merge
    pub(crate) merging_lock: Mutex<()>,
    // 事务序列号文件是否存在
    pub(crate) seq_file_exists: bool,
    pub(crate) is_initial: bool,
    lock_file: fs::File,
    bytes_write: AtomicUsize,
    pub(crate) reclaim_size: AtomicUsize, // 累计有多少空间可以 merge
}

impl LiteDb {
    pub fn open(config: Config) -> ResultDb<LiteDb> {
        if let Some(e) = config.check() {
            log::error!("{}", e.to_string());
            return Err(e);
        }
        let mut is_initial = false;
        let path_db = &config.path_db;
        if !path_db.is_dir() {
            is_initial = true;
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

        if let Ok(entries) = fs::read_dir(path_db.clone()) {
            if entries.count() < 1 {
                is_initial = true;
            }
        }

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
            active_file: RwLock::new(active_file),
            older_files: RwLock::new(older_files),
            index,
            batch_commit_lock: Mutex::new(()),
            seq_no: AtomicUsize::new(1),
            merging_lock: Mutex::new(()),
            seq_file_exists: false,
            is_initial,
            lock_file,
            bytes_write: AtomicUsize::new(0),
            reclaim_size: AtomicUsize::new(0),
        })
    }

    pub fn open_table(&self) -> ResultDb<Table> {
        todo!()
    }

    pub(crate) fn append_log_db(&self, log_db: &mut LogDb) -> ResultDb<LogDbPos> {
        let dir_path = self.config.path_db.clone();

        let enc_log_db = log_db.encode();
        let log_db_len = enc_log_db.len() as u64;

        let mut active_file = self.active_file.write();

        // 判断当前活跃文件是否达到了阈值
        if active_file.get_write_off() + log_db_len > self.config.file_size_db {
            active_file.sync()?;

            let current_fid = active_file.get_file_id();
            // 旧的数据文件存储到 map 中
            let mut older_files = self.older_files.write();
            let old_file = FileDb::new(dir_path.clone(), current_fid, IoType::StdIo)?;
            older_files.insert(current_fid, old_file);

            let new_file = FileDb::new(dir_path.clone(), current_fid + 1, IoType::StdIo)?;
            *active_file = new_file;
        }

        // 追加写数据到当前活跃文件中
        let write_off = active_file.get_write_off();
        active_file.write(&enc_log_db)?;

        let previous = self
            .bytes_write
            .fetch_add(enc_log_db.len(), Ordering::SeqCst);
        // 根据配置项决定是否持久化
        let mut need_sync = self.config.sync_writes;
        if !need_sync
            && self.config.bytes_per_sync > 0
            && previous + enc_log_db.len() >= self.config.bytes_per_sync
        {
            need_sync = true;
        }

        if need_sync {
            active_file.sync()?;
            self.bytes_write.store(0, Ordering::SeqCst);
        }

        Ok(LogDbPos {
            file_id: active_file.get_file_id(),
            offset: write_off,
            size: enc_log_db.len() as u32,
        })
    }

    fn get_value_by_pos(&self, log_db_pos: &LogDbPos) -> ResultDb<Bytes> {
        let active_file = self.active_file.read();
        let log_db = {
            if active_file.get_file_id() == log_db_pos.file_id {
                active_file.read_log_db(log_db_pos.offset)?.log_db
            } else {
                match self.older_files.read().get(&log_db_pos.file_id) {
                    None => return Err(ErrDb::new_io_file_not_find("")),
                    Some(data_file) => data_file.read_log_db(log_db_pos.offset)?.log_db,
                }
            }
        };

        if log_db.rec_type == LogDbType::DELETED {
            return Err(ErrDb::NotFindKey);
        }

        Ok(log_db.value.into())
    }

    //batch
    pub fn new_write_batch(&self, options: WriteBatchOptions) -> ResultDb<WriteBatch> {
        if self.config.index_type == BPlusTree && !self.seq_file_exists && !self.is_initial {
            return Err(ErrDb::InvalidBatch);
        }
        Ok(WriteBatch {
            pending: Arc::new(Mutex::new(HashMap::new())),
            db: self,
            options,
        })
    }
}

impl Getter for LiteDb {
    fn get(&self, key: &Key) -> ResultDb<Value> {
        let p = {
            match self.index.get(key.to_vec()) {
                Some(p) => p,
                None => return Err(ErrDb::NotFindKey),
            }
        };
        self.get_value_by_pos(&p)
    }
}

impl Adder for LiteDb {
    fn add(&self, k: &Key, v: &Value) -> ResultDb<()> {
        // 判断 key 的有效性
        if k.is_empty() {
            return Err(ErrDb::InvalidParameter);
        }

        // 构造 LogDb
        let mut log_db = LogDb {
            key: log_db_key_with_seq(k.to_vec(), NON_TRANSACTION_SEQ_NO),
            value: v.to_vec(),
            rec_type: LogDbType::NORMAL,
        };

        let log_db_pos = self.append_log_db(&mut log_db)?;

        if let Some(old_pos) = self.index.put(k.to_vec(), log_db_pos) {
            self.reclaim_size
                .fetch_add(old_pos.size as usize, Ordering::SeqCst);
        }

        Ok(())
    }
}

impl Remover for LiteDb {
    fn remove(&self, key: &Key) -> ResultDb<Option<Value>> {
        if key.is_empty() {
            return Err(ErrDb::InvalidParameter);
        }

        let p = {
            match self.index.get(key.to_vec()) {
                Some(p) => p,
                None => return Ok(None),
            }
        };
        let value = self.get_value_by_pos(&p)?;

        let mut log_db = LogDb {
            key: log_db_key_with_seq(key.to_vec(), NON_TRANSACTION_SEQ_NO),
            value: Default::default(),
            rec_type: LogDbType::DELETED,
        };

        let pos = self.append_log_db(&mut log_db)?;
        self.reclaim_size
            .fetch_add(pos.size as usize, Ordering::SeqCst);

        // delete the key in indexes
        if let Some(old_pos) = self.index.delete(key.to_vec()) {
            self.reclaim_size
                .fetch_add(old_pos.size as usize, Ordering::SeqCst);
        }

        Ok(Some(value))
    }

    fn remove_fast(&self, key: &Key) -> ResultDb<()> {
        if key.is_empty() {
            return Err(ErrDb::InvalidParameter);
        }

        let pos = self.index.get(key.to_vec());
        if pos.is_none() {
            return Ok(());
        }

        let mut log_db = LogDb {
            key: log_db_key_with_seq(key.to_vec(), NON_TRANSACTION_SEQ_NO),
            value: Default::default(),
            rec_type: LogDbType::DELETED,
        };

        let pos = self.append_log_db(&mut log_db)?;
        self.reclaim_size
            .fetch_add(pos.size as usize, Ordering::SeqCst);

        // delete the key in indexes
        if let Some(old_pos) = self.index.delete(key.to_vec()) {
            self.reclaim_size
                .fetch_add(old_pos.size as usize, Ordering::SeqCst);
        }

        Ok(())
    }
}

impl Closer for LiteDb {
    fn close(&self) -> ResultDb<()> {
        if !self.config.path_db.is_dir() {
            return Ok(());
        }
        let seq_no_file = FileDb::new_seq_no_file(self.config.path_db.clone())?;
        let seq_no = self.seq_no.load(Ordering::SeqCst);
        let log_db = LogDb {
            key: SEQ_NO_KEY.as_bytes().to_vec(),
            value: seq_no.to_string().into_bytes(),
            rec_type: LogDbType::NORMAL,
        };
        seq_no_file.write(&log_db.encode())?;
        seq_no_file.sync()?;

        let read_guard = self.active_file.read();
        read_guard.sync()?;
        self.lock_file.unlock()?;
        Ok(())
    }
}

impl Editor for LiteDb {}

impl Db for LiteDb {
    fn sync(&self) -> ResultDb<()> {
        let read_guard = self.active_file.read();
        read_guard.sync()
    }
}

fn load_data_files(dir_path: PathBuf, use_mmap: bool) -> ResultDb<Vec<FileDb>> {
    let dir = fs::read_dir(dir_path.clone())?;

    let mut file_ids: Vec<u32> = Vec::new();
    let mut data_files: Vec<FileDb> = Vec::new();
    for file in dir {
        if let Ok(entry) = file {
            let file_os_str = entry.file_name();
            let file_name = file_os_str.to_str().unwrap();

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
