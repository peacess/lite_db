use std::collections::HashMap;
use std::fs;
use std::io::IsTerminal;
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
use crate::db::IndexType::BTree;
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

        let index = new_indexer(config.index_type.clone(), config.path_db.clone())?;
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
        if self.config.index_type == BTree && !self.seq_file_exists && !self.is_initial {
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

impl Drop for LiteDb {
    fn drop(&mut self) {
        if let Err(e) = self.close() {
            log::error!("error whiling close engine: {}", e);
        }
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

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use bytes::Bytes;

    use crate::db::{Adder, Closer, Config, Db, ErrDb, Getter, Remover};
    use crate::kits::rand_kv::{get_test_key, get_test_value};
    use crate::lite::LiteDb;

    #[test]
    fn test_lite_db_put() {
        let mut config = Config::default();
        config.path_db = PathBuf::from("/tmp/bitcask-rs-put");
        config.file_size_db = 64 * 1024 * 1024;
        let lite_db = LiteDb::open(config.clone()).expect("failed to open engine");

        // 1.正常 Put 一条数据
        let res1 = lite_db.add(&get_test_key(11), &get_test_value(11));
        assert!(res1.is_ok());
        let res2 = lite_db.get(&get_test_key(11));
        assert!(res2.is_ok());
        assert!(res2.unwrap().len() > 0);

        // 2.重复 Put key 相同的数据
        let res3 = lite_db.add(&get_test_key(22), &get_test_value(22));
        assert!(res3.is_ok());
        let res4 = lite_db.add(&get_test_key(22), &Bytes::from("a new value"));
        assert!(res4.is_ok());
        let res5 = lite_db.get(&get_test_key(22));
        assert!(res5.is_ok());
        assert_eq!(res5.unwrap(), Bytes::from("a new value"));

        // 3.key 为空
        let res6 = lite_db.add(&Bytes::new(), &get_test_value(123));
        assert_eq!(ErrDb::InvalidParameter, res6.err().unwrap());

        // 4.value 为空
        let res7 = lite_db.add(&get_test_key(33), &Bytes::new());
        assert!(res7.is_ok());
        let res8 = lite_db.get(&get_test_key(33));
        assert_eq!(0, res8.ok().unwrap().len());

        // 5.写到数据文件进行了转换
        for i in 0..=1000000 {
            let res = lite_db.add(&get_test_key(i), &get_test_value(i));
            assert!(res.is_ok());
        }

        // 6.重启后再 Put 数据
        std::mem::drop(lite_db);

        let lite_db2 = LiteDb::open(config.clone()).expect("failed to open engine");
        let res9 = lite_db2.add(&get_test_key(55), &get_test_value(55));
        assert!(res9.is_ok());

        let res10 = lite_db2.get(&get_test_key(55));
        assert_eq!(res10.unwrap(), &get_test_value(55));

        // 删除测试的文件夹
        std::fs::remove_dir_all(config.path_db.clone()).expect("failed to remove path");
    }

    #[test]
    fn test_lite_db_get() {
        let mut confg = Config::default();
        confg.path_db = PathBuf::from("/tmp/bitcask-rs-get");
        confg.file_size_db = 64 * 1024 * 1024;
        let lite_db = LiteDb::open(confg.clone()).expect("failed to open engine");

        // 1.正常读取一条数据
        let res1 = lite_db.add(&get_test_key(111), &get_test_value(111));
        assert!(res1.is_ok());
        let res2 = lite_db.get(&get_test_key(111));
        assert!(res2.is_ok());
        assert!(res2.unwrap().len() > 0);

        // 2.读取一个不存在的 key
        let res3 = lite_db.get(&Bytes::from("not existed key"));
        assert!(matches!(res3, Err(ErrDb::NotFindKey)));

        // 3.值被重复 Put 后在读取
        let res4 = lite_db.add(&get_test_key(222), &get_test_value(222));
        assert!(res4.is_ok());
        let res5 = lite_db.add(&get_test_key(222), &Bytes::from("a new value"));
        assert!(res5.is_ok());
        let res6 = lite_db.get(&get_test_key(222));
        let ttt = std::str::from_utf8(res6.as_ref().unwrap().as_ref()).unwrap();
        println!("{}", std::str::from_utf8(res6.as_ref().unwrap().as_ref()).unwrap());
        assert_eq!(Bytes::from("a new value"), res6.unwrap());

        // 4.值被删除后再 Get
        let res7 = lite_db.add(&get_test_key(333), &get_test_value(333));
        assert!(res7.is_ok());
        let res8 = lite_db.remove(&get_test_key(333));
        assert!(res8.is_ok());
        let res9 = lite_db.get(&get_test_key(333));
        assert!(matches!(res9, Err(ErrDb::NotFindKey)));

        // 5.转换为了旧的数据文件，从旧的数据文件上获取 value
        for i in 500..=1000000 {
            let res = lite_db.add(&get_test_key(i), &get_test_value(i));
            assert!(res.is_ok());
        }
        let res10 = lite_db.get(&get_test_key(505));
        assert_eq!(get_test_value(505), res10.unwrap());

        // 6.重启后，前面写入的数据都能拿到
        std::mem::drop(lite_db);

        let lite_db2 = LiteDb::open(confg.clone()).expect("failed to open engine");
        let res11 = lite_db2.get(&get_test_key(111));
        assert_eq!(get_test_value(111), res11.unwrap());
        let res12 = lite_db2.get(&get_test_key(222));
        assert_eq!(Bytes::from("a new value"), res12.unwrap());
        let res13 = lite_db2.get(&get_test_key(333));
        assert!(matches!(res13, Err(ErrDb::NotFindKey)));

        // 删除测试的文件夹
        std::fs::remove_dir_all(confg.path_db.clone()).expect("failed to remove path");
    }

    #[test]
    fn test_lite_db_delete() {
        let mut config = Config::default();
        config.path_db = PathBuf::from("/tmp/bitcask-rs-delete");
        config.file_size_db = 64 * 1024 * 1024;
        let lite_db = LiteDb::open(config.clone()).expect("failed to open engine");

        // 1.正常删除一个存在的 key
        let re1 = lite_db.add(&get_test_key(111), &get_test_value(111));
        assert!(re1.is_ok());
        let re2 = lite_db.remove(&get_test_key(111));
        assert!(re2.is_ok());
        let re3 = lite_db.get(&get_test_key(111));
        assert!(matches!(re3, Err(ErrDb::NotFindKey)));

        // 2.删除一个不存在的 key
        let re4 = lite_db.remove(&Bytes::from("not-existed-key"));
        assert!(re4.is_ok());

        // 3.删除一个空的 key
        let re5 = lite_db.remove(&Bytes::new());
        assert!(matches!(re5, Err(ErrDb::InvalidParameter)));

        // 4.值被删除之后重新 Put
        let re6 = lite_db.add(&get_test_key(222), &get_test_value(222));
        assert!(re6.is_ok());
        let res7 = lite_db.remove(&get_test_key(222));
        assert!(res7.is_ok());
        let res8 = lite_db.add(&get_test_key(222), &Bytes::from("a new value"));
        assert!(res8.is_ok());
        let res9 = lite_db.get(&get_test_key(222));
        assert_eq!(Bytes::from("a new value"), res9.unwrap());

        // 5.重启后再 Put 数据
        std::mem::drop(lite_db);

        let lite_db2 = LiteDb::open(config.clone()).expect("failed to open engine");
        let res10 = lite_db2.get(&get_test_key(111));
        assert!(matches!(res10, Err(ErrDb::NotFindKey)));
        let res11 = lite_db2.get(&get_test_key(222));
        assert_eq!(Bytes::from("a new value"), res11.unwrap());

        // 删除测试的文件夹
        std::fs::remove_dir_all(config.path_db.clone()).expect("failed to remove path");
    }

    #[test]
    fn test_engine_close() {
        let mut config = Config::default();
        config.path_db = PathBuf::from("/tmp/bitcask-rs-close");
        config.file_size_db = 64 * 1024 * 1024;
        let lite_db = LiteDb::open(config.clone()).expect("failed to open engine");

        let res1 = lite_db.add(&get_test_key(222), &get_test_value(222));
        assert!(res1.is_ok());

        let close_res = lite_db.close();
        assert!(close_res.is_ok());

        // 删除测试的文件夹
        std::fs::remove_dir_all(config.path_db.clone()).expect("failed to remove path");
    }

    #[test]
    fn test_engine_sync() {
        let mut config = Config::default();
        config.path_db = PathBuf::from("/tmp/bitcask-rs-sync");
        config.file_size_db = 64 * 1024 * 1024;
        let lite_db = LiteDb::open(config.clone()).expect("failed to open engine");

        let res1 = lite_db.add(&get_test_key(222), &get_test_value(222));
        assert!(res1.is_ok());

        let close_res = lite_db.sync();
        assert!(close_res.is_ok());

        // 删除测试的文件夹
        std::fs::remove_dir_all(config.path_db.clone()).expect("failed to remove path");
    }

    #[test]
    fn test_engine_filelock() {
        let mut config = Config::default();
        config.path_db = PathBuf::from("/tmp/bitcask-rs-flock");
        let lite_db = LiteDb::open(config.clone()).expect("failed to open engine");

        let lite_db2 = LiteDb::open(config.clone());
        let _eof = ErrDb::new_io_eof("");
        assert!(matches!(lite_db2, Err(_eof)));

        let re2 = lite_db.close();
        assert!(matches!(re2,Ok(())));
        // std::mem::drop(lite_db);

        let re3 = LiteDb::open(config.clone());
        assert!(re3.is_ok());

        // 删除测试的文件夹
        std::fs::remove_dir_all(config.path_db.clone()).expect("failed to remove path");
    }

    #[test]
    fn test_engine_stat() {
        let mut config = Config::default();
        config.path_db = PathBuf::from("/tmp/bitcask-rs-stat");
        let lite_db = LiteDb::open(config.clone()).expect("failed to open engine");

        for i in 0..=10000 {
            let res = lite_db.add(&get_test_key(i), &get_test_value(i));
            assert!(res.is_ok());
        }
        for i in 0..=1000 {
            let res = lite_db.add(&get_test_key(i), &get_test_value(i));
            assert!(res.is_ok());
        }
        for i in 2000..=5000 {
            let res = lite_db.remove(&get_test_key(i));
            assert!(res.is_ok());
        }

        // let stat = lite_db.stat().unwrap();
        // assert!(stat.reclaim_size > 0);

        // 删除测试的文件夹
        std::fs::remove_dir_all(config.path_db.clone()).expect("failed to remove path");
    }

    // #[test]
    // fn test_engine_backup() {
    //     let mut config = Config::default();
    //     config.path_db = PathBuf::from("/tmp/bitcask-rs-backup");
    //     let lite_db = LiteDb::open(config.clone()).expect("failed to open engine");
    //
    //     for i in 0..=10000 {
    //         let res = lite_db.add(&get_test_key(i), &get_test_value(i));
    //         assert!(res.is_ok());
    //     }
    //
    //     let backup_dir = PathBuf::from("/tmp/bitcask-rs-backup-test");
    //     let bak_res = lite_db.backup(backup_dir.clone());
    //     assert!(bak_res.is_ok());
    //
    //     let mut config2 = Config::default();
    //     config2.path_db = backup_dir.clone();
    //     let lite_db2 = LiteDb::open(config2);
    //     assert!(lite_db2.is_ok());
    //
    //     // 删除测试的文件夹
    //     std::fs::remove_dir_all(config.path_db.clone()).expect("failed to remove path");
    //     std::fs::remove_dir_all(backup_dir).expect("failed to remove path");
    // }
}