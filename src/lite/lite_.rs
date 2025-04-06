use std::{
    collections::HashMap,
    fs,
    path::PathBuf,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use bytes::Bytes;
use fs2::FileExt;
use parking_lot::{Mutex, RwLock};

use crate::{
    db::{
        Adder, Closer, Config, Db, Editor, ErrDb, FileDb, Getter, IndexType, IndexType::BTree, Indexer, IoType, Key, LogDb, LogDbPos, LogDbType, Remover,
        ResultDb, TransactionLogDb, Value, WriteBatchOptions, DATA_FILE_NAME_SUFFIX, MERGE_FINISHED_FILE_NAME, SEQ_NO_FILE_NAME,
    },
    index::new_indexer,
    lite::{
        batch::{log_db_key_with_seq, parse_log_db_key, WriteBatch, NON_TRANSACTION_SEQ_NO},
        Table,
    },
};

pub(crate) const FILE_LOCK_NAME: &str = "___lite_db_file_lock_name___";
const SEQ_NO_KEY: &str = "___seq_no___";
const INITIAL_FILE_ID: u32 = 0;

pub struct LiteDb {
    pub(crate) config: Config,
    pub(crate) active_file: RwLock<FileDb>,
    pub(crate) older_files: RwLock<HashMap<u32, FileDb>>,
    pub(crate) index: Box<dyn Indexer>,
    file_ids: Vec<u32>,
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
            log::error!("{}", e);
            return Err(e);
        }
        let mut is_initial = false;
        let path_db = &config.path_db;
        if !path_db.is_dir() {
            is_initial = true;
            if let Err(e) = fs::create_dir_all(path_db.clone()) {
                log::error!("{}", e);
                return Err(ErrDb::IoErr(e));
            }
        }
        // check whether the file opened
        let lock_file = {
            match fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .open(path_db.join(FILE_LOCK_NAME))
            {
                Ok(f) => {
                    if let Err(e) = f.try_lock_exclusive() {
                        log::error!("{}", e);
                        return Err(ErrDb::IoErr(e));
                    }
                    f
                }
                Err(e) => {
                    log::error!("{}", e);
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

        let mut file_ids = Vec::new();
        for v in data_files.iter() {
            file_ids.push(v.get_file_id());
        }

        let active_file = match data_files.pop() {
            Some(v) => v,
            None => FileDb::new(path_db.clone(), INITIAL_FILE_ID, IoType::StdIo)?,
        };
        let older_files = {
            if !data_files.is_empty() {
                // 将旧的数据文件放到后面，新的数据文件在第一个位置
                data_files.into_iter().rev().map(|f| (f.get_file_id(), f)).collect()
            } else {
                HashMap::new()
            }
        };

        let index = new_indexer(config.index_type.clone(), config.path_db.clone())?;
        let mut db = LiteDb {
            config,
            active_file: RwLock::new(active_file),
            older_files: RwLock::new(older_files),
            index,
            file_ids,
            batch_commit_lock: Mutex::new(()),
            seq_no: AtomicUsize::new(1),
            merging_lock: Mutex::new(()),
            seq_file_exists: false,
            is_initial,
            lock_file,
            bytes_write: AtomicUsize::new(0),
            reclaim_size: AtomicUsize::new(0),
        };
        // B+ 树则不需要从数据文件中加载索引
        if db.config.index_type != IndexType::BPlusTree {
            // 从 hint 文件中加载索引
            db.load_index_from_hint_file()?;

            // 从数据文件中加载索引
            let current_seq_no = db.load_index_from_data_files()?;

            // 更新当前事务序列号
            if current_seq_no > 0 {
                db.seq_no.store(current_seq_no + 1, Ordering::SeqCst);
            }

            // 重置 IO 类型
            if db.config.mmap_at_startup {
                db.reset_io_type();
            }
        }

        if db.config.index_type == IndexType::BPlusTree {
            // 加载事务序列号
            let (exists, seq_no) = db.load_seq_no();
            if exists {
                db.seq_no.store(seq_no, Ordering::SeqCst);
                db.seq_file_exists = exists;
            }

            let active_file = db.active_file.write();
            active_file.set_write_off(active_file.file_size());
        }
        Ok(db)
    }

    pub fn open_table(&self) -> ResultDb<Table> {
        todo!()
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

        let previous = self.bytes_write.fetch_add(enc_log_db.len(), Ordering::SeqCst);
        // 根据配置项决定是否持久化
        let mut need_sync = self.config.sync_writes;
        if !need_sync && self.config.bytes_per_sync > 0 && previous + enc_log_db.len() >= self.config.bytes_per_sync {
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

    /// 从数据文件中加载内存索引
    /// 遍历数据文件中的内容，并依次处理其中的记录
    fn load_index_from_data_files(&self) -> ResultDb<usize> {
        let mut current_seq_no = NON_TRANSACTION_SEQ_NO;

        // 数据文件为空，直接返回
        if self.file_ids.is_empty() {
            return Ok(current_seq_no);
        }

        // 拿到最近未参与 merge 的文件 id
        let mut has_merge = false;
        let mut non_merge_fid = 0;
        let merge_fin_file = self.config.path_db.join(MERGE_FINISHED_FILE_NAME);
        if merge_fin_file.is_file() {
            let merge_fin_file = FileDb::new_merge_fin_file(self.config.path_db.clone())?;
            let merge_fin_record = merge_fin_file.read_log_db(0)?;
            let v = String::from_utf8(merge_fin_record.log_db.value).unwrap();

            non_merge_fid = v.parse::<u32>().unwrap();
            has_merge = true;
        }

        // 暂存事务相关的数据
        let mut transaction_log_dbs = HashMap::new();

        let active_file = self.active_file.read();
        let older_files = self.older_files.read();

        // 遍历每个文件 id，取出对应的数据文件，并加载其中的数据
        for (i, file_id) in self.file_ids.iter().enumerate() {
            // 如果比最近未参与 merge 的文件 id 更小，则已经从 hint 文件中加载索引了
            if has_merge && *file_id < non_merge_fid {
                continue;
            }

            let mut offset = 0;
            loop {
                let log_record_res = match *file_id == active_file.get_file_id() {
                    true => active_file.read_log_db(offset),
                    false => {
                        let data_file = older_files.get(file_id).unwrap();
                        data_file.read_log_db(offset)
                    }
                };

                let (mut log_db, size) = match log_record_res {
                    Ok(result) => (result.log_db, result.size),
                    Err(e) => {
                        if e == ErrDb::new_io_eof("") {
                            break;
                        }
                        return Err(e);
                    }
                };

                // 构建内存索引
                let log_db_pos = LogDbPos {
                    file_id: *file_id,
                    offset,
                    size: size as u32,
                };

                // 解析 key，拿到实际的 key 和 seq no
                let (real_key, seq_no) = parse_log_db_key(log_db.key.clone());
                // 非事务提交的情况，直接更新内存索引
                if seq_no == NON_TRANSACTION_SEQ_NO {
                    self.update_index(real_key, log_db.rec_type, log_db_pos);
                } else {
                    // 事务有提交的标识，更新内存索引
                    if log_db.rec_type == LogDbType::TXNFINISHED {
                        let records: &Vec<TransactionLogDb> = transaction_log_dbs.get(&seq_no).unwrap();
                        for txn_record in records.iter() {
                            self.update_index(txn_record.log_db.key.clone(), txn_record.log_db.rec_type, txn_record.pos);
                        }
                        transaction_log_dbs.remove(&seq_no);
                    } else {
                        log_db.key = real_key;
                        transaction_log_dbs
                            .entry(seq_no)
                            .or_insert(Vec::new())
                            .push(TransactionLogDb { log_db, pos: log_db_pos });
                    }
                }

                // 更新当前事务序列号
                if seq_no > current_seq_no {
                    current_seq_no = seq_no;
                }

                // 递增 offset，下一次读取的时候从新的位置开始
                offset += size as u64;
            }

            // 设置活跃文件的 offset
            if i == self.file_ids.len() - 1 {
                active_file.set_write_off(offset);
            }
        }
        Ok(current_seq_no)
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

    fn update_index(&self, key: Vec<u8>, rec_type: LogDbType, pos: LogDbPos) {
        if rec_type == LogDbType::NORMAL {
            if let Some(old_pos) = self.index.put(key.clone(), pos) {
                self.reclaim_size.fetch_add(old_pos.size as usize, Ordering::SeqCst);
            }
        }
        if rec_type == LogDbType::DELETED {
            let mut size = pos.size;
            if let Some(old_pos) = self.index.delete(key) {
                size += old_pos.size;
            }
            self.reclaim_size.fetch_add(size as usize, Ordering::SeqCst);
        }
    }

    // B+树索引模式下加载事务序列号
    fn load_seq_no(&self) -> (bool, usize) {
        let file_name = self.config.path_db.join(SEQ_NO_FILE_NAME);
        if !file_name.is_file() {
            return (false, 0);
        }

        let seq_no_file = FileDb::new_seq_no_file(self.config.path_db.clone()).unwrap();
        let log_db = match seq_no_file.read_log_db(0) {
            Ok(re) => re.log_db,
            Err(e) => panic!("failed to read seq no: {}", e),
        };
        let v = String::from_utf8(log_db.value).unwrap();
        let seq_no = v.parse::<usize>().unwrap();

        // 加载后删除掉，避免追加写入
        fs::remove_file(file_name).unwrap();

        (true, seq_no)
    }

    fn reset_io_type(&self) {
        let mut active_file = self.active_file.write();
        active_file.set_io_manager(self.config.path_db.clone(), IoType::StdIo);
        let mut older_files = self.older_files.write();
        for (_, file) in older_files.iter_mut() {
            file.set_io_manager(self.config.path_db.clone(), IoType::StdIo);
        }
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
            self.reclaim_size.fetch_add(old_pos.size as usize, Ordering::SeqCst);
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
        self.reclaim_size.fetch_add(pos.size as usize, Ordering::SeqCst);

        // delete the key in indexes
        if let Some(old_pos) = self.index.delete(key.to_vec()) {
            self.reclaim_size.fetch_add(old_pos.size as usize, Ordering::SeqCst);
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
        self.reclaim_size.fetch_add(pos.size as usize, Ordering::SeqCst);

        // delete the key in indexes
        if let Some(old_pos) = self.index.delete(key.to_vec()) {
            self.reclaim_size.fetch_add(old_pos.size as usize, Ordering::SeqCst);
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
    for entry in dir.flatten() {
        let file_os_str = entry.file_name();
        let file_name = file_os_str.to_str().unwrap();

        if file_name.ends_with(DATA_FILE_NAME_SUFFIX) {
            let split_names: Vec<&str> = file_name.split(".").collect();
            let file_id = split_names[0].parse::<u32>()?;

            file_ids.push(file_id);
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
    use std::{fs, path::PathBuf};

    use bytes::Bytes;
    use function_name::named;

    use crate::{
        db::{Adder, Closer, Config, Db, ErrDb, Getter, Remover},
        kits,
        kits::rand_kv::{get_test_key, get_test_value},
        lite::LiteDb,
    };

    fn ready_config(file: &str, name: &str) -> Config {
        let mut config = Config::default();
        config.path_db = PathBuf::from("temp").join(kits::com_names::path_name(file, name));
        config.file_size_db = 64u64 * 1024 * 1024;
        {
            //repeat run test
            let _ = fs::remove_dir_all(config.path_db.clone());
        }
        config
    }

    #[named]
    #[test]
    fn test_lite_db_put() {
        let config = ready_config(file!(), function_name!());
        {
            let lite_db = LiteDb::open(config.clone()).expect("failed to open engine");

            {
                let re = lite_db.add(&get_test_key(11), &get_test_value(11));
                assert!(re.is_ok());
                let re2 = lite_db.get(&get_test_key(11));
                assert_eq!(re2.unwrap(), get_test_value(11));
            }

            // 2. add the same key
            {
                let re = lite_db.add(&get_test_key(22), &get_test_value(22));
                assert!(re.is_ok());
                let re2 = lite_db.add(&get_test_key(22), &Bytes::from("a new value"));
                assert!(re2.is_ok());
                let re3 = lite_db.get(&get_test_key(22));
                assert_eq!(re3.unwrap(), Bytes::from("a new value"));
            }

            // 3.key is empty
            {
                let re = lite_db.add(&Bytes::new(), &get_test_value(123));
                assert_eq!(ErrDb::InvalidParameter, re.err().unwrap());
            }

            // 4.value is empty
            {
                let re = lite_db.add(&get_test_key(33), &Bytes::new());
                assert!(re.is_ok());
                let re2 = lite_db.get(&get_test_key(33));
                assert_eq!(0, re2.ok().unwrap().len());
            }

            // 5. write data till the make new data file
            for i in 0..=1000000 {
                let re = lite_db.add(&get_test_key(i), &get_test_value(i));
                assert!(re.is_ok());
            }
        }

        // 6. reopen db, and then Put data
        //must drop the db
        {
            let lite_db = LiteDb::open(config.clone()).expect("failed to open engine");
            let re = lite_db.add(&get_test_key(55), &get_test_value(55));
            assert!(re.is_ok());

            let re2 = lite_db.get(&get_test_key(55));
            assert_eq!(re2.unwrap(), &get_test_value(55));
        }

        // remove the test file
        fs::remove_dir_all(config.path_db.clone()).expect("failed to remove path");
    }

    #[named]
    #[test]
    fn test_lite_db_get() {
        let config = ready_config(file!(), function_name!());

        {
            let lite_db = LiteDb::open(config.clone()).expect("failed to open engine");

            // 1. normal
            {
                let re = lite_db.add(&get_test_key(111), &get_test_value(111));
                assert!(re.is_ok());
                let re2 = lite_db.get(&get_test_key(111));
                assert_eq!(re2.unwrap(), get_test_value(111));
            }

            // 2.get the key that not exist
            {
                let re = lite_db.get(&Bytes::from("not existed key"));
                assert!(matches!(re, Err(ErrDb::NotFindKey)));
            }

            // 3. repeat to add same key
            {
                let re1 = lite_db.add(&get_test_key(222), &get_test_value(222));
                assert!(re1.is_ok());
                let re2 = lite_db.add(&get_test_key(222), &Bytes::from("a new value"));
                assert!(re2.is_ok());
                let re3 = lite_db.get(&get_test_key(222));
                assert_eq!(Bytes::from("a new value"), re3.unwrap());
            }

            // 4. get after remove
            {
                let re1 = lite_db.add(&get_test_key(333), &get_test_value(333));
                assert!(re1.is_ok());
                let re2 = lite_db.remove(&get_test_key(333));
                assert!(re2.is_ok());
                let re3 = lite_db.get(&get_test_key(333));
                assert!(matches!(re3, Err(ErrDb::NotFindKey)));
            }

            // 5. to old file ，get value from the old file
            {
                for i in 500..=1000000 {
                    let re = lite_db.add(&get_test_key(i), &get_test_value(i));
                    assert!(re.is_ok());
                }
                let re2 = lite_db.get(&get_test_key(505));
                assert_eq!(get_test_value(505), re2.unwrap());
            }
        }

        // 6. reopen db, and then Put data
        //must drop the db
        {
            let lite_db = LiteDb::open(config.clone()).expect("failed to open engine");
            let re = lite_db.get(&get_test_key(111));
            assert_eq!(get_test_value(111), re.unwrap());
            let re2 = lite_db.get(&get_test_key(222));
            assert_eq!(Bytes::from("a new value"), re2.unwrap());
            let re3 = lite_db.get(&get_test_key(333));
            assert!(matches!(re3, Err(ErrDb::NotFindKey)));
        }

        // remove the test file
        fs::remove_dir_all(config.path_db.clone()).expect("failed to remove path");
    }

    #[named]
    #[test]
    fn test_lite_db_delete() {
        let config = ready_config(file!(), function_name!());

        {
            let lite_db = LiteDb::open(config.clone()).expect("failed to open engine");

            // 1.remove key
            {
                let re1 = lite_db.add(&get_test_key(111), &get_test_value(111));
                assert!(re1.is_ok());
                let re2 = lite_db.remove(&get_test_key(111));
                assert_eq!(re2.unwrap(), Some(get_test_value(111)));
                let re3 = lite_db.get(&get_test_key(111));
                assert!(matches!(re3, Err(ErrDb::NotFindKey)));
            }

            // 2.remove key that do not exist
            {
                let re = lite_db.remove(&Bytes::from("not-existed-key"));
                assert!(re.is_ok());
            }

            // 3. remove a empty key
            {
                let re = lite_db.remove(&Bytes::new());
                assert!(matches!(re, Err(ErrDb::InvalidParameter)));
            }

            // 4.add after remove
            {
                let re1 = lite_db.add(&get_test_key(222), &get_test_value(222));
                assert!(re1.is_ok());
                let re2 = lite_db.remove(&get_test_key(222));
                assert!(re2.is_ok());
                let re3 = lite_db.add(&get_test_key(222), &Bytes::from("a new value"));
                assert!(re3.is_ok());
                let re4 = lite_db.get(&get_test_key(222));
                assert_eq!(Bytes::from("a new value"), re4.unwrap());
            }
        }

        // 5.reopen db
        {
            let lite_db = LiteDb::open(config.clone()).expect("failed to open engine");
            let re1 = lite_db.get(&get_test_key(111));
            assert!(matches!(re1, Err(ErrDb::NotFindKey)));
            let re2 = lite_db.get(&get_test_key(222));
            assert_eq!(Bytes::from("a new value"), re2.unwrap());
        }

        // remove the test file
        fs::remove_dir_all(config.path_db.clone()).expect("failed to remove path");
    }

    #[named]
    #[test]
    fn test_lite_db_close() {
        let config = ready_config(file!(), function_name!());

        let lite_db = LiteDb::open(config.clone()).expect("failed to open engine");

        let re1 = lite_db.add(&get_test_key(222), &get_test_value(222));
        assert!(re1.is_ok());

        let close_re = lite_db.close();
        assert!(close_re.is_ok());

        // remove the test file
        fs::remove_dir_all(config.path_db.clone()).expect("failed to remove path");
    }

    #[named]
    #[test]
    fn test_lite_db_sync() {
        let config = ready_config(file!(), function_name!());

        let lite_db = LiteDb::open(config.clone()).expect("failed to open engine");
        let re1 = lite_db.add(&get_test_key(222), &get_test_value(222));
        assert!(re1.is_ok());

        let close_re = lite_db.sync();
        assert!(close_re.is_ok());

        // remove the test file
        fs::remove_dir_all(config.path_db.clone()).expect("failed to remove path");
    }

    #[named]
    #[test]
    fn test_lite_db_file_lock() {
        let config = ready_config(file!(), function_name!());

        let lite_db = LiteDb::open(config.clone()).expect("failed to open engine");

        let lite_db2 = LiteDb::open(config.clone());
        let _eof = ErrDb::new_io_eof("");
        assert!(matches!(lite_db2, Err(_eof)));

        let re2 = lite_db.close();
        assert!(matches!(re2, Ok(())));
        // std::mem::drop(lite_db);

        let re3 = LiteDb::open(config.clone());
        assert!(re3.is_ok());

        // remove the test file
        fs::remove_dir_all(config.path_db.clone()).expect("failed to remove path");
    }

    #[named]
    #[test]
    fn test_lite_db_stat() {
        let config = ready_config(file!(), function_name!());

        let lite_db = LiteDb::open(config.clone()).expect("failed to open engine");

        for i in 0..=10000 {
            let re = lite_db.add(&get_test_key(i), &get_test_value(i));
            assert!(re.is_ok());
        }
        for i in 0..=1000 {
            let re = lite_db.add(&get_test_key(i), &get_test_value(i));
            assert!(re.is_ok());
        }
        for i in 2000..=5000 {
            let re = lite_db.remove(&get_test_key(i));
            assert!(re.is_ok());
        }

        // let stat = lite_db.stat().unwrap();
        // assert!(stat.reclaim_size > 0);

        // remove the test file
        fs::remove_dir_all(config.path_db.clone()).expect("failed to remove path");
    }

    // #[named]
    // #[test]
    // fn test_lite_db_backup() {
    //     let mut config = ready_config(file!(), function_name!());
    //     let lite_db = LiteDb::open(config.clone()).expect("failed to open engine");
    //
    //     for i in 0..=10000 {
    //         let re = lite_db.add(&get_test_key(i), &get_test_value(i));
    //         assert!(re.is_ok());
    //     }
    //
    //     let backup_dir = PathBuf::from("/tmp/lite-db-backup-test");
    //     let bak_res = lite_db.backup(backup_dir.clone());
    //     assert!(bak_res.is_ok());
    //
    //     let mut config2 = Config::default();
    //     config2.path_db = backup_dir.clone();
    //     let lite_db2 = LiteDb::open(config2);
    //     assert!(lite_db2.is_ok());
    //
    // // remove the test file
    //     fs::remove_dir_all(config.path_db.clone()).expect("failed to remove path");
    // }
}
