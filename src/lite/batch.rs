use std::{
    collections::HashMap,
    sync::{Arc, atomic::Ordering},
};

use bytes::{BufMut, Bytes, BytesMut};
use parking_lot::Mutex;
use prost::{decode_length_delimiter, encode_length_delimiter};

use crate::db::{Db, ErrDb, LogDb, LogDbType, ResultDb, WriteBatchOptions};
use crate::lite::LiteDb;

const TXN_FIN_KEY: &[u8] = "txn-fin".as_bytes();
pub(crate) const NON_TRANSACTION_SEQ_NO: usize = 0;

/// 批量写操作，保证原子性
pub struct WriteBatch<'a> {
    pub(super) pending: Arc<Mutex<HashMap<Vec<u8>, LogDb>>>,
    // 暂存用户写入的数据
    pub(super) db: &'a LiteDb,
    pub(super) options: WriteBatchOptions,
}

impl WriteBatch<'_> {
    /// 批量操作写数据
    pub fn put(&self, key: Bytes, value: Bytes) -> ResultDb<()> {
        if key.is_empty() {
            return Err(ErrDb::InvalidParameter);
        }

        // 暂存数据
        let log_db = LogDb {
            key: key.to_vec(),
            value: value.to_vec(),
            rec_type: LogDbType::NORMAL,
        };

        let mut pending_writes = self.pending.lock();
        pending_writes.insert(key.to_vec(), log_db);
        Ok(())
    }

    /// 批量操作删除数据
    pub fn delete(&self, key: Bytes) -> ResultDb<()> {
        if key.is_empty() {
            return Err(ErrDb::InvalidParameter);
        }

        let mut pending_writes = self.pending.lock();
        // 如果数据不存在则直接返回
        let index_pos = self.db.index.get(key.to_vec());
        if index_pos.is_none() {
            if pending_writes.contains_key(&key.to_vec()) {
                pending_writes.remove(&key.to_vec());
            }
            return Ok(());
        }

        // 暂存数据
        let log_db = LogDb {
            key: key.to_vec(),
            value: Default::default(),
            rec_type: LogDbType::DELETED,
        };
        pending_writes.insert(key.to_vec(), log_db);
        Ok(())
    }

    /// 提交数据，将数据写到文件当中，并更新内存索引
    pub fn commit(&self) -> ResultDb<()> {
        let mut pending_writes = self.pending.lock();
        if pending_writes.len() == 0 {
            return Ok(());
        }
        if pending_writes.len() > self.options.max_batch_num {
            return Err(ErrDb::InvalidBatch);
        }

        // 加锁保证事务提交串行化
        let _lock = self.db.batch_commit_lock.lock();

        // 获取全局事务序列号
        let seq_no = self.db.seq_no.fetch_add(1, Ordering::SeqCst);

        let mut positions = HashMap::new();
        // 开始写数据到数据文件当中
        for (_, item) in pending_writes.iter() {
            let mut log_db = LogDb {
                key: log_db_key_with_seq(item.key.clone(), seq_no),
                value: item.value.clone(),
                rec_type: item.rec_type,
            };

            let pos = self.db.append_log_db(&mut log_db)?;
            positions.insert(item.key.clone(), pos);
        }

        // 写最后一条标识事务完成的数据
        let mut finish_log_db = LogDb {
            key: log_db_key_with_seq(TXN_FIN_KEY.to_vec(), seq_no),
            value: Default::default(),
            rec_type: LogDbType::TXNFINISHED,
        };
        self.db.append_log_db(&mut finish_log_db)?;

        // 如果配置了持久化，则 sync
        if self.options.sync_writes {
            self.db.sync()?;
        }

        // 数据全部写完之后更新内存索引
        for (_, item) in pending_writes.iter() {
            if item.rec_type == LogDbType::NORMAL {
                let log_db_pos = positions.get(&item.key).unwrap();
                if let Some(old_pos) = self.db.index.put(item.key.clone(), *log_db_pos) {
                    self.db
                        .reclaim_size
                        .fetch_add(old_pos.size as usize, Ordering::SeqCst);
                }
            }
            if item.rec_type == LogDbType::DELETED {
                if let Some(old_pos) = self.db.index.delete(item.key.clone()) {
                    self.db
                        .reclaim_size
                        .fetch_add(old_pos.size as usize, Ordering::SeqCst);
                }
            }
        }

        // 清空暂存数据
        pending_writes.clear();

        Ok(())
    }
}

// 编码 seq no 和 key
pub(crate) fn log_db_key_with_seq(key: Vec<u8>, seq_no: usize) -> Vec<u8> {
    let mut enc_key = BytesMut::new();
    encode_length_delimiter(seq_no, &mut enc_key).unwrap();
    enc_key.extend_from_slice(&key.to_vec());
    enc_key.to_vec()
}

// 解析 LogDb 的 key，拿到实际的 key 和 seq no
pub(crate) fn parse_log_db_key(key: Vec<u8>) -> (Vec<u8>, usize) {
    let mut buf = BytesMut::new();
    buf.put_slice(&key);
    let seq_no = decode_length_delimiter(&mut buf).unwrap();
    (buf.to_vec(), seq_no)
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::atomic::Ordering;

    use crate::db::{Closer, Config, ErrDb, Getter, WriteBatchOptions};
    use crate::kits;
    use crate::lite::LiteDb;

    #[test]
    fn test_write_batch_1() {
        let mut opts = Config::default();
        opts.dir_path = PathBuf::from("/tmp/bitcask-rs-batch-1");
        opts.data_file_size = 64 * 1024 * 1024;
        let engine = LiteDb::open(opts.clone()).expect("failed to open engine");

        let wb = engine
            .new_write_batch(WriteBatchOptions::default())
            .expect("failed to create write batch");
        // 写数据之后未提交
        let put_res1 = wb.put(
            kits::rand_kv::get_test_key(1),
            kits::rand_kv::get_test_value(10),
        );
        assert!(put_res1.is_ok());
        let put_res2 = wb.put(
            kits::rand_kv::get_test_key(2),
            kits::rand_kv::get_test_value(10),
        );
        assert!(put_res2.is_ok());

        let res1 = engine.get(kits::rand_kv::get_test_key(1));
        assert_eq!(ErrDb::NotFindKey, res1.err().unwrap());

        // 事务提交之后进行查询
        let commit_res = wb.commit();
        assert!(commit_res.is_ok());

        let res2 = engine.get(kits::rand_kv::get_test_key(1));
        assert!(res2.is_ok());

        // 验证事务序列号
        let seq_no = wb.db.seq_no.load(Ordering::SeqCst);
        assert_eq!(2, seq_no);

        // 删除测试的文件夹
        std::fs::remove_dir_all(opts.clone().dir_path).expect("failed to remove path");
    }

    #[test]
    fn test_write_batch_2() {
        let mut opts = Config::default();
        opts.dir_path = PathBuf::from("/tmp/bitcask-rs-batch-2");
        opts.data_file_size = 64 * 1024 * 1024;
        let engine = LiteDb::open(opts.clone()).expect("failed to open engine");

        let wb = engine
            .new_write_batch(WriteBatchOptions::default())
            .expect("failed to create write batch");
        let put_res1 = wb.put(
            kits::rand_kv::get_test_key(1),
            kits::rand_kv::get_test_value(10),
        );
        assert!(put_res1.is_ok());
        let put_res2 = wb.put(
            kits::rand_kv::get_test_key(2),
            kits::rand_kv::get_test_value(10),
        );
        assert!(put_res2.is_ok());
        let commit_res1 = wb.commit();
        assert!(commit_res1.is_ok());

        let put_res3 = wb.put(
            kits::rand_kv::get_test_key(1),
            kits::rand_kv::get_test_value(10),
        );
        assert!(put_res3.is_ok());

        let commit_res2 = wb.commit();
        assert!(commit_res2.is_ok());

        // 重启之后进行校验
        engine.close().expect("failed to close");
        std::mem::drop(engine);

        let engine2 = LiteDb::open(opts.clone()).expect("failed to open engine");
        let keys = engine2.list_keys();
        assert_eq!(2, keys.ok().unwrap().len());

        // 验证事务序列号
        let seq_no = engine2.seq_no.load(Ordering::SeqCst);
        assert_eq!(3, seq_no);

        // 删除测试的文件夹
        std::fs::remove_dir_all(opts.clone().dir_path).expect("failed to remove path");
    }

    // #[test]
    // fn test_write_batch_3() {
    //     let mut opts = Options::default();
    //     opts.dir_path = PathBuf::from("/tmp/bitcask-rs-batch-3");
    //     opts.data_file_size = 64 * 1024 * 1024;
    //     let engine = Engine::open(opts.clone()).expect("failed to open engine");

    //     let keys = engine.list_keys();
    //     println!("key len {:?}", keys);

    //     // let mut wb_opts = WriteBatchOptions::default();
    //     // wb_opts.max_batch_num = 10000000;
    //     // let wb = engine.new_write_batch(wb_opts).expect("failed to create write batch");

    //     // for i in 0..=1000000 {
    //     //     let put_res = wb.put(util::rand_kv::get_test_key(i), util::rand_kv::get_test_value(10));
    //     //     assert!(put_res.is_ok());
    //     // }

    //     // wb.commit();
    // }
}
