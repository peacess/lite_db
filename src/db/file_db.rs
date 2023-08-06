use std::path::PathBuf;
use std::sync::Arc;

use bytes::{Buf, BytesMut};
use parking_lot::RwLock;
use prost::{decode_length_delimiter, length_delimiter_len};

use crate::db::{LogDb, LogDbPos, LogDbType, max_log_db_header_size, ReadLogDb};
use crate::db::{ErrDb, IoType, ResultDb};
use crate::io_db;
use crate::io_db::new_dbio;

pub struct FileDb {
    file_id: Arc<RwLock<u32>>,
    // 数据文件id
    write_off: Arc<RwLock<u64>>,
    // 当前写偏移，记录该数据文件写到哪个位置了
    db_io: Box<dyn io_db::DbIo>,
}

pub const DATA_FILE_NAME_SUFFIX: &str = ".data";
pub const HINT_FILE_NAME: &str = "hint-index";
pub const MERGE_FINISHED_FILE_NAME: &str = "merge-finished";
pub const SEQ_NO_FILE_NAME: &str = "seq-no";

impl FileDb {
    pub fn new(dir_path: PathBuf, file_id: u32, io_type: IoType) -> ResultDb<FileDb> {
        // 根据 path 和 id 构造出完整的文件名称
        let file_name = FileDb::get_data_file_name(dir_path, file_id);
        // 初始化 io manager
        let io_manager = new_dbio(file_name, io_type);

        Ok(FileDb {
            file_id: Arc::new(RwLock::new(file_id)),
            write_off: Arc::new(RwLock::new(0)),
            db_io: io_manager,
        })
    }

    pub fn new_hint_file(dir_path: PathBuf) -> ResultDb<FileDb> {
        let file_name = dir_path.join(HINT_FILE_NAME);
        let io_manager = new_dbio(file_name, IoType::StdIo);

        Ok(FileDb {
            file_id: Arc::new(RwLock::new(0)),
            write_off: Arc::new(RwLock::new(0)),
            db_io: io_manager,
        })
    }

    pub fn new_merge_fin_file(dir_path: PathBuf) -> ResultDb<FileDb> {
        let file_name = dir_path.join(MERGE_FINISHED_FILE_NAME);
        let io_manager = new_dbio(file_name, IoType::StdIo);

        Ok(FileDb {
            file_id: Arc::new(RwLock::new(0)),
            write_off: Arc::new(RwLock::new(0)),
            db_io: io_manager,
        })
    }

    pub fn new_seq_no_file(dir_path: PathBuf) -> ResultDb<FileDb> {
        let file_name = dir_path.join(SEQ_NO_FILE_NAME);
        let io_manager = new_dbio(file_name, IoType::StdIo);

        Ok(FileDb {
            file_id: Arc::new(RwLock::new(0)),
            write_off: Arc::new(RwLock::new(0)),
            db_io: io_manager,
        })
    }

    pub fn file_size(&self) -> u64 {
        self.db_io.size()
    }

    pub fn get_write_off(&self) -> u64 {
        let read_guard = self.write_off.read();
        *read_guard
    }

    pub fn set_write_off(&self, offset: u64) {
        let mut write_guard = self.write_off.write();
        *write_guard = offset;
    }

    pub fn get_file_id(&self) -> u32 {
        let read_guard = self.file_id.read();
        *read_guard
    }

    pub fn read_log_db(&self, offset: u64) -> ResultDb<ReadLogDb> {
        // 先读取出 header 部分的数据
        let mut header_buf = BytesMut::zeroed(max_log_db_header_size());

        self.db_io.read(&mut header_buf, offset)?;

        // 取出 type，在第一个字节
        let rec_type = header_buf.get_u8();

        // 取出 key 和 value 的长度
        let key_size = decode_length_delimiter(&mut header_buf).unwrap();
        let value_size = decode_length_delimiter(&mut header_buf).unwrap();

        // 如果 key 和 value 均为空，则说明读取到了文件的末尾，直接返回
        if key_size == 0 && value_size == 0 {
            return Err(ErrDb::new_io_eof(""));
        }

        // 获取实际的 header 大小
        let actual_header_size = length_delimiter_len(key_size) + length_delimiter_len(value_size) + 1;

        // 读取实际的 key 和 value，最后的 4 个字节是 crc 校验值
        let mut kv_buf = BytesMut::zeroed(key_size + value_size + 4);
        self.db_io.read(&mut kv_buf, offset + actual_header_size as u64)?;

        let log_db = LogDb {
            key: kv_buf.get(..key_size).unwrap().to_vec(),
            value: kv_buf.get(key_size..kv_buf.len() - 4).unwrap().to_vec(),
            rec_type: LogDbType::from_u8(rec_type),
        };

        // 向前移动到最后的 4 个字节，就是 crc 的值
        kv_buf.advance(key_size + value_size);

        if kv_buf.get_u32() != log_db.get_crc() {
            return Err(ErrDb::InvalidLogDbCrc);
        }

        // 构造结果并返回
        Ok(ReadLogDb {
            log_db: log_db,
            size: actual_header_size + key_size + value_size + 4,
        })
    }

    pub fn write(&self, buf: &[u8]) -> ResultDb<usize> {
        let n_bytes = self.db_io.write(buf)?;
        // 更新 write_off 字段
        let mut write_off = self.write_off.write();
        *write_off += n_bytes as u64;

        Ok(n_bytes)
    }

    pub fn write_hint_log_db(&self, key: Vec<u8>, pos: LogDbPos) -> ResultDb<()> {
        let log_db = LogDb {
            key,
            value: pos.encode(),
            rec_type: LogDbType::NORMAL,
        };
        let enc_log_db = log_db.encode();
        self.write(&enc_log_db)?;
        Ok(())
    }

    pub fn sync(&self) -> ResultDb<()> {
        self.db_io.sync()
    }

    pub fn set_io_manager(&mut self, dir_path: PathBuf, io_type: IoType) {
        self.db_io = new_dbio(FileDb::get_data_file_name(dir_path, self.get_file_id()), io_type);
    }

    pub fn get_data_file_name(dir_path: PathBuf, file_id: u32) -> PathBuf {
        let name = std::format!("{:09}", file_id) + DATA_FILE_NAME_SUFFIX;
        dir_path.join(name)
    }
}
