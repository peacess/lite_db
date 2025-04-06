use crate::{
    db::{decode_log_db_pos, ErrDb, FileDb, ResultDb, HINT_FILE_NAME},
    lite::LiteDb,
};

impl LiteDb {
    /// 从 hint 索引文件中加载索引
    pub(crate) fn load_index_from_hint_file(&self) -> ResultDb<()> {
        let hint_file_name = self.config.path_db.join(HINT_FILE_NAME);
        // 如果 hint 文件不存在则返回
        if !hint_file_name.is_file() {
            return Ok(());
        }

        let hint_file = FileDb::new_hint_file(self.config.path_db.clone())?;
        let mut offset = 0;
        loop {
            let (log_record, size) = match hint_file.read_log_db(offset) {
                Ok(result) => (result.log_db, result.size),
                Err(e) => {
                    if e == ErrDb::new_io_eof("") {
                        break;
                    }
                    return Err(e);
                }
            };

            // 解码 value，拿到位置索引信息
            let log_record_pos = decode_log_db_pos(log_record.value);
            // 存储到内存索引中
            self.index.put(log_record.key, log_record_pos);
            offset += size as u64;
        }
        Ok(())
    }
}
