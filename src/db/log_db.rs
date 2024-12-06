use bytes::{BufMut, BytesMut};
use prost::{
    encode_length_delimiter,
    encoding::{decode_varint, encode_varint},
    length_delimiter_len,
};

#[derive(PartialEq, Clone, Copy, Debug)]
pub enum LogDbType {
    // 正常 put 的数据
    NORMAL = 1,

    // 被删除的数据标识，墓碑值
    DELETED = 2,

    // 事务完成的标识
    TXNFINISHED = 3,
}

/// LogDb log of db
#[derive(Debug)]
pub struct LogDb {
    pub(crate) key: Vec<u8>,
    pub(crate) value: Vec<u8>,
    pub(crate) rec_type: LogDbType,
}

#[derive(Clone, Copy, Debug)]
pub struct LogDbPos {
    pub(crate) file_id: u32,
    pub(crate) offset: u64,
    pub(crate) size: u32,
}

#[derive(Debug)]
pub struct ReadLogDb {
    pub(crate) log_db: LogDb,
    pub(crate) size: usize,
}

#[derive(Debug)]
pub struct TransactionLogDb {
    pub(crate) log_db: LogDb,
    pub(crate) pos: LogDbPos,
}

impl LogDb {
    // encode 对 LogDb 进行编码，返回字节数组及长度
    //
    //	+-------------+--------------+-------------+--------------+-------------+-------------+
    //	|  type 类型   |    key size |   value size |      key    |      value   |  crc 校验值  |
    //	+-------------+-------------+--------------+--------------+-------------+-------------+
    //	    1字节        变长（最大5）   变长（最大5）        变长           变长           4字节
    pub fn encode(&self) -> Vec<u8> {
        let (enc_buf, _) = self.encode_and_get_crc();
        enc_buf
    }

    pub fn get_crc(&self) -> u32 {
        let (_, crc_value) = self.encode_and_get_crc();
        crc_value
    }

    fn encode_and_get_crc(&self) -> (Vec<u8>, u32) {
        // 初始化字节数组，存放编码数据
        let mut buf = BytesMut::new();
        buf.reserve(self.encoded_length());

        // 第一个字节存放 Type 类型
        buf.put_u8(self.rec_type as u8);

        // 再存储 key 和 value 的长度
        encode_length_delimiter(self.key.len(), &mut buf).unwrap();
        encode_length_delimiter(self.value.len(), &mut buf).unwrap();

        // 存储 key 和 value
        buf.extend_from_slice(&self.key);
        buf.extend_from_slice(&self.value);

        // 计算并存储 CRC 校验值
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&buf);
        let crc = hasher.finalize();
        buf.put_u32(crc);

        (buf.to_vec(), crc)
    }

    // LogDb 编码后的长度
    fn encoded_length(&self) -> usize {
        std::mem::size_of::<u8>() + length_delimiter_len(self.key.len()) + length_delimiter_len(self.value.len()) + self.key.len() + self.value.len() + 4
    }
}

impl LogDbType {
    pub fn from_u8(v: u8) -> Self {
        match v {
            1 => LogDbType::NORMAL,
            2 => LogDbType::DELETED,
            3 => LogDbType::TXNFINISHED,
            _ => panic!("unknown log db type"),
        }
    }
}

impl LogDbPos {
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = BytesMut::new();
        encode_varint(self.file_id as u64, &mut buf);
        encode_varint(self.offset, &mut buf);
        encode_varint(self.size as u64, &mut buf);
        buf.to_vec()
    }
}

pub fn max_log_db_header_size() -> usize {
    std::mem::size_of::<u8>() + length_delimiter_len(std::u32::MAX as usize) * 2
}

pub fn decode_log_db_pos(pos: Vec<u8>) -> LogDbPos {
    let mut buf = BytesMut::new();
    buf.put_slice(&pos);

    let fid = match decode_varint(&mut buf) {
        Ok(fid) => fid,
        Err(e) => panic!("decode log db pos err: {}", e),
    };
    let offset = match decode_varint(&mut buf) {
        Ok(offset) => offset,
        Err(e) => panic!("decode log db pos err: {}", e),
    };
    let size = match decode_varint(&mut buf) {
        Ok(size) => size,
        Err(e) => panic!("decode log db pos err: {}", e),
    };
    LogDbPos {
        file_id: fid as u32,
        offset,
        size: size as u32,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_log_db_encode_and_crc() {
        // 正常的一条 LogDb 编码
        let log_db1 = LogDb {
            key: "name".as_bytes().to_vec(),
            value: "bitcask-rs".as_bytes().to_vec(),
            rec_type: LogDbType::NORMAL,
        };
        let en_log_db1 = log_db1.encode();
        assert!(en_log_db1.len() > 5);
        assert_eq!(1020360578, log_db1.get_crc());

        // LogDb 的 value 为空
        let log_db2 = LogDb {
            key: "name".as_bytes().to_vec(),
            value: Default::default(),
            rec_type: LogDbType::NORMAL,
        };
        let en_log_db2 = log_db2.encode();
        assert!(en_log_db2.len() > 5);
        assert_eq!(3756865478, log_db2.get_crc());

        // 类型为 Deleted 的情况
        let log_db3 = LogDb {
            key: "name".as_bytes().to_vec(),
            value: "bitcask-rs".as_bytes().to_vec(),
            rec_type: LogDbType::DELETED,
        };
        let en_log_db3 = log_db3.encode();
        assert!(en_log_db3.len() > 5);
        assert_eq!(1867197446, log_db3.get_crc());
    }
}
