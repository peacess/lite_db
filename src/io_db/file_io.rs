use std::{
    fs::{File, OpenOptions},
    io::Write,
    path::PathBuf,
    sync::Arc,
};

use log::error;
use parking_lot::RwLock;

use crate::db::{ErrDb, ResultDb};

use super::DbIo;

pub struct FileIo {
    fd: Arc<RwLock<File>>,
}

impl FileIo {
    pub fn new(file_name: PathBuf) -> ResultDb<Self> {
        match OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .append(true)
            .open(file_name)
        {
            Ok(file) => {
                return Ok(FileIo {
                    fd: Arc::new(RwLock::new(file)),
                });
            }
            Err(e) => {
                error!("{}", e);
                return Err(ErrDb::IoErr(e));
            }
        }
    }
}

impl DbIo for FileIo {
    #[cfg(not(windows))]
    fn read(&self, buf: &mut [u8], offset: u64) -> ResultDb<usize> {
        use std::os::unix::fs::FileExt;
        let read = self.fd.read();
        match read.read_at(buf, offset) {
            Ok(n) => return Ok(n),
            Err(e) => {
                error!("{}", e);
                return Err(ErrDb::IoErr(e));
            }
        };
    }

    #[cfg(windows)]
    fn read(&self, buf: &mut [u8], offset: u64) -> ResultDb<usize> {
        use std::os::windows::fs::FileExt;
        let read = self.fd.read();
        match read.seek_read(buf, offset) {
            Ok(n) => return Ok(n),
            Err(e) => {
                error!("{}", e);
                return Err(ErrDb::IoErr(e));
            }
        };
    }

    fn write(&self, buf: &[u8]) -> ResultDb<usize> {
        let mut write = self.fd.write();
        match write.write(buf) {
            Ok(n) => return Ok(n),
            Err(e) => {
                error!("{}", e);
                return Err(ErrDb::IoErr(e));
            }
        }
    }

    fn sync(&self) -> ResultDb<()> {
        let read = self.fd.read();
        if let Err(e) = read.sync_all() {
            error!("failed to sync data file: {}", e);
            return Err(ErrDb::IoErr(e));
        }
        Ok(())
    }

    fn size(&self) -> u64 {
        let read = self.fd.read();
        let metadata = read.metadata().unwrap();
        metadata.len()
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::*;

    #[test]
    fn test_file_io_write() {
        let path = PathBuf::from("/tmp/a.data");
        let fio_res = FileIo::new(path.clone());
        assert!(fio_res.is_ok());
        let fio = fio_res.ok().unwrap();

        let res1 = fio.write("key-a".as_bytes());
        assert!(res1.is_ok());
        assert_eq!(5, res1.ok().unwrap());

        let res2 = fio.write("key-b".as_bytes());
        assert!(res2.is_ok());
        assert_eq!(5, res2.ok().unwrap());

        let res3 = fs::remove_file(path.clone());
        assert!(res3.is_ok());
    }

    #[test]
    fn test_file_io_read() {
        let path = PathBuf::from("/tmp/b.data");
        let fio_res = FileIo::new(path.clone());
        assert!(fio_res.is_ok());
        let fio = fio_res.ok().unwrap();

        let res1 = fio.write("key-a".as_bytes());
        assert!(res1.is_ok());
        assert_eq!(5, res1.ok().unwrap());

        let res2 = fio.write("key-b".as_bytes());
        assert!(res2.is_ok());
        assert_eq!(5, res2.ok().unwrap());

        let mut buf = [0u8; 5];
        let read_res1 = fio.read(&mut buf, 0);
        assert!(read_res1.is_ok());
        assert_eq!(5, read_res1.ok().unwrap());

        let mut buf2 = [0u8; 5];
        let read_res2 = fio.read(&mut buf2, 5);
        assert!(read_res2.is_ok());
        assert_eq!(5, read_res2.ok().unwrap());

        let res3 = fs::remove_file(path.clone());
        assert!(res3.is_ok());
    }

    #[test]
    fn test_file_io_sync() {
        let path = PathBuf::from("/tmp/c.data");
        let fio_res = FileIo::new(path.clone());
        assert!(fio_res.is_ok());
        let fio = fio_res.ok().unwrap();

        let res1 = fio.write("key-a".as_bytes());
        assert!(res1.is_ok());
        assert_eq!(5, res1.ok().unwrap());

        let res2 = fio.write("key-b".as_bytes());
        assert!(res2.is_ok());
        assert_eq!(5, res2.ok().unwrap());

        let sync_res = fio.sync();
        assert!(sync_res.is_ok());

        let res3 = fs::remove_file(path.clone());
        assert!(res3.is_ok());
    }

    #[test]
    fn test_file_io_size() {
        let path = PathBuf::from("/tmp/d.data");
        let fio_res = FileIo::new(path.clone());
        assert!(fio_res.is_ok());
        let fio = fio_res.ok().unwrap();

        let size1 = fio.size();
        assert_eq!(size1, 0);

        let res2 = fio.write("key-b".as_bytes());
        assert!(res2.is_ok());

        let size2 = fio.size();
        assert_eq!(size2, 5);

        let res3 = fs::remove_file(path.clone());
        assert!(res3.is_ok());
    }
}
