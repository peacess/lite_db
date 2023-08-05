use std::{fs::OpenOptions, path::PathBuf};
use std::io::Write;
use std::sync::Arc;

use log::error;
use memmap2::MmapMut;
use parking_lot::RwLock;

use crate::db::{ErrDb, ResultDb};

use super::DbIo;

pub struct MMapIo {
    map: Arc<RwLock<MmapMut>>,
}

impl MMapIo {
    pub fn new(file_name: PathBuf) -> ResultDb<Self> {
        match OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(file_name)
        {
            Ok(file) => {
                let map = unsafe { MmapMut::map_mut(&file)? };
                return Ok(MMapIo {
                    map: Arc::new(RwLock::new(map)),
                });
            }
            Err(e) => {
                error!("{}", e);
                return Err(ErrDb::IoErr(e));
            }
        }
    }
}

impl DbIo for MMapIo {
    fn read(&self, buf: &mut [u8], offset: u64) -> ResultDb<usize> {
        let end = offset + buf.len() as u64;
        let r = self.map.read();
        if end > r.len() as u64 {
            return Err(ErrDb::new_io_eof("out of file index"));
        }
        let val = &r[offset as usize..end as usize];
        buf.copy_from_slice(val);
        Ok(val.len())
    }

    fn write(&self, _buf: &[u8]) -> ResultDb<usize> {
        let mut w = self.map.write();
        let r = (&mut w[..]).write(_buf)?;
        Ok(r)
    }

    fn sync(&self) -> ResultDb<()> {
        self.map.read().flush()?;
        Ok(())
    }

    fn size(&self) -> u64 {
        self.map.read().len() as u64
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use crate::io_db::MMapIo;

    use super::*;

    #[test]
    fn test_mmap_read() {
        let path = PathBuf::from("/tmp/mmap-test.data");

        // file is empty
        {
            let mmap_io1 = MMapIo::new(path.clone());
            assert!(mmap_io1.is_ok());
            let mmap_io1 = mmap_io1.ok().unwrap();
            let mut buf1 = [0u8; 10];
            let read_io1 = mmap_io1.read(&mut buf1, 0);
            assert_eq!(read_io1.is_err(), true);
        }

        // data
        {
            let mmap_io2 = MMapIo::new(path.clone());
            assert!(mmap_io2.is_ok());
            let mmap_io2 = mmap_io2.ok().unwrap();

            let mut buf2 = [0u8; 2];
            let read_io2 = mmap_io2.read(&mut buf2, 2);
            assert!(read_io2.is_ok());

            let remove_res = fs::remove_file(path.clone());
            assert!(remove_res.is_ok());
        }
    }
}
