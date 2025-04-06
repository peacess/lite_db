use std::{fs, fs::OpenOptions, path::PathBuf};

use log::error;
use memmap2::{MmapMut, RemapOptions};
use parking_lot::RwLock;

use super::DbIo;
use crate::db::{ErrDb, ResultDb};

pub struct MMapIo {
    map: RwLock<MmapMut>,
    file: fs::File,
}

impl MMapIo {
    pub fn new(file_name: PathBuf) -> ResultDb<Self> {
        match OpenOptions::new().create(true).truncate(true).read(true).write(true).open(file_name) {
            Ok(file) => {
                let map = unsafe { MmapMut::map_mut(&file)? };
                Ok(MMapIo { map: RwLock::new(map), file })
            }
            Err(e) => {
                error!("{}", e);
                Err(ErrDb::IoErr(e))
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
    #[cfg(not(windows))]
    fn write(&self, _buf: &[u8]) -> ResultDb<usize> {
        let mut w = self.map.write();
        let old_len = w.len();
        let file_len = self.file.metadata()?.len() as usize;
        if file_len < old_len + _buf.len() {
            self.file.set_len(old_len as u64 + _buf.len() as u64)?;
        }
        unsafe {
            w.remap(old_len + _buf.len(), RemapOptions::new().may_move(true))?;
        };

        w[old_len..].copy_from_slice(_buf);
        Ok(_buf.len())
    }
    #[cfg(windows)]
    fn write(&self, _buf: &[u8]) -> ResultDb<usize> {
        let mut w = self.map.write();
        let old_len = w.len();
        let file_len = self.file.metadata()?.len() as usize;
        if file_len < old_len + _buf.len() {
            self.file.set_len(old_len as u64 + _buf.len() as u64)?;
        }
        // os windows do not support the "remap"
        unsafe {
            *(self.map.data_ptr()) = MmapMut::map_mut(&self.file)?;
        }

        (&mut w[old_len..]).copy_from_slice(_buf);
        Ok(_buf.len())
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

    use function_name::named;

    use super::*;
    use crate::{io_db::MMapIo, kits};

    fn make_file_name(file: &str, name: &str) -> PathBuf {
        let mut path_buf = PathBuf::from("temp");
        path_buf = path_buf.join(kits::com_names::file_name(file, name, "data"));
        if let Some(parent) = path_buf.parent() {
            if !parent.exists() {
                fs::create_dir_all(parent).expect("");
            }
        }
        path_buf
    }

    #[named]
    #[test]
    fn test_mmap_read() {
        let path = make_file_name(file!(), function_name!());
        {
            let _ = fs::remove_file(path.clone());
        }

        // file is empty
        {
            let re = MMapIo::new(path.clone());
            assert!(re.is_ok());
            let mmap_io = re.ok().unwrap();
            let mut buf = [0u8; 10];
            let re2 = mmap_io.read(&mut buf, 0);
            let _err_eof = ErrDb::new_io_eof("");
            assert!(matches!(re2, Err(_err_eof)));
        }

        // data
        {
            let re = MMapIo::new(path.clone());
            assert!(re.is_ok());
            let mmap_io = re.ok().unwrap();

            let mut buf1 = [0u8; 2];
            buf1[0] = 1;
            buf1[1] = 3;
            let re2 = mmap_io.read(&mut buf1, 2);
            let _err_eof = ErrDb::new_io_eof("");
            assert!(matches!(re2, Err(_err_eof)));

            let re_write = mmap_io.write(&buf1);
            assert!(matches!(re_write, Ok(2)));

            let mut buf2 = [0u8; 2];
            let re_read = mmap_io.read(&mut buf2, 0);
            assert!(matches!(re_read, Ok(2)));
            assert_eq!(buf1, buf2);

            let re_remove = fs::remove_file(path.clone());
            assert!(re_remove.is_ok());
        }
    }
}
