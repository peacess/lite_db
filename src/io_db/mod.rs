pub use file_io::*;
pub use mmap::*;

use crate::db::{IoType, ResultDb};

mod file_io;
mod mmap;

pub trait DbIo: Sync + Send {
    fn read(&self, buf: &mut [u8], offset: u64) -> ResultDb<usize>;
    fn write(&self, buf: &[u8]) -> ResultDb<usize>;
    fn sync(&self) -> ResultDb<()>;
    fn size(&self) -> u64;
}

pub fn new_dbio(file_name: std::path::PathBuf, io_type: IoType) -> Box<dyn DbIo> {
    match io_type {
        IoType::StdIo => Box::new(FileIo::new(file_name).unwrap()),
        IoType::MemoryMap => Box::new(MMapIo::new(file_name).unwrap()),
    }
}
