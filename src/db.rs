use crate::DbResult;

pub struct Config {}

pub trait Db {
    fn open<T>(cfg: &Config) -> DbResult<T>;
    fn close(&self) -> DbResult<()>;
}