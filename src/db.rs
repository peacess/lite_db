use crate::{LiteErr, LiteResult};

pub struct Key{

}
pub struct Value{

}
pub struct Config {}

pub trait Db{
    fn get(&self,k:&Key) -> Result<Option<Value>,LiteErr>;
    fn add(&self, k: &Key, v: &Value) -> Result<(),LiteErr>;
    fn remove(&self,k: &Key) -> Result<Option<Value>, LiteErr>;
}

pub struct LiteDb{

}

impl LiteDb {
    pub fn open<T>(cfg: &Config) -> LiteResult<Self>{
        Err(LiteErr::None)
    }
    pub fn close(&self) -> LiteResult<()>{
        Ok(())
    }
}