use crate::db::ResultDb;

// pub struct Key {}
pub type Key = bytes::Bytes;

// pub struct Value {}
pub type Value = bytes::Bytes;

pub trait Getter {
    fn get(&self, key: &Key) -> ResultDb<Value>;
}

pub trait Adder {
    fn add(&self, key: &Key, v: &Value) -> ResultDb<()>;
}

pub trait Remover {
    /// if can not find Key，then return None
    fn remove(&self, key: &Key) -> ResultDb<Option<Value>>;
    fn remove_fast(&self, key: &Key) -> ResultDb<()>;
}

pub trait Closer {
    fn close(&self) -> ResultDb<()>;
}

pub trait Editor: Getter + Adder + Remover {}

pub trait Db: Editor + Closer {
    fn sync(&self) -> ResultDb<()>;
}
