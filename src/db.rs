use crate::DbResult;

pub struct Key {}

pub struct Value {}

pub trait Getter {
    fn get(&self, key: &Key) -> DbResult<Value>;
}

pub trait Adder {
    fn add(&self, key: &Key, v: &Value) -> DbResult<()>;
}

pub trait Remover {
    fn remove(&self, key: &Key) -> DbResult<Value>;
}

pub trait Closer {
    fn close(&self) -> DbResult<()>;
}

pub trait Editor: Getter + Adder + Remover {}

pub trait Db: Editor + Closer {}

