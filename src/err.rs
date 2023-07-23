pub type DbResult<T> = Result<T, DbErr>;

pub struct DbErr {}