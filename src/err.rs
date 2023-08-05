use std::fmt::{Debug, Display, Formatter};

pub type DbResult<T> = Result<T, DbErr>;

#[derive(Debug)]
pub enum DbErr {
    None,
    Err(String),
    IoErr(std::io::Error),
}

impl Display for DbErr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            DbErr::None => write!(f, "None"),
            DbErr::Err(e) => write!(f, "{}", e),
            DbErr::IoErr(e) => write!(f, "{}", e),
        }
    }
}

impl std::error::Error for DbErr {}

impl DbErr {
    #[inline]
    pub fn is_none(&self) -> bool {
        match self {
            DbErr::None => true,
            _ => false,
        }
    }

    #[inline]
    pub fn is_not_none(&self) -> bool {
        match self {
            DbErr::None => false,
            _ => true,
        }
    }
}
