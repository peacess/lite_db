use std::fmt::{Debug, Display, Formatter};
use std::io;

/// the "result" is easy to confuse, add "db" suffix
pub type ResultDb<T> = Result<T, ErrDb>;

/// the "err" is easy to confuse, add "db" suffix
#[derive(Debug)]
pub enum ErrDb {
    None,
    NotFindKey,
    Err(String),
    InvalidLogRecordCrc,
    IoErr(io::Error),
    ParseIntError(std::num::ParseIntError),

}

impl ErrDb {
    #[inline]
    pub fn is_none(&self) -> bool {
        match self {
            ErrDb::None => true,
            _ => false,
        }
    }

    #[inline]
    pub fn is_not_none(&self) -> bool {
        match self {
            ErrDb::None => false,
            _ => true,
        }
    }

    pub fn new_io_eof(info: &str) -> ErrDb {
        ErrDb::IoErr(io::Error::new(io::ErrorKind::UnexpectedEof, info))
    }
    pub fn new_io_file_not_find(info: &str) -> ErrDb {
        ErrDb::IoErr(io::Error::new(io::ErrorKind::NotFound, info))
    }
}

impl Display for ErrDb {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ErrDb::None => write!(f, "None"),
            ErrDb::NotFindKey => write!(f, "not find key"),
            ErrDb::Err(e) => write!(f, "{}", e),
            ErrDb::InvalidLogRecordCrc => write!(f, "invalid log record crc"),
            ErrDb::IoErr(e) => write!(f, "{}", e),
            ErrDb::ParseIntError(e) => write!(f, "{}", e),
        }
    }
}

impl std::error::Error for ErrDb {}

impl PartialEq for ErrDb {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (ErrDb::None, ErrDb::None) => true,
            (ErrDb::NotFindKey, ErrDb::NotFindKey) => true,
            (ErrDb::Err(s), ErrDb::Err(s2)) => s.eq(s2),
            (ErrDb::InvalidLogRecordCrc, ErrDb::InvalidLogRecordCrc) => true,
            (ErrDb::IoErr(e1), ErrDb::IoErr(e2)) => {
                (e1.kind() == e2.kind()) && (e1.to_string() == e2.to_string())
            }
            (ErrDb::ParseIntError(s1), ErrDb::ParseIntError(e2)) => s1.eq(e2),
            _ => false
        }
    }
}

impl From<io::Error> for ErrDb {
    fn from(e: io::Error) -> Self {
        ErrDb::IoErr(e)
    }
}

impl From<std::num::ParseIntError> for ErrDb {
    fn from(e: std::num::ParseIntError) -> Self {
        ErrDb::ParseIntError(e)
    }
}