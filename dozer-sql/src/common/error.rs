use std::fmt::{Display, Formatter};
use std::{error, result};

pub type Result<T> = result::Result<T, DozerSqlError>;

#[derive(Debug)]
pub enum DozerSqlError {
    NotImplemented(String),
}

impl Display for DozerSqlError {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match *self {
            DozerSqlError::NotImplemented(ref desc) => {
                write!(f, "This feature is not implemented: {}", desc)
            }
        }
    }
}

impl error::Error for DozerSqlError {}
