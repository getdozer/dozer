use std::{error, fmt, result};
use std::fmt::{Display, Formatter};

pub type Result<T> = result::Result<T, DozerError>;


#[derive(Debug)]
pub enum DozerError {
    NotImplemented(String),
}

impl Display for DozerError {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match *self {
            DozerError::NotImplemented(ref desc) => {
                write!(f, "This feature is not implemented: {}", desc)
            }
        }
    }
}

impl error::Error for DozerError {}