use thiserror::Error;

#[derive(Error, Debug)]
pub enum TypeError {
    #[error("Invalid field index: {0}")]
    InvalidFieldIndex(usize),
    #[error("Invalid field name: {0}")]
    InvalidFieldName(String),
    #[error("Invalid field type")]
    InvalidFieldType,
}
