use dozer_core::dozer_log::{storage::Object, tokio::sync::mpsc::error::SendError};
use dozer_types::{
    bincode,
    serde::Serialize,
    thiserror::{self, Error},
};

#[derive(Debug, Error)]
pub enum SerializationError {
    #[error("bincode error: {0}")]
    Bincode(#[from] bincode::Error),
    #[error("Cannot send value to persisting thread")]
    SendError,
}

impl<T> From<SendError<T>> for SerializationError {
    fn from(_: SendError<T>) -> Self {
        Self::SendError
    }
}

pub fn serialize_u64(value: u64, object: &mut Object) -> Result<(), SerializationError> {
    object.write(&value.to_le_bytes()).map_err(Into::into)
}

pub fn serialize_vec_u8(vec: &[u8], object: &mut Object) -> Result<(), SerializationError> {
    serialize_u64(vec.len() as u64, object)?;
    object.write(vec).map_err(Into::into)
}

pub fn serialize_bincode(
    value: impl Serialize,
    object: &mut Object,
) -> Result<(), SerializationError> {
    let data = bincode::serialize(&value)?;
    serialize_vec_u8(&data, object)
}
