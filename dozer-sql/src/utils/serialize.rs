use dozer_core::dozer_log::{storage::Object, tokio::sync::mpsc::error::SendError};
use dozer_recordstore::{ProcessorRecord, ProcessorRecordStore, ProcessorRecordStoreDeserializer};
use dozer_types::{
    bincode,
    serde::{de::DeserializeOwned, Serialize},
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

pub struct Cursor<'a>(&'a [u8]);

impl<'a> Cursor<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        Self(data)
    }

    pub fn consume(&mut self, len: usize) -> Result<&'a [u8], DeserializationError> {
        if self.0.len() < len {
            return Err(DeserializationError::NotEnoughData {
                requested: len,
                remaining: self.0.len(),
            });
        }

        let (head, tail) = self.0.split_at(len);
        self.0 = tail;
        Ok(head)
    }
}

#[derive(Debug, Error)]
pub enum DeserializationError {
    #[error("not enough data: requested {requested}, remaining {remaining}")]
    NotEnoughData { requested: usize, remaining: usize },
    #[error("bincode error: {0}")]
    Bincode(#[from] bincode::Error),
    #[error("record store error: {0}")]
    RecordStore(#[from] dozer_recordstore::RecordStoreError),
}

pub fn serialize_u64(value: u64, object: &mut Object) -> Result<(), SerializationError> {
    object.write(&value.to_le_bytes()).map_err(Into::into)
}

pub fn deserialize_u64(cursor: &mut Cursor) -> Result<u64, DeserializationError> {
    let bytes = cursor.consume(8)?;
    Ok(u64::from_le_bytes(bytes.try_into().unwrap()))
}

pub fn serialize_vec_u8(vec: &[u8], object: &mut Object) -> Result<(), SerializationError> {
    serialize_u64(vec.len() as u64, object)?;
    object.write(vec).map_err(Into::into)
}

pub fn deserialize_vec_u8<'a>(cursor: &mut Cursor<'a>) -> Result<&'a [u8], DeserializationError> {
    let len = deserialize_u64(cursor)? as usize;
    let data = cursor.consume(len)?;
    Ok(data)
}

pub fn serialize_bincode(
    value: impl Serialize,
    object: &mut Object,
) -> Result<(), SerializationError> {
    let data = bincode::serialize(&value)?;
    serialize_vec_u8(&data, object)
}

pub fn deserialize_bincode<T: DeserializeOwned>(
    cursor: &mut Cursor,
) -> Result<T, DeserializationError> {
    let data = deserialize_vec_u8(cursor)?;
    Ok(bincode::deserialize(data)?)
}

pub fn serialize_record(
    record: &ProcessorRecord,
    record_store: &ProcessorRecordStore,
    object: &mut Object,
) -> Result<(), SerializationError> {
    serialize_vec_u8(&record_store.serialize_record(record)?, object)
}

pub fn deserialize_record(
    cursor: &mut Cursor,
    record_store: &ProcessorRecordStoreDeserializer,
) -> Result<ProcessorRecord, DeserializationError> {
    record_store
        .deserialize_record(deserialize_vec_u8(cursor)?)
        .map_err(Into::into)
}
