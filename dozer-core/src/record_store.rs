use crate::checkpoint::serialize::{
    deserialize_record, deserialize_u64, deserialize_vec_u8, serialize_record, serialize_u64,
    serialize_vec_u8, Cursor, DeserializationError, SerializationError,
};
use dozer_log::storage::Object;
use dozer_recordstore::{ProcessorRecordStore, ProcessorRecordStoreDeserializer, RecordStoreError};
use dozer_types::thiserror::{self, Error};
use dozer_types::types::{Operation, Record, Schema};
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};

#[derive(Debug, Error)]
pub enum RecordWriterError {
    #[error("Record not found")]
    RecordNotFound,
    #[error("Recordstore error: {0}")]
    RecordStore(#[from] RecordStoreError),
}

pub trait RecordWriter: Send + Sync {
    fn write(
        &mut self,
        record_store: &ProcessorRecordStore,
        op: Operation,
    ) -> Result<Operation, RecordWriterError>;
    fn serialize(
        &self,
        record_store: &ProcessorRecordStore,
        object: Object,
    ) -> Result<(), SerializationError>;
}

impl Debug for dyn RecordWriter {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("RecordWriter")
    }
}

pub fn create_record_writer(
    schema: Schema,
    record_store: &ProcessorRecordStoreDeserializer,
    checkpoint_data: Option<Vec<u8>>,
) -> Result<Box<dyn RecordWriter>, DeserializationError> {
    let writer = Box::new(PrimaryKeyLookupRecordWriter::new(
        schema,
        record_store,
        checkpoint_data,
    )?);
    Ok(writer)
}

#[derive(Debug)]
pub(crate) struct PrimaryKeyLookupRecordWriter {
    schema: Schema,
    index: HashMap<Vec<u8>, Record>,
}

impl PrimaryKeyLookupRecordWriter {
    pub(crate) fn new(
        schema: Schema,
        record_store: &ProcessorRecordStoreDeserializer,
        checkpoint_data: Option<Vec<u8>>,
    ) -> Result<Self, DeserializationError> {
        debug_assert!(
            !schema.primary_index.is_empty(),
            "PrimaryKeyLookupRecordWriter can only be used with a schema that has a primary key."
        );

        let index = if let Some(checkpoint_data) = checkpoint_data {
            let mut cursor = Cursor::new(&checkpoint_data);
            let mut index = HashMap::new();
            let len = deserialize_u64(&mut cursor)?;
            for _ in 0..len {
                let key = deserialize_vec_u8(&mut cursor)?.to_vec();
                let record = deserialize_record(&mut cursor, record_store)?;
                index.insert(key, record);
            }
            index
        } else {
            HashMap::new()
        };

        Ok(Self { schema, index })
    }
}

impl RecordWriter for PrimaryKeyLookupRecordWriter {
    fn write(
        &mut self,
        _record_store: &ProcessorRecordStore,
        op: Operation,
    ) -> Result<Operation, RecordWriterError> {
        match op {
            Operation::Insert { new } => {
                let new_key = new.get_key(&self.schema.primary_index);
                self.index.insert(new_key, new.clone());
                Ok(Operation::Insert { new })
            }
            Operation::Delete { mut old } => {
                let old_key = old.get_key(&self.schema.primary_index);
                old = self
                    .index
                    .remove_entry(&old_key)
                    .ok_or(RecordWriterError::RecordNotFound)?
                    .1;
                Ok(Operation::Delete { old })
            }
            Operation::Update { mut old, new } => {
                let old_key = old.get_key(&self.schema.primary_index);
                old = self
                    .index
                    .remove_entry(&old_key)
                    .ok_or(RecordWriterError::RecordNotFound)?
                    .1;
                let new_key = new.get_key(&self.schema.primary_index);
                self.index.insert(new_key, new.clone());
                Ok(Operation::Update { old, new })
            }
        }
    }

    fn serialize(
        &self,
        record_store: &ProcessorRecordStore,
        mut object: Object,
    ) -> Result<(), SerializationError> {
        serialize_u64(self.index.len() as u64, &mut object)?;
        for (key, record) in &self.index {
            serialize_vec_u8(key, &mut object)?;
            serialize_record(record, record_store, &mut object)?;
        }
        Ok(())
    }
}
