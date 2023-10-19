use crate::checkpoint::serialize::{
    deserialize_record, deserialize_u64, deserialize_vec_u8, serialize_record, serialize_u64,
    serialize_vec_u8, Cursor, DeserializationError, SerializationError,
};
use crate::executor_operation::ProcessorOperation;
use dozer_log::storage::Object;
use dozer_recordstore::{
    ProcessorRecord, ProcessorRecordStore, ProcessorRecordStoreDeserializer, RecordStoreError,
    StoreRecord,
};
use dozer_types::thiserror::{self, Error};
use dozer_types::types::Schema;
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
        op: ProcessorOperation,
    ) -> Result<ProcessorOperation, RecordWriterError>;
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
    index: HashMap<Vec<u8>, ProcessorRecord>,
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
        record_store: &ProcessorRecordStore,
        op: ProcessorOperation,
    ) -> Result<ProcessorOperation, RecordWriterError> {
        match op {
            ProcessorOperation::Insert { new } => {
                let new_record = record_store.load_record(&new)?;
                let new_key = new_record.get_key(&self.schema.primary_index);
                self.index.insert(new_key, new.clone());
                Ok(ProcessorOperation::Insert { new })
            }
            ProcessorOperation::Delete { mut old } => {
                let old_record = record_store.load_record(&old)?;
                let old_key = old_record.get_key(&self.schema.primary_index);
                old = self
                    .index
                    .remove_entry(&old_key)
                    .ok_or(RecordWriterError::RecordNotFound)?
                    .1;
                Ok(ProcessorOperation::Delete { old })
            }
            ProcessorOperation::Update { mut old, new } => {
                let old_record = record_store.load_record(&old)?;
                let old_key = old_record.get_key(&self.schema.primary_index);
                old = self
                    .index
                    .remove_entry(&old_key)
                    .ok_or(RecordWriterError::RecordNotFound)?
                    .1;
                let new_record = record_store.load_record(&new)?;
                let new_key = new_record.get_key(&self.schema.primary_index);
                self.index.insert(new_key, new.clone());
                Ok(ProcessorOperation::Update { old, new })
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
