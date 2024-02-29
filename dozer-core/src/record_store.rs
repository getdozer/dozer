use dozer_types::errors::types::DeserializationError;
use dozer_types::thiserror::Error;
use dozer_types::types::{Operation, Record, Schema};
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};

#[derive(Debug, Error)]
pub enum RecordWriterError {
    #[error("Record not found")]
    RecordNotFound,
}

pub trait RecordWriter: Send + Sync {
    fn write(&mut self, op: Operation) -> Result<Operation, RecordWriterError>;
}

impl Debug for dyn RecordWriter {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("RecordWriter")
    }
}

pub fn create_record_writer(schema: Schema) -> Result<Box<dyn RecordWriter>, DeserializationError> {
    let writer = Box::new(PrimaryKeyLookupRecordWriter::new(schema)?);
    Ok(writer)
}

#[derive(Debug)]
pub(crate) struct PrimaryKeyLookupRecordWriter {
    schema: Schema,
    index: HashMap<Vec<u8>, Record>,
}

impl PrimaryKeyLookupRecordWriter {
    pub(crate) fn new(schema: Schema) -> Result<Self, DeserializationError> {
        debug_assert!(
            !schema.primary_index.is_empty(),
            "PrimaryKeyLookupRecordWriter can only be used with a schema that has a primary key."
        );

        Ok(Self {
            schema,
            index: Default::default(),
        })
    }
}

impl RecordWriter for PrimaryKeyLookupRecordWriter {
    fn write(&mut self, op: Operation) -> Result<Operation, RecordWriterError> {
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
            Operation::BatchInsert { new } => {
                let mut new_records = Vec::with_capacity(new.len());
                for record in new {
                    let new_key = record.get_key(&self.schema.primary_index);
                    self.index.insert(new_key, record.clone());
                    new_records.push(record);
                }
                Ok(Operation::BatchInsert { new: new_records })
            }
        }
    }
}
