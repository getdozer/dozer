use crate::executor_operation::ProcessorOperation;
use crate::node::OutputPortType;
use crate::processor_record::{ProcessorRecord, ProcessorRecordStore};
use dozer_storage::errors::StorageError;
use dozer_types::thiserror::{self, Error};
use dozer_types::types::Schema;
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use super::node::PortHandle;

#[derive(Debug, Error)]
pub enum RecordWriterError {
    #[error("Record not found")]
    RecordNotFound,
    #[error("Storage error: {0}")]
    Storage(#[from] StorageError),
}

pub trait RecordWriter: Send + Sync {
    fn write(&mut self, op: ProcessorOperation) -> Result<ProcessorOperation, RecordWriterError>;
}

impl Debug for dyn RecordWriter {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("RecordWriter")
    }
}

pub fn create_record_writer(
    _output_port: PortHandle,
    output_port_type: OutputPortType,
    schema: Schema,
    record_store: Arc<ProcessorRecordStore>,
) -> Option<Box<dyn RecordWriter>> {
    match output_port_type {
        OutputPortType::Stateless => None,

        OutputPortType::StatefulWithPrimaryKeyLookup => {
            let writer = Box::new(PrimaryKeyLookupRecordWriter::new(schema, record_store));
            Some(writer)
        }
    }
}

#[derive(Debug)]
pub(crate) struct PrimaryKeyLookupRecordWriter {
    schema: Schema,
    record_store: Arc<ProcessorRecordStore>,
    index: HashMap<Vec<u8>, ProcessorRecord>,
}

impl PrimaryKeyLookupRecordWriter {
    pub(crate) fn new(schema: Schema, record_store: Arc<ProcessorRecordStore>) -> Self {
        debug_assert!(
            !schema.primary_index.is_empty(),
            "PrimaryKeyLookupRecordWriter can only be used with a schema that has a primary key."
        );
        Self {
            schema,
            record_store,
            index: HashMap::new(),
        }
    }
}

impl RecordWriter for PrimaryKeyLookupRecordWriter {
    fn write(&mut self, op: ProcessorOperation) -> Result<ProcessorOperation, RecordWriterError> {
        match op {
            ProcessorOperation::Insert { new } => {
                let new_record = self.record_store.load_record(&new)?;
                let new_key = new_record.get_key(&self.schema.primary_index);
                self.index.insert(new_key, new.clone());
                Ok(ProcessorOperation::Insert { new })
            }
            ProcessorOperation::Delete { mut old } => {
                let old_record = self.record_store.load_record(&old)?;
                let old_key = old_record.get_key(&self.schema.primary_index);
                old = self
                    .index
                    .remove_entry(&old_key)
                    .ok_or(RecordWriterError::RecordNotFound)?
                    .1;
                Ok(ProcessorOperation::Delete { old })
            }
            ProcessorOperation::Update { mut old, new } => {
                let old_record = self.record_store.load_record(&old)?;
                let old_key = old_record.get_key(&self.schema.primary_index);
                old = self
                    .index
                    .remove_entry(&old_key)
                    .ok_or(RecordWriterError::RecordNotFound)?
                    .1;
                let new_record = self.record_store.load_record(&new)?;
                let new_key = new_record.get_key(&self.schema.primary_index);
                self.index.insert(new_key, new.clone());
                Ok(ProcessorOperation::Update { old, new })
            }
        }
    }
}
