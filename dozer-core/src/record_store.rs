use crate::node::OutputPortType;
use dozer_types::thiserror::Error;
use dozer_types::types::{Operation, ProcessorRecord, Schema};
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};

use super::node::PortHandle;

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

pub fn create_record_writer(
    _output_port: PortHandle,
    output_port_type: OutputPortType,
    schema: Schema,
) -> Option<Box<dyn RecordWriter>> {
    match output_port_type {
        OutputPortType::Stateless => None,

        OutputPortType::StatefulWithPrimaryKeyLookup => {
            let writer = Box::new(PrimaryKeyLookupRecordWriter::new(schema));
            Some(writer)
        }
    }
}

#[derive(Debug)]
pub(crate) struct PrimaryKeyLookupRecordWriter {
    schema: Schema,
    index: HashMap<Vec<u8>, ProcessorRecord>,
}

impl PrimaryKeyLookupRecordWriter {
    pub(crate) fn new(schema: Schema) -> Self {
        debug_assert!(
            !schema.primary_index.is_empty(),
            "PrimaryKeyLookupRecordWriter can only be used with a schema that has a primary key."
        );
        Self {
            schema,
            index: HashMap::new(),
        }
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
        }
    }
}
