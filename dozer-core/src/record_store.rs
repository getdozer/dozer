use crate::errors::ExecutionError;
use crate::errors::ExecutionError::RecordNotFound;
use crate::node::OutputPortType;
use dozer_storage::errors::StorageError;
use dozer_types::types::{Operation, Record, Schema};
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};

use super::node::PortHandle;

pub trait RecordWriter: Send + Sync {
    fn write(&mut self, op: Operation) -> Result<Operation, ExecutionError>;
}

impl Debug for dyn RecordWriter {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("RecordWriter")
    }
}

#[allow(clippy::type_complexity)]
pub fn create_record_store(
    _output_port: PortHandle,
    output_port_type: OutputPortType,
    schema: Schema,
) -> Result<Option<Box<dyn RecordWriter>>, StorageError> {
    match output_port_type {
        OutputPortType::Stateless => Ok(None),

        OutputPortType::StatefulWithPrimaryKeyLookup => {
            let writer = Box::new(PrimaryKeyLookupRecordWriter::new(schema));
            Ok(Some(writer))
        }
    }
}

#[derive(Debug)]
pub(crate) struct PrimaryKeyLookupRecordWriter {
    schema: Schema,
    index: HashMap<u64, Record>,
}

impl PrimaryKeyLookupRecordWriter {
    pub(crate) fn new(schema: Schema) -> Self {
        Self {
            schema,
            index: HashMap::new(),
        }
    }
}

impl RecordWriter for PrimaryKeyLookupRecordWriter {
    fn write(&mut self, op: Operation) -> Result<Operation, ExecutionError> {
        match op {
            Operation::Insert { new } => {
                let new_key = new.get_hashed_primary_key(&self.schema);
                self.index.insert(new_key, new.clone());
                Ok(Operation::Insert { new })
            }
            Operation::Delete { mut old } => {
                let old_key = old.get_hashed_primary_key(&self.schema);
                old = self
                    .index
                    .remove_entry(&old_key)
                    .ok_or_else(RecordNotFound)?
                    .1;
                Ok(Operation::Delete { old })
            }
            Operation::Update { mut old, new } => {
                let old_key = old.get_hashed_primary_key(&self.schema);
                old = self
                    .index
                    .remove_entry(&old_key)
                    .ok_or_else(RecordNotFound)?
                    .1;
                let new_key = new.get_hashed_primary_key(&self.schema);
                self.index.insert(new_key, new.clone());
                Ok(Operation::Update { old, new })
            }
        }
    }
}
