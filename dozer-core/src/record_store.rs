use crate::errors::ExecutionError;
use crate::errors::ExecutionError::{
    RecordNotFound, UnsupportedDeleteOperation, UnsupportedUpdateOperation,
};
use crate::node::OutputPortType;
use std::collections::{HashMap, VecDeque};

use dozer_storage::common::Database;
use dozer_storage::errors::StorageError;
use dozer_storage::errors::StorageError::{DeserializationError, SerializationError};
use dozer_storage::lmdb::DatabaseFlags;
use dozer_storage::lmdb_storage::SharedTransaction;
use dozer_storage::prefix_transaction::PrefixTransaction;
use dozer_types::bincode;
use dozer_types::types::{
    Field, FieldDefinition, FieldType, Operation, Record, Schema, SourceDefinition,
};
use dyn_clone::DynClone;
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
    output_port: PortHandle,
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

const PORT_STATE_KEY: &str = "__PORT_STATE_";

const VERSIONED_RECORDS_INDEX_ID: u32 = 0x01;
const RECORD_VERSIONS_INDEX_ID: u32 = 0x02;
const INITIAL_RECORD_VERSION: u32 = 1_u32;

const RECORD_PRESENT_FLAG: u8 = 0x01;
const RECORD_DELETED_FLAG: u8 = 0x00;

#[derive(Debug)]
pub(crate) struct PrimaryKeyLookupRecordWriter {
    schema: Schema,
    index: HashMap<Vec<Field>, Record>,
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
            Operation::Insert { mut new } => {
                let new_key = new.get_key_fields(&self.schema);
                self.index.insert(new_key, new.clone());
                Ok(Operation::Insert { new })
            }
            Operation::Delete { mut old } => {
                let old_key = old.get_key_fields(&self.schema);
                old = self
                    .index
                    .remove_entry(&old_key)
                    .ok_or_else(RecordNotFound)?
                    .1;
                Ok(Operation::Delete { old })
            }
            Operation::Update { mut old, mut new } => {
                let old_key = old.get_key_fields(&self.schema);
                old = self
                    .index
                    .remove_entry(&old_key)
                    .ok_or_else(RecordNotFound)?
                    .1;
                let new_key = new.get_key_fields(&self.schema);
                self.index.insert(new_key, new.clone());
                Ok(Operation::Update { old, new })
            }
        }
    }
}
