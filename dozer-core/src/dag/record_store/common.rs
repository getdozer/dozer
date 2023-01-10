use crate::dag::errors::ExecutionError;
use crate::dag::errors::ExecutionError::{UnsupportedDeleteOperation, UnsupportedUpdateOperation};
use crate::dag::node::OutputPortType;

use crate::dag::record_store::autogen::AutogenRowKeyLookupRecordWriter;
use crate::dag::record_store::pk::PrimaryKeyLookupRecordWriter;
use crate::storage::common::Database;
use crate::storage::errors::StorageError;
use crate::storage::errors::StorageError::SerializationError;
use crate::storage::lmdb_storage::SharedTransaction;
use dozer_types::types::{Field, FieldDefinition, FieldType, Operation, Record, Schema};
use std::fmt::{Debug, Formatter};

pub trait RecordWriter {
    fn write(&mut self, op: Operation, tx: &SharedTransaction)
        -> Result<Operation, ExecutionError>;
}

impl Debug for dyn RecordWriter {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("RecordWriter")
    }
}

pub(crate) struct RecordWriterUtils {}

impl RecordWriterUtils {
    pub fn create_writer(
        typ: OutputPortType,
        db: Database,
        meta_db: Database,
        schema: Schema,
    ) -> Result<Box<dyn RecordWriter>, ExecutionError> {
        match typ {
            OutputPortType::StatefulWithPrimaryKeyLookup {
                retr_old_records_for_updates,
                retr_old_records_for_deletes,
            } => Ok(Box::new(PrimaryKeyLookupRecordWriter::new(
                db,
                meta_db,
                schema,
                retr_old_records_for_deletes,
                retr_old_records_for_updates,
            ))),
            OutputPortType::AutogenRowKeyLookup => Ok(Box::new(
                AutogenRowKeyLookupRecordWriter::new(db, meta_db, schema),
            )),
            _ => panic!(
                "Unexpected port type in RecordWriterUtils::create_writer(): {}",
                typ
            ),
        }
    }
}

#[derive(Debug)]
pub struct RecordReader {
    tx: SharedTransaction,
    db: Database,
}

impl RecordReader {
    pub fn new(tx: SharedTransaction, db: Database) -> Self {
        Self { tx, db }
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError> {
        self.tx
            .read()
            .get(self.db, key)
            .map(|b| b.map(|b| b.to_vec()))
    }
}
