use dozer_storage::{lmdb::Database, lmdb_storage::SharedTransaction};
use dozer_types::{
    bincode,
    types::{Field, FieldDefinition, FieldType, Operation, Record, Schema, SourceDefinition},
};

use crate::{
    errors::ExecutionError,
    record_store::{RecordWriter, INITIAL_RECORD_VERSION},
};
use crate::{
    errors::ExecutionError::{UnsupportedDeleteOperation, UnsupportedUpdateOperation},
    record_store::DOZER_ROWID,
};
use dozer_storage::errors::StorageError::SerializationError;

#[derive(Debug)]
pub struct HashKeyRecordWriter {
    db: Database,
    schema: Schema,
}

impl HashKeyRecordWriter {
    pub fn prepare_schema(mut schema: Schema) -> Schema {
        schema.fields.push(FieldDefinition::new(
            DOZER_ROWID.to_string(),
            FieldType::UInt,
            false,
            SourceDefinition::Dynamic,
        ));
        schema.primary_index = vec![schema.fields.len() - 1];
        schema
    }

    pub fn new(db: Database, schema: Schema) -> Self {
        Self { db, schema }
    }

    fn write_record(
        &self,
        rec: &Record,
        schema: &Schema,
        tx: &SharedTransaction,
    ) -> Result<(), ExecutionError> {
        let key = rec.get_key(&schema.primary_index);
        let value = bincode::serialize(&rec).map_err(|e| SerializationError {
            typ: "Record".to_string(),
            reason: Box::new(e),
        })?;
        tx.write().put(self.db, key.as_slice(), value.as_slice())?;
        Ok(())
    }
}

impl RecordWriter for HashKeyRecordWriter {
    fn write(
        &mut self,
        op: Operation,
        tx: &SharedTransaction,
    ) -> Result<Operation, ExecutionError> {
        match op {
            Operation::Insert { mut new } => {
                let record_hash = 0;
                new.values.push(Field::UInt(record_hash));
                assert!(
                    self.schema.primary_index.len() == 1
                        && self.schema.primary_index[0] == new.values.len() - 1
                );
                self.write_record(&new, &self.schema, tx)?;
                new.version = Some(INITIAL_RECORD_VERSION);
                Ok(Operation::Insert { new })
            }
            Operation::Update { .. } => Err(UnsupportedUpdateOperation(
                "HashKeyRecordWriter does not support update operations".to_string(),
            )),
            Operation::Delete { .. } => Err(UnsupportedDeleteOperation(
                "HashKeyRecordWriter does not support delete operations".to_string(),
            )),
        }
    }
}
