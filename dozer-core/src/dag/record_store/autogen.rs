use crate::dag::errors::ExecutionError;
use crate::dag::errors::ExecutionError::{UnsupportedDeleteOperation, UnsupportedUpdateOperation};
use crate::dag::record_store::common::RecordWriter;
use crate::storage::errors::StorageError;
use crate::storage::errors::StorageError::SerializationError;
use crate::storage::lmdb_storage::SharedTransaction;
use dozer_types::types::{Field, FieldDefinition, FieldType, Operation, Record, Schema};
use lmdb::Database;

const DOZER_ROWID: &str = "_DOZER_ROWID";

#[derive(Debug)]
pub struct AutogenRowKeyLookupRecordWriter {
    db: Database,
    meta_db: Database,
    schema: Schema,
}

impl AutogenRowKeyLookupRecordWriter {
    const COUNTER_KEY: u16 = 0_u16;

    pub fn prepare_schema(mut schema: Schema) -> Schema {
        schema.fields.push(FieldDefinition::new(
            DOZER_ROWID.to_string(),
            FieldType::UInt,
            false,
        ));
        schema.primary_index = vec![schema.fields.len() - 1];
        schema
    }

    pub fn new(db: Database, meta_db: Database, schema: Schema) -> Self {
        Self {
            db,
            meta_db,
            schema,
        }
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

    fn retr_record(&self, key: &[u8], tx: &SharedTransaction) -> Result<Record, ExecutionError> {
        let tx = tx.read();
        let curr = tx
            .get(self.db, key)?
            .ok_or_else(ExecutionError::RecordNotFound)?;

        let r: Record = bincode::deserialize(curr).map_err(|e| SerializationError {
            typ: "Record".to_string(),
            reason: Box::new(e),
        })?;
        Ok(r)
    }

    fn get_autogen_counter(&mut self, tx: &SharedTransaction) -> Result<u64, StorageError> {
        let curr_counter = match tx
            .read()
            .get(self.meta_db, &Self::COUNTER_KEY.to_le_bytes())?
        {
            Some(c) => u64::from_le_bytes(c.try_into().map_err(|e| {
                StorageError::DeserializationError {
                    typ: "u64".to_string(),
                    reason: Box::new(e),
                }
            })?),
            _ => 1_u64,
        };
        tx.write().put(
            self.meta_db,
            &Self::COUNTER_KEY.to_le_bytes(),
            &(curr_counter + 1).to_le_bytes(),
        )?;
        Ok(curr_counter)
    }
}

impl RecordWriter for AutogenRowKeyLookupRecordWriter {
    fn write(
        &mut self,
        op: Operation,
        tx: &SharedTransaction,
    ) -> Result<Operation, ExecutionError> {
        match op {
            Operation::Insert { mut new } => {
                let ctr = self.get_autogen_counter(tx)?;
                new.values.push(Field::UInt(ctr));
                assert!(
                    self.schema.primary_index.len() == 1
                        && self.schema.primary_index[0] == new.values.len() - 1
                );
                self.write_record(&new, &self.schema, tx)?;
                Ok(Operation::Insert { new })
            }
            Operation::Update { .. } => Err(UnsupportedUpdateOperation(
                "AutogenRowsIdLookupRecordWriter does not support update operations".to_string(),
            )),
            Operation::Delete { .. } => Err(UnsupportedDeleteOperation(
                "AutogenRowsIdLookupRecordWriter does not support delete operations".to_string(),
            )),
        }
    }
}
