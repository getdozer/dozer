use crate::dag::errors::ExecutionError;
use crate::dag::record_store::common::{RecordWriter, RecordWriterUtils};
use crate::storage::errors::StorageError::SerializationError;
use crate::storage::lmdb_storage::SharedTransaction;
use dozer_types::types::{Operation, Record, Schema};
use lmdb::Database;

#[derive(Debug)]
pub struct PrimaryKeyLookupRecordWriter {
    db: Database,
    meta_db: Database,
    schema: Schema,
    retr_old_records_for_deletes: bool,
    retr_old_records_for_updates: bool,
}

impl PrimaryKeyLookupRecordWriter {
    pub(crate) fn new(
        db: Database,
        meta_db: Database,
        schema: Schema,
        retr_old_records_for_deletes: bool,
        retr_old_records_for_updates: bool,
    ) -> Self {
        Self {
            db,
            meta_db,
            schema,
            retr_old_records_for_deletes,
            retr_old_records_for_updates,
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
}

impl RecordWriter for PrimaryKeyLookupRecordWriter {
    fn write(
        &mut self,
        op: Operation,
        tx: &SharedTransaction,
    ) -> Result<Operation, ExecutionError> {
        match op {
            Operation::Insert { new } => {
                self.write_record(&new, &self.schema, tx)?;
                Ok(Operation::Insert { new })
            }
            Operation::Delete { mut old } => {
                let key = old.get_key(&self.schema.primary_index);
                if self.retr_old_records_for_deletes {
                    old = self.retr_record(&key, tx)?;
                }
                tx.write().del(self.db, &key, None)?;
                Ok(Operation::Delete { old })
            }
            Operation::Update { mut old, new } => {
                let key = old.get_key(&self.schema.primary_index);
                if self.retr_old_records_for_updates {
                    old = self.retr_record(&key, tx)?;
                }
                self.write_record(&new, &self.schema, tx)?;
                Ok(Operation::Update { old, new })
            }
        }
    }
}
