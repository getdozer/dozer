use crate::dag::errors::ExecutionError;
use crate::dag::errors::ExecutionError::{UnsupportedDeleteOperation, UnsupportedUpdateOperation};
use crate::dag::node::OutputPortType;
use crate::dag::node::OutputPortType::AutogenRowKeyLookup;
use crate::storage::common::Database;
use crate::storage::errors::StorageError;
use crate::storage::errors::StorageError::SerializationError;
use crate::storage::lmdb_storage::SharedTransaction;
use dozer_types::types::{Field, Operation, Record, Schema};
use std::fmt::{Debug, Formatter};

pub trait RecordWriter {
    fn write(&self, op: Operation, tx: &SharedTransaction) -> Result<Operation, ExecutionError>;
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
                AutogenRowsIdLookupRecordWriter::new(db, meta_db, schema),
            )),
            _ => panic!(
                "Unexpected port type in RecordWriterUtils::create_writer(): {}",
                typ
            ),
        }
    }

    fn write_record(
        db: Database,
        rec: &Record,
        schema: &Schema,
        tx: &SharedTransaction,
    ) -> Result<(), ExecutionError> {
        let key = rec.get_key(&schema.primary_index);
        let value = bincode::serialize(&rec).map_err(|e| SerializationError {
            typ: "Record".to_string(),
            reason: Box::new(e),
        })?;
        tx.write().put(db, key.as_slice(), value.as_slice())?;
        Ok(())
    }

    fn retr_record(
        db: Database,
        key: &[u8],
        tx: &SharedTransaction,
    ) -> Result<Record, ExecutionError> {
        let tx = tx.read();
        let curr = tx
            .get(db, key)?
            .ok_or_else(ExecutionError::RecordNotFound)?;

        let r: Record = bincode::deserialize(curr).map_err(|e| SerializationError {
            typ: "Record".to_string(),
            reason: Box::new(e),
        })?;
        Ok(r)
    }
}

#[derive(Debug)]
struct PrimaryKeyLookupRecordWriter {
    db: Database,
    meta_db: Database,
    schema: Schema,
    retr_old_records_for_deletes: bool,
    retr_old_records_for_updates: bool,
}

impl PrimaryKeyLookupRecordWriter {
    fn new(
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
}

impl RecordWriter for PrimaryKeyLookupRecordWriter {
    fn write(
        &self,
        mut op: Operation,
        tx: &SharedTransaction,
    ) -> Result<Operation, ExecutionError> {
        match op {
            Operation::Insert { new } => {
                RecordWriterUtils::write_record(self.db, &new, &self.schema, tx)?;
                Ok(Operation::Insert { new })
            }
            Operation::Delete { mut old } => {
                let key = old.get_key(&self.schema.primary_index);
                if self.retr_old_records_for_deletes {
                    old = RecordWriterUtils::retr_record(self.db, &key, tx)?;
                }
                tx.write().del(self.db, &key, None)?;
                Ok(Operation::Delete { old })
            }
            Operation::Update { mut old, new } => {
                let key = old.get_key(&self.schema.primary_index);
                if self.retr_old_records_for_updates {
                    old = RecordWriterUtils::retr_record(self.db, &key, tx)?;
                }
                RecordWriterUtils::write_record(self.db, &new, &self.schema, tx)?;
                Ok(Operation::Update { old, new })
            }
        }
    }
}

#[derive(Debug)]
struct AutogenRowsIdLookupRecordWriter {
    db: Database,
    meta_db: Database,
    schema: Schema,
}

impl AutogenRowsIdLookupRecordWriter {
    const COUNTER_KEY: u16 = 0_u16;

    pub fn new(db: Database, meta_db: Database, schema: Schema) -> Self {
        Self {
            db,
            meta_db,
            schema,
        }
    }

    fn get_autogen_counter(&self, tx: &SharedTransaction) -> Result<u64, StorageError> {
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
            _ => 0_u64,
        };
        tx.write().put(
            self.meta_db,
            &Self::COUNTER_KEY.to_le_bytes(),
            &(curr_counter + 1).to_le_bytes(),
        )?;
        Ok(curr_counter)
    }
}

impl RecordWriter for AutogenRowsIdLookupRecordWriter {
    fn write(&self, op: Operation, tx: &SharedTransaction) -> Result<Operation, ExecutionError> {
        match op {
            Operation::Insert { mut new } => {
                let ctr = self.get_autogen_counter(tx)?;
                new.values.push(Field::UInt(ctr));
                assert!(
                    self.schema.primary_index.len() == 1
                        && self.schema.primary_index[0] == new.values.len() - 1
                );
                RecordWriterUtils::write_record(self.db, &new, &self.schema, tx)?;
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
