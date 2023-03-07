use crate::errors::ExecutionError;
use crate::errors::ExecutionError::{
    RecordNotFound, UnsupportedDeleteOperation, UnsupportedUpdateOperation,
};
use crate::node::OutputPortType;
use std::collections::VecDeque;

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
    fn write(&mut self, op: Operation, tx: &SharedTransaction)
        -> Result<Operation, ExecutionError>;
}

impl Debug for dyn RecordWriter {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("RecordWriter")
    }
}

pub trait RecordReader: Send + Sync + DynClone {
    fn get(&self, key: &[u8], version: u32) -> Result<Option<Record>, ExecutionError>;
}

dyn_clone::clone_trait_object!(RecordReader);

impl Debug for dyn RecordReader {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("RecordReader")
    }
}

#[allow(clippy::type_complexity)]
pub fn create_record_store(
    tx: &SharedTransaction,
    output_port: PortHandle,
    output_port_type: OutputPortType,
    schema: Schema,
    retention_queue_size: usize,
) -> Result<Option<(Box<dyn RecordWriter>, Box<dyn RecordReader>)>, StorageError> {
    let create_databases = || {
        let mut tx = tx.write();
        let db = tx.create_database(
            Some(&format!("{PORT_STATE_KEY}_{output_port}")),
            Some(DatabaseFlags::empty()),
        )?;
        let meta_db = tx.create_database(
            Some(&format!("{PORT_STATE_KEY}_{output_port}_META")),
            Some(DatabaseFlags::empty()),
        )?;
        Ok::<_, StorageError>((db, meta_db))
    };

    match output_port_type {
        OutputPortType::Stateless => Ok(None),

        OutputPortType::StatefulWithPrimaryKeyLookup {
            retr_old_records_for_updates,
            retr_old_records_for_deletes,
        } => {
            let (db, meta_db) = create_databases()?;
            let writer = Box::new(PrimaryKeyLookupRecordWriter::new(
                db,
                meta_db,
                schema,
                retr_old_records_for_deletes,
                retr_old_records_for_updates,
                retention_queue_size,
            ));
            let reader = Box::new(PrimaryKeyLookupRecordReader::new(tx.clone(), db));
            Ok(Some((writer, reader)))
        }

        OutputPortType::AutogenRowKeyLookup => {
            let (db, meta_db) = create_databases()?;
            let writer = Box::new(AutogenRowKeyLookupRecordWriter::new(db, meta_db, schema));
            let reader = Box::new(AutogenRowKeyLookupRecordReader::new(tx.clone(), db));
            Ok(Some((writer, reader)))
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
    db: Database,
    _meta_db: Database,
    schema: Schema,
    retr_old_records_for_deletes: bool,
    retr_old_records_for_updates: bool,
    retention_queue_size: usize,
    retention_queue: VecDeque<(Vec<u8>, u32)>,
}

impl PrimaryKeyLookupRecordWriter {
    pub(crate) fn new(
        db: Database,
        meta_db: Database,
        schema: Schema,
        retr_old_records_for_deletes: bool,
        retr_old_records_for_updates: bool,
        retention_queue_size: usize,
    ) -> Self {
        Self {
            db,
            _meta_db: meta_db,
            schema,
            retr_old_records_for_deletes,
            retr_old_records_for_updates,
            retention_queue_size,
            retention_queue: VecDeque::with_capacity(retention_queue_size),
        }
    }

    #[inline]
    pub(crate) fn get_last_record_version(
        &self,
        rec_key: &[u8],
        tx: &SharedTransaction,
    ) -> Result<u32, ExecutionError> {
        let mut exclusive_tx = Box::new(tx.write());
        let versions_tx = PrefixTransaction::new(exclusive_tx.as_mut(), RECORD_VERSIONS_INDEX_ID);
        match versions_tx.get(self.db, rec_key)? {
            Some(payload) => Ok(u32::from_le_bytes(payload.try_into().unwrap())),
            None => Err(ExecutionError::RecordNotFound()),
        }
    }

    #[inline]
    fn put_last_record_version(
        &self,
        rec_key: &[u8],
        version: u32,
        tx: &SharedTransaction,
    ) -> Result<(), ExecutionError> {
        let mut exclusive_tx = Box::new(tx.write());
        let mut versions_tx =
            PrefixTransaction::new(exclusive_tx.as_mut(), RECORD_VERSIONS_INDEX_ID);
        versions_tx
            .put(self.db, rec_key, &version.to_le_bytes())
            .map_err(ExecutionError::InternalDatabaseError)
    }

    pub(crate) fn write_versioned_record(
        &self,
        record: Option<&Record>,
        mut key: Vec<u8>,
        version: u32,
        _schema: &Schema,
        tx: &SharedTransaction,
    ) -> Result<(), ExecutionError> {
        self.put_last_record_version(&key, version, tx)?;
        key.extend(version.to_le_bytes());
        let value = match record {
            Some(r) => {
                let mut v = Vec::with_capacity(32);
                v.extend(RECORD_PRESENT_FLAG.to_le_bytes());
                v.extend(bincode::serialize(r).map_err(|e| SerializationError {
                    typ: "Record",
                    reason: Box::new(e),
                })?);
                v
            }
            None => Vec::from(RECORD_DELETED_FLAG.to_le_bytes()),
        };
        let mut exclusive_tx = Box::new(tx.write());
        let mut store = PrefixTransaction::new(exclusive_tx.as_mut(), VERSIONED_RECORDS_INDEX_ID);
        store.put(self.db, key.as_slice(), value.as_slice())?;
        Ok(())
    }

    pub(crate) fn retr_versioned_record(
        &self,
        mut key: Vec<u8>,
        version: u32,
        tx: &SharedTransaction,
    ) -> Result<Option<Record>, ExecutionError> {
        key.extend(version.to_le_bytes());
        let mut exclusive_tx = Box::new(tx.write());
        let store = PrefixTransaction::new(exclusive_tx.as_mut(), VERSIONED_RECORDS_INDEX_ID);
        let curr = store
            .get(self.db, &key)?
            .ok_or_else(ExecutionError::RecordNotFound)?;
        let rec: Option<Record> = match curr[0] {
            RECORD_PRESENT_FLAG => {
                Some(
                    bincode::deserialize(&curr[1..]).map_err(|e| DeserializationError {
                        typ: "Record",
                        reason: Box::new(e),
                    })?,
                )
            }
            _ => None,
        };
        Ok(rec)
    }

    fn del_versioned_record(
        &self,
        mut key: Vec<u8>,
        version: u32,
        tx: &SharedTransaction,
    ) -> Result<bool, ExecutionError> {
        key.extend(version.to_le_bytes());
        let mut exclusive_tx = Box::new(tx.write());
        let mut store = PrefixTransaction::new(exclusive_tx.as_mut(), VERSIONED_RECORDS_INDEX_ID);
        Ok(store.del(self.db, &key, None)?)
    }

    fn push_pop_retention_queue(
        &mut self,
        key: Vec<u8>,
        version: u32,
        tx: &SharedTransaction,
    ) -> Result<(), ExecutionError> {
        self.retention_queue.push_back((key, version));
        if self.retention_queue.len() > self.retention_queue_size {
            if let Some((key, ver)) = self.retention_queue.pop_front() {
                self.del_versioned_record(key, ver, tx)?;
            }
        }
        Ok(())
    }
}

impl RecordWriter for PrimaryKeyLookupRecordWriter {
    fn write(
        &mut self,
        op: Operation,
        tx: &SharedTransaction,
    ) -> Result<Operation, ExecutionError> {
        match op {
            Operation::Insert { mut new } => {
                let key = new.get_key(&self.schema.primary_index);
                self.write_versioned_record(
                    Some(&new),
                    key,
                    INITIAL_RECORD_VERSION,
                    &self.schema,
                    tx,
                )?;
                new.version = Some(INITIAL_RECORD_VERSION);
                Ok(Operation::Insert { new })
            }
            Operation::Delete { mut old } => {
                let key = old.get_key(&self.schema.primary_index);
                let curr_version = self.get_last_record_version(&key, tx)?;
                if self.retr_old_records_for_deletes {
                    old = self
                        .retr_versioned_record(key.to_owned(), curr_version, tx)?
                        .ok_or_else(RecordNotFound)?;
                }
                self.push_pop_retention_queue(key.clone(), curr_version, tx)?;
                self.write_versioned_record(None, key, curr_version + 1, &self.schema, tx)?;
                old.version = Some(curr_version);
                Ok(Operation::Delete { old })
            }
            Operation::Update { mut old, mut new } => {
                let key = old.get_key(&self.schema.primary_index);
                let curr_version = self.get_last_record_version(&key, tx)?;
                if self.retr_old_records_for_updates {
                    old = self
                        .retr_versioned_record(key.to_owned(), curr_version, tx)?
                        .ok_or_else(RecordNotFound)?;
                }
                self.push_pop_retention_queue(key.clone(), curr_version, tx)?;
                self.write_versioned_record(Some(&new), key, curr_version + 1, &self.schema, tx)?;
                old.version = Some(curr_version);
                new.version = Some(curr_version + 1);
                Ok(Operation::Update { old, new })
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct PrimaryKeyLookupRecordReader {
    tx: SharedTransaction,
    db: Database,
}

impl PrimaryKeyLookupRecordReader {
    pub fn new(tx: SharedTransaction, db: Database) -> Self {
        Self { tx, db }
    }
}

impl RecordReader for PrimaryKeyLookupRecordReader {
    fn get(&self, key: &[u8], version: u32) -> Result<Option<Record>, ExecutionError> {
        let mut versioned_key: Vec<u8> = Vec::with_capacity(key.len() + 4);
        versioned_key.extend(VERSIONED_RECORDS_INDEX_ID.to_be_bytes());
        versioned_key.extend(key);
        versioned_key.extend(version.to_le_bytes());

        let guard = self.tx.read();

        let buf = guard
            .get(self.db, &versioned_key)?
            .ok_or_else(RecordNotFound)?;
        let rec: Option<Record> = match buf[0] {
            RECORD_PRESENT_FLAG => {
                let mut r: Record =
                    bincode::deserialize(&buf[1..]).map_err(|e| DeserializationError {
                        typ: "Record",
                        reason: Box::new(e),
                    })?;
                r.version = Some(INITIAL_RECORD_VERSION);
                Some(r)
            }
            _ => None,
        };
        Ok(rec)
    }
}

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
            SourceDefinition::Dynamic,
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
            typ: "Record",
            reason: Box::new(e),
        })?;
        tx.write().put(self.db, key.as_slice(), value.as_slice())?;
        Ok(())
    }

    fn get_autogen_counter(&mut self, tx: &SharedTransaction) -> Result<u64, StorageError> {
        let curr_counter = match tx
            .read()
            .get(self.meta_db, &Self::COUNTER_KEY.to_le_bytes())?
        {
            Some(c) => u64::from_le_bytes(c.try_into().map_err(|e| {
                StorageError::DeserializationError {
                    typ: "u64",
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
                new.version = Some(INITIAL_RECORD_VERSION);
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

#[derive(Debug, Clone)]
pub struct AutogenRowKeyLookupRecordReader {
    tx: SharedTransaction,
    db: Database,
}

impl AutogenRowKeyLookupRecordReader {
    pub fn new(tx: SharedTransaction, db: Database) -> Self {
        Self { tx, db }
    }
}

impl RecordReader for AutogenRowKeyLookupRecordReader {
    fn get(&self, key: &[u8], _version: u32) -> Result<Option<Record>, ExecutionError> {
        Ok(match self.tx.read().get(self.db, key)? {
            Some(buf) => {
                let mut r: Record =
                    bincode::deserialize(buf).map_err(|e| DeserializationError {
                        typ: "Record",
                        reason: Box::new(e),
                    })?;
                r.version = Some(INITIAL_RECORD_VERSION);
                Some(r)
            }
            None => None,
        })
    }
}
