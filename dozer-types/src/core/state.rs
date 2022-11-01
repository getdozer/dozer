#![allow(clippy::type_complexity)]

use crate::errors::connector::ConnectorError;
use crate::errors::database::DatabaseError;
use crate::types::{Field, OperationEvent, Record, Schema};

pub trait StateStoresManager: Send + Sync {
    fn init_state_store(
        &self,
        id: String,
        options: StateStoreOptions,
    ) -> Result<Box<dyn StateStore>, DatabaseError>;
}

pub struct StateStoreOptions {
    pub allow_duplicate_keys: bool,
}

impl StateStoreOptions {
    pub fn default() -> Self {
        Self {
            allow_duplicate_keys: false,
        }
    }
}

pub trait StateStore {
    fn checkpoint(&mut self) -> Result<(), DatabaseError>;
    fn commit(&mut self) -> Result<(), DatabaseError>;
    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), DatabaseError>;
    fn get(&self, key: &[u8]) -> Result<Option<&[u8]>, DatabaseError>;
    fn del(&mut self, key: &[u8], value: Option<&[u8]>) -> Result<(), DatabaseError>;
    fn cursor(&mut self) -> Result<Box<dyn StateStoreCursor>, DatabaseError>;
}

pub trait StateStoreCursor {
    fn seek(&mut self, key: &[u8]) -> Result<bool, DatabaseError>;
    fn next(&mut self) -> Result<bool, DatabaseError>;
    fn prev(&mut self) -> Result<bool, DatabaseError>;
    fn read(&mut self) -> Result<Option<(&[u8], &[u8])>, DatabaseError>;
}

pub trait Source {
    fn get_id(&self) -> String;
    fn get_datasets() -> Vec<String>;
    fn start() -> Result<(), ConnectorError>;
    fn stop() -> Result<(), ConnectorError>;
    fn get_record_reader(&self, dataset: String, snapshot: bool) -> Box<dyn RecordStoreReader>;
    fn operations_iterator(
        &self,
        dataset: String,
        start_from: u64,
    ) -> Box<dyn Iterator<Item = OperationEvent> + 'static>;
}

pub trait RecordStore: RecordStoreReader {
    fn put_delete_record(
        &self,
        key: Vec<Field>,
        original_seq_no: &[u8],
    ) -> Result<(), DatabaseError>;
    fn put_update_record(
        &self,
        removed: Vec<Field>,
        updated: Record,
        original_seq_no: &[u8],
    ) -> Result<(), DatabaseError>;
    fn put_insert_record(&self, added: Record, original_seq_no: &[u8])
        -> Result<(), DatabaseError>;
    fn put_schema_update(schema: Schema, original_seq_no: &[u8]) -> Result<(), DatabaseError>;
}

pub trait RecordStoreReader {
    fn get_seq_no_range(&self) -> Result<(u64, u64), DatabaseError>;
    fn get_schema(&self) -> Result<Schema, DatabaseError>;
    fn get_record(&self, key: Vec<Field>) -> Result<(u64, Record), DatabaseError>;
    fn records_iterator(&self) -> Box<dyn Iterator<Item = (u64, Record)> + 'static>;
    fn operations_iterator(&self) -> Box<dyn Iterator<Item = OperationEvent> + 'static>;
}
