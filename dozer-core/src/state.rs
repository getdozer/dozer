mod lmdb_state;
mod accumulators;

use std::sync::Arc;
use dozer_shared::types::{Field, Record};

#[derive(Debug)]
pub enum StateStoreErrorType {
    InternalError,
    OpenOrCreateError,
    TransactionError,
    SchemaMismatchError,
    AggregatorError,
    StoreOperationError,
    GetOperationError
}


#[derive(Debug)]
pub struct StateStoreError {
    err_code: StateStoreErrorType,
    desc: String
}

impl StateStoreError {
    pub fn new(err_code: StateStoreErrorType, desc: String) -> Self {
        Self { err_code, desc }
    }
}

trait StateStoresManager {
    fn init_state_store<'a> (&'a self, id: String) -> Result<Box<dyn StateStore + 'a>, StateStoreError>;
}

trait StateStore {
    fn checkpoint(&mut self) -> Result<(), StateStoreError>;
    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), StateStoreError>;
    fn get(&mut self, key: &[u8]) -> Result<Option<&[u8]>, StateStoreError>;
    fn del(&mut self, key: &[u8]) -> Result<(), StateStoreError>;
}

trait Aggregator {
    fn get_type(&self) -> u8;
    fn insert(&self, curr_state: Option<&[u8]>, new: &Record) -> Result<Vec<u8>, StateStoreError>;
    fn update(&self, curr_state: Option<&[u8]>, old: &Record, new: &Record) -> Result<Vec<u8>, StateStoreError>;
    fn delete(&self, curr_state: Option<&[u8]>, old: &Record) -> Result<Option<Vec<u8>>, StateStoreError>;
    fn get_value(&self, f: &[u8]) -> Field;
}


