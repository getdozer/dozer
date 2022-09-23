mod lmdb_state;
mod accumulators;

use std::sync::Arc;
use dozer_shared::types::{Field};

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
    fn get(&self, key: &[u8]) -> Result<Option<&[u8]>, StateStoreError>;
}

trait Aggregator {
    fn get_type(&self) -> u8;
    fn get_state_size(&self) -> usize;
    fn insert(&self, bool: initial, prev: &mut [u8], curr: Field) -> Result<(), StateStoreError>;
    fn delete(&self, bool: initial, prev: &mut [u8], curr: Field) -> Result<(), StateStoreError>;
    fn get_value(&self, f: &[u8]) -> Field;
}


