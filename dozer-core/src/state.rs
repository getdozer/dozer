mod lmdb_state;
mod accumulators;

use std::sync::Arc;
use dozer_shared::types::{Field};

pub enum StateStoreErrorType {
    InternalError,
    OpenOrCreateError,
    TransactionError,
    SchemaMismatchError,
    AccumulationError,
    StoreOperationError,
    GetOperationError
}

pub struct StateStoreError {
    err_code: StateStoreErrorType,
    desc: String
}

impl StateStoreError {
    pub fn new(err_code: StateStoreErrorType, desc: String) -> Self {
        Self { err_code, desc }
    }
}

trait StateStoresManager<'a> {
    fn init_state_store(&'a self, id: String) -> Result<Box<dyn StateStore<'a>>, StateStoreError>;
}

trait StateStore<'a> {
    fn checkpoint(&mut self) -> Result<(), StateStoreError>;
    fn init_accumulation_dataset(&'a mut self, dataset: u8, acc: Box<dyn Accumulator>) -> Result<Box<dyn AccumulationDataset<'a>>, StateStoreError>;
}

trait AccumulationDataset<'a> {
    fn accumulate(&mut self, key: &[u8], values: Field, retrieve: bool) -> Result<Option<Field>, StateStoreError>;
    fn get_accumulated(&self, key: &[u8]) -> Result<Option<Field>, StateStoreError>;
}

trait Accumulator {
    fn get_type(&self) -> u8;
    fn accumulate(&self, prev: Option<&[u8]>, curr: Field) -> Result<Vec<u8>, StateStoreError>;
    fn get_value(&self, f: &[u8]) -> Field;
}


