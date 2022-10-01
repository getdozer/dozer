pub mod lmdb;
pub mod memory;
mod lmdb_sys;

#[derive(Debug)]
pub enum StateStoreErrorType {
    InternalError,
    OpenOrCreateError,
    TransactionError,
    SchemaMismatchError,
    AggregatorError,
    StoreOperationError,
    GetOperationError,
    InvalidPath
}


#[derive(Debug)]
pub struct StateStoreError {
    pub err_code: StateStoreErrorType,
    pub desc: String
}

impl StateStoreError {
    pub fn new(err_code: StateStoreErrorType, desc: String) -> Self {
        Self { err_code, desc }
    }
}

pub trait StateStoresManager : Send + Sync {
    fn init_state_store(&self, id: String) -> Result<Box<dyn StateStore>, StateStoreError>;
}

pub trait StateStore {
    fn checkpoint(&mut self) -> Result<(), StateStoreError>;
    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), StateStoreError>;
    fn get(&mut self, key: &[u8]) -> Result<Option<&[u8]>, StateStoreError>;
    fn del(&mut self, key: &[u8]) -> Result<(), StateStoreError>;
}



