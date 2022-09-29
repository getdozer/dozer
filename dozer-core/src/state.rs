pub mod lmdb;
pub mod memory;

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

pub trait StateStoresManager : Send + Sync {
    fn init_state_store<'a> (&'a self, id: String) -> Result<Box<dyn StateStore + 'a>, StateStoreError>;
}

pub trait StateStore {
    fn checkpoint(&mut self) -> Result<(), StateStoreError>;
    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), StateStoreError>;
    fn get(&mut self, key: &[u8]) -> Result<Option<&[u8]>, StateStoreError>;
    fn del(&mut self, key: &[u8]) -> Result<(), StateStoreError>;
}



