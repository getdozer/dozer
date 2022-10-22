#![allow(clippy::type_complexity)]
use crate::errors::database::DatabaseError;

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
    fn del(&mut self, key: &[u8]) -> Result<(), DatabaseError>;
    fn cursor(&mut self) -> Result<Box<dyn StateStoreCursor>, DatabaseError>;
}

pub trait StateStoreCursor {
    fn seek(&mut self, key: &[u8]) -> Result<bool, DatabaseError>;
    fn next(&mut self) -> Result<bool, DatabaseError>;
    fn prev(&mut self) -> Result<bool, DatabaseError>;
    fn read(&mut self) -> Result<Option<(&[u8], &[u8])>, DatabaseError>;
}
