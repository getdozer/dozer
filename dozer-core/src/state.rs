#![allow(clippy::type_complexity)]
use crate::state::error::StateStoreError;

pub mod lmdb;
mod lmdb_sys;
pub mod memory;
pub mod null;

mod error;
#[cfg(test)]
mod tests;

pub trait StateStoresManager: Send + Sync {
    fn init_state_store(
        &self,
        id: String,
        options: StateStoreOptions,
    ) -> Result<Box<dyn StateStore>, StateStoreError>;
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
    fn checkpoint(&mut self) -> Result<(), StateStoreError>;
    fn commit(&mut self) -> Result<(), StateStoreError>;
    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), StateStoreError>;
    fn get(&self, key: &[u8]) -> Result<Option<&[u8]>, StateStoreError>;
    fn del(&mut self, key: &[u8]) -> Result<(), StateStoreError>;
    fn cursor(&mut self) -> Result<Box<dyn StateStoreCursor>, StateStoreError>;
}

pub trait StateStoreCursor {
    fn seek(&mut self, key: &[u8]) -> Result<bool, StateStoreError>;
    fn next(&mut self) -> Result<bool, StateStoreError>;
    fn prev(&mut self) -> Result<bool, StateStoreError>;
    fn read(&mut self) -> Result<Option<(&[u8], &[u8])>, StateStoreError>;
}
