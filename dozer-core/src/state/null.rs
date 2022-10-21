use crate::state::error::StateStoreError;
use crate::state::error::StateStoreError::InvalidOperation;
use crate::state::{StateStore, StateStoreCursor};

pub struct NullStateStore {}

impl StateStore for NullStateStore {
    fn checkpoint(&mut self) -> Result<(), StateStoreError> {
        Err(InvalidOperation)
    }

    fn commit(&mut self) -> Result<(), StateStoreError> {
        Err(InvalidOperation)
    }

    fn put(&mut self, _key: &[u8], _value: &[u8]) -> Result<(), StateStoreError> {
        Err(InvalidOperation)
    }

    fn get(&self, _key: &[u8]) -> Result<Option<&[u8]>, StateStoreError> {
        Err(InvalidOperation)
    }

    fn del(&mut self, _key: &[u8]) -> Result<(), StateStoreError> {
        Err(InvalidOperation)
    }

    fn cursor(&mut self) -> Result<Box<dyn StateStoreCursor>, StateStoreError> {
        Err(InvalidOperation)
    }
}
