use dozer_types::core::state::{StateStore, StateStoreCursor};
use dozer_types::errors::state::StateStoreError;
use dozer_types::errors::state::StateStoreError::InvalidOperation;

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
