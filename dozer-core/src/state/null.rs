use dozer_types::core::state::{StateStore, StateStoreCursor};
use dozer_types::errors::database::DatabaseError;
use dozer_types::errors::database::DatabaseError::InvalidOperation;

pub struct NullStateStore {}

impl StateStore for NullStateStore {
    fn checkpoint(&mut self) -> Result<(), DatabaseError> {
        Err(InvalidOperation("CHECKPOINT".to_string()))
    }

    fn commit(&mut self) -> Result<(), DatabaseError> {
        Err(InvalidOperation("COMMIT".to_string()))
    }

    fn put(&mut self, _key: &[u8], _value: &[u8]) -> Result<(), DatabaseError> {
        Err(InvalidOperation("PUT".to_string()))
    }

    fn get(&self, _key: &[u8]) -> Result<Option<&[u8]>, DatabaseError> {
        Err(InvalidOperation("GET".to_string()))
    }

    fn del(&mut self, _key: &[u8]) -> Result<(), DatabaseError> {
        Err(InvalidOperation("DEL".to_string()))
    }

    fn cursor(&mut self) -> Result<Box<dyn StateStoreCursor>, DatabaseError> {
        Err(InvalidOperation("CURSOR".to_string()))
    }
}
