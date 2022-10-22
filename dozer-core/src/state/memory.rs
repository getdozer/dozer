use dozer_types::core::state::{StateStore, StateStoreCursor};
use dozer_types::errors::database::DatabaseError;
use std::collections::HashMap;

pub struct MemoryStateStore {
    data: HashMap<Vec<u8>, Vec<u8>>,
}

impl MemoryStateStore {
    pub fn new() -> Self {
        Self {
            data: HashMap::new(),
        }
    }
}

impl Default for MemoryStateStore {
    fn default() -> Self {
        Self::new()
    }
}

impl StateStore for MemoryStateStore {
    fn checkpoint(&mut self) -> Result<(), DatabaseError> {
        Ok(())
    }

    fn commit(&mut self) -> Result<(), DatabaseError> {
        Ok(())
    }

    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), DatabaseError> {
        self.data.insert(Vec::from(key), Vec::from(value));
        Ok(())
    }

    fn get(&self, key: &[u8]) -> Result<Option<&[u8]>, DatabaseError> {
        Ok(self.data.get(key).map(|e| e.as_slice()))
    }

    fn del(&mut self, key: &[u8], _value: Option<&[u8]>) -> Result<(), DatabaseError> {
        self.data.remove(key);
        Ok(())
    }

    fn cursor(&mut self) -> Result<Box<dyn StateStoreCursor>, DatabaseError> {
        todo!()
    }
}
