use std::collections::HashMap;
use crate::state::{StateStore, StateStoreError};

pub struct MemoryStateStore {
    data: HashMap<Vec<u8>,Vec<u8>>
}

impl MemoryStateStore {
    pub fn new() -> Self {
        Self { data: HashMap::new() }
    }
}

impl StateStore for MemoryStateStore {

    fn checkpoint(&mut self) -> Result<(), StateStoreError> {
        todo!()
    }

    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), StateStoreError> {
        self.data.insert(Vec::from(key), Vec::from(value));
        Ok(())
    }

    fn get(&mut self, key: &[u8]) -> Result<Option<&[u8]>, StateStoreError> {
        let r = self.data.get(key);
        Ok(if r.is_none() { None } else { Some(r.unwrap().as_slice()) })
    }

    fn del(&mut self, key: &[u8]) -> Result<(), StateStoreError> {
        self.data.remove(key);
        Ok(())
    }
}
