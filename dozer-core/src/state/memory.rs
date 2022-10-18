use crate::state::{StateStore, StateStoreCursor};
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
    fn checkpoint(&mut self) -> anyhow::Result<()> {
        todo!()
    }

    fn put(&mut self, key: &[u8], value: &[u8]) -> anyhow::Result<()> {
        self.data.insert(Vec::from(key), Vec::from(value));
        Ok(())
    }

    fn get(&self, key: &[u8]) -> anyhow::Result<Option<&[u8]>> {
        Ok(self.data.get(key).map(|e| e.as_slice()))
    }

    fn del(&mut self, key: &[u8]) -> anyhow::Result<()> {
        self.data.remove(key);
        Ok(())
    }

    fn cursor(&mut self) -> anyhow::Result<Box<dyn StateStoreCursor>> {
        todo!()
    }
}
