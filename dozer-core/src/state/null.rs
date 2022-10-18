use crate::state::{StateStore, StateStoreCursor};
use anyhow::anyhow;

pub struct NullStateStore {}

impl StateStore for NullStateStore {
    fn checkpoint(&mut self) -> anyhow::Result<()> {
        Err(anyhow!("Invalid operation"))
    }

    fn put(&mut self, _key: &[u8], _value: &[u8]) -> anyhow::Result<()> {
        Err(anyhow!("Invalid operation"))
    }

    fn get(&self, _key: &[u8]) -> anyhow::Result<Option<&[u8]>> {
        Err(anyhow!("Invalid operation"))
    }

    fn del(&mut self, _key: &[u8]) -> anyhow::Result<()> {
        Err(anyhow!("Invalid operation"))
    }

    fn cursor(&mut self) -> anyhow::Result<Box<dyn StateStoreCursor>> {
        todo!()
    }
}
