use crate::state::StateStore;
use anyhow::anyhow;

pub struct NullStateStore {}

impl StateStore for NullStateStore {
    fn checkpoint(&mut self) -> anyhow::Result<()> {
        Err(anyhow!("Invalid operation"))
    }

    fn put(&mut self, _key: &[u8], _value: &[u8]) -> anyhow::Result<()> {
        Err(anyhow!("Invalid operation"))
    }

    fn get(&mut self, _key: &[u8]) -> anyhow::Result<Option<&[u8]>> {
        Err(anyhow!("Invalid operation"))
    }

    fn del(&mut self, _key: &[u8]) -> anyhow::Result<()> {
        Err(anyhow!("Invalid operation"))
    }
}
