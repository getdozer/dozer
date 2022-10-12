pub mod lmdb;
mod lmdb_sys;
pub mod memory;

pub trait StateStoresManager: Send + Sync {
    fn init_state_store(&self, id: String) -> anyhow::Result<Box<dyn StateStore>>;
}

pub trait StateStore {
    fn checkpoint(&mut self) -> anyhow::Result<()>;
    fn put(&mut self, key: &[u8], value: &[u8]) -> anyhow::Result<()>;
    fn get(&mut self, key: &[u8]) -> anyhow::Result<Option<&[u8]>>;
    fn del(&mut self, key: &[u8]) -> anyhow::Result<()>;
}
