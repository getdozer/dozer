pub mod lmdb;
mod lmdb_sys;
pub mod memory;
pub mod null;
mod tests;

pub trait StateStoresManager: Send + Sync {
    fn init_state_store(
        &self,
        id: String,
        options: StateStoreOptions,
    ) -> anyhow::Result<Box<dyn StateStore>>;
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
    fn checkpoint(&mut self) -> anyhow::Result<()>;
    fn put(&mut self, key: &[u8], value: &[u8]) -> anyhow::Result<()>;
    fn get(&mut self, key: &[u8]) -> anyhow::Result<Option<&[u8]>>;
    fn del(&mut self, key: &[u8]) -> anyhow::Result<()>;
    fn cursor(&mut self) -> anyhow::Result<Box<dyn StateStoreCursor>>;
}

pub trait StateStoreCursor {
    fn seek(&mut self, key: &[u8]) -> anyhow::Result<bool>;
    fn next(&mut self) -> anyhow::Result<bool>;
    fn prev(&mut self) -> anyhow::Result<bool>;
    fn read(&mut self) -> anyhow::Result<Option<(&[u8], &[u8])>>;
}
