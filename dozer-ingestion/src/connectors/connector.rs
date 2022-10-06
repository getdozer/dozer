use super::storage::RocksStorage;
use dozer_types::types::{OperationEvent, Schema};
use std::sync::Arc;
pub trait Connector: Send + Sync {
    fn get_schema(&self, name: String) -> anyhow::Result<Schema>;
    fn get_all_schema(&self) -> anyhow::Result<Vec<(String, Schema)>>;
    fn initialize(&mut self, storage_client: Arc<RocksStorage>) -> anyhow::Result<()>;
    fn iterator(&mut self) -> Box<dyn Iterator<Item = OperationEvent> + 'static>;
    fn stop(&self);
    fn test_connection(&self) -> anyhow::Result<()>;
}
