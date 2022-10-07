use super::storage::RocksStorage;
use dozer_types::types::{OperationEvent, Schema};
use std::sync::Arc;
pub trait Connector: Send + Sync {
    fn get_schema(&self, name: String) -> anyhow::Result<Schema>;
    fn get_all_schema(&self) -> anyhow::Result<Vec<(String, Schema)>>;
    fn get_tables(&self) -> anyhow::Result<Vec<TableInfo>>;
    fn initialize(
        &mut self,
        storage_client: Arc<RocksStorage>,
        tables: Option<Vec<TableInfo>>,
    ) -> anyhow::Result<()>;
    fn iterator(&mut self) -> Box<dyn Iterator<Item = OperationEvent> + 'static>;
    fn stop(&self);
    fn test_connection(&self) -> anyhow::Result<()>;
}

#[derive(Clone, Debug)]
pub struct TableInfo {
    pub name: String,
    pub id: u32,
    pub columns: Option<Vec<String>>,
}
