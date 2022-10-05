use super::storage::RocksStorage;
use dozer_schema::registry::SchemaRegistryClient;
use dozer_types::types::{OperationEvent, Schema};
use std::{error::Error, sync::Arc};
pub trait Connector {
    fn get_schema(&self, name: String) -> anyhow::Result<Schema>;
    fn get_all_schema(&self) -> anyhow::Result<Vec<(String, Schema)>>;
    fn initialize(
        &mut self,
        storage_client: Arc<RocksStorage>,
        schema_client: Arc<SchemaRegistryClient>,
    ) -> Result<(), Box<dyn Error>>;
    fn iterator(&mut self) -> Box<dyn Iterator<Item = OperationEvent> + 'static>;
    fn stop(&self);
    fn test_connection(&self) -> anyhow::Result<()>;
}
