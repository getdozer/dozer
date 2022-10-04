use dozer_schema::registry::SchemaRegistryClient;
use dozer_types::types::{OperationEvent, TableInfo};
use std::{error::Error, sync::Arc};

use super::storage::RocksStorage;

pub trait Connector {
    fn get_schema(&self) -> anyhow::Result<Vec<TableInfo>>;
    fn initialize(
        &mut self,
        storage_client: Arc<RocksStorage>,
        schema_client: Arc<SchemaRegistryClient>,
    ) -> Result<(), Box<dyn Error>>;
    fn iterator(&mut self) -> Box<dyn Iterator<Item = OperationEvent> + 'static>;
    fn stop(&self);
    fn test_connection(&self) -> anyhow::Result<()>;
}
