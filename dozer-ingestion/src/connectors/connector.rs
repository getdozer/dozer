use dozer_shared::types::{OperationEvent, TableInfo};
use std::sync::Arc;

use super::storage::RocksStorage;

pub trait Connector<C, E> {
    fn new(connector_config: C) -> Self;
    fn get_schema(&self) -> Vec<TableInfo>;
    fn initialize(&mut self, storage_client: Arc<RocksStorage>) -> Result<(), E>;
    fn iterator(&mut self) -> Box<dyn Iterator<Item = OperationEvent> + 'static>;
    fn stop(&self);
    fn test_connection(&self) -> Result<(), E>;
}
