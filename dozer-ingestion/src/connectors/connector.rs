use dozer_shared::types::{OperationEvent, TableInfo};
use std::{sync::Arc, error::Error};

use super::storage::RocksStorage;

pub trait Connector<C> {
    fn get_schema(&self) -> Vec<TableInfo>;
    fn initialize(&mut self, storage_client: Arc<RocksStorage>) -> Result<(), Box<dyn Error>>;
    fn iterator(&mut self) -> Box<dyn Iterator<Item = OperationEvent> + 'static>;
    fn stop(&self);
    fn test_connection(&self) -> Result<(), Box<dyn Error>>;
}
