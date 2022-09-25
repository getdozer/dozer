use dozer_types::types::{OperationEvent, TableInfo};
use std::{error::Error, sync::Arc};

use super::storage::RocksStorage;

pub trait Connector {
    fn get_schema(&self) -> Result<Vec<TableInfo>, Box<dyn Error>>;
    fn initialize(&mut self, storage_client: Arc<RocksStorage>) -> Result<(), Box<dyn Error>>;
    fn iterator(&mut self) -> Box<dyn Iterator<Item = OperationEvent> + 'static>;
    fn stop(&self);
    fn test_connection(&self) -> Result<(), Box<dyn Error>>;
}
