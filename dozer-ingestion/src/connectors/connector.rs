use crate::connectors::postgres::iterator::PostgresIterator;
use crate::connectors::storage::RocksStorage;
use async_trait::async_trait;
use dozer_shared::types::{OperationEvent, TableInfo};
use std::sync::Arc;

#[async_trait]
pub trait Connector<T, A, E> {
    fn new(connector_config: T) -> Self;
    fn initialize(&mut self, storage_client: Arc<RocksStorage>) -> Result<(), E>;
    fn get_schema(&self) -> Vec<TableInfo>;
    fn start(&mut self) -> PostgresIterator;
    fn stop(&self);
    fn test_connection(&self) -> Result<(), E>;
}
