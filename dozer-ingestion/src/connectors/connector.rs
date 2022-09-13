use crate::connectors::storage::RocksStorage;
use async_trait::async_trait;
use dozer_shared::types::TableInfo;
use std::{io::Result, sync::Arc};
#[async_trait]
pub trait Connector<T, A> {
    fn new(connector_config: T) -> Self;
    async fn initialize(&mut self, storage_client: Arc<RocksStorage>);
    async fn connect(&mut self) -> A;
    async fn get_schema(&self) -> Vec<TableInfo>;
    async fn start(&mut self);
    async fn stop(&self);
    async fn test_connection(&self) -> Result<()>;
}
