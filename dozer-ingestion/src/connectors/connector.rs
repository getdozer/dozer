use async_trait::async_trait;
use dozer_shared::ingestion::TableInfo;
use dozer_storage::storage::RocksStorage;
use std::sync::Arc;
#[async_trait]
pub trait Connector<T, A> {
    fn new(connector_config: T, storage_client: Arc<RocksStorage>) -> Self;
    async fn initialize(&mut self);
    async fn connect(&mut self) -> A;
    async fn get_schema(&self) -> Vec<TableInfo>;
    async fn start(&mut self);
    async fn stop(&self);
}
