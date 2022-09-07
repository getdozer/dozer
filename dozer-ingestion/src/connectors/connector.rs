use async_trait::async_trait;
use dozer_shared::storage::storage_client::StorageClient;
use dozer_shared::ingestion::TableInfo;
#[async_trait]
pub trait Connector<T, A> {
    fn new(connector_config: T, client: StorageClient<tonic::transport::channel::Channel>) -> Self;
    async fn initialize(&mut self);
    async fn connect(&mut self) -> A;
    async fn get_schema(&self)-> Vec<TableInfo>;
    async fn start(&mut self);
    async fn stop(&self);
}
