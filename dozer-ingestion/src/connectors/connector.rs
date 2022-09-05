use async_trait::async_trait;
use dozer_shared::storage::storage_client::StorageClient;

#[async_trait]
pub trait Connector {
    fn new(
        conn_str: String,
        tables: Option<Vec<String>>,
        client: StorageClient<tonic::transport::channel::Channel>,
    ) -> Self;
    async fn connect(&mut self);
    async fn get_schema(&self);
    async fn start(&mut self);
    async fn stop(&self);
}
