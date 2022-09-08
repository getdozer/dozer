use dozer_shared::ingestion::{ingestion_client::IngestionClient};

pub async fn initialize() -> IngestionClient<tonic::transport::channel::Channel> {
    StorageClient::connect("http://[::1]:8081").await.unwrap()
}
