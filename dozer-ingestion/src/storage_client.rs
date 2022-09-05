use dozer_shared::storage as storage_proto;
use storage_proto::storage_client::StorageClient;

pub async fn initialize() -> StorageClient<tonic::transport::channel::Channel> {
    StorageClient::connect("http://[::1]:50051").await.unwrap()
}
