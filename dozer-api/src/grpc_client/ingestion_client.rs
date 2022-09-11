use dozer_shared::ingestion::{ingestion_client::IngestionClient, Connection, ConnectionAuth};

use crate::models::{ConnectionRequest};

pub async fn initialize() -> IngestionClient<tonic::transport::channel::Channel> {
    IngestionClient::connect("http://[::1]:8081").await.unwrap()
}

pub async fn test_connection(input: ConnectionRequest) -> Result<tonic::Response<dozer_shared::ingestion::ConnectionResponse>, tonic::Status> {
    let mut client = initialize().await;
    let request = tonic::Request::new(Connection {
        connection_type: 0,
        detail: Some(ConnectionAuth {
            database: input.authentication.database,
            user: input.authentication.user,
            host: input.authentication.host,
            port: input.authentication.port.unwrap(),
            name: input.authentication.name,
            password: input.authentication.password,
        }),
      });
      let response = client.test_connection(request).await;
      response
}

