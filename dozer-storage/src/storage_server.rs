use dozer_shared::storage as storage_proto;
use storage_proto::storage_server::{Storage, StorageServer};
use storage_proto::{Record, Schema, ServerResponse};
use tonic::{transport::Server, Request, Response, Status};

#[derive(Debug, Default)]
pub struct MyStorage {}

#[tonic::async_trait]
impl Storage for MyStorage {
    async fn insert_record(
        &self,
        request: Request<Record>,
    ) -> Result<Response<ServerResponse>, Status> {
        println!("Got a request: {:?}", request);

        let reply = storage_proto::ServerResponse {
            status: 1,
            message: "Inserted Record".to_string(),
        };

        Ok(Response::new(reply))
    }

    async fn insert_schema(
        &self,
        request: Request<Schema>,
    ) -> Result<Response<ServerResponse>, Status> {
        println!("Got a request: {:?}", request);

        let reply = storage_proto::ServerResponse {
            status: 1,
            message: "Inserted Schema".to_string(),
        };

        Ok(Response::new(reply))
    }
}

pub async fn get_server() -> Result<(), tonic::transport::Error> {
    let addr = "[::1]:50051".parse().unwrap();
    let my_storage = MyStorage::default();

    Server::builder()
        .add_service(StorageServer::new(my_storage))
        .serve(addr)
        .await
}
