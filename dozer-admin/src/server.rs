use crate::services::{connection_service::ConnectionService, source_service::SourceService};
use dotenvy::dotenv;
use std::env;
use tonic::{transport::Server, Request, Response, Status};
pub mod dozer_api_grpc {
    tonic::include_proto!("dozer_api_grpc");
}
use dozer_api_grpc::{
    dozer_api_server::{DozerApi, DozerApiServer},
    CreateConnectionRequest, CreateConnectionResponse, CreateSourceRequest, CreateSourceResponse,
    GetAllConnectionRequest, GetAllConnectionResponse, GetConnectionDetailsRequest,
    GetConnectionDetailsResponse, GetSchemaRequest, GetSchemaResponse, GetSourceRequest,
    GetSourceResponse, TestConnectionRequest, TestConnectionResponse,
};

pub struct GrpcService {
    connection_service: ConnectionService,
    source_service: SourceService,
}

#[tonic::async_trait]
impl DozerApi for GrpcService {
    async fn test_connection(
        &self,
        request: Request<TestConnectionRequest>,
    ) -> Result<Response<TestConnectionResponse>, Status> {
        let result = self
            .connection_service
            .test_connection(request.into_inner())
            .await;
        match result {
            Ok(response) => Ok(Response::new(response)),
            Err(e) => Err(Status::new(tonic::Code::Internal, e.message)),
        }
    }

    async fn create_connection(
        &self,
        request: Request<CreateConnectionRequest>,
    ) -> Result<Response<CreateConnectionResponse>, Status> {
        let result = self
            .connection_service
            .create_connection(request.into_inner());
        match result {
            Ok(response) => Ok(Response::new(response)),
            Err(e) => Err(Status::new(tonic::Code::Internal, e.message)),
        }
    }

    async fn get_connection_details(
        &self,
        request: Request<GetConnectionDetailsRequest>,
    ) -> Result<Response<GetConnectionDetailsResponse>, Status> {
        let result = self
            .connection_service
            .get_connection_details(request.into_inner())
            .await;
        match result {
            Ok(response) => Ok(Response::new(response)),
            Err(e) => Err(Status::new(tonic::Code::Internal, e.message)),
        }
    }

    async fn get_all_connections(
        &self,
        request: Request<GetAllConnectionRequest>,
    ) -> Result<Response<GetAllConnectionResponse>, Status> {
        let result = self
            .connection_service
            .get_all_connections(request.into_inner());
        match result {
            Ok(response) => Ok(Response::new(response)),
            Err(e) => Err(Status::new(tonic::Code::Internal, e.message)),
        }
    }

    async fn get_schema(
        &self,
        request: Request<GetSchemaRequest>,
    ) -> Result<Response<GetSchemaResponse>, Status> {
        let result = self
            .connection_service
            .get_schema(request.into_inner())
            .await;
        match result {
            Ok(response) => Ok(Response::new(response)),
            Err(e) => Err(Status::new(tonic::Code::Internal, e.message)),
        }
    }
    async fn get_source(
        &self,
        request: Request<GetSourceRequest>,
    ) -> Result<Response<GetSourceResponse>, Status> {
        let result = self.source_service.get_source(request.into_inner());
        match result {
            Ok(response) => Ok(Response::new(response)),
            Err(e) => Err(Status::new(tonic::Code::Internal, e.message)),
        }
    }
    async fn create_source(
        &self,
        request: Request<CreateSourceRequest>,
    ) -> Result<Response<CreateSourceResponse>, Status> {
        let result = self.source_service.create_source(request.into_inner());
        match result {
            Ok(response) => Ok(Response::new(response)),
            Err(e) => Err(Status::new(tonic::Code::Internal, e.message)),
        }
    }
}

pub async fn get_server() -> Result<(), tonic::transport::Error> {
    let addr = "[::1]:8081".parse().unwrap();
    dotenv().ok();
    let database_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let grpc_service = GrpcService {
        connection_service: ConnectionService::new(database_url.clone()),
        source_service: SourceService::new(database_url),
    };
    let server = DozerApiServer::new(grpc_service);
    let server = tonic_web::config()
        .allow_origins(vec![
            "127.0.0.1",
            "localhost",
            "localhost:3001",
            "http://localhost:3001",
        ])
        .enable(server);

    Server::builder()
        .accept_http1(true)
        .add_service(server)
        .serve(addr)
        .await
}
