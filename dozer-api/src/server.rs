use tonic::{transport::Server, Request, Response, Status};
pub mod dozer_api_grpc {
    tonic::include_proto!("dozer_api_grpc");
}
use self::dozer_api_grpc::{
    GetAllConnectionRequest, GetAllConnectionResponse, GetConnectionDetailsRequest,
    GetConnectionDetailsResponse,
};
use crate::{db::pool::establish_connection, services::{grpc_connection_service::GRPCConnectionService}};
use dozer_api_grpc::{
    dozer_api_server::{DozerApi, DozerApiServer},
    CreateConnectionRequest, CreateConnectionResponse, GetSchemaRequest, GetSchemaResponse,
    TestConnectionRequest, TestConnectionResponse,
};

pub struct GrpcService {
    grpc_connection_svc: GRPCConnectionService,
}

#[tonic::async_trait]
impl DozerApi for GrpcService {
    async fn test_connection(
        &self,
        request: Request<TestConnectionRequest>,
    ) -> Result<Response<TestConnectionResponse>, Status> {
        let result = self
            .grpc_connection_svc
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
        let result = self.grpc_connection_svc.create_connection(request.into_inner());
        match result {
            Ok(response) => Ok(Response::new(response)),
            Err(e) => Err(Status::new(tonic::Code::Internal, e.message)),
        }
    }

    async fn get_connection_details(
        &self,
        request: Request<GetConnectionDetailsRequest>,
    ) -> Result<Response<GetConnectionDetailsResponse>, Status> {
        let result = self.grpc_connection_svc.get_connection_details(request.into_inner()).await;
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
            .grpc_connection_svc
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
        let result = self.grpc_connection_svc.get_schema(request.into_inner()).await;
        match result {
            Ok(response) => Ok(Response::new(response)),
            Err(e) => Err(Status::new(tonic::Code::Internal, e.message)),
        }
    }
}

pub async fn get_server() -> Result<(), tonic::transport::Error> {
    let addr = "[::1]:8081".parse().unwrap();
    let db_connection = establish_connection();
    let grpc_service = GrpcService {
        grpc_connection_svc: GRPCConnectionService::new(db_connection),
    };
    let server = DozerApiServer::new(grpc_service);
    let server =  tonic_web::config()
    .allow_origins(vec!["127.0.0.1", "localhost", "localhost:3001", "http://localhost:3001"]).enable(server);
    
    Server::builder()
        .accept_http1(true)
        .add_service(server)
        .serve(addr)
        .await
}
