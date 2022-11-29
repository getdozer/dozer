use crate::{
    db::pool::establish_connection,
    services::{
        application_service::AppService, connection_service::ConnectionService,
        endpoint_service::EndpointService, source_service::SourceService,
    },
};
use dotenvy::dotenv;
use std::env;
use tonic::{transport::Server, Request, Response, Status};
pub mod dozer_admin_grpc {
    #![allow(clippy::derive_partial_eq_without_eq, clippy::large_enum_variant)]
    tonic::include_proto!("dozer_admin_grpc");
    pub(crate) const FILE_DESCRIPTOR_SET: &[u8] =
        tonic::include_file_descriptor_set!("dozer_admin_grpc_descriptor");
}
use dozer_admin_grpc::{
    dozer_admin_server::{DozerAdmin, DozerAdminServer},
    CreateAppRequest, CreateAppResponse, CreateConnectionRequest, CreateConnectionResponse,
    CreateEndpointRequest, CreateEndpointResponse, CreateSourceRequest, CreateSourceResponse,
    DeleteEndpointRequest, DeleteEndpointResponse, GetAllConnectionRequest,
    GetAllConnectionResponse, GetConnectionDetailsRequest, GetConnectionDetailsResponse,
    GetEndpointRequest, GetEndpointResponse, GetSchemaRequest, GetSchemaResponse, GetSourceRequest,
    GetSourceResponse, StartPipelineRequest, StartPipelineResponse, TestConnectionRequest,
    TestConnectionResponse, UpdateConnectionRequest, UpdateConnectionResponse,
    UpdateEndpointRequest, UpdateEndpointResponse, UpdateSourceRequest, UpdateSourceResponse,
};

use self::dozer_admin_grpc::{
    GetAllEndpointRequest, GetAllEndpointResponse, GetAllSourceRequest, GetAllSourceResponse,
    ListAppRequest, ListAppResponse, UpdateAppRequest, UpdateAppResponse,
    ValidateConnectionRequest, ValidateConnectionResponse,
};

pub struct GrpcService {
    app_service: AppService,
    connection_service: ConnectionService,
    source_service: SourceService,
    endpoint_service: EndpointService,
}

#[tonic::async_trait]
impl DozerAdmin for GrpcService {
    async fn start_pipeline(
        &self,
        request: tonic::Request<StartPipelineRequest>,
    ) -> Result<tonic::Response<StartPipelineResponse>, tonic::Status> {
        let result = self.app_service.start_pipeline(request.into_inner());
        match result {
            Ok(response) => Ok(Response::new(response)),
            Err(e) => Err(Status::new(tonic::Code::Internal, e.message)),
        }
    }
    async fn create_application(
        &self,
        request: tonic::Request<CreateAppRequest>,
    ) -> Result<tonic::Response<CreateAppResponse>, tonic::Status> {
        let result = self.app_service.create(request.into_inner());
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
    async fn create_endpoint(
        &self,
        request: tonic::Request<CreateEndpointRequest>,
    ) -> Result<tonic::Response<CreateEndpointResponse>, tonic::Status> {
        let result = self.endpoint_service.create_endpoint(request.into_inner());
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

    async fn list_connections(
        &self,
        request: Request<GetAllConnectionRequest>,
    ) -> Result<Response<GetAllConnectionResponse>, Status> {
        let result = self.connection_service.list(request.into_inner());
        match result {
            Ok(response) => Ok(Response::new(response)),
            Err(e) => Err(Status::new(tonic::Code::Internal, e.message)),
        }
    }

    async fn list_endpoints(
        &self,
        request: tonic::Request<GetAllEndpointRequest>,
    ) -> Result<tonic::Response<GetAllEndpointResponse>, tonic::Status> {
        let result = self.endpoint_service.list(request.into_inner());
        match result {
            Ok(response) => Ok(Response::new(response)),
            Err(e) => Err(Status::new(tonic::Code::Internal, e.message)),
        }
    }

    async fn list_sources(
        &self,
        request: tonic::Request<GetAllSourceRequest>,
    ) -> Result<tonic::Response<GetAllSourceResponse>, tonic::Status> {
        let result = self.source_service.list(request.into_inner());
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
    async fn get_endpoint(
        &self,
        request: tonic::Request<GetEndpointRequest>,
    ) -> Result<tonic::Response<GetEndpointResponse>, tonic::Status> {
        let result = self.endpoint_service.get_endpoint(request.into_inner());
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
    async fn list_applications(
        &self,
        request: tonic::Request<ListAppRequest>,
    ) -> Result<tonic::Response<ListAppResponse>, tonic::Status> {
        let result = self.app_service.list(request.into_inner());
        match result {
            Ok(response) => Ok(Response::new(response)),
            Err(e) => Err(Status::new(tonic::Code::Internal, e.message)),
        }
    }
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
    async fn update_application(
        &self,
        request: tonic::Request<UpdateAppRequest>,
    ) -> Result<tonic::Response<UpdateAppResponse>, tonic::Status> {
        let result = self.app_service.update_app(request.into_inner());
        match result {
            Ok(response) => Ok(Response::new(response)),
            Err(e) => Err(Status::new(tonic::Code::Internal, e.message)),
        }
    }
    async fn update_connection(
        &self,
        request: Request<UpdateConnectionRequest>,
    ) -> Result<Response<UpdateConnectionResponse>, Status> {
        let result = self.connection_service.update(request.into_inner());
        match result {
            Ok(response) => Ok(Response::new(response)),
            Err(e) => Err(Status::new(tonic::Code::Internal, e.message)),
        }
    }

    async fn update_endpoint(
        &self,
        request: tonic::Request<UpdateEndpointRequest>,
    ) -> Result<tonic::Response<UpdateEndpointResponse>, tonic::Status> {
        let result = self.endpoint_service.update_endpoint(request.into_inner());
        match result {
            Ok(response) => Ok(Response::new(response)),
            Err(e) => Err(Status::new(tonic::Code::Internal, e.message)),
        }
    }

    async fn update_source(
        &self,
        request: Request<UpdateSourceRequest>,
    ) -> Result<Response<UpdateSourceResponse>, Status> {
        let result = self.source_service.update_source(request.into_inner());
        match result {
            Ok(response) => Ok(Response::new(response)),
            Err(e) => Err(Status::new(tonic::Code::Internal, e.message)),
        }
    }

    async fn validate_connection(
        &self,
        request: tonic::Request<ValidateConnectionRequest>,
    ) -> Result<tonic::Response<ValidateConnectionResponse>, tonic::Status> {
        let result = self
            .connection_service
            .validate_connection(request.into_inner());
        match result.await {
            Ok(response) => Ok(Response::new(response)),
            Err(e) => Err(Status::new(tonic::Code::Internal, e.message)),
        }
    }
    async fn delete_endpoint(
        &self,
        request: tonic::Request<DeleteEndpointRequest>,
    ) -> Result<tonic::Response<DeleteEndpointResponse>, tonic::Status> {
        let result = self.endpoint_service.delete(request.into_inner());
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
    let db_pool = establish_connection(database_url);
    let grpc_service = GrpcService {
        connection_service: ConnectionService::new(db_pool.to_owned()),
        source_service: SourceService::new(db_pool.to_owned()),
        endpoint_service: EndpointService::new(db_pool.to_owned()),
        app_service: AppService::new(db_pool.to_owned()),
    };
    let server = DozerAdminServer::new(grpc_service);
    let server = tonic_web::config().allow_all_origins().enable(server);
    let reflection_service = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(dozer_admin_grpc::FILE_DESCRIPTOR_SET)
        .build()
        .unwrap();
    Server::builder()
        .accept_http1(true)
        .add_service(reflection_service)
        .add_service(server)
        .serve(addr)
        .await
}
