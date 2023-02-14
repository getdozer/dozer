use crate::{
    cli::{utils::get_db_path, AdminCliConfig},
    db::pool::establish_connection,
    services::{application_service::AppService, connection_service::ConnectionService},
};
use dotenvy::dotenv;
use dozer_types::{log::info, tracing::Level};
use tonic::{transport::Server, Request, Response, Status};
use tower_http::trace::{self, TraceLayer};
pub mod dozer_admin_grpc {
    #![allow(clippy::derive_partial_eq_without_eq, clippy::large_enum_variant)]
    tonic::include_proto!("dozer_admin_grpc");
    pub(crate) const FILE_DESCRIPTOR_SET: &[u8] =
        tonic::include_file_descriptor_set!("dozer_admin_grpc_descriptor");
}
use dozer_admin_grpc::{
    dozer_admin_server::{DozerAdmin, DozerAdminServer},
    AppResponse, ConnectionResponse, CreateAppRequest, CreateConnectionRequest,
    GetAllConnectionRequest, GetAllConnectionResponse, GetAppRequest, GetTablesRequest,
    GetTablesResponse, StartPipelineRequest, StartPipelineResponse, UpdateConnectionRequest,
};

use self::dozer_admin_grpc::{
    ListAppRequest, ListAppResponse, UpdateAppRequest, ValidateConnectionRequest,
    ValidateConnectionResponse,
};

pub struct GrpcService {
    app_service: AppService,
    connection_service: ConnectionService,
}

#[tonic::async_trait]
impl DozerAdmin for GrpcService {
    async fn create_application(
        &self,
        request: tonic::Request<CreateAppRequest>,
    ) -> Result<tonic::Response<AppResponse>, tonic::Status> {
        let result = self.app_service.create(request.into_inner());
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
    async fn update_application(
        &self,
        request: tonic::Request<UpdateAppRequest>,
    ) -> Result<tonic::Response<AppResponse>, tonic::Status> {
        let result = self.app_service.update_app(request.into_inner());
        match result {
            Ok(response) => Ok(Response::new(response)),
            Err(e) => Err(Status::new(tonic::Code::Internal, e.message)),
        }
    }

    async fn get_application(
        &self,
        request: tonic::Request<GetAppRequest>,
    ) -> Result<tonic::Response<AppResponse>, tonic::Status> {
        let result = self.app_service.get_app(request.into_inner());
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

    async fn create_connection(
        &self,
        request: Request<CreateConnectionRequest>,
    ) -> Result<Response<ConnectionResponse>, Status> {
        let result = self
            .connection_service
            .create_connection(request.into_inner());
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

    async fn get_tables(
        &self,
        request: Request<GetTablesRequest>,
    ) -> Result<Response<GetTablesResponse>, Status> {
        let result = self
            .connection_service
            .get_tables(request.into_inner())
            .await;
        match result {
            Ok(response) => Ok(Response::new(response)),
            Err(e) => Err(Status::new(tonic::Code::Internal, e.message)),
        }
    }

    async fn update_connection(
        &self,
        request: Request<UpdateConnectionRequest>,
    ) -> Result<Response<ConnectionResponse>, Status> {
        let result = self.connection_service.update(request.into_inner());
        match result {
            Ok(response) => Ok(Response::new(response)),
            Err(e) => Err(Status::new(tonic::Code::Internal, e.message)),
        }
    }

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
}

pub async fn start_admin_server(config: AdminCliConfig) -> Result<(), tonic::transport::Error> {
    dozer_tracing::init_telemetry(false).unwrap();
    let host = config.host;
    let port = config.port;
    let dozer_path = config.dozer_path;
    let addr = format!("{host:}:{port:}").parse().unwrap();
    dotenv().ok();
    let database_url: String = get_db_path();
    let db_pool = establish_connection(database_url);
    let grpc_service = GrpcService {
        connection_service: ConnectionService::new(db_pool.to_owned()),
        app_service: AppService::new(db_pool.to_owned(), dozer_path),
    };
    let server = DozerAdminServer::new(grpc_service);
    let server = tonic_web::config().allow_all_origins().enable(server);
    let reflection_service = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(dozer_admin_grpc::FILE_DESCRIPTOR_SET)
        .build()
        .unwrap();

    info!("Starting Dozer Admin server on http://{}:{} ", host, port,);

    Server::builder()
        .layer(
            TraceLayer::new_for_http()
                .make_span_with(trace::DefaultMakeSpan::new().level(Level::INFO))
                .on_response(trace::DefaultOnResponse::new().level(Level::INFO)),
        )
        .accept_http1(true)
        .add_service(reflection_service)
        .add_service(server)
        .serve(addr)
        .await
}
