use crate::{
    cli::{utils::get_db_path, AdminCliConfig},
    db::pool::establish_connection,
    services::{application_service::AppService, connection_service::ConnectionService},
};
use dozer_types::{log::info, tracing::Level};
use tonic::{transport::Server, Request, Response, Status};
use tower_http::trace::{self, TraceLayer};

use dozer_types::grpc_types::admin::LogMessage;
use dozer_types::grpc_types::admin::LogMessageRequest;
use dozer_types::grpc_types::admin::{
    dozer_admin_server::{DozerAdmin, DozerAdminServer},
    AppResponse, ConnectionRequest, ConnectionResponse, CreateAppRequest, GetAllConnectionRequest,
    GetAllConnectionResponse, GetAppRequest, GetTablesRequest, GetTablesResponse, ListFilesRequest,
    ListFilesResponse, SaveFilesRequest, SaveFilesResponse, StatusUpdate, StatusUpdateRequest,
    UpdateConnectionRequest,
};
use dozer_types::grpc_types::admin::{
    GenerateGraphRequest, GenerateGraphResponse, GenerateYamlRequest, GenerateYamlResponse,
    ListAppRequest, ListAppResponse, ParseRequest, ParseResponse, ParseYamlRequest,
    ParseYamlResponse, StartRequest, StartResponse, StopRequest, StopResponse, UpdateAppRequest,
    ValidateConnectionResponse,
};

pub struct GrpcService {
    app_service: AppService,
    connection_service: ConnectionService,
}

use tokio_stream::wrappers::ReceiverStream;
type EventResult<T> = Result<Response<T>, Status>;

#[tonic::async_trait]
impl DozerAdmin for GrpcService {
    async fn parse_sql(
        &self,
        request: tonic::Request<ParseRequest>,
    ) -> Result<tonic::Response<ParseResponse>, tonic::Status> {
        let result = self.app_service.parse_sql(request.into_inner());
        match result {
            Ok(response) => Ok(Response::new(response)),
            Err(e) => Err(Status::new(tonic::Code::Internal, e.message)),
        }
    }

    async fn parse_yaml(
        &self,
        request: tonic::Request<ParseYamlRequest>,
    ) -> Result<tonic::Response<ParseYamlResponse>, tonic::Status> {
        let result = self.app_service.parse_yaml(request.into_inner());
        match result {
            Ok(response) => Ok(Response::new(response)),
            Err(e) => Err(Status::new(tonic::Code::Internal, e.message)),
        }
    }

    async fn generate_yaml(
        &self,
        request: tonic::Request<GenerateYamlRequest>,
    ) -> Result<tonic::Response<GenerateYamlResponse>, tonic::Status> {
        let result = self.app_service.generate_yaml(request.into_inner());
        match result {
            Ok(response) => Ok(Response::new(response)),
            Err(e) => Err(Status::new(tonic::Code::Internal, e.message)),
        }
    }

    async fn generate_graph(
        &self,
        request: tonic::Request<GenerateGraphRequest>,
    ) -> Result<tonic::Response<GenerateGraphResponse>, tonic::Status> {
        let result = self.app_service.generate(request.into_inner());
        match result {
            Ok(response) => Ok(Response::new(response)),
            Err(e) => Err(Status::new(tonic::Code::Internal, e.message)),
        }
    }

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
        request: tonic::Request<ConnectionRequest>,
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
        request: Request<ConnectionRequest>,
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

    async fn start_dozer(
        &self,
        request: tonic::Request<StartRequest>,
    ) -> Result<tonic::Response<StartResponse>, tonic::Status> {
        let result = self.app_service.start_dozer(request.into_inner());
        match result {
            Ok(response) => Ok(Response::new(response)),
            Err(e) => Err(Status::new(tonic::Code::Internal, e.message)),
        }
    }

    async fn stop_dozer(
        &self,
        request: tonic::Request<StopRequest>,
    ) -> Result<tonic::Response<StopResponse>, tonic::Status> {
        let result = self.app_service.stop_dozer(request.into_inner());
        match result {
            Ok(response) => Ok(Response::new(response)),
            Err(e) => Err(Status::new(tonic::Code::Internal, e.message)),
        }
    }

    async fn list_files(
        &self,
        _request: tonic::Request<ListFilesRequest>,
    ) -> Result<tonic::Response<ListFilesResponse>, tonic::Status> {
        let result = self.app_service.list_files();
        match result {
            Ok(response) => Ok(Response::new(response)),
            Err(e) => Err(Status::new(tonic::Code::Internal, e.message)),
        }
    }

    async fn save_files(
        &self,
        request: tonic::Request<SaveFilesRequest>,
    ) -> Result<tonic::Response<SaveFilesResponse>, tonic::Status> {
        let result = self.app_service.save_files(request.into_inner());
        match result {
            Ok(response) => Ok(Response::new(response)),
            Err(e) => Err(Status::new(tonic::Code::Internal, e.message)),
        }
    }

    type OnLogMessageStream = ReceiverStream<Result<LogMessage, tonic::Status>>;

    async fn on_log_message(
        &self,
        _request: Request<LogMessageRequest>,
    ) -> EventResult<Self::OnLogMessageStream> {
        let (tx, rx) = tokio::sync::mpsc::channel(100);

        tokio::spawn(async move { AppService::read_logs(tx).await });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    type OnStatusUpdateStream = ReceiverStream<Result<StatusUpdate, tonic::Status>>;

    async fn on_status_update(
        &self,
        request: Request<StatusUpdateRequest>,
    ) -> Result<Response<Self::OnStatusUpdateStream>, Status> {
        let (tx, rx) = tokio::sync::mpsc::channel(1000);

        tokio::spawn(
            async move { AppService::stream_status_update(request.into_inner(), tx).await },
        );

        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

pub async fn start_admin_server(config: AdminCliConfig) -> Result<(), tonic::transport::Error> {
    let _guard = dozer_tracing::init_telemetry(None, None);

    let host = config.host;
    let port = config.port;
    let addr = format!("{host:}:{port:}").parse().unwrap();
    let database_url: String = get_db_path();

    let db_pool = establish_connection(database_url);
    let app_service = AppService::new(db_pool.to_owned());
    let grpc_service = GrpcService {
        connection_service: ConnectionService::new(db_pool.to_owned()),
        app_service: app_service.to_owned(),
    };
    let server = DozerAdminServer::new(grpc_service);
    let server = tonic_web::config().allow_all_origins().enable(server);
    let reflection_service = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(dozer_types::grpc_types::admin::FILE_DESCRIPTOR_SET)
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
        .await?;

    for (_, shutdown) in app_service.apps.write().drain() {
        shutdown.shutdown();
    }
    Ok(())
}
