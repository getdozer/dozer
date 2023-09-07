use std::sync::Arc;

use dozer_api::{tonic_reflection, tonic_web, tower_http};
use dozer_types::{
    grpc_types::{
        api_explorer::{
            api_explorer_service_server::{ApiExplorerService, ApiExplorerServiceServer},
            GetApiTokenRequest, GetApiTokenResponse,
        },
        contract::{
            contract_service_server::{ContractService, ContractServiceServer},
            CommonRequest, DotResponse, ProtoResponse, SchemasResponse, SourcesRequest,
        },
        live::{
            code_service_server::{CodeService, CodeServiceServer},
            ConnectResponse, Label, Labels, RunRequest,
        },
    },
    log::info,
};
use futures::stream::BoxStream;
use metrics::IntoLabels;
use tokio::sync::broadcast::Receiver;

use super::state::LiveState;
use dozer_types::tracing::Level;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};
use tower_http::trace::{self, TraceLayer};
pub const LIVE_PORT: u16 = 4556;

struct ContractServer {
    state: Arc<LiveState>,
}

#[tonic::async_trait]
impl ContractService for ContractServer {
    async fn sources(
        &self,
        request: Request<SourcesRequest>,
    ) -> Result<Response<SchemasResponse>, Status> {
        let req = request.into_inner();
        let res = self.state.get_source_schemas(req.connection_name).await;
        match res {
            Ok(res) => Ok(Response::new(res)),
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    async fn endpoints(
        &self,
        _request: Request<CommonRequest>,
    ) -> Result<Response<SchemasResponse>, Status> {
        let state = self.state.clone();
        let res = state.get_endpoints_schemas().await;

        match res {
            Ok(res) => Ok(Response::new(res)),
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    async fn generate_dot(
        &self,
        _request: Request<CommonRequest>,
    ) -> Result<Response<DotResponse>, Status> {
        let state = self.state.clone();
        let res = state.generate_dot().await;

        match res {
            Ok(res) => Ok(Response::new(res)),
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    async fn get_graph_schemas(
        &self,
        _request: Request<CommonRequest>,
    ) -> Result<Response<SchemasResponse>, Status> {
        let state = self.state.clone();
        let res = state.get_graph_schemas().await;

        match res {
            Ok(res) => Ok(Response::new(res)),
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    async fn get_protos(
        &self,
        _request: Request<CommonRequest>,
    ) -> Result<Response<ProtoResponse>, Status> {
        let state = self.state.clone();
        let res = state.get_protos().await;

        match res {
            Ok(res) => Ok(Response::new(res)),
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }
}

struct LiveServer {
    receiver: Receiver<ConnectResponse>,
    state: Arc<LiveState>,
}

#[tonic::async_trait]
impl CodeService for LiveServer {
    type LiveConnectStream = BoxStream<'static, Result<ConnectResponse, Status>>;

    async fn live_connect(
        &self,
        _request: Request<()>,
    ) -> Result<Response<Self::LiveConnectStream>, Status> {
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        let mut receiver = self.receiver.resubscribe();

        let initial_state = self.state.clone();
        tokio::spawn(async move {
            let initial_state = initial_state.get_current().await;
            if let Err(e) = tx
                .send(Ok(ConnectResponse {
                    live: Some(initial_state),
                    progress: None,
                    build: None,
                }))
                .await
            {
                info!("Error getting initial state");
                info!("{}", e.to_string());
                return {};
            }
            loop {
                let Ok(connect_response) = receiver.recv().await else {
                    break;
                };
                if tx.send(Ok(connect_response)).await.is_err() {
                    break;
                }
            }
        });
        let stream = ReceiverStream::new(rx);

        Ok(Response::new(Box::pin(stream) as Self::LiveConnectStream))
    }

    async fn run(&self, request: Request<RunRequest>) -> Result<Response<Labels>, Status> {
        let req = request.into_inner();
        let state = self.state.clone();
        info!("Starting dozer");
        match state.run(req).await {
            Ok(labels) => {
                let labels = labels
                    .into_labels()
                    .into_iter()
                    .map(|label| Label {
                        key: label.key().to_string(),
                        value: label.value().to_string(),
                    })
                    .collect();
                Ok(Response::new(Labels { labels }))
            }
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    async fn stop(&self, _request: Request<()>) -> Result<Response<()>, Status> {
        let state = self.state.clone();
        info!("Stopping dozer");
        match state.stop().await {
            Ok(()) => Ok(Response::new(())),
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }
}

struct ApiExplorerServer {
    state: Arc<LiveState>,
}
#[tonic::async_trait]
impl ApiExplorerService for ApiExplorerServer {
    async fn get_api_token(
        &self,
        request: Request<GetApiTokenRequest>,
    ) -> Result<Response<GetApiTokenResponse>, Status> {
        let state = self.state.clone();
        let input = request.into_inner();
        let res = state.get_api_token(input.ttl).await;
        match res {
            Ok(res) => Ok(Response::new(GetApiTokenResponse { token: res })),
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }
}
pub async fn serve(
    receiver: Receiver<ConnectResponse>,
    state: Arc<LiveState>,
) -> Result<(), tonic::transport::Error> {
    let addr = format!("0.0.0.0:{LIVE_PORT}").parse().unwrap();
    let contract_server = ContractServer {
        state: state.clone(),
    };
    let api_explorer_server = ApiExplorerServer {
        state: state.clone(),
    };
    let live_server = LiveServer { receiver, state };
    let contract_service = ContractServiceServer::new(contract_server);
    let code_service = CodeServiceServer::new(live_server);
    let api_explorer_service = ApiExplorerServiceServer::new(api_explorer_server);
    // Enable CORS for local development
    let contract_service = tonic_web::config()
        .allow_all_origins()
        .enable(contract_service);
    let code_service = tonic_web::config().allow_all_origins().enable(code_service);
    let api_explorer_service = tonic_web::config()
        .allow_all_origins()
        .enable(api_explorer_service);

    let reflection_service = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(
            dozer_types::grpc_types::contract::FILE_DESCRIPTOR_SET,
        )
        .register_encoded_file_descriptor_set(dozer_types::grpc_types::live::FILE_DESCRIPTOR_SET)
        .register_encoded_file_descriptor_set(
            dozer_types::grpc_types::api_explorer::FILE_DESCRIPTOR_SET,
        )
        .build()
        .unwrap();

    tonic::transport::Server::builder()
        .layer(
            TraceLayer::new_for_http()
                .make_span_with(trace::DefaultMakeSpan::new().level(Level::INFO))
                .on_response(trace::DefaultOnResponse::new().level(Level::INFO))
                .on_failure(trace::DefaultOnFailure::new().level(Level::ERROR)),
        )
        .accept_http1(true)
        .concurrency_limit_per_connection(32)
        .add_service(contract_service)
        .add_service(code_service)
        .add_service(api_explorer_service)
        .add_service(reflection_service)
        .serve(addr)
        .await
}
