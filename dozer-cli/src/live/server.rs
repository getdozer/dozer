use std::sync::Arc;

use dozer_api::{tonic_reflection, tonic_web, tower_http};
use dozer_types::{
    grpc_types::{
        contract::{
            contract_service_server::{ContractService, ContractServiceServer},
            CommonRequest, DotResponse, SchemasResponse, SourcesRequest,
        },
        live::{
            code_service_server::{CodeService, CodeServiceServer},
            CommonResponse, ConnectResponse, RunRequest,
        },
    },
    log::{error, info},
};
use futures::stream::BoxStream;
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
                }))
                .await
            {
                info!("Error getting initial state");
                info!("{:?}", e);
            }
            loop {
                let res = receiver.recv().await;
                match res {
                    Ok(res) => {
                        let res = tx.send(Ok(res)).await;
                        match res {
                            Ok(_) => {}
                            Err(e) => {
                                error!("Error sending to channel");
                                error!("{:?}", e);
                                break;
                            }
                        }
                    }
                    Err(_) => {
                        break;
                    }
                }
            }
        });
        let stream = ReceiverStream::new(rx);

        Ok(Response::new(Box::pin(stream) as Self::LiveConnectStream))
    }

    async fn run(&self, request: Request<RunRequest>) -> Result<Response<CommonResponse>, Status> {
        let req = request.into_inner();
        let state = self.state.clone();
        info!("Starting dozer");
        match state.run(req).await {
            Ok(_) => {
                // let _err = state.broadcast();
                Ok(Response::new(CommonResponse {}))
            }
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    async fn stop(&self, _request: Request<()>) -> Result<Response<CommonResponse>, Status> {
        let state = self.state.clone();
        info!("Stopping dozer");
        match state.stop().await {
            Ok(_) => {
                // let _err = state.broadcast();
                Ok(Response::new(CommonResponse {}))
            }
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
    let live_server = LiveServer { receiver, state };
    let contract_service = ContractServiceServer::new(contract_server);
    let code_service = CodeServiceServer::new(live_server);

    // Enable CORS for local development
    let contract_service = tonic_web::config()
        .allow_all_origins()
        .enable(contract_service);
    let code_service = tonic_web::config().allow_all_origins().enable(code_service);

    let reflection_service = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(
            dozer_types::grpc_types::contract::FILE_DESCRIPTOR_SET,
        )
        .register_encoded_file_descriptor_set(dozer_types::grpc_types::live::FILE_DESCRIPTOR_SET)
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
        .add_service(reflection_service)
        .serve(addr)
        .await
}
