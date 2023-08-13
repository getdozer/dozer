use std::sync::Arc;

use dozer_api::{tonic_reflection, tonic_web};
use dozer_types::{
    grpc_types::live::{
        code_service_server::{CodeService, CodeServiceServer},
        CommonRequest, CommonResponse, ConnectResponse, DotResponse, RunRequest, SchemasResponse,
        SourcesRequest, SqlResponse,
    },
    log::{error, info},
};
use futures::stream::BoxStream;
use tokio::sync::broadcast::Receiver;

use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use super::state::LiveState;
const LIVE_PORT: u16 = 4556;
pub struct LiveServer {
    pub receiver: Receiver<ConnectResponse>,
    pub state: Arc<LiveState>,
}

#[tonic::async_trait]
impl CodeService for LiveServer {
    type LiveConnectStream = BoxStream<'static, Result<ConnectResponse, Status>>;

    async fn live_connect(
        &self,
        _request: Request<CommonRequest>,
    ) -> Result<Response<Self::LiveConnectStream>, Status> {
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        let mut receiver = self.receiver.resubscribe();

        let initial_state = self.state.clone();
        tokio::spawn(async move {
            let initial_state = initial_state.get_current();
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
        let handle = std::thread::spawn(move || state.get_endpoints_schemas());
        let res = handle.join().unwrap();

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
        let handle = std::thread::spawn(move || state.generate_dot());
        let res = handle.join().unwrap();

        match res {
            Ok(res) => Ok(Response::new(res)),
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    async fn get_sql(
        &self,
        _request: Request<CommonRequest>,
    ) -> Result<Response<SqlResponse>, Status> {
        let res = self.state.get_sql();

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
        let handle = std::thread::spawn(move || state.get_graph_schemas());
        let res = handle.join().unwrap();

        match res {
            Ok(res) => Ok(Response::new(res)),
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    async fn run(&self, request: Request<RunRequest>) -> Result<Response<CommonResponse>, Status> {
        let req = request.into_inner();
        let state = self.state.clone();
        info!("Starting dozer");
        match state.run(req) {
            Ok(_) => {
                // let _err = state.broadcast();
                Ok(Response::new(CommonResponse {}))
            }
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    async fn stop(
        &self,
        _request: Request<CommonRequest>,
    ) -> Result<Response<CommonResponse>, Status> {
        let state = self.state.clone();
        info!("Stopping dozer");
        match state.stop() {
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
    let live_server = LiveServer { receiver, state };
    let svc = CodeServiceServer::new(live_server);

    // Enable CORS for local development
    let svc = tonic_web::config().allow_all_origins().enable(svc);

    let reflection_service = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(dozer_types::grpc_types::live::FILE_DESCRIPTOR_SET)
        .build()
        .unwrap();

    tonic::transport::Server::builder()
        .accept_http1(true)
        .add_service(svc)
        .add_service(reflection_service)
        .serve(addr)
        .await
}
