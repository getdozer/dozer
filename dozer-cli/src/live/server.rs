use std::sync::Arc;

use dozer_api::{tonic_reflection, tonic_web};
use dozer_types::{
    grpc_types::{
        live::{
            code_service_server::{CodeService, CodeServiceServer},
            CommonRequest, DotResponse, LiveResponse, RunSqlRequest, SchemasResponse,
            SourcesRequest, SqlRequest,
        },
        types::Operation,
    },
    log::{error, info},
};
use futures::stream::BoxStream;
use tokio::sync::broadcast::Receiver;

use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use super::state::LiveState;
pub struct LiveServer {
    pub receiver: Receiver<LiveResponse>,
    pub state: Arc<LiveState>,
}

#[tonic::async_trait]
impl CodeService for LiveServer {
    type LiveConnectStream = BoxStream<'static, Result<LiveResponse, Status>>;
    type RunSqlStream = BoxStream<'static, Result<Operation, Status>>;

    async fn live_connect(
        &self,
        _request: Request<CommonRequest>,
    ) -> Result<Response<Self::LiveConnectStream>, Status> {
        let (tx, rx) = tokio::sync::mpsc::channel(1);

        let mut receiver = self.receiver.resubscribe();
        let initial_state = self.state.clone();
        tokio::spawn(async move {
            let initial_state = initial_state.get_current();
            match initial_state {
                Ok(initial_state) => {
                    if let Err(e) = tx.send(Ok(initial_state)).await {
                        info!("Error getting initial state");
                        info!("{:?}", e);
                    }
                }
                Err(e) => {
                    info!("Error sending to channel");
                    info!("{:?}", e);
                }
            };

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

    async fn build_sql(
        &self,
        request: Request<SqlRequest>,
    ) -> Result<Response<SchemasResponse>, Status> {
        let state = self.state.clone();
        let handle = std::thread::spawn(move || state.build_sql(request.into_inner().sql));
        let res = handle.join().unwrap();

        match res {
            Ok(res) => Ok(Response::new(res)),
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    async fn run_sql(
        &self,
        request: Request<RunSqlRequest>,
    ) -> Result<Response<Self::RunSqlStream>, Status> {
        let (tx, rx) = tokio::sync::mpsc::channel(1);

        let req = request.into_inner();
        let state = self.state.clone();
        std::thread::spawn(move || {
            state.run_sql(req.sql, req.endpoints, tx).unwrap();
        });

        let stream = ReceiverStream::new(rx);

        Ok(Response::new(Box::pin(stream) as Self::RunSqlStream))
    }
}

pub async fn serve(
    receiver: Receiver<LiveResponse>,
    state: Arc<LiveState>,
) -> Result<(), tonic::transport::Error> {
    let addr = "0.0.0.0:4556".parse().unwrap();
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
