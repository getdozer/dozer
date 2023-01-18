use crate::grpc::internal_grpc::{
    internal_pipeline_service_server::{self, InternalPipelineService},
    GetAppConfigRequest, GetAppConfigResponse, PipelineRequest, PipelineResponse,
    RestartPipelineRequest, RestartPipelineResponse,
};
use crossbeam::channel::Receiver;
use dozer_types::{crossbeam, log::info, models::app_config::Config, tracing::warn};
use std::{net::ToSocketAddrs, pin::Pin};
use tokio::{
    runtime::Runtime,
    sync::broadcast::{self, Sender},
};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{codegen::futures_core::Stream, transport::Server, Response, Status};

pub struct InternalPipelineServer {
    app_config: Config,
    receiver: broadcast::Receiver<PipelineResponse>,
}
impl InternalPipelineServer {
    pub fn new(app_config: Config, receiver: Receiver<PipelineResponse>) -> Self {
        let (tx, rx1) = broadcast::channel::<PipelineResponse>(16);
        tokio::spawn(async move {
            Self::setup_broad_cast_channel(tx, receiver);
        });
        Self {
            app_config,
            receiver: rx1,
        }
    }

    fn setup_broad_cast_channel(
        tx: Sender<PipelineResponse>,
        receiver: Receiver<PipelineResponse>,
    ) {
        loop {
            let message = receiver.recv();
            match message {
                Ok(message) => {
                    let result = tx.send(message);
                    if let Err(e) = result {
                        warn!("Internal Pipeline server - Error sending message to broadcast channel: {:?}", e);
                    }
                }
                Err(err) => {
                    warn!(
                        "Internal Pipeline server - message reveived error: {:?}",
                        err
                    );
                    break;
                }
            }
        }
    }
}
type ResponseStream = Pin<Box<dyn Stream<Item = Result<PipelineResponse, Status>> + Send>>;

#[tonic::async_trait]
impl InternalPipelineService for InternalPipelineServer {
    type StreamPipelineRequestStream = ResponseStream;
    async fn stream_pipeline_request(
        &self,
        _request: tonic::Request<PipelineRequest>,
    ) -> Result<Response<ResponseStream>, Status> {
        let (tx, rx) = tokio::sync::mpsc::channel(1000);
        let mut receiver = self.receiver.resubscribe();
        tokio::spawn(async move {
            loop {
                let result = receiver.try_recv();
                match result {
                    Ok(message) => {
                        let result = tx.send(Ok(message)).await;
                        if let Err(e) = result {
                            warn!("Error sending message to mpsc channel: {:?}", e);
                            break;
                        }
                    }
                    Err(err) => {
                        if err == broadcast::error::TryRecvError::Closed {
                            break;
                        }
                    }
                }
                tokio::time::sleep(tokio::time::Duration::from_millis(1)).await;
            }
        });
        let output_stream = ReceiverStream::new(rx);
        Ok(Response::new(
            Box::pin(output_stream) as Self::StreamPipelineRequestStream
        ))
    }

    async fn get_config(
        &self,
        _request: tonic::Request<GetAppConfigRequest>,
    ) -> Result<tonic::Response<GetAppConfigResponse>, tonic::Status> {
        Ok(Response::new(GetAppConfigResponse {
            data: Some(self.app_config.to_owned()),
        }))
    }
    async fn restart(
        &self,
        _request: tonic::Request<RestartPipelineRequest>,
    ) -> Result<tonic::Response<RestartPipelineResponse>, tonic::Status> {
        todo!();
    }
}

pub fn start_internal_pipeline_server(
    app_config: Config,
    receiver: Receiver<PipelineResponse>,
) -> Result<(), tonic::transport::Error> {
    let rt = Runtime::new().unwrap();
    rt.block_on(async { _start_internal_pipeline_server(app_config, receiver).await })
}
async fn _start_internal_pipeline_server(
    app_config: Config,
    receiver: Receiver<PipelineResponse>,
) -> Result<(), tonic::transport::Error> {
    let server = InternalPipelineServer::new(app_config.to_owned(), receiver);

    let internal_config = app_config
        .api
        .unwrap_or_default()
        .pipeline_internal
        .unwrap_or_default();

    info!(
        "Starting Internal Server on http://{}:{}",
        internal_config.host, internal_config.port,
    );
    let mut addr = format!("{}:{}", internal_config.host, internal_config.port)
        .to_socket_addrs()
        .unwrap();
    Server::builder()
        .add_service(internal_pipeline_service_server::InternalPipelineServiceServer::new(server))
        .serve(addr.next().unwrap())
        .await
}
