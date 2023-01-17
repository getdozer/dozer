use crate::grpc::internal_grpc::{
    internal_pipeline_service_server::{self, InternalPipelineService},
    GetAppConfigRequest, GetAppConfigResponse, PipelineRequest, PipelineResponse,
    RestartPipelineRequest, RestartPipelineResponse,
};
use crossbeam::channel::Receiver;
use dozer_types::{crossbeam, log::info, models::app_config::Config, tracing::warn};
use std::{net::ToSocketAddrs, pin::Pin, thread};
use tokio::runtime::Runtime;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{codegen::futures_core::Stream, transport::Server, Response, Status};

pub struct InternalPipelineServer {
    app_config: Config,
    receiver: Receiver<PipelineResponse>,
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
        let receiver = self.receiver.to_owned();
        thread::spawn(move || loop {
            let event_received = receiver.recv();
            match event_received {
                Ok(event) => {
                    let result = tx.try_send(Ok(event));
                    if let Err(e) = result {
                        match e {
                            tokio::sync::mpsc::error::TrySendError::Closed(err) => {
                                warn!("Channel closed {:?}", err);
                                break;
                            }
                            tokio::sync::mpsc::error::TrySendError::Full(err) => {
                                warn!("Channel full {:?}", err);
                            }
                        }
                    }
                }
                Err(e) => {
                    warn!("Error receiving event: {:?}", e);
                    break;
                }
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
    let server = InternalPipelineServer {
        app_config: app_config.to_owned(),
        receiver,
    };

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
