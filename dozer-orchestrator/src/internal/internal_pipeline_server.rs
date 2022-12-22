use std::{net::ToSocketAddrs, pin::Pin};

use crossbeam::channel::Receiver;
use dozer_api::grpc::internal_grpc::{
    internal_pipeline_service_server::{self, InternalPipelineService},
    GetAppConfigRequest, GetAppConfigResponse, PipelineRequest, PipelineResponse,
    RestartPipelineRequest, RestartPipelineResponse,
};
use dozer_types::{crossbeam, models::app_config::Config, tracing::warn};
use tokio::runtime::Runtime;
use tokio_stream::{wrappers::ReceiverStream, StreamExt};
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
        let iterator = InternalIterator {
            receiver: self.receiver.to_owned(),
        };
        let in_stream = tokio_stream::iter(iterator);
        let mut stream = Box::pin(in_stream);
        tokio::spawn(async move {
            while let Some(item) = stream.next().await {
                if let Err(_item) = tx.send(Result::<_, Status>::Ok(item)).await {
                    warn!("output_stream was build from rx and both are dropped");
                    break;
                }
            }
            warn!("client disconnected");
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
    rt.block_on(_start_internal_pipeline_server(app_config, receiver))
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
    let mut addr = format!("{}:{}", internal_config.host, internal_config.port)
        .to_socket_addrs()
        .unwrap();
    Server::builder()
        .add_service(internal_pipeline_service_server::InternalPipelineServiceServer::new(server))
        .serve(addr.next().unwrap())
        .await
}

struct InternalIterator {
    receiver: Receiver<PipelineResponse>,
}
impl Iterator for InternalIterator {
    type Item = PipelineResponse;

    fn next(&mut self) -> Option<Self::Item> {
        match self.receiver.recv() {
            Ok(msg) => Some(msg),
            Err(_) => None,
        }
    }
}
