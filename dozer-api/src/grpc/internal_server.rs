use super::internal_grpc::{
    self, internal_pipeline_service_client::InternalPipelineServiceClient,
    internal_pipeline_service_server::InternalPipelineService, PipelineRequest, PipelineResponse,
};
use crossbeam::channel::Receiver;
use dozer_types::{crossbeam::channel::Sender, log};
use log::warn;
use std::net::ToSocketAddrs;
use tokio::runtime::Runtime;
use tokio_stream::StreamExt;
use tonic::{transport::Server, Response, Status};
pub struct InternalServer {
    sender: Sender<PipelineRequest>,
}

type PipelineResult<T> = Result<Response<T>, Status>;

#[tonic::async_trait]
impl InternalPipelineService for InternalServer {
    async fn stream_pipeline_request(
        &self,
        request: tonic::Request<tonic::Streaming<PipelineRequest>>,
    ) -> PipelineResult<PipelineResponse> {
        let mut in_stream = request.into_inner();

        // let (sender, receiver) = channel::unbounded::<PipelineRequest>();

        while let Some(result) = in_stream.next().await {
            match result {
                Ok(msg) => {
                    if self.sender.send(msg).is_err() {
                        warn!("on_internal_grpc_stream send error");
                        // receiver drop
                        break;
                    }
                }
                Err(err) => {
                    warn!("{:?}", err);
                    break;
                }
            }
        }

        Ok(Response::new(PipelineResponse {}))
    }
}

pub fn start_internal_server(
    port: u16,
    sender: Sender<PipelineRequest>,
) -> Result<(), Box<dyn std::error::Error>> {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let server = InternalServer { sender };

        let mut addr = format!("[::1]:{}", port).to_socket_addrs().unwrap();
        Server::builder()
            .add_service(
                internal_grpc::internal_pipeline_service_server::InternalPipelineServiceServer::new(
                    server,
                ),
            )
            .serve(addr.next().unwrap())
            .await
            .unwrap();
    });

    Ok(())
}

pub fn start_internal_client(receiver: Receiver<PipelineRequest>) {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let mut client = InternalPipelineServiceClient::connect("http://[::1]:50051")
            .await
            .unwrap();

        let iterator = InternalIterator { receiver };
        let in_stream = tokio_stream::iter(iterator);

        client.stream_pipeline_request(in_stream).await.unwrap();
    });
}

struct InternalIterator {
    receiver: Receiver<PipelineRequest>,
}
impl Iterator for InternalIterator {
    type Item = PipelineRequest;

    fn next(&mut self) -> Option<Self::Item> {
        match self.receiver.recv() {
            Ok(msg) => Some(msg),
            Err(_) => None,
        }
    }
}
