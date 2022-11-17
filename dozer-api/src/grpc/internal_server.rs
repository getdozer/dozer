use super::internal_grpc::{
    self, internal_pipeline_service_server::InternalPipelineService, PipelineRequest,
    PipelineResponse,
};
use dozer_types::{crossbeam::channel::Sender, log};
use log::warn;
use std::net::ToSocketAddrs;
use tokio_stream::StreamExt;
use tonic::{transport::Server, Response, Status};
pub struct InternalGrpcServer {
    sender: Sender<PipelineRequest>,
}

type PipelineResult<T> = Result<Response<T>, Status>;

#[tonic::async_trait]
impl InternalPipelineService for InternalGrpcServer {
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

async fn run(port: u16, sender: Sender<PipelineRequest>) -> Result<(), Box<dyn std::error::Error>> {
    let server = InternalGrpcServer { sender };

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

    Ok(())
}
