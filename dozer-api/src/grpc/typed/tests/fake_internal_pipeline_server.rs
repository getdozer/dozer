use crate::grpc::internal_grpc::{
    internal_pipeline_service_server::{InternalPipelineService, InternalPipelineServiceServer},
    GetAppConfigRequest, GetAppConfigResponse, PipelineRequest, RestartPipelineRequest,
    RestartPipelineResponse,
};
use crate::grpc::{
    internal_grpc::{pipeline_response::ApiEvent, PipelineResponse},
    types::{value, Operation, OperationType, Record, Value},
};
use core::time;
use crossbeam::channel::Receiver;
use dozer_types::crossbeam;
use futures_util::FutureExt;
use std::{net::ToSocketAddrs, pin::Pin, thread};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{codegen::futures_core::Stream, transport::Server, Response, Status};
pub struct FakeInternalPipelineServer {}
type ResponseStream = Pin<Box<dyn Stream<Item = Result<PipelineResponse, Status>> + Send>>;

#[tonic::async_trait]
impl InternalPipelineService for FakeInternalPipelineServer {
    type StreamPipelineRequestStream = ResponseStream;
    async fn stream_pipeline_request(
        &self,
        _request: tonic::Request<PipelineRequest>,
    ) -> Result<Response<ResponseStream>, Status> {
        let (tx, rx) = tokio::sync::mpsc::channel(1000);
        thread::spawn(move || loop {
            thread::sleep(time::Duration::from_millis(100));
            let fake_event = PipelineResponse {
                endpoint: "films".to_string(),
                api_event: Some(ApiEvent::Op(Operation {
                    typ: OperationType::Insert as i32,
                    old: None,
                    new: Some(Record {
                        values: vec![
                            Value {
                                value: Some(value::Value::UintValue(32)),
                            },
                            Value {
                                value: Some(value::Value::StringValue("description".to_string())),
                            },
                            Value { value: None },
                            Value { value: None },
                        ],
                        version: 1,
                    }),
                    endpoint_name: "films".to_string(),
                })),
            };
            tx.try_send(Ok(fake_event)).unwrap();
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
        todo!();
    }
    async fn restart(
        &self,
        _request: tonic::Request<RestartPipelineRequest>,
    ) -> Result<tonic::Response<RestartPipelineResponse>, tonic::Status> {
        todo!();
    }
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

pub async fn start_fake_internal_grpc_pipeline(
    host: String,
    port: u32,
    receiver_shutdown: tokio::sync::oneshot::Receiver<()>,
) -> Result<(), tonic::transport::Error> {
    let mut addr = format!("{host}:{port}").to_socket_addrs().unwrap();
    let server = FakeInternalPipelineServer {};

    Server::builder()
        .add_service(InternalPipelineServiceServer::new(server))
        .serve_with_shutdown(addr.next().unwrap(), receiver_shutdown.map(drop))
        .await
}
