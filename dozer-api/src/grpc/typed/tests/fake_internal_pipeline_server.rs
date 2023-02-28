use core::time;
use dozer_types::grpc_types::internal::internal_pipeline_service_server::{
    InternalPipelineService, InternalPipelineServiceServer,
};
use dozer_types::grpc_types::internal::{AliasEventsRequest, AliasRedirected, OperationsRequest};
use dozer_types::grpc_types::types::{value, Operation, OperationType, Record, Value};
use futures_util::FutureExt;
use std::{net::ToSocketAddrs, pin::Pin, thread};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{codegen::futures_core::Stream, transport::Server, Response, Status};
pub struct FakeInternalPipelineServer {}

type OperationsStream = Pin<Box<dyn Stream<Item = Result<Operation, Status>> + Send>>;
type AliasEventsStream = Pin<Box<dyn Stream<Item = Result<AliasRedirected, Status>> + Send>>;

#[tonic::async_trait]
impl InternalPipelineService for FakeInternalPipelineServer {
    type StreamOperationsStream = OperationsStream;
    async fn stream_operations(
        &self,
        _request: tonic::Request<OperationsRequest>,
    ) -> Result<Response<OperationsStream>, Status> {
        let (tx, rx) = tokio::sync::mpsc::channel(1000);
        thread::spawn(move || loop {
            thread::sleep(time::Duration::from_millis(100));
            let op = Operation {
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
                new_id: Some(0),
                endpoint_name: "films".to_string(),
            };
            tx.try_send(Ok(op)).unwrap();
        });
        let output_stream = ReceiverStream::new(rx);
        Ok(Response::new(Box::pin(output_stream)))
    }

    type StreamAliasEventsStream = AliasEventsStream;

    async fn stream_alias_events(
        &self,
        _request: tonic::Request<AliasEventsRequest>,
    ) -> Result<Response<Self::StreamAliasEventsStream>, Status> {
        let (_, alias_redirected_receiver) = tokio::sync::mpsc::channel(1000);
        let output_stream = ReceiverStream::new(alias_redirected_receiver);
        Ok(Response::new(Box::pin(output_stream)))
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
