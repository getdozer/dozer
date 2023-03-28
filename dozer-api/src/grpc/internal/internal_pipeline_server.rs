use crossbeam::channel::{Receiver, Sender};
use dozer_types::grpc_types::internal::{StatusUpdate, StatusUpdateRequest};
use dozer_types::{crossbeam, log::info, models::app_config::Config, tracing::warn};
use dozer_types::{
    grpc_types::{
        internal::{
            internal_pipeline_service_server::{self, InternalPipelineService},
            AliasEventsRequest, AliasRedirected, OperationsRequest,
        },
        types::Operation,
    },
    log::debug,
};
use std::{fmt::Debug, net::ToSocketAddrs, pin::Pin, thread};
use tokio::sync::broadcast;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{codegen::futures_core::Stream, transport::Server, Response, Status};

pub type PipelineEventSenders = (
    Sender<AliasRedirected>,
    Sender<Operation>,
    Sender<StatusUpdate>,
);
pub type PipelineEventReceivers = (
    Receiver<AliasRedirected>,
    Receiver<Operation>,
    Receiver<StatusUpdate>,
);

pub struct InternalPipelineServer {
    alias_redirected_receiver: broadcast::Receiver<AliasRedirected>,
    operation_receiver: broadcast::Receiver<Operation>,
    status_updates_receiver: broadcast::Receiver<StatusUpdate>,
}
impl InternalPipelineServer {
    pub fn new(pipeline_event_receivers: PipelineEventReceivers) -> Self {
        let alias_redirected_receiver =
            crossbeam_mpsc_receiver_to_tokio_broadcast_receiver(pipeline_event_receivers.0);
        let operation_receiver =
            crossbeam_mpsc_receiver_to_tokio_broadcast_receiver(pipeline_event_receivers.1);
        let status_updates_receiver =
            crossbeam_mpsc_receiver_to_tokio_broadcast_receiver(pipeline_event_receivers.2);
        Self {
            alias_redirected_receiver,
            operation_receiver,
            status_updates_receiver,
        }
    }
}

fn crossbeam_mpsc_receiver_to_tokio_broadcast_receiver<T: Clone + Debug + Send + 'static>(
    crossbeam_receiver: Receiver<T>,
) -> broadcast::Receiver<T> {
    let (broadcast_sender, broadcast_receiver) = broadcast::channel(16);
    thread::Builder::new().name("crossbeam_mpsc_receiver_to_tokio_broadcast_receiver".to_string()).spawn(move || loop {
        let message = crossbeam_receiver.recv();
        match message {
            Ok(message) => {
                let result = broadcast_sender.send(message);
                if let Err(e) = result {
                    warn!("Internal Pipeline server - Error sending message to broadcast channel: {:?}", e);
                }
            }
            Err(err) => {
                debug!(
                    "Error receiving: {:?}. Exiting crossbeam_mpsc_receiver_to_tokio_broadcast_receiver thread",
                    err
                );
                break;
            }
        }
    }).expect("Failed to spawn crossbeam_mpsc_receiver_to_tokio_broadcast_receiver thread");
    broadcast_receiver
}

type OperationsStream = Pin<Box<dyn Stream<Item = Result<Operation, Status>> + Send>>;
type AliasEventsStream = Pin<Box<dyn Stream<Item = Result<AliasRedirected, Status>> + Send>>;
type StatusUpdateStream = Pin<Box<dyn Stream<Item = Result<StatusUpdate, Status>> + Send>>;

#[tonic::async_trait]
impl InternalPipelineService for InternalPipelineServer {
    type StreamOperationsStream = OperationsStream;
    async fn stream_operations(
        &self,
        _request: tonic::Request<OperationsRequest>,
    ) -> Result<Response<OperationsStream>, Status> {
        let (operation_sender, operation_receiver) = tokio::sync::mpsc::channel(1000);
        let mut receiver = self.operation_receiver.resubscribe();
        tokio::spawn(async move {
            loop {
                let result = receiver.try_recv();
                match result {
                    Ok(operation) => {
                        let result = operation_sender.send(Ok(operation)).await;
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
        let output_stream = ReceiverStream::new(operation_receiver);
        Ok(Response::new(Box::pin(output_stream)))
    }

    type StreamAliasEventsStream = AliasEventsStream;

    async fn stream_alias_events(
        &self,
        _request: tonic::Request<AliasEventsRequest>,
    ) -> Result<Response<Self::StreamAliasEventsStream>, Status> {
        let (alias_redirected_sender, alias_redirected_receiver) = tokio::sync::mpsc::channel(1000);
        let mut receiver = self.alias_redirected_receiver.resubscribe();
        tokio::spawn(async move {
            loop {
                let result = receiver.try_recv();
                match result {
                    Ok(alias_redirected) => {
                        let result = alias_redirected_sender.send(Ok(alias_redirected)).await;
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
        let output_stream = ReceiverStream::new(alias_redirected_receiver);
        Ok(Response::new(Box::pin(output_stream)))
    }

    type StreamStatusUpdatesStream = StatusUpdateStream;

    async fn stream_status_updates(
        &self,
        _request: tonic::Request<StatusUpdateRequest>,
    ) -> Result<Response<Self::StreamStatusUpdatesStream>, Status> {
        let (status_updates_sender, status_updates_receiver) = tokio::sync::mpsc::channel(1000);
        let mut receiver = self.status_updates_receiver.resubscribe();
        tokio::spawn(async move {
            loop {
                let result = receiver.try_recv();
                match result {
                    Ok(alias_redirected) => {
                        let result = status_updates_sender.send(Ok(alias_redirected)).await;
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
        let output_stream = ReceiverStream::new(status_updates_receiver);
        Ok(Response::new(Box::pin(output_stream)))
    }
}

pub async fn start_internal_pipeline_server(
    app_config: Config,
    receivers: PipelineEventReceivers,
) -> Result<(), tonic::transport::Error> {
    let server = InternalPipelineServer::new(receivers);

    let internal_config = app_config
        .api
        .unwrap_or_default()
        .app_grpc
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
