use crossbeam::channel::{Receiver, Sender};
use dozer_types::grpc_types::internal::{
    internal_pipeline_service_server::{self, InternalPipelineService},
    AliasEventsRequest, AliasRedirected, PipelineRequest, PipelineResponse,
};
use dozer_types::{crossbeam, log::info, models::app_config::Config, tracing::warn};
use std::{fmt::Debug, net::ToSocketAddrs, pin::Pin, thread};
use tokio::{runtime::Runtime, sync::broadcast};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{codegen::futures_core::Stream, transport::Server, Response, Status};

pub type PipelineEventSenders = (Sender<AliasRedirected>, Sender<PipelineResponse>);
pub type PipelineEventReceivers = (Receiver<AliasRedirected>, Receiver<PipelineResponse>);

pub struct InternalPipelineServer {
    alias_redirected_receiver: broadcast::Receiver<AliasRedirected>,
    pipeline_response_receiver: broadcast::Receiver<PipelineResponse>,
}
impl InternalPipelineServer {
    pub fn new(pipeline_event_receivers: PipelineEventReceivers) -> Self {
        let alias_redirected_receiver =
            crossbeam_mpsc_receiver_to_tokio_broadcast_receiver(pipeline_event_receivers.0);
        let pipeline_response_receiver =
            crossbeam_mpsc_receiver_to_tokio_broadcast_receiver(pipeline_event_receivers.1);
        Self {
            alias_redirected_receiver,
            pipeline_response_receiver,
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
                warn!(
                    "Internal Pipeline server - message reveived error: {:?}",
                    err
                );
                break;
            }
        }
    }).expect("Failed to spawn crossbeam_mpsc_receiver_to_tokio_broadcast_receiver thread");
    broadcast_receiver
}

type ResponseStream = Pin<Box<dyn Stream<Item = Result<PipelineResponse, Status>> + Send>>;
type AliasEventsStream = Pin<Box<dyn Stream<Item = Result<AliasRedirected, Status>> + Send>>;

#[tonic::async_trait]
impl InternalPipelineService for InternalPipelineServer {
    type StreamPipelineRequestStream = ResponseStream;
    async fn stream_pipeline_request(
        &self,
        _request: tonic::Request<PipelineRequest>,
    ) -> Result<Response<ResponseStream>, Status> {
        let (pipeline_response_sender, pipeline_response_receiver) =
            tokio::sync::mpsc::channel(1000);
        let mut receiver = self.pipeline_response_receiver.resubscribe();
        tokio::spawn(async move {
            loop {
                let result = receiver.try_recv();
                match result {
                    Ok(pipeline_response) => {
                        let result = pipeline_response_sender.send(Ok(pipeline_response)).await;
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
        let output_stream = ReceiverStream::new(pipeline_response_receiver);
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
}

pub fn start_internal_pipeline_server(
    app_config: Config,
    receivers: PipelineEventReceivers,
) -> Result<(), tonic::transport::Error> {
    let rt = Runtime::new().unwrap();
    rt.block_on(async { _start_internal_pipeline_server(app_config, receivers).await })
}
async fn _start_internal_pipeline_server(
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
