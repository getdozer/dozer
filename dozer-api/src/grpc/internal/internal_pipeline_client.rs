use std::fmt::Debug;

use dozer_types::{
    grpc_types::internal::{
        internal_pipeline_service_client::InternalPipelineServiceClient, AliasEventsRequest,
        AliasRedirected, PipelineRequest, PipelineResponse,
    },
    log::warn,
    models::api_config::GrpcApiOptions,
};
use futures_util::{Future, StreamExt};
use tokio::sync::broadcast::{Receiver, Sender};
use tonic::{transport::Channel, Streaming};

use crate::errors::GrpcError;

#[derive(Debug)]
pub struct InternalPipelineClient {
    client: InternalPipelineServiceClient<Channel>,
}

impl InternalPipelineClient {
    pub async fn new(app_grpc_config: &GrpcApiOptions) -> Result<Self, GrpcError> {
        let address = format!(
            "http://{:}:{:}",
            &app_grpc_config.host, app_grpc_config.port
        );
        let client = InternalPipelineServiceClient::connect(address)
            .await
            .map_err(|err| GrpcError::InternalError(Box::new(err)))?;
        Ok(Self { client })
    }

    pub async fn stream_alias_events(
        &mut self,
    ) -> Result<
        (
            Receiver<AliasRedirected>,
            impl Future<Output = Result<(), GrpcError>>,
        ),
        GrpcError,
    > {
        let stream = self
            .client
            .stream_alias_events(AliasEventsRequest {})
            .await
            .map_err(|err| GrpcError::InternalError(Box::new(err)))?
            .into_inner();
        let (sender, receiver) = tokio::sync::broadcast::channel(16);
        let future = redirect_loop(stream, sender);
        Ok((receiver, future))
    }

    pub async fn stream_pipeline_responses(
        &mut self,
    ) -> Result<
        (
            Receiver<PipelineResponse>,
            impl Future<Output = Result<(), GrpcError>>,
        ),
        GrpcError,
    > {
        let stream = self
            .client
            .stream_pipeline_request(PipelineRequest {})
            .await
            .map_err(|err| GrpcError::InternalError(Box::new(err)))?
            .into_inner();
        let (sender, receiver) = tokio::sync::broadcast::channel(16);
        let future = redirect_loop(stream, sender);
        Ok((receiver, future))
    }
}

async fn redirect_loop<T: Debug>(
    mut stream: Streaming<T>,
    sender: Sender<T>,
) -> Result<(), GrpcError> {
    while let Some(event) = stream.next().await {
        let event = event.map_err(|err| GrpcError::InternalError(Box::new(err)))?;
        sender
            .send(event)
            .map_err(|_| GrpcError::CannotSendToBroadcastChannel)?;
    }
    warn!("exiting internal grpc connection on api thread");
    Ok(())
}
