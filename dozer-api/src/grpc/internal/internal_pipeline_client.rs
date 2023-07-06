use std::{fmt::Debug, ops::Range};

use dozer_cache::dozer_log::replication::LogResponse;
use dozer_types::{
    bincode,
    grpc_types::internal::{
        internal_pipeline_service_client::InternalPipelineServiceClient, LogRequest,
    },
    models::api_config::AppGrpcOptions,
    thiserror,
};
use futures_util::{Stream, StreamExt};
use tokio::sync::mpsc::Receiver;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{transport::Channel, Status};

use crate::errors::GrpcError;

#[derive(Debug)]
pub struct InternalPipelineClient {
    client: InternalPipelineServiceClient<Channel>,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Tonic status: {0}")]
    Tonic(#[from] Status),
    #[error("Deserialization error: {0}")]
    Deserialization(#[from] bincode::Error),
}

impl InternalPipelineClient {
    pub async fn new(app_grpc_config: &AppGrpcOptions) -> Result<Self, GrpcError> {
        let address = format!(
            "http://{:}:{:}",
            &app_grpc_config.host, app_grpc_config.port
        );
        let client = InternalPipelineServiceClient::connect(address).await?;
        Ok(Self { client })
    }

    pub async fn get_log(
        &mut self,
        endpoint: String,
        requests: Receiver<Range<usize>>,
    ) -> Result<impl Stream<Item = Result<LogResponse, Error>>, Error> {
        let request_stream = ReceiverStream::new(requests).map(move |request| LogRequest {
            endpoint: endpoint.clone(),
            start: request.start as u64,
            end: request.end as u64,
        });
        Ok(self
            .client
            .get_log(request_stream)
            .await?
            .into_inner()
            .map(|response| {
                let response = response?;
                let response = bincode::deserialize(&response.data)?;
                Ok(response)
            }))
    }
}
