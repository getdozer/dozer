use std::sync::Arc;

use dozer_types::{grpc_types::ingest::IngestArrowRequest, log::error};
use futures::StreamExt;
use tonic::Streaming;

use dozer_types::grpc_types::ingest::{
    ingest_service_server::IngestService, IngestRequest, IngestResponse,
};

use crate::ingestion::Ingestor;

use super::adapter::{GrpcIngestMessage, GrpcIngestor, IngestAdapter};

pub struct IngestorServiceImpl<T>
where
    T: IngestAdapter,
{
    adapter: Arc<GrpcIngestor<T>>,
    ingestor: &'static Ingestor,
}
impl<T> IngestorServiceImpl<T>
where
    T: IngestAdapter,
{
    pub fn new(adapter: GrpcIngestor<T>, ingestor: &'static Ingestor) -> Self {
        Self {
            adapter: Arc::new(adapter),
            ingestor,
        }
    }
}
#[tonic::async_trait]
impl<T> IngestService for IngestorServiceImpl<T>
where
    T: IngestAdapter,
{
    async fn ingest(
        &self,
        request: tonic::Request<IngestRequest>,
    ) -> Result<tonic::Response<IngestResponse>, tonic::Status> {
        let req = request.into_inner();
        let seq_no = req.seq_no;
        self.adapter
            .handle_message(GrpcIngestMessage::Default(req), self.ingestor)
            .map_err(|e| tonic::Status::internal(format!("ingestion stream error: {e}")))?;

        Ok(tonic::Response::new(IngestResponse { seq_no }))
    }

    async fn ingest_stream(
        &self,
        req: tonic::Request<Streaming<IngestRequest>>,
    ) -> Result<tonic::Response<IngestResponse>, tonic::Status> {
        let mut in_stream = req.into_inner();

        let adapter = self.adapter.clone();
        let ingestor = self.ingestor;
        let seq_no = tokio::spawn(async move {
            let mut seq_no = 0;
            while let Some(result) = in_stream.next().await {
                if let Ok(req) = result {
                    seq_no = req.seq_no;
                    let res = adapter.handle_message(GrpcIngestMessage::Default(req), ingestor);
                    if let Err(e) = res {
                        error!("ingestion stream insertion errored: {:#?}", e);
                        break;
                    }
                } else {
                    error!("ingestion stream errored: {:#?}", result);
                    break;
                }
            }
            seq_no
        })
        .await
        .map_err(|e| tonic::Status::internal(format!("ingestion stream error: {e}")))?;
        Ok(tonic::Response::new(IngestResponse { seq_no }))
    }

    async fn ingest_arrow(
        &self,
        request: tonic::Request<IngestArrowRequest>,
    ) -> Result<tonic::Response<IngestResponse>, tonic::Status> {
        let req = request.into_inner();
        let seq_no = req.seq_no;
        self.adapter
            .handle_message(GrpcIngestMessage::Arrow(req), self.ingestor)
            .map_err(|e| tonic::Status::internal(format!("ingestion stream error: {e}")))?;

        Ok(tonic::Response::new(IngestResponse { seq_no }))
    }

    async fn ingest_arrow_stream(
        &self,
        req: tonic::Request<Streaming<IngestArrowRequest>>,
    ) -> Result<tonic::Response<IngestResponse>, tonic::Status> {
        let mut in_stream = req.into_inner();

        let adapter = self.adapter.clone();
        let ingestor = self.ingestor;
        let seq_no = tokio::spawn(async move {
            let mut seq_no = 0;
            while let Some(result) = in_stream.next().await {
                if let Ok(req) = result {
                    seq_no = req.seq_no;
                    let res = adapter.handle_message(GrpcIngestMessage::Arrow(req), ingestor);
                    if let Err(e) = res {
                        error!("ingestion stream insertion errored: {:#?}", e);
                        break;
                    }
                } else {
                    error!("ingestion stream errored: {:#?}", result);
                    break;
                }
            }
            seq_no
        })
        .await
        .map_err(|e| tonic::Status::internal(format!("ingestion stream error: {e}")))?;
        Ok(tonic::Response::new(IngestResponse { seq_no }))
    }
}
