use crate::ingestion::Ingestor;

use dozer_types::arrow;

use dozer_types::grpc_types::ingest::{
    ingest_service_server::IngestService, IngestRequest, IngestResponse,
};
use dozer_types::{
    ingestion_types::IngestionMessage,
    log::error,
    types::{Operation, Record, Schema},
};
use futures::StreamExt;
use std::collections::HashMap;
use std::sync::Arc;
use tonic::Streaming;

use super::adapter::GrpcIngestAdapter;

pub struct IngestorServiceImpl<T> {
    ingestor: &'static Ingestor,
    adapter: Arc<Box<dyn GrpcIngestAdapter<T>>>,
    schema_map: &'static HashMap<String, Schema>,
}
impl<T> IngestorServiceImpl<T> {
    pub fn new(
        schema_map: &HashMap<String, Schema>,
        ingestor: &'static Ingestor,
        adapter: Arc<Box<dyn GrpcIngestAdapter<T>>>,
    ) -> Self {
        let schema_map = Box::leak(Box::new(schema_map.clone()));
        Self {
            ingestor,
            schema_map,
            adapter,
        }
    }
}

#[tonic::async_trait]
impl IngestService for IngestorServiceImpl {
    async fn ingest(
        &self,
        request: tonic::Request<IngestRequest>,
    ) -> Result<tonic::Response<IngestResponse>, tonic::Status> {
        let req = request.into_inner();
        let seq_no = req.seq_no;

        adap
        Self::insert(req, self.schema_map, self.ingestor)?;
        Ok(tonic::Response::new(IngestResponse { seq_no }))
    }

    async fn ingest_stream(
        &self,
        req: tonic::Request<Streaming<IngestRequest>>,
    ) -> Result<tonic::Response<IngestResponse>, tonic::Status> {
        let mut in_stream = req.into_inner();

        let ingestor = self.ingestor;
        let schema_map = self.schema_map;

        let seq_no = tokio::spawn(async move {
            let mut seq_no = 0;
            while let Some(result) = in_stream.next().await {
                if let Ok(req) = result {
                    seq_no = req.seq_no;
                    let res = Self::insert(req, schema_map, ingestor);
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
