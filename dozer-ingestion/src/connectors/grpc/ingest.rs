use crate::ingestion::Ingestor;
use arrow::ipc::reader::StreamReader;
use dozer_types::arrow;
use dozer_types::arrow_types::from_arrow::map_record_batch_to_dozer_records;
use dozer_types::bytes::{Buf, Bytes};
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
use tonic::Streaming;

pub struct IngestorServiceImpl {
    ingestor: &'static Ingestor,
    schema_map: &'static HashMap<String, Schema>,
}
impl IngestorServiceImpl {
    pub fn new(schema_map: &HashMap<String, Schema>, ingestor: &'static Ingestor) -> Self {
        let schema_map = Box::leak(Box::new(schema_map.clone()));
        Self {
            ingestor,
            schema_map,
        }
    }

    pub fn insert(
        req: IngestRequest,
        schema_map: &'static HashMap<String, Schema>,
        ingestor: &'static Ingestor,
    ) -> Result<(), tonic::Status> {
        let schema = schema_map.get(&req.schema_name).ok_or_else(|| {
            tonic::Status::invalid_argument(format!("schema not found: {}", req.schema_name))
        })?;
        let seq_no = req.seq_no;
        let records = map_record_batch(req, schema)?;

        for r in records {
            let op = Operation::Insert { new: r };
            ingestor
                .handle_message(IngestionMessage::new_op(0, req.seq_no as u64, op))
                .map_err(|e| tonic::Status::internal(format!("ingestion error: {e}")))?;
        }

        Ok(())
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

fn map_record_batch(req: IngestRequest, schema: &Schema) -> Result<Vec<Record>, tonic::Status> {
    let mut buf = Bytes::from(req.records).reader();
    // read stream back
    let mut reader = StreamReader::try_new(&mut buf, None).unwrap();
    let mut records = Vec::new();
    while let Some(Ok(batch)) = reader.next() {
        let b_recs = map_record_batch_to_dozer_records(batch, schema).map_err(|e| {
            tonic::Status::internal(format!(
                "error converting arrow record batch to dozer records: {:#?}",
                e
            ))
        })?;
        records.extend(b_recs);
    }

    Ok(records)
}
