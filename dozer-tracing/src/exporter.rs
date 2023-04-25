use dozer_types::grpc_types::ingest::ingest_service_client::IngestServiceClient;
use dozer_types::log::debug;
use dozer_types::tonic::transport::Channel;
use dozer_types::types::{Record, Schema};
use opentelemetry::sdk::export::trace::SpanExporter;

use std::collections::HashMap;
use std::sync::atomic::AtomicU32;
use std::sync::Arc;
use std::thread;

use crate::helper::{self, events_schema, spans_schema};
use dozer_types::arrow_types::from_arrow::serialize_record_batch;
use dozer_types::arrow_types::to_arrow::map_record_to_arrow;
use dozer_types::grpc_types::ingest::IngestArrowRequest;
use dozer_types::models::telemetry::DozerTelemetryConfig;
use dozer_types::tonic;
use opentelemetry::sdk::export::trace::SpanData;
use std::sync::atomic::Ordering;

#[derive(Debug)]
pub struct DozerExporter {
    config: DozerTelemetryConfig,
    seq_no: Arc<AtomicU32>,
}
impl DozerExporter {
    pub fn new(config: DozerTelemetryConfig) -> Self {
        Self {
            config,
            seq_no: Arc::new(AtomicU32::new(1)),
        }
    }
}

// build_batch<R>(mut self, runtime: R) -> Result<TracerProvider, TraceError>

impl SpanExporter for DozerExporter {
    fn export(
        &mut self,
        batch: Vec<SpanData>,
    ) -> dozer_types::tonic::codegen::futures_core::future::BoxFuture<
        'static,
        opentelemetry::sdk::export::trace::ExportResult,
    > {
        let endpoint = self.config.endpoint.clone();
        let seq_no = self.seq_no.clone();
        Box::pin(async move {
            let res = ingest_span(batch, endpoint, seq_no).await;
            if let Err(e) = res {
                debug!("Failed to send traces: {}", e);
            }

            Ok(())
        })
    }
}

async fn get_grpc_client(endpoint: String) -> Result<IngestServiceClient<Channel>, tonic::Status> {
    let retries = 10;
    let mut res = IngestServiceClient::connect(endpoint.clone()).await;
    for r in 0..retries {
        if res.is_ok() {
            break;
        }
        if r == retries - 1 {
            return Err(tonic::Status::internal(
                "Failed to connect to ingest service".to_string(),
            ));
        }
        thread::sleep(std::time::Duration::from_millis(300));
        res = IngestServiceClient::connect(endpoint.clone()).await;
    }
    res.map_err(|e| tonic::Status::internal(e.to_string()))
}

async fn ingest_span(
    batch: Vec<SpanData>,
    endpoint: String,
    seq_no: Arc<AtomicU32>,
) -> Result<Arc<AtomicU32>, tonic::Status> {
    let mut client = get_grpc_client(endpoint)
        .await
        .map_err(|e| tonic::Status::internal(e.to_string()))?;

    let span_schema = spans_schema();
    let event_schema = events_schema();
    for span_data in batch {
        let (span, events) = helper::map_span_data(span_data);
        ingest_record("spans", &mut client, span, &span_schema, seq_no.clone()).await?;
        for evt in events {
            ingest_record("events", &mut client, evt, &event_schema, seq_no.clone()).await?;
        }
    }
    Ok(seq_no)
}
async fn ingest_record(
    schema_name: &str,
    client: &mut IngestServiceClient<Channel>,
    record: Record,
    schema: &Schema,
    seq_no: Arc<AtomicU32>,
) -> Result<(), tonic::Status> {
    let rec_batch =
        map_record_to_arrow(record, schema).map_err(|e| tonic::Status::internal(e.to_string()))?;
    let request = IngestArrowRequest {
        schema_name: schema_name.to_string(),
        records: serialize_record_batch(&rec_batch),
        seq_no: seq_no.fetch_add(1, Ordering::SeqCst),
        metadata: HashMap::new(),
    };

    client.ingest_arrow(request).await?;
    Ok(())
}
