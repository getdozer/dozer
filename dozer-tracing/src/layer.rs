use std::collections::HashMap;
use std::thread;

use dozer_types::arrow_types::from_arrow::serialize_record_batch;
use dozer_types::arrow_types::to_arrow::map_record_to_arrow;
use dozer_types::crossbeam::channel::{unbounded, Sender};
use dozer_types::grpc_types::ingest::ingest_service_client::IngestServiceClient;
use dozer_types::grpc_types::ingest::IngestArrowRequest;
use dozer_types::models::telemetry::DozerTelemetryConfig;
use dozer_types::tonic::transport::Channel;
use dozer_types::tracing::field::ValueSet;
use dozer_types::tracing::{span, Metadata, Subscriber};
use dozer_types::types::Field;
use dozer_types::types::SchemaIdentifier;
use dozer_types::{log, tonic};
use tokio::runtime::Runtime;
use tracing_subscriber::{registry::LookupSpan, Layer};

use crate::helper;
pub struct DozerLayer {
    tx: Sender<dozer_types::types::Record>,
}

impl DozerLayer {
    pub fn new(config: DozerTelemetryConfig) -> Self {
        // Initialize a channel to send events in the background
        let (tx, rx) = unbounded();

        let schemas = helper::get_schemas();
        let endpoint = config.endpoint.clone();
        let mut seq_no = 0;
        std::thread::spawn(move || {
            Runtime::new().unwrap().block_on(async {
                let mut client = get_grpc_client(endpoint).await.unwrap();
                loop {
                    let record: dozer_types::types::Record = rx.recv().unwrap();

                    let (schema_name, schema) = schemas.get(&record.schema_id.unwrap().id).unwrap();
                    let rec_batch = map_record_to_arrow(record, schema.clone()).unwrap();
                    let request = IngestArrowRequest {
                        schema_name: schema_name.clone(),
                        records: serialize_record_batch(&rec_batch),
                        seq_no,
                        metadata: HashMap::new(),
                    };

                    let response = client.ingest_arrow(request).await;
                    if let Err(r) = response {
                        log::warn!("Failed to ingest record: {}", r);
                    }
                    seq_no += 1;
                }
            });
        });

        Self { tx }
    }
}

impl<S> Layer<S> for DozerLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(
        &self,
        attrs: &span::Attributes<'_>,
        id: &span::Id,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        let values = ValueMapper::new()
            .add_span(id)
            .add_parent(attrs.parent())
            .add_metadata(attrs.metadata())
            .add_values(attrs.values())
            .get();

        let record = dozer_types::types::Record {
            schema_id: Some(SchemaIdentifier { id: 1, version: 1 }),
            values,
            version: None,
        };
        if let Err(e) = self.tx.send(record) {
            log::warn!("Failed to send trace record: {}", e);
        }
    }

    fn on_event(
        &self,
        event: &dozer_types::tracing::Event<'_>,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        let values = ValueMapper::new()
            .add_parent(event.parent())
            .add_metadata(event.metadata())
            .get();
        let record = dozer_types::types::Record {
            schema_id: Some(SchemaIdentifier { id: 1, version: 1 }),
            values,
            version: None,
        };
        if let Err(e) = self.tx.send(record) {
            log::warn!("Failed to send trace record: {}", e);
        }
    }
}

struct ValueMapper(Vec<Field>);
impl ValueMapper {
    fn new() -> Self {
        Self(Vec::new())
    }
    fn add_span(mut self, span: &span::Id) -> Self {
        self.0.push(Field::UInt(span.into_u64()));
        self
    }
    fn add_parent(mut self, parent: Option<&span::Id>) -> Self {
        self.0.push(match parent {
            Some(id) => Field::UInt(id.into_u64()),
            None => Field::Null,
        });
        self
    }

    fn add_values(self, _values: &ValueSet) -> Self {
        self
    }

    fn add_metadata(mut self, metadata: &Metadata) -> Self {
        self.0.extend([
            Field::String(metadata.name().to_string()),
            Field::String(metadata.target().to_string()),
            Field::String(metadata.level().to_string()),
            metadata
                .module_path()
                .map_or(Field::Null, |m| Field::String(m.to_string())),
            metadata
                .file()
                .map_or(Field::Null, |m| Field::String(m.to_string())),
            metadata
                .line()
                .map_or(Field::Null, |m| Field::String(m.to_string())),
        ]);
        self
    }
    fn get(self) -> Vec<Field> {
        self.0
    }
}

async fn get_grpc_client(
    endpoint: String,
) -> Result<IngestServiceClient<Channel>, tonic::transport::Error> {
    let retries = 10;
    let mut res = IngestServiceClient::connect(endpoint.clone()).await;
    for r in 0..retries {
        if res.is_ok() {
            break;
        }
        if r == retries - 1 {
            panic!("failed to connect after {r} times");
        }
        thread::sleep(std::time::Duration::from_millis(300));
        res = IngestServiceClient::connect(endpoint.clone()).await;
    }
    res
}
