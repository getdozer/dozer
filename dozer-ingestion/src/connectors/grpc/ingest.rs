use dozer_types::{
    ingestion_types::IngestionMessage,
    ordered_float::OrderedFloat,
    types::{Operation, Record, Schema},
};
use futures::StreamExt;
use std::collections::HashMap;
use tonic::Streaming;

use crate::ingestion::Ingestor;

use super::ingest_grpc::{ingest_service_server::IngestService, IngestRequest, IngestResponse};

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
    ) -> Result<tonic::Response<IngestResponse>, tonic::Status> {
        let schema = schema_map.get(&req.schema_name).ok_or_else(|| {
            tonic::Status::invalid_argument(format!("schema not found: {}", req.schema_name))
        })?;

        let op = match req.typ() {
            super::types::OperationType::Insert => Operation::Insert {
                new: map_record(req.new.unwrap(), &schema),
            },
            super::types::OperationType::Delete => Operation::Delete {
                old: map_record(req.old.unwrap(), &schema),
            },
            super::types::OperationType::Update => Operation::Update {
                old: map_record(req.old.unwrap(), &schema),
                new: map_record(req.new.unwrap(), &schema),
            },
        };
        ingestor
            .handle_message(((0, req.seq_no as u64), IngestionMessage::OperationEvent(op)))
            .map_err(|e| tonic::Status::internal(format!("ingestion error: {}", e)))?;
        Ok(tonic::Response::new(IngestResponse { seq_no: req.seq_no }))
    }
}
#[tonic::async_trait]
impl IngestService for IngestorServiceImpl {
    async fn ingest(
        &self,
        request: tonic::Request<IngestRequest>,
    ) -> Result<tonic::Response<IngestResponse>, tonic::Status> {
        let req = request.into_inner();
        Self::insert(req, self.schema_map, self.ingestor)
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
                let req = result.unwrap();
                seq_no = req.seq_no;
                Self::insert(req, schema_map, ingestor).unwrap();
            }
            seq_no
        })
        .await
        .map_err(|e| tonic::Status::internal(format!("ingestion stream error: {}", e)))?;
        Ok(tonic::Response::new(IngestResponse { seq_no }))
    }
}

fn map_record(rec: super::types::Record, schema: &Schema) -> Record {
    let mut values = vec![];
    debug_assert!(
        rec.values.len() == schema.fields.len(),
        "record is not properly formed"
    );
    for (_idx, v) in rec.values.iter().enumerate() {
        let v = v.value.as_ref().map(|v| match v {
            super::types::value::Value::UintValue(a) => dozer_types::types::Field::UInt(*a),
            super::types::value::Value::IntValue(a) => dozer_types::types::Field::Int(*a),
            super::types::value::Value::FloatValue(a) => {
                dozer_types::types::Field::Float(OrderedFloat(*a as f64))
            }
            super::types::value::Value::BoolValue(a) => dozer_types::types::Field::Boolean(*a),
            super::types::value::Value::StringValue(a) => {
                dozer_types::types::Field::String(a.clone())
            }
            super::types::value::Value::BytesValue(a) => {
                dozer_types::types::Field::Binary(a.clone())
            }
            super::types::value::Value::ArrayValue(_) => todo!(),
            super::types::value::Value::DoubleValue(a) => {
                dozer_types::types::Field::Float(OrderedFloat(*a as f64))
            }
        });
        values.push(v.unwrap_or(dozer_types::types::Field::Null));
    }
    Record {
        schema_id: schema.identifier.clone(),
        values,
        version: None,
    }
}
