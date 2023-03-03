use dozer_types::{
    chrono,
    ingestion_types::IngestionMessage,
    log::error,
    ordered_float::OrderedFloat,
    types::{Operation, Record, Schema},
};
use futures::StreamExt;
use std::collections::HashMap;
use tonic::Streaming;

use crate::ingestion::Ingestor;

use dozer_types::grpc_types;
use dozer_types::grpc_types::ingest::{
    ingest_service_server::IngestService, IngestRequest, IngestResponse,
};

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
            grpc_types::types::OperationType::Insert => Operation::Insert {
                new: map_record(req.new.unwrap(), schema)?,
            },
            grpc_types::types::OperationType::Delete => Operation::Delete {
                old: map_record(req.old.unwrap(), schema)?,
            },
            grpc_types::types::OperationType::Update => Operation::Update {
                old: map_record(req.old.unwrap(), schema)?,
                new: map_record(req.new.unwrap(), schema)?,
            },
        };
        ingestor
            .handle_message(IngestionMessage::new_op(0, req.seq_no as u64, vec![op]))
            .map_err(|e| tonic::Status::internal(format!("ingestion error: {e}")))?;
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

fn map_record(rec: grpc_types::types::Record, schema: &Schema) -> Result<Record, tonic::Status> {
    let mut values = vec![];
    let values_count = rec.values.len();
    let schema_fields_count = schema.fields.len();
    if values_count != schema_fields_count {
        return Err(tonic::Status::invalid_argument(
            format!("record is not properly formed. Length of values {values_count} does not match schema: {schema_fields_count} "),
        ));
    }

    for (idx, v) in rec.values.iter().enumerate() {
        let typ = schema.fields[idx].typ;

        let v = v.value.as_ref().map(|v| match (v, typ) {
            (
                grpc_types::types::value::Value::UintValue(a),
                dozer_types::types::FieldType::UInt,
            ) => Ok(dozer_types::types::Field::UInt(*a)),

            (grpc_types::types::value::Value::IntValue(a), dozer_types::types::FieldType::Int) => {
                Ok(dozer_types::types::Field::Int(*a))
            }

            (
                grpc_types::types::value::Value::FloatValue(a),
                dozer_types::types::FieldType::Float,
            ) => Ok(dozer_types::types::Field::Float(OrderedFloat(*a))),

            (
                grpc_types::types::value::Value::BoolValue(a),
                dozer_types::types::FieldType::Boolean,
            ) => Ok(dozer_types::types::Field::Boolean(*a)),

            (
                grpc_types::types::value::Value::StringValue(a),
                dozer_types::types::FieldType::String,
            ) => Ok(dozer_types::types::Field::String(a.clone())),

            (
                grpc_types::types::value::Value::BytesValue(a),
                dozer_types::types::FieldType::Binary,
            ) => Ok(dozer_types::types::Field::Binary(a.clone())),
            (
                grpc_types::types::value::Value::StringValue(a),
                dozer_types::types::FieldType::Text,
            ) => Ok(dozer_types::types::Field::Text(a.clone())),
            (
                grpc_types::types::value::Value::BytesValue(a),
                dozer_types::types::FieldType::Bson,
            ) => Ok(dozer_types::types::Field::Bson(a.clone())),
            (
                grpc_types::types::value::Value::TimestampValue(a),
                dozer_types::types::FieldType::Timestamp,
            ) => Ok(
                chrono::NaiveDateTime::from_timestamp_opt(a.seconds, a.nanos as u32)
                    .map(|t| {
                        dozer_types::types::Field::Timestamp(
                            chrono::DateTime::<chrono::Utc>::from_utc(t, chrono::Utc).into(),
                        )
                    })
                    .unwrap_or(dozer_types::types::Field::Null),
            ),
            (
                grpc_types::types::value::Value::DecimalValue(_),
                dozer_types::types::FieldType::Decimal,
            )
            | (
                grpc_types::types::value::Value::DateValue(_),
                dozer_types::types::FieldType::UInt,
            )
            | (
                grpc_types::types::value::Value::DateValue(_),
                dozer_types::types::FieldType::Date,
            )
            | (
                grpc_types::types::value::Value::PointValue(_),
                dozer_types::types::FieldType::Point,
            ) => Ok(dozer_types::types::Field::Null),
            (a, b) => {
                return Err(tonic::Status::invalid_argument(format!(
                    "data is not valid at index: {idx}, Type: {a:?}, Expected Type: {b}"
                )))
            }
        });
        values.push(v.unwrap_or(Ok(dozer_types::types::Field::Null))?);
    }
    Ok(Record {
        schema_id: schema.identifier,
        values,
        version: None,
    })
}
