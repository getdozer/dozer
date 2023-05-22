use dozer_types::{serde_json, types::SchemaIdentifier};

use crate::{connectors::SourceSchema, errors::ConnectorError};

use super::{GrpcIngestMessage, IngestAdapter};
use dozer_types::{
    chrono,
    ingestion_types::IngestionMessage,
    ordered_float::OrderedFloat,
    types::{Field, Operation, Record, Schema},
};

use std::collections::HashMap;

use crate::ingestion::Ingestor;

use dozer_types::grpc_types;
use dozer_types::grpc_types::ingest::IngestRequest;
use dozer_types::json_types::prost_to_json_value;
use dozer_types::rust_decimal::Decimal;

#[derive(Debug)]
pub struct DefaultAdapter {
    schema_map: HashMap<String, SourceSchema>,
}

impl DefaultAdapter {
    fn parse_schemas(schemas_str: &str) -> Result<HashMap<String, SourceSchema>, ConnectorError> {
        let mut schemas: HashMap<String, SourceSchema> =
            serde_json::from_str(schemas_str).map_err(ConnectorError::map_serialization_error)?;

        schemas
            .iter_mut()
            .enumerate()
            .for_each(|(id, (_, schema))| {
                schema.schema.identifier = Some(SchemaIdentifier {
                    id: id as u32,
                    version: 1,
                });
            });

        Ok(schemas)
    }
}
impl IngestAdapter for DefaultAdapter {
    fn new(schemas_str: String) -> Result<Self, ConnectorError> {
        let schema_map = Self::parse_schemas(&schemas_str)?;
        Ok(Self { schema_map })
    }
    fn get_schemas(&self) -> Vec<(String, SourceSchema)> {
        self.schema_map
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect()
    }
    fn handle_message(
        &self,
        msg: GrpcIngestMessage,
        ingestor: &'static Ingestor,
    ) -> Result<(), ConnectorError> {
        match msg {
            GrpcIngestMessage::Default(msg) => handle_message(msg, &self.schema_map, ingestor),
            GrpcIngestMessage::Arrow(_) => Err(ConnectorError::InitializationError(
                "Wrong message format!".to_string(),
            )),
        }
    }
}

pub fn handle_message(
    req: IngestRequest,
    schema_map: &HashMap<String, SourceSchema>,
    ingestor: &'static Ingestor,
) -> Result<(), ConnectorError> {
    let schema = &schema_map
        .get(&req.schema_name)
        .ok_or_else(|| {
            ConnectorError::InitializationError(format!("schema not found: {}", req.schema_name))
        })?
        .schema;

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
        .handle_message(IngestionMessage::new_op(0, req.seq_no as u64, op))
        .map_err(ConnectorError::IngestorError)
}

fn map_record(rec: grpc_types::types::Record, schema: &Schema) -> Result<Record, ConnectorError> {
    let mut values: Vec<Field> = vec![];
    let values_count = rec.values.len();
    let schema_fields_count = schema.fields.len();
    if values_count != schema_fields_count {
        return Err(ConnectorError::InitializationError(
            format!("record is not properly formed. Length of values {values_count} does not match schema: {schema_fields_count} "),
        ));
    }

    for (idx, v) in rec.values.into_iter().enumerate() {
        let typ = schema.fields[idx].typ;

        let val = v.value.map(|value| match (value, typ) {
            (
                grpc_types::types::value::Value::UintValue(a),
                dozer_types::types::FieldType::UInt,
            ) => Ok(dozer_types::types::Field::UInt(a)),

            (grpc_types::types::value::Value::IntValue(a), dozer_types::types::FieldType::Int) => {
                Ok(dozer_types::types::Field::Int(a))
            }

            (
                grpc_types::types::value::Value::FloatValue(a),
                dozer_types::types::FieldType::Float,
            ) => Ok(dozer_types::types::Field::Float(OrderedFloat(a))),

            (
                grpc_types::types::value::Value::BoolValue(a),
                dozer_types::types::FieldType::Boolean,
            ) => Ok(dozer_types::types::Field::Boolean(a)),

            (
                grpc_types::types::value::Value::StringValue(a),
                dozer_types::types::FieldType::String,
            ) => Ok(dozer_types::types::Field::String(a)),

            (
                grpc_types::types::value::Value::BytesValue(a),
                dozer_types::types::FieldType::Binary,
            ) => Ok(dozer_types::types::Field::Binary(a)),
            (
                grpc_types::types::value::Value::StringValue(a),
                dozer_types::types::FieldType::Text,
            ) => Ok(dozer_types::types::Field::Text(a)),
            (
                grpc_types::types::value::Value::JsonValue(a),
                dozer_types::types::FieldType::Json,
            ) => Ok(dozer_types::types::Field::Json(prost_to_json_value(a))),
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
                grpc_types::types::value::Value::DecimalValue(d),
                dozer_types::types::FieldType::Decimal,
            ) => Ok(dozer_types::types::Field::Decimal(Decimal::from_parts(d.lo, d.mid, d.hi, d.negative, d.scale))),
            (
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
            (a, b) => Err(ConnectorError::InitializationError(format!(
                "data is not valid at index: {idx}, Type: {a:?}, Expected Type: {b}"
            ))),
        });
        values.push(val.unwrap_or(Ok(dozer_types::types::Field::Null))?);
    }
    Ok(Record {
        schema_id: schema.identifier,
        values,
        lifetime: None,
    })
}
