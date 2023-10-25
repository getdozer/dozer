use dozer_ingestion_connector::{
    async_trait,
    dozer_types::{
        self, chrono,
        grpc_types::{self, ingest::IngestRequest},
        json_types::prost_to_json_value,
        models::ingestion_types::IngestionMessage,
        ordered_float::OrderedFloat,
        rust_decimal::Decimal,
        serde_json,
        types::{Field, Operation, Record, Schema},
    },
    Ingestor, SourceSchema,
};

use crate::Error;

use super::{GrpcIngestMessage, IngestAdapter};

use std::collections::HashMap;

#[derive(Debug)]
pub struct DefaultAdapter {
    schema_map: HashMap<String, SourceSchema>,
}

impl DefaultAdapter {
    fn parse_schemas(schemas_str: &str) -> Result<HashMap<String, SourceSchema>, Error> {
        let schemas: HashMap<String, SourceSchema> = serde_json::from_str(schemas_str)?;

        Ok(schemas)
    }
}

#[async_trait]
impl IngestAdapter for DefaultAdapter {
    fn new(schemas_str: String) -> Result<Self, Error> {
        let schema_map = Self::parse_schemas(&schemas_str)?;
        Ok(Self { schema_map })
    }
    fn get_schemas(&self) -> Vec<(String, SourceSchema)> {
        self.schema_map
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect()
    }

    async fn handle_message(
        &self,
        table_index: usize,
        msg: GrpcIngestMessage,
        ingestor: &'static Ingestor,
    ) -> Result<(), Error> {
        match msg {
            GrpcIngestMessage::Default(msg) => {
                handle_message(table_index, msg, &self.schema_map, ingestor).await
            }
            GrpcIngestMessage::Arrow(_) => Err(Error::CannotHandleArrowMessage),
        }
    }
}

pub async fn handle_message(
    table_index: usize,
    req: IngestRequest,
    schema_map: &HashMap<String, SourceSchema>,
    ingestor: &'static Ingestor,
) -> Result<(), Error> {
    let schema = &schema_map
        .get(&req.schema_name)
        .ok_or_else(|| Error::SchemaNotFound(req.schema_name.clone()))?
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
    // If receiving end is closed, then we can just ignore the message
    let _ = ingestor
        .handle_message(IngestionMessage::OperationEvent {
            table_index,
            op,
            id: None,
        })
        .await;
    Ok(())
}

fn map_record(rec: grpc_types::types::Record, schema: &Schema) -> Result<Record, Error> {
    let mut values: Vec<Field> = vec![];
    let values_count = rec.values.len();
    let schema_fields_count = schema.fields.len();
    if values_count != schema_fields_count {
        return Err(Error::NumFieldsMismatch {
            values_count,
            schema_fields_count,
        });
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
            ) => Ok(dozer_types::types::Field::Decimal(Decimal::from_parts(
                d.lo, d.mid, d.hi, d.negative, d.scale,
            ))),
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
            (a, b) => Err(Error::FieldTypeMismatch {
                index: idx,
                value: a,
                field_type: b,
            }),
        });
        values.push(val.unwrap_or(Ok(dozer_types::types::Field::Null))?);
    }
    Ok(Record {
        values,
        lifetime: None,
    })
}
