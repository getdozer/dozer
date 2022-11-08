use std::collections::HashMap;

use crate::grpc::services::common::common_grpc::common_grpc_service_server::CommonGrpcService;
use crate::grpc::services::common::common_grpc::{value, ArrayValue, Record, Type, Value};
use crate::{api_helper, api_server::PipelineDetails};
use dozer_cache::cache::expression::QueryExpression;
use dozer_types::chrono::SecondsFormat;
use dozer_types::types::{Field, FieldType};
use tonic::transport::NamedService;
use tonic::{Request, Response, Status};

use super::common_grpc::{FieldDefinition, QueryRequest, QueryResponse};

#[derive(Clone)]
pub struct ApiService {
    pub pipeline_map: HashMap<String, PipelineDetails>,
}

impl NamedService for ApiService {
    const NAME: &'static str = "CommonService";
}
#[tonic::async_trait]
impl CommonGrpcService for ApiService {
    async fn query(
        &self,
        request: Request<QueryRequest>,
    ) -> Result<Response<QueryResponse>, Status> {
        let request = request.into_inner();
        let endpoint = request.endpoint;
        let pipeline_details = self
            .pipeline_map
            .get(&endpoint)
            .map_or(Err(Status::invalid_argument(&endpoint)), Ok)?;

        let api_helper = api_helper::ApiHelper::new(pipeline_details.to_owned(), None)?;

        let query: QueryExpression =
            dozer_types::serde_json::from_str(&request.query.map_or("{}".to_string(), |f| f))
                .map_err(|e| Status::from_error(Box::new(e)))?;

        let (schema, records) = api_helper
            .get_records(query)
            .map_err(|e| Status::from_error(Box::new(e)))?;

        let fields = schema
            .fields
            .iter()
            .map(|f| FieldDefinition {
                typ: match f.typ {
                    FieldType::UInt => Type::UInt,
                    FieldType::Int => Type::Int,
                    FieldType::Float => Type::Float,
                    FieldType::Boolean => Type::Boolean,
                    FieldType::String => Type::String,
                    FieldType::Text => Type::Text,
                    FieldType::Binary => Type::Binary,
                    FieldType::UIntArray => Type::UIntArray,
                    FieldType::IntArray => Type::IntArray,
                    FieldType::FloatArray => Type::FloatArray,
                    FieldType::BooleanArray => Type::BooleanArray,
                    FieldType::StringArray => Type::StringArray,
                    FieldType::Decimal => Type::Decimal,
                    FieldType::Timestamp => Type::Timestamp,
                    FieldType::Bson => Type::Bson,
                    FieldType::Null => Type::Null,
                } as i32,
                name: f.name.to_owned(),
                nullable: f.nullable,
            })
            .collect();
        let records: Vec<Record> = records
            .iter()
            .map(|r| {
                let values: Vec<Value> = r
                    .to_owned()
                    .values
                    .iter()
                    .map(|v| {
                        let val = field_to_prost_value(v);
                        val
                    })
                    .collect();

                Record { values }
            })
            .collect();
        let reply = QueryResponse { fields, records };

        Ok(Response::new(reply)) // Send back our formatted greeting
    }
}

fn field_to_prost_value(f: &Field) -> Value {
    match f {
        Field::UInt(n) => Value {
            value: Some(value::Value::UintValue(*n)),
        },
        Field::Int(n) => Value {
            value: Some(value::Value::IntValue(*n)),
        },
        Field::Float(n) => Value {
            value: Some(value::Value::FloatValue(*n as f32)),
        },

        Field::Boolean(n) => Value {
            value: Some(value::Value::BoolValue(*n)),
        },

        Field::String(s) => Value {
            value: Some(value::Value::StringValue(s.to_owned())),
        },
        Field::Text(s) => Value {
            value: Some(value::Value::StringValue(s.to_owned())),
        },
        Field::Binary(b) => Value {
            value: Some(value::Value::BytesValue(b.to_owned())),
        },
        Field::UIntArray(arr) => Value {
            value: Some(value::Value::ArrayValue(ArrayValue {
                array_value: arr
                    .iter()
                    .map(|v| Value {
                        value: Some(value::Value::UintValue(*v)),
                    })
                    .collect(),
            })),
        },
        Field::IntArray(arr) => Value {
            value: Some(value::Value::ArrayValue(ArrayValue {
                array_value: arr
                    .iter()
                    .map(|v| Value {
                        value: Some(value::Value::IntValue(*v)),
                    })
                    .collect(),
            })),
        },
        Field::FloatArray(arr) => Value {
            value: Some(value::Value::ArrayValue(ArrayValue {
                array_value: arr
                    .iter()
                    .map(|v| Value {
                        value: Some(value::Value::FloatValue(*v as f32)),
                    })
                    .collect(),
            })),
        },
        Field::BooleanArray(arr) => Value {
            value: Some(value::Value::ArrayValue(ArrayValue {
                array_value: arr
                    .iter()
                    .map(|v| Value {
                        value: Some(value::Value::BoolValue(*v)),
                    })
                    .collect(),
            })),
        },
        Field::StringArray(arr) => Value {
            value: Some(value::Value::ArrayValue(ArrayValue {
                array_value: arr
                    .iter()
                    .map(|v| Value {
                        value: Some(value::Value::StringValue(v.to_owned())),
                    })
                    .collect(),
            })),
        },
        Field::Decimal(n) => Value {
            value: Some(value::Value::StringValue((*n).to_string())),
        },
        Field::Timestamp(ts) => Value {
            value: Some(value::Value::StringValue(
                ts.to_rfc3339_opts(SecondsFormat::Millis, true),
            )),
        },

        Field::Bson(b) => Value {
            value: Some(value::Value::BytesValue(b.to_owned())),
        },
        Field::Null => Value { value: None },
    }
}
