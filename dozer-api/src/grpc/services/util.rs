use crate::grpc::util::from_dynamic_message_to_json;
use dozer_cache::{cache::expression::QueryExpression, errors::CacheError};
use dozer_types::{
    errors::types::TypeError,
    serde_json::{self, Value},
    types::Field,
};
use prost_reflect::DynamicMessage;
use tonic::{Code, Status};
pub fn dozer_field_to_json_value(field: &Field) -> Result<Value, TypeError> {
    match field {
        Field::Int(n) => {
            let result = serde_json::to_value(n)
                .map_err(|e| TypeError::FieldValueToJsonValue(e.to_string()))?;
            Ok(result)
        }
        Field::Float(n) => {
            let result = serde_json::to_value(n)
                .map_err(|e| TypeError::FieldValueToJsonValue(e.to_string()))?;
            Ok(result)
        }
        Field::Boolean(b) => {
            let result = serde_json::to_value(b)
                .map_err(|e| TypeError::FieldValueToJsonValue(e.to_string()))?;
            Ok(result)
        }
        Field::String(s) => {
            let result = serde_json::to_value(s)
                .map_err(|e| TypeError::FieldValueToJsonValue(e.to_string()))?;
            Ok(result)
        }
        Field::UInt(n) => {
            let result = serde_json::to_value(n)
                .map_err(|e| TypeError::FieldValueToJsonValue(e.to_string()))?;
            Ok(result)
        }
        Field::Text(n) => {
            let result = serde_json::to_value(n)
                .map_err(|e| TypeError::FieldValueToJsonValue(e.to_string()))?;
            Ok(result)
        }
        Field::Binary(n) => {
            let result = serde_json::to_value(n)
                .map_err(|e| TypeError::FieldValueToJsonValue(e.to_string()))?;
            Ok(result)
        }
        Field::UIntArray(n) => {
            let result = serde_json::to_value(n)
                .map_err(|e| TypeError::FieldValueToJsonValue(e.to_string()))?;
            Ok(result)
        }
        Field::IntArray(n) => {
            let result = serde_json::to_value(n)
                .map_err(|e| TypeError::FieldValueToJsonValue(e.to_string()))?;
            Ok(result)
        }
        Field::FloatArray(n) => {
            let result = serde_json::to_value(n)
                .map_err(|e| TypeError::FieldValueToJsonValue(e.to_string()))?;
            Ok(result)
        }
        Field::BooleanArray(n) => {
            let result = serde_json::to_value(n)
                .map_err(|e| TypeError::FieldValueToJsonValue(e.to_string()))?;
            Ok(result)
        }
        Field::StringArray(n) => {
            let result = serde_json::to_value(n)
                .map_err(|e| TypeError::FieldValueToJsonValue(e.to_string()))?;
            Ok(result)
        }
        Field::Decimal(n) => {
            let result = serde_json::to_value(n)
                .map_err(|e| TypeError::FieldValueToJsonValue(e.to_string()))?;
            Ok(result)
        }
        Field::Timestamp(n) => {
            let result = serde_json::to_value(n)
                .map_err(|e| TypeError::FieldValueToJsonValue(e.to_string()))?;
            Ok(result)
        }
        Field::Bson(n) => {
            let result = serde_json::to_value(n)
                .map_err(|e| TypeError::FieldValueToJsonValue(e.to_string()))?;
            Ok(result)
        }
        Field::Null => Ok(Value::Null),
    }
}

pub fn from_cache_error(error: CacheError) -> Status {
    Status::new(Code::Internal, error.to_string())
}

pub fn convert_grpc_message_to_query_exp(input: DynamicMessage) -> Result<QueryExpression, Status> {
    let json_present = from_dynamic_message_to_json(input)?;
    let mut string_present = json_present.to_string();
    let key_replace = vec![
        "filter", "and", "limit", "skip", "order_by", "eq", "lt", "lte", "gt", "gte",
    ];
    key_replace.iter().for_each(|&key| {
        let from = format!("\"{}\"", key);
        let to = format!("\"${}\"", key);
        string_present = string_present.replace(&from, &to);
    });
    let query_expression: QueryExpression = serde_json::from_str(&string_present)
        .map_err(|err| Status::new(Code::Internal, err.to_string()))?;
    Ok(query_expression)
}
