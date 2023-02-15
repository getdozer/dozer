use crate::errors::types::{DeserializationError, TypeError};
use crate::types::DATE_FORMAT;
use crate::types::{Field, FieldType};
use chrono::{DateTime, NaiveDate};
use rust_decimal::Decimal;
use serde_json::Value;
use std::str::FromStr;

/// Used in REST APIs and query expressions for converting JSON value to `Field`
pub fn json_value_to_field(
    value: Value,
    typ: FieldType,
    nullable: bool,
) -> Result<Field, TypeError> {
    if nullable {
        if let Value::Null = value {
            return Ok(Field::Null);
        }
    }

    match (typ, &value) {
        (FieldType::UInt, _) => serde_json::from_value(value)
            .map_err(DeserializationError::Json)
            .map(Field::UInt),
        (FieldType::Int, _) => serde_json::from_value(value)
            .map_err(DeserializationError::Json)
            .map(Field::Int),
        (FieldType::Float, _) => serde_json::from_value(value)
            .map_err(DeserializationError::Json)
            .map(Field::Float),
        (FieldType::Boolean, _) => serde_json::from_value(value)
            .map_err(DeserializationError::Json)
            .map(Field::Boolean),
        (FieldType::String, _) => serde_json::from_value(value)
            .map_err(DeserializationError::Json)
            .map(Field::String),
        (FieldType::Text, _) => serde_json::from_value(value)
            .map_err(DeserializationError::Json)
            .map(Field::Text),
        (FieldType::Binary, _) => serde_json::from_value(value)
            .map_err(DeserializationError::Json)
            .map(Field::Binary),
        (FieldType::Decimal, Value::String(str)) => Decimal::from_str(str)
            .map_err(|e| DeserializationError::Custom(Box::new(e)))
            .map(Field::Decimal),
        (FieldType::Timestamp, Value::String(str)) => DateTime::parse_from_rfc3339(str)
            .map_err(|e| DeserializationError::Custom(Box::new(e)))
            .map(Field::Timestamp),
        (FieldType::Date, Value::String(str)) => NaiveDate::parse_from_str(str, DATE_FORMAT)
            .map_err(|e| DeserializationError::Custom(Box::new(e)))
            .map(Field::Date),
        (FieldType::Bson, _) => serde_json::from_value(value)
            .map_err(DeserializationError::Json)
            .map(Field::Bson),
        (FieldType::Coord, _) => serde_json::from_value(value)
            .map_err(DeserializationError::Json)
            .map(Field::Coord),
        (FieldType::Point, _) => serde_json::from_value(value)
            .map_err(DeserializationError::Json)
            .map(Field::Point),
        _ => Err(DeserializationError::Custom(
            "Json value type does not match field type"
                .to_string()
                .into(),
        )),
    }
    .map_err(TypeError::DeserializationError)
}
