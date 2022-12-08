use crate::errors::types::{DeserializationError, TypeError};
use crate::types::DATE_FORMAT;
use crate::types::{Field, FieldType, Record, Schema};
use chrono::{DateTime, NaiveDate, SecondsFormat};
use indexmap::IndexMap;
use rust_decimal::Decimal;
use serde_json::Value;
use std::str::FromStr;
use std::string::FromUtf8Error;
/// Used in REST APIs for converting to JSON
pub fn record_to_map(rec: &Record, schema: &Schema) -> Result<IndexMap<String, Value>, TypeError> {
    let mut map = IndexMap::new();

    for (idx, field_def) in schema.fields.iter().enumerate() {
        if rec.values.len() > idx {
            let field = rec.values[idx].clone();
            let val = field_to_json_value(field)
                .map_err(|_| TypeError::InvalidFieldValue("Bson field is not valid utf8".into()))?;
            map.insert(field_def.name.clone(), val);
        }
    }

    Ok(map)
}

/// Used in REST APIs for converting raw value back and forth
fn field_to_json_value(field: Field) -> Result<Value, FromUtf8Error> {
    match field {
        Field::UInt(n) => Ok(Value::from(n)),
        Field::Int(n) => Ok(Value::from(n)),
        Field::Float(n) => Ok(Value::from(n.0)),
        Field::Boolean(b) => Ok(Value::from(b)),
        Field::String(s) => Ok(Value::from(s)),
        Field::Text(n) => Ok(Value::from(n)),
        Field::Binary(b) => Ok(Value::from(b)),
        Field::Decimal(n) => Ok(Value::String(n.to_string())),
        Field::Timestamp(ts) => Ok(Value::String(
            ts.to_rfc3339_opts(SecondsFormat::Millis, true),
        )),
        Field::Date(n) => Ok(Value::String(n.format(DATE_FORMAT).to_string())),
        Field::Bson(b) => Ok(Value::from(b)),
        Field::Null => Ok(Value::Null),
    }
}

/// Used in REST APIs for converting raw value back and forth
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
        (FieldType::Null, Value::Null) => Ok(Field::Null),
        _ => Err(DeserializationError::Custom(
            "Json value type does not match field type"
                .to_string()
                .into(),
        )),
    }
    .map_err(TypeError::DeserializationError)
}

pub fn json_str_to_field(value: &str, typ: FieldType, nullable: bool) -> Result<Field, TypeError> {
    let value = serde_json::from_str(value)
        .map_err(|e| TypeError::DeserializationError(DeserializationError::Json(e)))?;
    json_value_to_field(value, typ, nullable)
}

#[cfg(test)]
mod tests {
    use crate::{
        helper::{field_to_json_value, json_value_to_field},
        json_str_to_field,
        types::{Field, FieldType},
    };
    use chrono::{NaiveDate, Offset, TimeZone, Utc};
    use ordered_float::OrderedFloat;
    use rust_decimal::Decimal;
    fn test_field_conversion(field_type: FieldType, field: Field) {
        // Convert the field to a JSON value.
        let value = field_to_json_value(field.clone()).unwrap();

        // Convert the JSON value back to a Field.
        let deserialized = json_value_to_field(value, field_type, true).unwrap();

        assert_eq!(deserialized, field, "must be equal");
    }

    #[test]
    fn test_field_types_json_conversion() {
        let fields = vec![
            (FieldType::Int, Field::Int(-1)),
            (FieldType::UInt, Field::UInt(1)),
            (FieldType::Float, Field::Float(OrderedFloat(1.1))),
            (FieldType::Boolean, Field::Boolean(true)),
            (FieldType::String, Field::String("a".to_string())),
            (FieldType::Binary, Field::Binary(b"asdf".to_vec())),
            (FieldType::Decimal, Field::Decimal(Decimal::new(202, 2))),
            (
                FieldType::Timestamp,
                Field::Timestamp(Utc.fix().ymd(2001, 1, 1).and_hms_milli(0, 4, 0, 42)),
            ),
            (
                FieldType::Date,
                Field::Date(NaiveDate::from_ymd(2022, 11, 24)),
            ),
            (
                FieldType::Bson,
                Field::Bson(vec![
                    // BSON representation of `{"abc":"foo"}`
                    123, 34, 97, 98, 99, 34, 58, 34, 102, 111, 111, 34, 125,
                ]),
            ),
            (FieldType::Text, Field::Text("lorem ipsum".to_string())),
        ];
        for (field_type, field) in fields {
            test_field_conversion(field_type, field);
        }
    }

    #[test]
    fn test_nullable_field_conversion() {
        assert_eq!(
            json_str_to_field("null", FieldType::Int, true).unwrap(),
            Field::Null
        );
        assert!(json_str_to_field("null", FieldType::Int, false).is_err());
    }
}
