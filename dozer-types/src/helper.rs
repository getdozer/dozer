use crate::errors::types::{DeserializationError, SerializationError, TypeError};
use crate::types::DATE_FORMAT;
use crate::{
    errors::types,
    types::{Field, FieldType, Record, Schema},
};
use chrono::{DateTime, NaiveDate, SecondsFormat};
use indexmap::IndexMap;
use rust_decimal::Decimal;
use std::str::FromStr;
/// Used in REST APIs for converting to JSON
pub fn record_to_json(
    rec: &Record,
    schema: &Schema,
) -> Result<IndexMap<String, String>, types::TypeError> {
    let mut map: IndexMap<String, String> = IndexMap::new();

    for (idx, field_def) in schema.fields.iter().enumerate() {
        if rec.values.len() > idx {
            let field = rec.values[idx].clone();
            let val = field_to_json_value(&field)?;
            map.insert(field_def.name.clone(), val);
        }
    }

    Ok(map)
}

/// Used in REST APIs for converting raw value back and forth
pub fn field_to_json_value(field: &Field) -> Result<String, TypeError> {
    match field {
        Field::Int(n) => Ok(serde_json::to_string(n).map_err(SerializationError::Json))?,
        Field::Float(n) => Ok(serde_json::to_string(n).map_err(SerializationError::Json))?,
        Field::Boolean(b) => Ok(serde_json::to_string(b).map_err(SerializationError::Json))?,
        Field::String(s) => Ok(s.to_owned()),
        Field::Binary(b) => Ok(serde_json::to_string(b).map_err(SerializationError::Json))?,
        Field::Null => Ok("null".to_owned()),
        Field::Decimal(n) => Ok(serde_json::to_string(n).map_err(SerializationError::Json))?,
        Field::Timestamp(ts) => Ok(ts.to_rfc3339_opts(SecondsFormat::Millis, true)),
        Field::Bson(b) => Ok(serde_json::to_string(b).map_err(SerializationError::Json))?,
        Field::UInt(n) => Ok(serde_json::to_string(n).map_err(SerializationError::Json))?,
        Field::Text(n) => Ok(serde_json::to_string(n).map_err(SerializationError::Json))?,
        Field::Date(n) => Ok(n.format(DATE_FORMAT).to_string()),
    }
    .map_err(TypeError::SerializationError)
}

/// Used in REST APIs for converting raw value back and forth
pub fn json_value_to_field(val: &str, typ: &FieldType) -> Result<Field, TypeError> {
    match typ {
        FieldType::Int => serde_json::from_str(val)
            .map_err(DeserializationError::Json)
            .map(Field::Int),
        FieldType::Float => serde_json::from_str(val)
            .map_err(DeserializationError::Json)
            .map(Field::Float),
        FieldType::Boolean => serde_json::from_str(val)
            .map_err(DeserializationError::Json)
            .map(Field::Boolean),
        FieldType::String => Ok(Field::String(val.to_string())),
        FieldType::Binary => serde_json::from_str(val)
            .map_err(DeserializationError::Json)
            .map(Field::Binary),

        FieldType::Decimal => Decimal::from_str(val)
            .map_err(|e| DeserializationError::Custom(Box::new(e)))
            .map(Field::Decimal),
        FieldType::Timestamp => DateTime::parse_from_rfc3339(val)
            .map_err(|e| DeserializationError::Custom(Box::new(e)))
            .map(Field::Timestamp),
        FieldType::Bson => serde_json::from_str(val)
            .map_err(DeserializationError::Json)
            .map(Field::Bson),
        FieldType::Null => Ok(Field::Null),
        FieldType::UInt => serde_json::from_str(val)
            .map_err(DeserializationError::Json)
            .map(Field::UInt),
        FieldType::Text => serde_json::from_str(val)
            .map_err(DeserializationError::Json)
            .map(Field::Text),
        FieldType::Date => NaiveDate::parse_from_str(val, DATE_FORMAT)
            .map_err(|e| DeserializationError::Custom(Box::new(e)))
            .map(Field::Date),
    }
    .map_err(TypeError::DeserializationError)
}

#[cfg(test)]
mod tests {
    use crate::{
        helper::{field_to_json_value, json_value_to_field},
        types::{Field, FieldType},
    };
    use chrono::{NaiveDate, Offset, TimeZone, Utc};
    use ordered_float::OrderedFloat;
    use rust_decimal::Decimal;
    use serde_json::json;
    fn test_field_conversion(field_type: FieldType, field: Field) -> anyhow::Result<()> {
        // Convert the field to a JSON string.
        let serialized = field_to_json_value(&field).unwrap();

        // Convert the JSON string back to a Field.
        let deserialized = json_value_to_field(&serialized, &field_type)?;

        assert_eq!(deserialized, field, "must be equal");
        Ok(())
    }

    #[test]
    fn test_field_types_str_conversion() -> anyhow::Result<()> {
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
                Field::Bson(bincode::serialize(&json!({"a": 1}))?),
            ),
            (FieldType::Text, Field::Text("lorem ipsum".to_string())),
        ];
        for (field_type, field) in fields {
            test_field_conversion(field_type, field)?;
        }
        Ok(())
    }
}
