use crate::errors::types::{DeserializationError, SerializationError, TypeError};
use crate::{
    errors::types,
    types::{Field, FieldType, Record, Schema},
};
use chrono::{DateTime, SecondsFormat, Utc};
use rust_decimal::Decimal;
use std::{collections::HashMap, str::FromStr};
/// Used in REST APIs for converting to JSON
pub fn record_to_json(
    rec: &Record,
    schema: &Schema,
) -> Result<HashMap<String, String>, types::TypeError> {
    let mut map: HashMap<String, String> = HashMap::new();

    for (idx, field_def) in schema.fields.iter().enumerate() {
        let field = rec.values[idx].clone();
        let val = field_to_json_value(&field)?;
        map.insert(field_def.name.clone(), val);
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
        Field::Null => Ok("null".to_string()),
        Field::Decimal(n) => Ok(serde_json::to_string(n).map_err(SerializationError::Json))?,
        Field::Timestamp(ts) => Ok(ts.to_rfc3339_opts(SecondsFormat::Millis, true)),
        Field::Bson(b) => Ok(serde_json::to_string(b).map_err(SerializationError::Json))?,
        Field::RecordArray(arr) => {
            Ok(serde_json::to_string(arr).map_err(SerializationError::Json))?
        }
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
        FieldType::String => serde_json::from_str(val)
            .map_err(DeserializationError::Json)
            .map(Field::String),
        FieldType::Binary => serde_json::from_str(val)
            .map_err(DeserializationError::Json)
            .map(Field::Binary),

        FieldType::Decimal => Decimal::from_str(val)
            .map_err(|e| DeserializationError::Custom(Box::new(e)))
            .map(Field::Decimal),
        FieldType::Timestamp => DateTime::parse_from_rfc3339(val)
            .map_err(|e| DeserializationError::Custom(Box::new(e)))
            .map(|date| {
                let val: DateTime<Utc> = date.with_timezone(&Utc);
                Field::Timestamp(val)
            }),
        FieldType::Bson => serde_json::from_str(val)
            .map_err(DeserializationError::Json)
            .map(Field::Bson),
        FieldType::Null => Ok(Field::Null),
        FieldType::RecordArray(_) => serde_json::from_str(val)
            .map_err(DeserializationError::Json)
            .map(Field::RecordArray),
    }
    .map_err(TypeError::DeserializationError)
}

#[cfg(test)]
mod tests {
    use crate::{
        helper::{field_to_json_value, json_value_to_field},
        types::{Field, FieldDefinition, FieldType, Schema},
    };
    use chrono::{TimeZone, Utc};
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
            (FieldType::Int, Field::Int(1)),
            (FieldType::Float, Field::Float(1.1)),
            (FieldType::Boolean, Field::Boolean(true)),
            (FieldType::String, Field::String("a".to_string())),
            (FieldType::Binary, Field::Binary(b"asdf".to_vec())),
            (FieldType::Decimal, Field::Decimal(Decimal::new(202, 2))),
            (
                FieldType::Timestamp,
                Field::Timestamp(Utc.ymd(2001, 1, 1).and_hms_milli(0, 4, 0, 42)),
            ),
            (
                FieldType::Bson,
                Field::Bson(bincode::serialize(&json!({"a": 1}))?),
            ),
            (
                FieldType::RecordArray(Schema {
                    identifier: None,
                    fields: vec![FieldDefinition {
                        name: "foo".to_string(),
                        typ: FieldType::String,
                        nullable: true,
                    }],
                    values: vec![],
                    primary_index: vec![],
                    secondary_indexes: vec![],
                }),
                Field::RecordArray(vec![]),
            ),
            (FieldType::Null, Field::Null),
        ];
        for (field_type, field) in fields {
            test_field_conversion(field_type, field)?;
        }
        Ok(())
    }
}
