use crate::types::{Field, FieldType, Record, Schema};

use chrono::{DateTime, SecondsFormat, Utc};
use rust_decimal::Decimal;
use std::{collections::HashMap, str::FromStr};

/// Used in REST APIs for converting to JSON
pub fn record_to_json(rec: &Record, schema: &Schema) -> anyhow::Result<HashMap<String, String>> {
    let mut map: HashMap<String, String> = HashMap::new();

    for (idx, field_def) in schema.fields.iter().enumerate() {
        let field = rec.values[idx].clone();
        let val = field_to_json_value(&field)?;
        map.insert(field_def.name.clone(), val);
    }

    Ok(map)
}

/// Used in REST APIs for converting raw value back and forth
pub fn field_to_json_value(field: &Field) -> anyhow::Result<String> {
    let val = match field {
        Field::Int(n) => serde_json::to_string(n)?,
        Field::Float(n) => serde_json::to_string(n)?,
        Field::Boolean(b) => serde_json::to_string(b)?,
        Field::String(s) => serde_json::to_string(s)?,
        Field::Binary(b) => serde_json::to_string(b)?,
        Field::Null => "null".to_string(),
        Field::Decimal(n) => serde_json::to_string(n)?,
        Field::Timestamp(ts) => ts.to_rfc3339_opts(SecondsFormat::Millis, true),
        Field::Bson(b) => serde_json::to_string(b)?,
        Field::RecordArray(arr) => serde_json::to_string(arr)?,
        Field::Invalid(s) => serde_json::to_string(s)?,
    };
    Ok(val)
}

/// Used in REST APIs for converting raw value back and forth
pub fn json_value_to_field(val: &str, typ: &FieldType) -> anyhow::Result<Field> {
    let field = match typ {
        FieldType::Int => {
            let val = serde_json::from_str(val)?;
            Field::Int(val)
        }
        FieldType::Float => {
            let val = serde_json::from_str(val)?;
            Field::Float(val)
        }
        FieldType::Boolean => {
            let val = serde_json::from_str(val)?;
            Field::Boolean(val)
        }
        FieldType::String => {
            let val = serde_json::from_str(val)?;
            Field::String(val)
        }
        FieldType::Binary => {
            let val: Vec<u8> = serde_json::from_str(val)?;
            Field::Binary(val)
        }
        FieldType::Decimal => Field::Decimal(Decimal::from_str(val)?),
        FieldType::Timestamp => {
            let date = DateTime::parse_from_rfc3339(val)?;
            let val: DateTime<Utc> = date.with_timezone(&Utc);
            Field::Timestamp(val)
        }
        FieldType::Bson => {
            let val: Vec<u8> = serde_json::from_str(val)?;
            Field::Bson(val)
        }
        FieldType::Null => Field::Null,
        FieldType::RecordArray(_) => {
            let records: Vec<Record> = serde_json::from_str(val)?;
            Field::RecordArray(records)
        }
        FieldType::Invalid => {
            let val = serde_json::from_str(val)?;
            Field::Invalid(val)
        }
    };
    Ok(field)
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
            (
                FieldType::Invalid,
                Field::Invalid("invalid_String".to_string()),
            ),
        ];
        for (field_type, field) in fields {
            test_field_conversion(field_type, field)?;
        }
        Ok(())
    }
}
