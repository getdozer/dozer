use crate::errors::DebeziumSchemaError;
use crate::errors::DebeziumSchemaError::{
    BinaryDecodeError, DecimalConvertError, FieldNotFound, InvalidDateError, InvalidJsonError,
    InvalidTimestampError, ScaleIsInvalid, ScaleNotFound, TypeNotSupported,
};
use base64::{engine, Engine};
use dozer_types::chrono::{NaiveDate, NaiveDateTime};

use crate::connectors::kafka::debezium::stream_consumer::DebeziumSchemaStruct;
use dozer_types::json_types::JsonValue;
use dozer_types::rust_decimal::Decimal;
use dozer_types::serde_json::Value;
use dozer_types::types::{Field, Schema};
use std::collections::HashMap;
use std::str::FromStr;

fn convert_decimal(value: &str, scale: u32) -> Result<Field, DebeziumSchemaError> {
    let decoded_value = engine::general_purpose::STANDARD
        .decode(value)
        .map_err(BinaryDecodeError)
        .unwrap();

    let mut multiplier: u64 = 1;
    let mut result: u64 = 0;
    decoded_value.iter().rev().for_each(|w| {
        let number = *w as u64;
        result += number * multiplier;
        multiplier *= 256;
    });

    Ok(Field::from(
        Decimal::try_new(result as i64, scale).map_err(DecimalConvertError)?,
    ))
}

fn convert_value(
    value: Value,
    schema: &&DebeziumSchemaStruct,
) -> Result<Field, DebeziumSchemaError> {
    match schema.name.clone() {
        None => match schema.r#type.clone() {
            Value::String(typ) => match typ.as_str() {
                "int8" | "int16" | "int32" | "int64" => value
                    .as_i64()
                    .map_or(Ok(Field::Null), |v| Ok(Field::from(v))),
                "string" => value
                    .as_str()
                    .map_or(Ok(Field::Null), |s| Ok(Field::from(s.to_string()))),
                "bytes" => value.as_str().map_or(Ok(Field::Null), |s| {
                    Ok(Field::Binary(
                        engine::general_purpose::STANDARD
                            .decode(s)
                            .map_err(BinaryDecodeError)?,
                    ))
                }),
                "float32" | "float64" | "double" => value
                    .as_f64()
                    .map_or(Ok(Field::Null), |s| Ok(Field::from(s))),
                "boolean" => value
                    .as_bool()
                    .map_or(Ok(Field::Null), |s| Ok(Field::from(s))),
                _ => Err(TypeNotSupported(typ)),
            },
            _ => Err(TypeNotSupported("Unexpected value type".to_string())),
        },
        Some(name) => {
            match name.as_str() {
                "io.debezium.time.MicroTimestamp" => value.as_i64().map_or(Ok(Field::Null), |v| {
                    let sec = v / 1000000;
                    let nsecs = (v % 1000000) * 1000;
                    let date = NaiveDateTime::from_timestamp_opt(sec, nsecs as u32)
                        .map_or_else(|| Err(InvalidTimestampError), Ok)?;
                    Ok(Field::from(date))
                }),
                "io.debezium.time.Timestamp" | "org.apache.kafka.connect.data.Timestamp" => {
                    value.as_i64().map_or(Ok(Field::Null), |v| {
                        let sec = v / 1000;
                        let nsecs = (v % 1000) * 1000000;
                        let date = NaiveDateTime::from_timestamp_opt(sec, nsecs as u32)
                            .map_or_else(|| Err(InvalidTimestampError), Ok)?;
                        Ok(Field::from(date))
                    })
                }
                "org.apache.kafka.connect.data.Decimal" => {
                    let parameters = schema.parameters.as_ref().ok_or(ScaleNotFound)?;
                    let scale: u32 = parameters
                        .scale
                        .as_ref()
                        .ok_or(ScaleNotFound)?
                        .parse()
                        .map_err(|_| ScaleIsInvalid)?;
                    value
                        .as_str()
                        .map_or(Ok(Field::Null), |value| convert_decimal(value, scale))
                }
                "io.debezium.data.VariableScaleDecimal" => {
                    value.as_object().map_or(Ok(Field::Null), |map| {
                        let scale: u32 = map
                            .get("scale")
                            .ok_or(ScaleNotFound)?
                            .as_u64()
                            .ok_or(ScaleIsInvalid)? as u32;
                        map.get("value").map_or(Ok(Field::Null), |dec_val| {
                            dec_val
                                .as_str()
                                .map_or(Ok(Field::Null), |v| convert_decimal(v, scale))
                        })
                    })
                }
                "io.debezium.time.Date" | "org.apache.kafka.connect.data.Date" => {
                    value.as_i64().map_or(Ok(Field::Null), |v| {
                        Ok(Field::from(
                            NaiveDate::from_num_days_from_ce_opt(v as i32)
                                .map_or_else(|| Err(InvalidDateError), Ok)?,
                        ))
                    })
                }
                "io.debezium.time.MicroTime" => Ok(Field::Null),
                "io.debezium.data.Json" => value.as_str().map_or(Ok(Field::Null), |s| {
                    Ok(Field::Json(
                        JsonValue::from_str(s).map_err(InvalidJsonError)?,
                    ))
                }),
                // | "io.debezium.time.MicroTime" | "org.apache.kafka.connect.data.Time" => Ok(FieldType::Timestamp),
                _ => Err(TypeNotSupported(name)),
            }
        }
    }
}

pub fn convert_value_to_schema(
    value: Value,
    schema: Schema,
    fields_map: HashMap<String, &DebeziumSchemaStruct>,
) -> Result<Vec<Field>, DebeziumSchemaError> {
    schema
        .fields
        .iter()
        .map(|f| match value.get(f.name.clone()).cloned() {
            None => Ok(Field::Null),
            Some(field_value) => {
                let schema_struct = fields_map
                    .get(&f.name)
                    .ok_or_else(|| FieldNotFound(f.name.clone()))?;
                convert_value(field_value, schema_struct)
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use crate::connectors::kafka::debezium::mapper::{convert_value, convert_value_to_schema};
    use crate::connectors::kafka::debezium::stream_consumer::DebeziumSchemaParameters;
    use crate::connectors::kafka::debezium::stream_consumer::DebeziumSchemaStruct;
    use crate::errors::DebeziumSchemaError::TypeNotSupported;
    use base64::{engine, Engine};
    use dozer_types::chrono::{NaiveDate, NaiveDateTime};
    use dozer_types::json_types::JsonValue;
    use dozer_types::ordered_float::OrderedFloat;
    use dozer_types::rust_decimal;
    use dozer_types::serde_json::{Map, Value};
    use dozer_types::types::{Field, FieldDefinition, FieldType, Schema, SourceDefinition};
    use std::collections::{BTreeMap, HashMap};

    #[macro_export]
    macro_rules! test_conversion_debezium {
        ($a:expr,$b:expr,$c:expr,$d:expr,$e:expr) => {
            let value = convert_value(
                Value::from($a),
                &&DebeziumSchemaStruct {
                    r#type: Value::String($b.to_string()),
                    fields: None,
                    optional: Some(false),
                    name: $c,
                    field: None,
                    version: None,
                    parameters: $e,
                },
            );
            assert_eq!(value.unwrap(), $d);
        };
    }

    macro_rules! test_conversion_debezium_error {
        ($a:expr,$b:expr,$c:expr,$d:expr,$e:expr) => {
            let actual_error = convert_value(
                Value::from($a),
                &&DebeziumSchemaStruct {
                    r#type: Value::String($b.to_string()),
                    fields: None,
                    optional: Some(false),
                    name: $c,
                    field: None,
                    version: None,
                    parameters: $e,
                },
            )
            .unwrap_err();
            assert_eq!(actual_error.to_string(), $d.to_string());
        };
    }
    #[test]
    fn it_converts_value_to_field() {
        test_conversion_debezium!(159, "int8", None, Field::from(159), None);
        test_conversion_debezium!(
            "ABC165",
            "string",
            None,
            Field::from("ABC165".to_string()),
            None
        );
        // MTIzNA== -> 1234
        test_conversion_debezium!(
            "MTIzNA==",
            "bytes",
            None,
            Field::Binary(vec![49, 50, 51, 52]),
            None
        );
        test_conversion_debezium!(16.8, "float32", None, Field::from(16.8), None);
        test_conversion_debezium!(false, "boolean", None, Field::from(false), None);
        let current_date =
            NaiveDateTime::parse_from_str("2022-11-28 16:55:43", "%Y-%m-%d %H:%M:%S").unwrap();
        test_conversion_debezium!(
            1669654543000000_i64,
            "-",
            Some("io.debezium.time.MicroTimestamp".to_string()),
            Field::from(current_date),
            None
        );
        test_conversion_debezium!(
            1669654543000_i64,
            "-",
            Some("io.debezium.time.Timestamp".to_string()),
            Field::from(current_date),
            None
        );

        // 4 x 256 + 210 = 1234
        test_conversion_debezium!(
            engine::general_purpose::STANDARD.encode(vec![4, 210]),
            "-",
            Some("org.apache.kafka.connect.data.Decimal".to_string()),
            Field::from(rust_decimal::Decimal::new(1234, 2)),
            Some(DebeziumSchemaParameters {
                scale: Some(2.to_string()),
                precision: None
            })
        );

        let mut v: Map<String, Value> = Map::new();
        v.insert(
            "value".to_string(),
            Value::from(engine::general_purpose::STANDARD.encode(vec![4, 211])),
        );
        v.insert("scale".to_string(), Value::from(2_u64));
        test_conversion_debezium!(
            v,
            "-",
            Some("io.debezium.data.VariableScaleDecimal".to_string()),
            Field::from(rust_decimal::Decimal::new(1235, 2)),
            None
        );

        let current_date = NaiveDate::from_ymd_opt(2022, 11, 28).unwrap();
        test_conversion_debezium!(
            738487,
            "-",
            Some("io.debezium.time.Date".to_string()),
            Field::from(current_date),
            None
        );
        test_conversion_debezium!(
            "{\"abc\":123}",
            "-",
            Some("io.debezium.data.Json".to_string()),
            Field::Json(JsonValue::Object(BTreeMap::from([(
                String::from("abc"),
                JsonValue::Number(OrderedFloat(123_f64))
            )]))),
            None
        );
    }

    #[test]
    fn it_converts_value_to_field_error() {
        test_conversion_debezium_error!(
            "Unknown type value",
            "Unknown type",
            None,
            TypeNotSupported("Unknown type".to_string()),
            None
        );
        test_conversion_debezium_error!(
            1234,
            "-",
            Some("org.apache.kafka.connect.data.Decimal".to_string()),
            crate::errors::DebeziumSchemaError::ScaleNotFound,
            None
        );
        test_conversion_debezium_error!(
            1234,
            "-",
            Some("org.apache.kafka.connect.data.Decimal".to_string()),
            crate::errors::DebeziumSchemaError::ScaleIsInvalid,
            Some(DebeziumSchemaParameters {
                scale: Some("ABCD".to_string()),
                precision: None
            })
        );
    }

    #[test]
    fn it_converts_value_to_schema() {
        let mut v: Map<String, Value> = Map::new();
        v.insert("id".to_string(), Value::from(1));
        v.insert("name".to_string(), Value::from("Product"));
        v.insert("description".to_string(), Value::from("Description"));
        v.insert("weight".to_string(), Value::from(12.34));

        let value = Value::from(v);

        let schema = Schema {
            identifier: None,
            fields: vec![
                FieldDefinition {
                    name: "id".to_string(),
                    typ: FieldType::Int,
                    nullable: false,
                    source: SourceDefinition::Dynamic,
                },
                FieldDefinition {
                    name: "name".to_string(),
                    typ: FieldType::String,
                    nullable: false,
                    source: SourceDefinition::Dynamic,
                },
                FieldDefinition {
                    name: "description".to_string(),
                    typ: FieldType::String,
                    nullable: false,
                    source: SourceDefinition::Dynamic,
                },
                FieldDefinition {
                    name: "weight".to_string(),
                    typ: FieldType::Float,
                    nullable: false,
                    source: SourceDefinition::Dynamic,
                },
            ],
            primary_index: vec![],
        };

        let mut fields_map: HashMap<String, &DebeziumSchemaStruct> = HashMap::new();
        let id_struct = DebeziumSchemaStruct {
            r#type: Value::String("int64".to_string()),
            fields: None,
            optional: Some(false),
            name: None,
            field: None,
            version: None,
            parameters: None,
        };
        fields_map.insert("id".to_string(), &id_struct);
        let name_struct = DebeziumSchemaStruct {
            r#type: Value::String("string".to_string()),
            fields: None,
            optional: Some(false),
            name: None,
            field: None,
            version: None,
            parameters: None,
        };
        fields_map.insert("name".to_string(), &name_struct);
        let description_struct = DebeziumSchemaStruct {
            r#type: Value::String("string".to_string()),
            fields: None,
            optional: Some(false),
            name: None,
            field: None,
            version: None,
            parameters: None,
        };
        fields_map.insert("description".to_string(), &description_struct);
        let weight_struct = DebeziumSchemaStruct {
            r#type: Value::String("float64".to_string()),
            fields: None,
            optional: Some(false),
            name: None,
            field: None,
            version: None,
            parameters: None,
        };
        fields_map.insert("weight".to_string(), &weight_struct);

        let fields = convert_value_to_schema(value, schema, fields_map).unwrap();
        assert_eq!(*fields.get(0).unwrap(), Field::from(1));
        assert_eq!(*fields.get(1).unwrap(), Field::from("Product".to_string()));
        assert_eq!(
            *fields.get(2).unwrap(),
            Field::from("Description".to_string())
        );
        assert_eq!(*fields.get(3).unwrap(), Field::from(12.34));
    }

    #[test]
    fn it_converts_null_value_to_schema() {
        let mut v: Map<String, Value> = Map::new();
        v.insert("id".to_string(), Value::from(1));

        let value = Value::from(v);

        let schema = Schema {
            identifier: None,
            fields: vec![
                FieldDefinition {
                    name: "id".to_string(),
                    typ: FieldType::Int,
                    nullable: false,
                    source: SourceDefinition::Dynamic,
                },
                FieldDefinition {
                    name: "name".to_string(),
                    typ: FieldType::String,
                    nullable: true,
                    source: SourceDefinition::Dynamic,
                },
            ],
            primary_index: vec![],
        };

        let mut fields_map: HashMap<String, &DebeziumSchemaStruct> = HashMap::new();
        let id_struct = DebeziumSchemaStruct {
            r#type: Value::String("int64".to_string()),
            fields: None,
            optional: Some(false),
            name: None,
            field: None,
            version: None,
            parameters: None,
        };
        fields_map.insert("id".to_string(), &id_struct);
        let name_struct = DebeziumSchemaStruct {
            r#type: Value::String("string".to_string()),
            fields: None,
            optional: Some(false),
            name: None,
            field: None,
            version: None,
            parameters: None,
        };
        fields_map.insert("name".to_string(), &name_struct);

        let fields = convert_value_to_schema(value, schema, fields_map).unwrap();
        assert_eq!(*fields.get(0).unwrap(), Field::from(1));
        assert_eq!(*fields.get(1).unwrap(), Field::Null);
    }
}
