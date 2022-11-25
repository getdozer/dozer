use crate::errors::DebeziumSchemaError;
use crate::errors::DebeziumSchemaError::{
    BinaryDecodeError, DecimalConvertError, FieldNotFound, ScaleIsInvalid, ScaleNotFound,
    TypeNotSupported,
};
use base64::STANDARD;
use dozer_types::chrono::{NaiveDate, NaiveDateTime};

use crate::connectors::kafka::debezium::stream_consumer::DebeziumSchemaStruct;
use dozer_types::rust_decimal::Decimal;
use dozer_types::serde_json::Value;
use dozer_types::types::{Field, Schema};
use std::collections::HashMap;

fn convert_decimal(value: &str, scale: u32) -> Result<Field, DebeziumSchemaError> {
    let decoded_value = base64::decode_config(value, STANDARD)
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
        None => match schema.r#type.as_str() {
            "int8" | "int16" | "int32" | "int64" => value
                .as_i64()
                .map_or(Ok(Field::Null), |v| Ok(Field::from(v))),
            "string" => value
                .as_str()
                .map_or(Ok(Field::Null), |s| Ok(Field::from(s.to_string()))),
            "bytes" => value.as_str().map_or(Ok(Field::Null), |s| {
                Ok(Field::Binary(
                    base64::decode_config(s, STANDARD).map_err(BinaryDecodeError)?,
                ))
            }),
            "float32" | "float64" | "double" => value
                .as_f64()
                .map_or(Ok(Field::Null), |s| Ok(Field::from(s))),
            "boolean" => value
                .as_bool()
                .map_or(Ok(Field::Null), |s| Ok(Field::from(s))),
            type_name => Err(TypeNotSupported(type_name.to_string())),
        },
        Some(name) => {
            match name.as_str() {
                "io.debezium.time.MicroTimestamp" => value.as_i64().map_or(Ok(Field::Null), |v| {
                    let sec = v / 1000000;
                    let nsecs = (v % 1000000) * 1000;
                    let date = NaiveDateTime::from_timestamp(sec, nsecs as u32);
                    Ok(Field::from(date))
                }),
                "io.debezium.time.Timestamp" | "org.apache.kafka.connect.data.Timestamp" => {
                    value.as_i64().map_or(Ok(Field::Null), |v| {
                        let sec = v / 1000;
                        let nsecs = (v % 1000) * 1000000;
                        let date = NaiveDateTime::from_timestamp(sec, nsecs as u32);
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
                        .map_err(|_| ScaleIsInvalid())?;
                    value
                        .as_str()
                        .map_or(Ok(Field::Null), |value| convert_decimal(value, scale))
                }
                "io.debezium.data.VariableScaleDecimal" => {
                    value.as_object().map_or(Ok(Field::Null), |map| {
                        let scale: u32 =
                            map.get("scale")
                                .ok_or(ScaleNotFound)?
                                .as_u64()
                                .ok_or(ScaleIsInvalid())? as u32;
                        map.get("value").map_or(Ok(Field::Null), |dec_val| {
                            dec_val
                                .as_str()
                                .map_or(Ok(Field::Null), |v| convert_decimal(v, scale))
                        })
                    })
                }
                "io.debezium.time.Date" | "org.apache.kafka.connect.data.Date" => {
                    value.as_i64()
                        .map_or(Ok(Field::Null), |v| {
                            Ok(Field::from(NaiveDate::from_num_days_from_ce(v as i32)))
                        })
                }
                "io.debezium.time.MicroTime" => Ok(Field::Null),
                "io.debezium.data.Json" => value
                    .as_str()
                    .map_or(Ok(Field::Null), |s| Ok(Field::Bson(s.as_bytes().to_vec()))),
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
