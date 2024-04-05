use crate::Error;
use dozer_ingestion_connector::{
    dozer_types::{
        chrono::{self, NaiveDate},
        json_types::json_from_str,
        models::ingestion_types::WebhookConfigSchemas,
        ordered_float::OrderedFloat,
        rust_decimal::Decimal,
        serde_json,
        types::{Field, FieldType, Record, Schema},
    },
    SourceSchema,
};
use std::{collections::HashMap, fs, path::Path};

pub fn extract_source_schema(input: WebhookConfigSchemas) -> HashMap<String, SourceSchema> {
    match input {
        WebhookConfigSchemas::Inline(schema_str) => {
            let schemas: HashMap<String, SourceSchema> = serde_json::from_str(&schema_str).unwrap();
            schemas
        }
        WebhookConfigSchemas::Path(path) => {
            let path = Path::new(&path);
            let schema_str = fs::read_to_string(path).unwrap();
            let schemas: HashMap<String, SourceSchema> = serde_json::from_str(&schema_str).unwrap();
            schemas
        }
    }
}

pub fn map_record(
    rec: serde_json::map::Map<String, serde_json::Value>,
    schema: &Schema,
) -> Result<Record, Error> {
    let mut values: Vec<Field> = vec![];
    let fields = schema.fields.clone();
    for field in fields.into_iter() {
        let field_name = field.name.clone();
        let field_value = rec.get(&field_name);
        if !field.nullable && field_value.is_none() {
            return Err(Error::FieldNotFound(field_name));
        }
        match field_value {
            Some(value) => match field.typ {
                FieldType::String => {
                    let str_value: String = serde_json::from_value(value.clone())?;
                    let field = Field::String(str_value);
                    values.push(field);
                }
                FieldType::Int => {
                    let i64_value: i64 = serde_json::from_value(value.clone())?;
                    let field = Field::Int(i64_value);
                    values.push(field);
                }
                FieldType::Int8 => {
                    let i8_value: i8 = serde_json::from_value(value.clone())?;
                    let field = Field::Int8(i8_value);
                    values.push(field);
                }
                FieldType::Float => {
                    let float_value: f64 = serde_json::from_value(value.clone())?;
                    let field = Field::Float(OrderedFloat(float_value));
                    values.push(field);
                }
                FieldType::Boolean => {
                    let bool_value: bool = serde_json::from_value(value.clone())?;
                    let field = Field::Boolean(bool_value);
                    values.push(field);
                }
                FieldType::Timestamp => {
                    let i64_value: i64 = serde_json::from_value(value.clone())?;
                    let timestamp_value = chrono::NaiveDateTime::from_timestamp_millis(i64_value)
                        .map(|t| {
                            Field::Timestamp(
                                chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(
                                    t,
                                    chrono::Utc,
                                )
                                .into(),
                            )
                        })
                        .unwrap_or(Field::Null);
                    values.push(timestamp_value);
                }
                FieldType::Date => {
                    let i64_value: NaiveDate = serde_json::from_value(value.clone())?;
                    let field = Field::Date(i64_value);
                    values.push(field);
                }
                FieldType::UInt => {
                    let u64_value: u64 = serde_json::from_value(value.clone())?;
                    let field = Field::UInt(u64_value);
                    values.push(field);
                }
                FieldType::U128 => {
                    let u128_value: u128 = serde_json::from_value(value.clone())?;
                    let field = Field::U128(u128_value);
                    values.push(field);
                }
                FieldType::I128 => {
                    let i128_value: i128 = serde_json::from_value(value.clone())?;
                    let field = Field::I128(i128_value);
                    values.push(field);
                }
                FieldType::Text => {
                    let str_value: String = serde_json::from_value(value.clone())?;
                    let field = Field::Text(str_value);
                    values.push(field);
                }
                FieldType::Binary => {
                    let str_value: String = serde_json::from_value(value.clone())?;
                    let field = Field::Binary(str_value.into_bytes());
                    values.push(field);
                }
                FieldType::Decimal => {
                    let str_value: String = serde_json::from_value(value.clone())?;
                    let decimal_value: Decimal =
                        Decimal::from_str_exact(str_value.as_str()).unwrap();
                    let field = Field::Decimal(decimal_value);
                    values.push(field);
                }
                FieldType::Json => {
                    let str_value: String = serde_json::to_string(value)?;
                    let ivalue_str = json_from_str(str_value.as_str()).unwrap();
                    let field = Field::Json(ivalue_str);
                    values.push(field);
                }
                FieldType::Point => {
                    values.push(Field::Null);
                }
                FieldType::Duration => {
                    values.push(Field::Null);
                }
            },
            None => {
                let field = Field::Null;
                values.push(field);
            }
        }
    }

    Ok(Record {
        values,
        lifetime: None,
    })
}
