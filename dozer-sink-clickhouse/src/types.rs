use crate::errors::QueryError;

use chrono_tz::{Tz, UTC};
use clickhouse_rs::{Block, ClientHandle};
use dozer_types::chrono::{DateTime, FixedOffset, NaiveDate, Offset, TimeZone};
use dozer_types::json_types::JsonValue;
use dozer_types::ordered_float::OrderedFloat;
use dozer_types::rust_decimal::prelude::ToPrimitive;
use dozer_types::rust_decimal::{self};
use dozer_types::serde_json;
use dozer_types::types::{Field, FieldDefinition, FieldType};
use either::Either;

use clickhouse_rs::types::{FromSql, Value, ValueRef};

pub const DECIMAL_SCALE: u8 = 4;
pub struct ValueWrapper(pub Value);

impl<'a> FromSql<'a> for ValueWrapper {
    fn from_sql(value: ValueRef<'a>) -> clickhouse_rs::errors::Result<Self> {
        let v = Value::from(value);
        Ok(ValueWrapper(v))
    }
}

pub fn map_value_wrapper_to_field(
    value: ValueWrapper,
    field: FieldDefinition,
) -> Result<Field, QueryError> {
    if field.nullable {
        if let clickhouse_rs::types::Value::Nullable(v) = value.0 {
            match v {
                Either::Left(_) => Ok(Field::Null),
                Either::Right(data) => {
                    let mut fd = field.clone();
                    fd.nullable = false;
                    map_value_wrapper_to_field(ValueWrapper(*data), fd)
                }
            }
        } else {
            Err(QueryError::CustomError(format!(
                "Field is marked as nullable in Schema but not as per the database {0:?}",
                field.name
            )))
        }
    } else {
        let value = value.0;
        match field.typ {
            FieldType::UInt => match value {
                clickhouse_rs::types::Value::UInt8(val) => Ok(Field::UInt(val.into())),
                clickhouse_rs::types::Value::UInt16(val) => Ok(Field::UInt(val.into())),
                clickhouse_rs::types::Value::UInt32(val) => Ok(Field::UInt(val.into())),
                clickhouse_rs::types::Value::UInt64(val) => Ok(Field::UInt(val)),
                _ => Err(QueryError::CustomError("Invalid UInt value".to_string())),
            },
            FieldType::U128 => match value {
                clickhouse_rs::types::Value::UInt128(val) => Ok(Field::U128(val)),
                _ => Err(QueryError::CustomError("Invalid U128 value".to_string())),
            },
            FieldType::Int => match value {
                clickhouse_rs::types::Value::Int8(val) => Ok(Field::Int(val.into())),
                clickhouse_rs::types::Value::Int16(val) => Ok(Field::Int(val.into())),
                clickhouse_rs::types::Value::Int32(val) => Ok(Field::Int(val.into())),
                clickhouse_rs::types::Value::Int64(val) => Ok(Field::Int(val)),
                _ => Err(QueryError::CustomError("Invalid Int value".to_string())),
            },
            FieldType::I128 => match value {
                clickhouse_rs::types::Value::Int128(val) => Ok(Field::I128(val)),
                _ => Err(QueryError::CustomError("Invalid I128 value".to_string())),
            },
            FieldType::Float => match value {
                clickhouse_rs::types::Value::Float64(val) => Ok(Field::Float(OrderedFloat(val))),
                _ => Err(QueryError::CustomError("Invalid Float value".to_string())),
            },
            FieldType::Boolean => match value {
                clickhouse_rs::types::Value::UInt8(val) => Ok(Field::Boolean(val != 0)),
                _ => Err(QueryError::CustomError("Invalid Boolean value".to_string())),
            },
            FieldType::String => match value {
                clickhouse_rs::types::Value::String(_) => Ok(Field::String(value.to_string())),
                _ => Err(QueryError::CustomError("Invalid String value".to_string())),
            },
            FieldType::Text => match value {
                clickhouse_rs::types::Value::String(_) => Ok(Field::String(value.to_string())),
                _ => Err(QueryError::CustomError("Invalid String value".to_string())),
            },
            FieldType::Binary => match value {
                clickhouse_rs::types::Value::String(val) => {
                    let val = (*val).clone();
                    Ok(Field::Binary(val))
                }
                _ => Err(QueryError::CustomError("Invalid Binary value".to_string())),
            },
            FieldType::Decimal => match value {
                clickhouse_rs::types::Value::Decimal(v) => Ok(Field::Decimal(
                    rust_decimal::Decimal::new(v.internal(), v.scale() as u32),
                )),
                _ => Err(QueryError::CustomError("Invalid Decimal value".to_string())),
            },
            FieldType::Timestamp => {
                let v: DateTime<Tz> = value.into();
                let dt = convert_to_fixed_offset(v);
                match dt {
                    Some(dt) => Ok(Field::Timestamp(dt)),
                    None => Err(QueryError::CustomError(
                        "Invalid Timestamp value".to_string(),
                    )),
                }
            }
            FieldType::Date => Ok(Field::Date(value.into())),
            FieldType::Json => match value {
                clickhouse_rs::types::Value::String(_) => {
                    let json = value.to_string();
                    let json = serde_json::from_str(&json);
                    json.map(Field::Json)
                        .map_err(|e| QueryError::CustomError(e.to_string()))
                }
                _ => Err(QueryError::CustomError("Invalid Json value".to_string())),
            },
            x => Err(QueryError::CustomError(format!(
                "Unsupported type {0:?}",
                x
            ))),
        }
    }
}

fn convert_to_fixed_offset(datetime_tz: DateTime<Tz>) -> Option<DateTime<FixedOffset>> {
    // Get the offset from UTC in seconds for the specific datetime
    let offset_seconds = datetime_tz
        .timezone()
        .offset_from_utc_datetime(&datetime_tz.naive_utc())
        .fix()
        .local_minus_utc();

    // Create a FixedOffset timezone from the offset in seconds using east_opt()
    FixedOffset::east_opt(offset_seconds)
        .map(|fixed_offset| fixed_offset.from_utc_datetime(&datetime_tz.naive_utc()))
}

fn type_mismatch_error(expected_type: &str, field_name: &str) -> QueryError {
    QueryError::TypeMismatch(expected_type.to_string(), field_name.to_string())
}

macro_rules! handle_type {
    ($nullable: expr, $b: expr, $field_type:ident, $rust_type:ty, $column_values:expr, $n:expr) => {{
        if $nullable {
            let column_data: Vec<Option<&&Field>> = $column_values.iter().map(Some).collect();
            let mut col: Vec<Option<$rust_type>> = vec![];
            for f in column_data {
                let v = match f {
                    Some(Field::$field_type(v)) => Ok(Some(*v)),
                    None => Ok(None),
                    _ => Err(type_mismatch_error(stringify!($field_type), $n)),
                }?;
                col.push(v);
            }
            $b = $b.column($n, col);
        } else {
            let mut col: Vec<$rust_type> = vec![];
            for f in $column_values {
                let v = match f {
                    Field::$field_type(v) => Ok(*v),
                    _ => Err(type_mismatch_error(stringify!($field_type), $n)),
                }?;
                col.push(v);
            }
            $b = $b.column($n, col);
        }
    }};
}

macro_rules! handle_complex_type {
    ($nullable: expr, $b: expr, $field_type:ident, $rust_type:ty, $column_values:expr, $n:expr, $complex_expr:expr) => {{
        if $nullable {
            let column_data: Vec<Option<&&Field>> = $column_values.iter().map(Some).collect();
            let mut col: Vec<Option<$rust_type>> = vec![];
            for f in column_data {
                let v = match f {
                    Some(Field::$field_type(v)) => {
                        let v = $complex_expr(v);
                        Ok(v)
                    }
                    None => Ok(None),
                    _ => Err(type_mismatch_error(stringify!($field_type), $n)),
                }?;
                col.push(v);
            }
            $b = $b.column($n, col);
        } else {
            let mut col: Vec<$rust_type> = vec![];
            for f in $column_values {
                let v = match f {
                    Field::$field_type(v) => {
                        let v = $complex_expr(v);
                        match v {
                            Some(v) => Ok(v),
                            None => Err(type_mismatch_error(stringify!($field_type), $n)),
                        }
                    }
                    _ => Err(type_mismatch_error(stringify!($field_type), $n)),
                }?;
                col.push(v);
            }
            $b = $b.column($n, col);
        }
    }};
}

pub async fn insert_multi(
    mut client: ClientHandle,
    table_name: &str,
    fields: &[FieldDefinition],
    values: &[Vec<Field>], // Now expects a Vec of Vec of Field
) -> Result<(), QueryError> {
    let mut b = Block::<clickhouse_rs::Simple>::new();

    for (field_index, fd) in fields.iter().enumerate() {
        let column_values: Vec<_> = values.iter().map(|row| &row[field_index]).collect();

        let n = &fd.name;
        let nullable = fd.nullable;
        match fd.typ {
            FieldType::UInt => handle_type!(nullable, b, UInt, u64, column_values, n),
            FieldType::U128 => handle_type!(nullable, b, U128, u128, column_values, n),
            FieldType::Int => handle_type!(nullable, b, Int, i64, column_values, n),
            FieldType::I128 => handle_type!(nullable, b, I128, i128, column_values, n),
            FieldType::Boolean => handle_type!(nullable, b, Boolean, bool, column_values, n),
            FieldType::Float => {
                handle_complex_type!(
                    nullable,
                    b,
                    Float,
                    f64,
                    column_values,
                    n,
                    |f: &OrderedFloat<f64>| -> Option<f64> { f.to_f64() }
                )
            }
            FieldType::String | FieldType::Text => {
                handle_complex_type!(
                    nullable,
                    b,
                    String,
                    String,
                    column_values,
                    n,
                    |f: &String| -> Option<String> { Some(f.to_string()) }
                )
            }
            FieldType::Binary => {
                handle_complex_type!(
                    nullable,
                    b,
                    Binary,
                    Vec<u8>,
                    column_values,
                    n,
                    |f: &Vec<u8>| -> Option<Vec<u8>> { Some(f.clone()) }
                )
            }
            FieldType::Decimal => {
                handle_complex_type!(
                    nullable,
                    b,
                    Decimal,
                    clickhouse_rs::types::Decimal,
                    column_values,
                    n,
                    |f: &rust_decimal::Decimal| -> Option<clickhouse_rs::types::Decimal> {
                        f.to_f64()
                            .map(|f| clickhouse_rs::types::Decimal::of(f, DECIMAL_SCALE))
                    }
                )
            }
            FieldType::Timestamp => {
                handle_complex_type!(
                    nullable,
                    b,
                    Timestamp,
                    DateTime<Tz>,
                    column_values,
                    n,
                    |dt: &DateTime<FixedOffset>| -> Option<DateTime<Tz>> {
                        Some(dt.with_timezone(&UTC))
                    }
                )
            }
            FieldType::Date => {
                handle_complex_type!(
                    nullable,
                    b,
                    Date,
                    NaiveDate,
                    column_values,
                    n,
                    |f: &NaiveDate| -> Option<NaiveDate> { Some(f.clone()) }
                )
            }
            FieldType::Json => {
                handle_complex_type!(
                    nullable,
                    b,
                    Json,
                    Vec<u8>,
                    column_values,
                    n,
                    |f: &JsonValue| -> Option<Vec<u8>> {
                        Some(dozer_types::json_types::json_to_bytes(f))
                    }
                )
            }
            ft => {
                return Err(QueryError::CustomError(format!(
                    "Unsupported field_type {} for {}",
                    ft, n
                )));
            }
        }
    }

    // Insert the block into the table
    client.insert(table_name, b).await?;

    Ok(())
}
