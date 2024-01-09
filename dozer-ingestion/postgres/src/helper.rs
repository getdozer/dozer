use dozer_ingestion_connector::dozer_types::{
    bytes::Bytes,
    chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, Offset, Utc},
    errors::types::TypeError,
    geo::Point as GeoPoint,
    json_types::{parse_json_slice, serde_json_to_json_value, JsonArray, JsonValue},
    ordered_float::OrderedFloat,
    rust_decimal, serde_json,
    types::*,
};
use postgres_types::{FromSql, Type, WasNull};
use rust_decimal::prelude::FromPrimitive;
use rust_decimal::Decimal;
use std::error::Error;
use std::num::ParseIntError;

use dozer_ingestion_connector::dozer_types::chrono::{LocalResult, NaiveTime};
use std::vec;
use tokio_postgres::{Column, Row};
use uuid::Uuid;

use crate::DateConversionError::{AmbiguousTimeResult, InvalidDate, InvalidTime};
use crate::{
    xlog_mapper::TableColumn, DateConversionError, PostgresConnectorError, PostgresSchemaError,
};

/// This function converts any offset string (+03, +03:00 and etc) to FixedOffset
///
fn parse_timezone_offset(offset_string: String) -> Result<Option<FixedOffset>, ParseIntError> {
    // Fill right side with zeros when offset is not full length
    let offset_string = format!("{:0<9}", offset_string);

    let sign = &offset_string[0..1];
    let hour = offset_string[1..3].parse::<i32>()?;
    let min = offset_string[4..6].parse::<i32>()?;
    let sec = offset_string[7..9].parse::<i32>()?;

    let secs = (hour * 3600) + (min * 60) + sec;

    if sign == "-" {
        Ok(FixedOffset::west_opt(secs))
    } else {
        Ok(FixedOffset::east_opt(secs))
    }
}

fn convert_date(date: String) -> Result<NaiveDateTime, DateConversionError> {
    // Fill right side with zeros when date time is not full
    let date_string = format!("{:0<26}", date);

    let year: i32 = date_string[0..4].parse()?;
    let month: u32 = date_string[5..7].parse()?;
    let day: u32 = date_string[8..10].parse()?;
    let hour: u32 = date_string[11..13].parse()?;
    let minute: u32 = date_string[14..16].parse()?;
    let second: u32 = date_string[17..19].parse()?;
    let microseconds = if date_string.len() == 19 {
        0
    } else {
        date_string[20..26].parse()?
    };

    let naive_date = NaiveDate::from_ymd_opt(year, month, day).ok_or(InvalidDate)?;
    let naive_time =
        NaiveTime::from_hms_micro_opt(hour, minute, second, microseconds).ok_or(InvalidTime)?;

    Ok(NaiveDateTime::new(naive_date, naive_time))
}

fn convert_date_with_timezone(date: String) -> Result<DateTime<FixedOffset>, DateConversionError> {
    // Find position of last + or -, which is the start of timezone offset
    let pos_plus = date.rfind('+');
    let pos_min = date.rfind('-');

    let pos = match (pos_plus, pos_min) {
        (Some(plus), Some(min)) => {
            if plus > min {
                plus
            } else {
                min
            }
        }
        (None, Some(pos)) | (Some(pos), None) => pos,
        (None, None) => 0,
    };

    let (date, offset_string) = date.split_at(pos);

    let offset = parse_timezone_offset(offset_string.to_string())?.map_or(Utc.fix(), |x| x);

    match convert_date(date.to_string())?.and_local_timezone(offset) {
        LocalResult::None => Err(InvalidTime),
        LocalResult::Single(date) => Ok(date),
        LocalResult::Ambiguous(_, _) => Err(AmbiguousTimeResult),
    }
}

pub fn postgres_type_to_field(
    value: Option<&Bytes>,
    column: &TableColumn,
) -> Result<Field, PostgresSchemaError> {
    let column_type = column.r#type.clone();
    value.map_or(Ok(Field::Null), |v| match column_type {
        Type::INT2 | Type::INT4 | Type::INT8 => Ok(Field::Int(
            String::from_utf8(v.to_vec()).unwrap().parse().unwrap(),
        )),
        Type::FLOAT4 | Type::FLOAT8 => Ok(Field::Float(OrderedFloat(
            String::from_utf8(v.to_vec())
                .unwrap()
                .parse::<f64>()
                .unwrap(),
        ))),
        Type::TEXT | Type::VARCHAR | Type::CHAR | Type::BPCHAR | Type::ANYENUM => {
            Ok(Field::String(String::from_utf8(v.to_vec()).unwrap()))
        }
        Type::UUID => Ok(Field::String(String::from_utf8(v.to_vec()).unwrap())),
        Type::BYTEA => Ok(Field::Binary(v.to_vec())),
        Type::NUMERIC => Ok(Field::Decimal(
            Decimal::from_f64(
                String::from_utf8(v.to_vec())
                    .unwrap()
                    .parse::<f64>()
                    .unwrap(),
            )
            .unwrap(),
        )),
        Type::TIMESTAMP => {
            let date_string = String::from_utf8(v.to_vec())?;

            Ok(Field::Timestamp(DateTime::from_naive_utc_and_offset(
                convert_date(date_string)?,
                Utc.fix(),
            )))
        }
        Type::TIMESTAMPTZ => {
            let date_string = String::from_utf8(v.to_vec())?;

            Ok(convert_date_with_timezone(date_string).map(Field::Timestamp)?)
        }
        Type::DATE => {
            let date: NaiveDate = NaiveDate::parse_from_str(
                String::from_utf8(v.to_vec()).unwrap().as_str(),
                DATE_FORMAT,
            )
            .unwrap();
            Ok(Field::from(date))
        }
        Type::JSONB | Type::JSON => {
            let val: serde_json::Value = serde_json::from_slice(v).map_err(|_| {
                PostgresSchemaError::JSONBParseError(format!(
                    "Error converting to a single row for: {}",
                    column_type.name()
                ))
            })?;
            let json: JsonValue = serde_json_to_json_value(val)
                .map_err(|e| PostgresSchemaError::TypeError(TypeError::DeserializationError(e)))?;
            Ok(Field::Json(json))
        }
        Type::JSONB_ARRAY | Type::JSON_ARRAY => {
            let json_val = parse_json_slice(v).map_err(|_| {
                PostgresSchemaError::JSONBParseError(format!(
                    "Error converting to a single row for: {}",
                    column_type.name()
                ))
            })?;
            Ok(Field::Json(json_val))
        }
        Type::BOOL => Ok(Field::Boolean(v.slice(0..1) == "t")),
        Type::POINT => Ok(Field::Point(
            String::from_utf8(v.to_vec())
                .map_err(PostgresSchemaError::StringParseError)?
                .parse::<DozerPoint>()
                .map_err(|_| PostgresSchemaError::PointParseError)?,
        )),
        _ => Err(PostgresSchemaError::ColumnTypeNotSupported(
            column_type.name().to_string(),
        )),
    })
}

pub fn postgres_type_to_dozer_type(column_type: Type) -> Result<FieldType, PostgresSchemaError> {
    match column_type {
        Type::BOOL => Ok(FieldType::Boolean),
        Type::INT2 | Type::INT4 | Type::INT8 => Ok(FieldType::Int),
        Type::CHAR | Type::TEXT | Type::VARCHAR | Type::BPCHAR | Type::UUID | Type::ANYENUM => {
            Ok(FieldType::String)
        }
        Type::FLOAT4 | Type::FLOAT8 => Ok(FieldType::Float),
        Type::BYTEA => Ok(FieldType::Binary),
        Type::TIMESTAMP | Type::TIMESTAMPTZ => Ok(FieldType::Timestamp),
        Type::NUMERIC => Ok(FieldType::Decimal),
        Type::JSONB
        | Type::JSON
        | Type::JSONB_ARRAY
        | Type::JSON_ARRAY
        | Type::TEXT_ARRAY
        | Type::CHAR_ARRAY
        | Type::VARCHAR_ARRAY
        | Type::BPCHAR_ARRAY => Ok(FieldType::Json),
        Type::DATE => Ok(FieldType::Date),
        Type::POINT => Ok(FieldType::Point),
        _ => Err(PostgresSchemaError::ColumnTypeNotSupported(
            column_type.name().to_string(),
        )),
    }
}

fn handle_error(e: tokio_postgres::error::Error) -> Result<Field, PostgresSchemaError> {
    if let Some(e) = e.source() {
        if let Some(_e) = e.downcast_ref::<WasNull>() {
            Ok(Field::Null)
        } else {
            Err(PostgresSchemaError::ValueConversionError(e.to_string()))
        }
    } else {
        Err(PostgresSchemaError::ValueConversionError(e.to_string()))
    }
}

macro_rules! conversion_fn {
    ($name:ident, $typ:expr) => {
        fn $name(row: &Row, idx: usize) -> Result<Field, PostgresSchemaError> {
            row.try_get(idx).map($typ).or_else(handle_error)
        }
    };
}

conversion_fn!(convert_bool, Field::Boolean);
conversion_fn!(convert_string, Field::String);
conversion_fn!(convert_timestamp, |v: NaiveDateTime| Field::Timestamp(
    v.and_utc().fixed_offset()
));
conversion_fn!(convert_timestamptz, Field::Timestamp);
conversion_fn!(convert_date_snapshot, Field::Date);
conversion_fn!(convert_binary, Field::Binary);
conversion_fn!(convert_point, |v: GeoPoint| Field::Point(v.x_y().into()));
conversion_fn!(convert_decimal, Field::Decimal);

#[inline(always)]
fn convert_int<'a, T: Into<i64> + FromSql<'a>>(
    row: &'a Row,
    idx: usize,
) -> Result<Field, PostgresSchemaError> {
    row.try_get(idx)
        .map(|v: T| Field::Int(v.into()))
        .or_else(handle_error)
}

fn convert_int2(row: &Row, idx: usize) -> Result<Field, PostgresSchemaError> {
    convert_int::<i16>(row, idx)
}
fn convert_int4(row: &Row, idx: usize) -> Result<Field, PostgresSchemaError> {
    convert_int::<i32>(row, idx)
}
fn convert_int8(row: &Row, idx: usize) -> Result<Field, PostgresSchemaError> {
    convert_int::<i64>(row, idx)
}

conversion_fn!(convert_float, |v: f32| Field::Float(OrderedFloat(v.into())));
conversion_fn!(convert_double, |v| Field::Float(OrderedFloat(v)));

fn convert_json(row: &Row, idx: usize) -> Result<Field, PostgresSchemaError> {
    let value: Result<serde_json::Value, _> = row.try_get(idx);
    value.map_or_else(handle_error, |val| {
        Ok(Field::Json(serde_json_to_json_value(val).map_err(|e| {
            PostgresSchemaError::TypeError(TypeError::DeserializationError(e))
        })?))
    })
}
fn convert_jsonarray(row: &Row, idx: usize) -> Result<Field, PostgresSchemaError> {
    let value: Result<Vec<serde_json::Value>, _> = row.try_get(idx);
    value.map_or_else(handle_error, |val| {
        let field = val
            .into_iter()
            .map(serde_json_to_json_value)
            .collect::<Result<JsonArray, _>>()
            .map_err(TypeError::DeserializationError)?;
        Ok(Field::Json(field.into()))
    })
}
fn convert_textarray(row: &Row, idx: usize) -> Result<Field, PostgresSchemaError> {
    let value: Result<Vec<String>, _> = row.try_get(idx);
    value.map_or_else(handle_error, |val| Ok(Field::Json(val.into())))
}

fn convert_uuid(row: &Row, idx: usize) -> Result<Field, PostgresSchemaError> {
    let value: Result<Uuid, _> = row.try_get(idx);
    value.map_or_else(handle_error, |val| Ok(Field::from(val.to_string())))
}

type ConversionFn = fn(&Row, usize) -> Result<Field, PostgresSchemaError>;

pub fn get_conversion_fn(col_type: &Type) -> Result<ConversionFn, PostgresSchemaError> {
    match col_type {
        &Type::BOOL => Ok(convert_bool),
        &Type::INT2 => Ok(convert_int2),
        &Type::INT4 => Ok(convert_int4),
        &Type::INT8 => Ok(convert_int8),
        &Type::CHAR | &Type::TEXT | &Type::VARCHAR | &Type::BPCHAR | &Type::ANYENUM => {
            Ok(convert_string)
        }
        &Type::FLOAT4 => Ok(convert_float),
        &Type::FLOAT8 => Ok(convert_double),
        &Type::TIMESTAMP => Ok(convert_timestamp),
        &Type::TIMESTAMPTZ => Ok(convert_timestamptz),
        &Type::NUMERIC => Ok(convert_decimal),
        &Type::DATE => Ok(convert_date_snapshot),
        &Type::BYTEA => Ok(convert_binary),
        &Type::JSONB | &Type::JSON => Ok(convert_json),
        &Type::JSONB_ARRAY | &Type::JSON_ARRAY => Ok(convert_jsonarray),
        &Type::CHAR_ARRAY | &Type::TEXT_ARRAY | &Type::VARCHAR_ARRAY | &Type::BPCHAR_ARRAY => {
            Ok(convert_textarray)
        }
        &Type::POINT => Ok(convert_point),
        // &Type::UUID => convert_row_value_to_field!(row, idx, Uuid),
        &Type::UUID => Ok(convert_uuid),
        _ => {
            if col_type.schema() == "pg_catalog" {
                Err(PostgresSchemaError::ColumnTypeNotSupported(
                    col_type.name().to_string(),
                ))
            } else {
                Err(PostgresSchemaError::CustomTypeNotSupported(
                    col_type.name().to_string(),
                ))
            }
        }
    }
}

pub fn get_values(
    row: &Row,
    conversion: &[ConversionFn],
) -> Result<Vec<Field>, PostgresSchemaError> {
    let mut values: Vec<Field> = Vec::with_capacity(conversion.len());
    for (idx, fun) in conversion.iter().enumerate() {
        values.push(fun(row, idx)?)
    }
    Ok(values)
}

pub fn map_row_to_record(
    row: &Row,
    conversion: &[ConversionFn],
) -> Result<Record, PostgresSchemaError> {
    get_values(row, conversion).map(Record::new)
}

pub fn map_schema(columns: &[Column]) -> Result<Schema, PostgresConnectorError> {
    let field_defs: Result<Vec<FieldDefinition>, _> =
        columns.iter().map(convert_column_to_field).collect();

    Ok(Schema {
        fields: field_defs.unwrap(),
        primary_index: vec![0],
    })
}

pub fn convert_column_to_field(column: &Column) -> Result<FieldDefinition, PostgresSchemaError> {
    postgres_type_to_dozer_type(column.type_().clone()).map(|typ| FieldDefinition {
        name: column.name().to_string(),
        typ,
        nullable: true,
        source: SourceDefinition::Dynamic,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use dozer_ingestion_connector::dozer_types::{chrono::NaiveDate, json_types::json};

    #[macro_export]
    macro_rules! test_conversion {
        ($a:expr,$b:expr,$c:expr) => {
            let value = postgres_type_to_field(
                Some(&Bytes::from($a)),
                &TableColumn {
                    name: "column".to_string(),
                    flags: 0,
                    r#type: $b,
                    column_index: 0,
                },
            );
            assert_eq!(value.unwrap(), $c);
        };
    }

    #[macro_export]
    macro_rules! test_type_mapping {
        ($a:expr,$b:expr) => {
            let value = postgres_type_to_dozer_type($a);
            assert_eq!(value.unwrap(), $b);
        };
    }

    #[test]
    fn it_converts_postgres_type_to_field() {
        test_conversion!("12", Type::INT8, Field::Int(12));
        test_conversion!("4.7809", Type::FLOAT8, Field::Float(OrderedFloat(4.7809)));
        let value = String::from("Test text");
        test_conversion!("Test text", Type::TEXT, Field::String(value.clone()));
        test_conversion!("Test text", Type::ANYENUM, Field::String(value));

        let value = String::from("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11");
        test_conversion!(
            "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11",
            Type::UUID,
            Field::String(value)
        );

        // UTF-8 bytes representation of json (https://www.charset.org/utf-8)
        let value: Vec<u8> = vec![98, 121, 116, 101, 97];
        test_conversion!("bytea", Type::BYTEA, Field::Binary(value));

        let value = rust_decimal::Decimal::from_f64(8.28).unwrap();
        test_conversion!("8.28", Type::NUMERIC, Field::Decimal(value));

        let value = DateTime::from_naive_utc_and_offset(
            NaiveDate::from_ymd_opt(2022, 9, 16)
                .unwrap()
                .and_hms_opt(5, 56, 29)
                .unwrap(),
            Utc.fix(),
        );
        test_conversion!(
            "2022-09-16 05:56:29",
            Type::TIMESTAMP,
            Field::Timestamp(value)
        );

        let value = DateTime::from_naive_utc_and_offset(
            NaiveDate::from_ymd_opt(2022, 9, 16)
                .unwrap()
                .and_hms_milli_opt(7, 59, 29, 321)
                .unwrap(),
            Utc.fix(),
        );
        test_conversion!(
            "2022-09-16 07:59:29.321",
            Type::TIMESTAMP,
            Field::Timestamp(value)
        );

        let value = DateTime::from_naive_utc_and_offset(
            NaiveDate::from_ymd_opt(2022, 9, 16)
                .unwrap()
                .and_hms_micro_opt(3, 56, 30, 959787)
                .unwrap(),
            Utc.fix(),
        );
        test_conversion!(
            "2022-09-16 10:56:30.959787+07",
            Type::TIMESTAMPTZ,
            Field::Timestamp(value)
        );

        let value = DateTime::from_naive_utc_and_offset(
            NaiveDate::from_ymd_opt(2022, 9, 16)
                .unwrap()
                .and_hms_micro_opt(7, 56, 30, 959787)
                .unwrap(),
            Utc.fix(),
        );
        test_conversion!(
            "2022-09-16 10:56:30.959787+03",
            Type::TIMESTAMPTZ,
            Field::Timestamp(value)
        );

        let value = DateTime::from_naive_utc_and_offset(
            NaiveDate::from_ymd_opt(2022, 9, 16)
                .unwrap()
                .and_hms_micro_opt(14, 26, 30, 959787)
                .unwrap(),
            Utc.fix(),
        );
        test_conversion!(
            "2022-09-16 10:56:30.959787-03:30",
            Type::TIMESTAMPTZ,
            Field::Timestamp(value)
        );

        let value = json!({"abc": "foo"});
        test_conversion!("{\"abc\":\"foo\"}", Type::JSONB, Field::Json(value.clone()));
        test_conversion!("{\"abc\":\"foo\"}", Type::JSON, Field::Json(value));

        let value = json!([{"abc": "foo"}]);
        test_conversion!(
            "[{\"abc\":\"foo\"}]",
            Type::JSON_ARRAY,
            Field::Json(value.clone())
        );
        test_conversion!("[{\"abc\":\"foo\"}]", Type::JSONB_ARRAY, Field::Json(value));

        test_conversion!("t", Type::BOOL, Field::Boolean(true));
        test_conversion!("f", Type::BOOL, Field::Boolean(false));

        test_conversion!(
            "(1.234,2.456)",
            Type::POINT,
            Field::Point(DozerPoint::from((1.234, 2.456)))
        );
    }

    #[test]
    fn it_maps_postgres_type_to_dozer_type() {
        test_type_mapping!(Type::INT8, FieldType::Int);
        test_type_mapping!(Type::FLOAT8, FieldType::Float);
        test_type_mapping!(Type::VARCHAR, FieldType::String);
        test_type_mapping!(Type::ANYENUM, FieldType::String);
        test_type_mapping!(Type::UUID, FieldType::String);
        test_type_mapping!(Type::BYTEA, FieldType::Binary);
        test_type_mapping!(Type::NUMERIC, FieldType::Decimal);
        test_type_mapping!(Type::TIMESTAMP, FieldType::Timestamp);
        test_type_mapping!(Type::TIMESTAMPTZ, FieldType::Timestamp);
        test_type_mapping!(Type::JSONB, FieldType::Json);
        test_type_mapping!(Type::JSON, FieldType::Json);
        test_type_mapping!(Type::JSONB_ARRAY, FieldType::Json);
        test_type_mapping!(Type::JSON_ARRAY, FieldType::Json);
        test_type_mapping!(Type::BOOL, FieldType::Boolean);
        test_type_mapping!(Type::POINT, FieldType::Point);
    }

    #[test]
    fn test_none_value() {
        let value = postgres_type_to_field(
            None,
            &TableColumn {
                name: "column".to_string(),
                flags: 0,
                r#type: Type::VARCHAR,
                column_index: 0,
            },
        );
        assert_eq!(value.unwrap(), Field::Null);
    }
}
