use crate::errors::ObjectStoreSchemaError;
use crate::errors::ObjectStoreSchemaError::DateConversionError;
use crate::errors::ObjectStoreSchemaError::DateTimeConversionError;
use crate::errors::ObjectStoreSchemaError::DurationConversionError;
use crate::errors::ObjectStoreSchemaError::FieldTypeNotSupported;
use crate::errors::ObjectStoreSchemaError::TimeConversionError;
use deltalake::arrow::array;
use deltalake::arrow::array::{Array, ArrayRef};
use deltalake::arrow::datatypes::{DataType, Field, TimeUnit};
use dozer_types::json_types::serde_json_to_json_value;
use dozer_types::serde_json;

use dozer_types::types::{Field as DozerField, FieldDefinition, FieldType, SourceDefinition};

macro_rules! make_from {
    ($array_type:ty, $column: ident, $row: ident) => {{
        let array = $column.as_any().downcast_ref::<$array_type>();

        if let Some(r) = array {
            let s: DozerField = if r.is_null($row.clone()) {
                DozerField::Null
            } else {
                DozerField::from(r.value($row.clone()))
            };

            Ok(s)
        } else {
            Ok(DozerField::Null)
        }
    }};
}

macro_rules! make_binary {
    ($array_type:ty, $column: ident, $row: ident) => {{
        let array = $column.as_any().downcast_ref::<$array_type>();

        if let Some(r) = array {
            let s: DozerField = if r.is_null($row.clone()) {
                DozerField::Null
            } else {
                DozerField::Binary(r.value($row.clone()).to_vec())
            };

            Ok(s)
        } else {
            Ok(DozerField::Null)
        }
    }};
}

macro_rules! make_timestamp {
    ($array_type:ty, $column: ident, $row: ident) => {{
        let array = $column.as_any().downcast_ref::<$array_type>();

        if let Some(r) = array {
            if r.is_null($row.clone()) {
                Ok(DozerField::Null)
            } else {
                r.value_as_datetime($row.clone())
                    .map(DozerField::from)
                    .map_or_else(|| Err(DateTimeConversionError), |v| Ok(DozerField::from(v)))
            }
        } else {
            Ok(DozerField::Null)
        }
    }};
}

macro_rules! make_date {
    ($array_type:ty, $column: ident, $row: ident) => {{
        let array = $column.as_any().downcast_ref::<$array_type>();

        if let Some(r) = array {
            if r.is_null($row.clone()) {
                Ok(DozerField::Null)
            } else {
                r.value_as_date($row.clone())
                    .map_or_else(|| Err(DateConversionError), |v| Ok(DozerField::from(v)))
            }
        } else {
            Ok(DozerField::Null)
        }
    }};
}

macro_rules! make_time {
    ($array_type:ty, $column: ident, $row: ident) => {{
        let array = $column.as_any().downcast_ref::<$array_type>();

        if let Some(r) = array {
            if r.is_null($row.clone()) {
                Ok(DozerField::Null)
            } else {
                r.value_as_time($row.clone())
                    .map_or_else(|| Err(TimeConversionError), |v| Ok(DozerField::from(v)))
            }
        } else {
            Ok(DozerField::Null)
        }
    }};
}

macro_rules! make_duration {
    ($array_type:ty, $column: ident, $row: ident) => {{
        let array = $column.as_any().downcast_ref::<$array_type>();

        if let Some(r) = array {
            if r.is_null($row.clone()) {
                Ok(DozerField::Null)
            } else {
                r.value_as_duration($row.clone()).map_or_else(
                    || Err(DurationConversionError),
                    |v| Ok(DozerField::from(v.num_nanoseconds().unwrap())),
                )
            }
        } else {
            Ok(DozerField::Null)
        }
    }};
}

macro_rules! make_text {
    ($array_type:ty, $column: ident, $row: ident) => {{
        let array = $column.as_any().downcast_ref::<$array_type>();

        if let Some(r) = array {
            let s: DozerField = if r.is_null($row.clone()) {
                DozerField::Null
            } else {
                DozerField::Text(r.value($row.clone()).to_string())
            };

            Ok(s)
        } else {
            Ok(DozerField::Null)
        }
    }};
}

pub fn map_schema_to_dozer<'a, I: Iterator<Item = &'a Field>>(
    fields_list: I,
) -> Result<Vec<FieldDefinition>, ObjectStoreSchemaError> {
    fields_list
        .map(|field| {
            let mapped_field_type = match field.data_type() {
                DataType::Boolean => FieldType::Boolean,
                DataType::Duration(_)
                | DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64 => FieldType::Int,
                DataType::UInt8
                | DataType::UInt16
                | DataType::UInt32
                | DataType::UInt64
                | DataType::Time32(_)
                | DataType::Time64(_) => FieldType::UInt,
                DataType::Float16 | DataType::Float32 | DataType::Float64 => FieldType::Float,
                DataType::Timestamp(_, _) => FieldType::Timestamp,
                DataType::Date32 | DataType::Date64 => FieldType::Date,
                DataType::Binary | DataType::FixedSizeBinary(_) | DataType::LargeBinary => {
                    FieldType::Binary
                }
                DataType::Utf8 => FieldType::String,
                DataType::LargeUtf8 => FieldType::Text,
                // DataType::List(_) => {}
                // DataType::FixedSizeList(_, _) => {}
                // DataType::LargeList(_) => {}
                // DataType::Struct(_) => {}
                // DataType::Union(_, _, _) => {}
                // DataType::Dictionary(_, _) => {}
                // DataType::Decimal128(_, _) => {}
                // DataType::Decimal256(_, _) => {}
                // DataType::Map(_, _) => {}
                _ => return Err(FieldTypeNotSupported(field.name().clone())),
            };

            Ok(FieldDefinition {
                name: field.name().clone(),
                typ: mapped_field_type,
                nullable: field.is_nullable(),
                source: SourceDefinition::Dynamic,
            })
        })
        .collect()
}

pub fn map_value_to_dozer_field(
    column: &ArrayRef,
    row: &usize,
    column_name: &str,
) -> Result<DozerField, ObjectStoreSchemaError> {
    match column.data_type() {
        DataType::Null => Ok(DozerField::Null),
        DataType::Boolean => make_from!(array::BooleanArray, column, row),
        DataType::Int8 => make_from!(array::Int8Array, column, row),
        DataType::Int16 => make_from!(array::Int16Array, column, row),
        DataType::Int32 => make_from!(array::Int32Array, column, row),
        DataType::Int64 => make_from!(array::Int64Array, column, row),
        DataType::UInt8 => make_from!(array::UInt8Array, column, row),
        DataType::UInt16 => make_from!(array::UInt16Array, column, row),
        DataType::UInt32 => make_from!(array::UInt32Array, column, row),
        DataType::UInt64 => make_from!(array::UInt64Array, column, row),
        DataType::Float16 => make_from!(array::Float32Array, column, row),
        DataType::Float32 => make_from!(array::Float32Array, column, row),
        DataType::Float64 => make_from!(array::Float64Array, column, row),
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            make_timestamp!(array::TimestampMicrosecondArray, column, row)
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            make_timestamp!(array::TimestampMillisecondArray, column, row)
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            make_timestamp!(array::TimestampNanosecondArray, column, row)
        }
        DataType::Timestamp(TimeUnit::Second, _) => {
            make_timestamp!(array::TimestampSecondArray, column, row)
        }
        DataType::Date32 => make_date!(array::Date32Array, column, row),
        DataType::Date64 => make_date!(array::Date64Array, column, row),
        DataType::Time32(TimeUnit::Millisecond) => {
            make_time!(array::Time32MillisecondArray, column, row)
        }
        DataType::Time32(TimeUnit::Second) => make_time!(array::Time32SecondArray, column, row),
        DataType::Time64(TimeUnit::Microsecond) => {
            make_time!(array::Time64MicrosecondArray, column, row)
        }
        DataType::Time64(TimeUnit::Nanosecond) => {
            make_time!(array::Time64NanosecondArray, column, row)
        }
        DataType::Duration(TimeUnit::Microsecond) => {
            make_duration!(array::DurationMicrosecondArray, column, row)
        }
        DataType::Duration(TimeUnit::Millisecond) => {
            make_duration!(array::DurationMillisecondArray, column, row)
        }
        DataType::Duration(TimeUnit::Nanosecond) => {
            make_duration!(array::DurationNanosecondArray, column, row)
        }
        DataType::Duration(TimeUnit::Second) => {
            make_duration!(array::DurationSecondArray, column, row)
        }
        DataType::Binary => make_binary!(array::BinaryArray, column, row),
        DataType::FixedSizeBinary(_) => make_binary!(array::FixedSizeBinaryArray, column, row),
        DataType::LargeBinary => make_binary!(array::LargeBinaryArray, column, row),
        DataType::Utf8 => {
            let array: Option<&serde_json::Value> =
                column.as_any().downcast_ref::<serde_json::Value>();
            if let Some(r) = array {
                let s: DozerField = if r.is_null() {
                    DozerField::Null
                } else {
                    DozerField::Json(
                        serde_json_to_json_value(r.clone())
                            .map_err(|_| FieldTypeNotSupported(column_name.to_string()))
                            .unwrap(),
                    )
                };
                Ok(s)
            } else {
                make_from!(array::StringArray, column, row)
            }
        }
        DataType::LargeUtf8 => make_text!(array::LargeStringArray, column, row),
        // DataType::Interval(TimeUnit::) => make_from!(array::BooleanArray, x, x0),
        // DataType::List(_) => {}
        // DataType::FixedSizeList(_, _) => {}
        // DataType::LargeList(_) => {}
        // DataType::Struct(_) => {}
        // DataType::Union(_, _, _) => {}
        // DataType::Dictionary(_, _) => {}
        // DataType::Decimal128(_, _) => {}
        // DataType::Decimal256(_, _) => {}
        // DataType::Map(_, _) => {}
        _ => Err(FieldTypeNotSupported(column_name.to_string())),
    }
}
