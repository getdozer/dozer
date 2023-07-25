use super::errors::FromArrowError;
use super::errors::FromArrowError::DateConversionError;
use super::errors::FromArrowError::DateTimeConversionError;
use super::errors::FromArrowError::DurationConversionError;
use super::errors::FromArrowError::FieldTypeNotSupported;
use super::errors::FromArrowError::TimeConversionError;
use super::to_arrow;
use crate::arrow_types::to_arrow::DOZER_SCHEMA_KEY;
use crate::json_types::JsonValue;
use crate::types::{
    Field as DozerField, FieldDefinition, FieldType, Record, Schema as DozerSchema, Schema,
    SourceDefinition,
};
use arrow::array;
use arrow::array::ArrayAccessor;
use arrow::array::{Array, ArrayRef};
use arrow::datatypes::{DataType, TimeUnit};
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use arrow::row::SortField;

use crate::arrow_types::errors::FromArrowError::DeserializationError;
use log::error;
use std::str::FromStr;
use std::sync::Arc;

fn make_from<A: Array + 'static>(column: &Arc<dyn Array>, row: usize) -> DozerField
where
    for<'a> &'a A: ArrayAccessor,
    for<'a> DozerField: From<<&'a A as ArrayAccessor>::Item>,
{
    let array = column.as_any().downcast_ref::<A>();

    if let Some(r) = array {
        if r.is_null(row) {
            DozerField::Null
        } else {
            DozerField::from(r.value(row))
        }
    } else {
        DozerField::Null
    }
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

fn make_json(column: &ArrayRef, row: usize) -> Result<DozerField, FromArrowError> {
    let array = column.as_any().downcast_ref::<array::StringArray>();

    return if let Some(r) = array {
        let s: DozerField = if r.is_null(row) {
            DozerField::Null
        } else {
            match JsonValue::from_str(r.value(row)) {
                Ok(j) => DozerField::Json(j),
                Err(e) => return Err(DeserializationError(e)),
            }
        };
        Ok(s)
    } else {
        Ok(DozerField::Null)
    };
}

pub fn map_schema_to_dozer(
    schema: &arrow::datatypes::Schema,
) -> Result<DozerSchema, FromArrowError> {
    match schema.metadata.get(DOZER_SCHEMA_KEY) {
        Some(schema_val) => match serde_json::from_str(schema_val.as_str()) {
            Ok(s) => Ok(s),
            Err(e) => {
                error!("Dozer schema deserialization error {}", e.to_string());
                handle_with_dozer_schema(schema)
            }
        },
        None => handle_with_dozer_schema(schema),
    }
}

fn handle_with_dozer_schema(
    schema: &arrow::datatypes::Schema,
) -> Result<DozerSchema, FromArrowError> {
    let mut fields = vec![];
    for field in schema.fields() {
        let typ = map_arrow_to_dozer_type(field.data_type())?;

        fields.push(FieldDefinition {
            name: field.name().clone(),
            typ,
            nullable: field.is_nullable(),
            source: SourceDefinition::Dynamic,
        });
    }

    Ok(DozerSchema {
        fields,
        primary_index: vec![],
    })
}

pub fn map_arrow_to_dozer_type(dt: &DataType) -> Result<FieldType, FromArrowError> {
    match dt {
        DataType::Boolean => Ok(FieldType::Boolean),
        DataType::Time32(_)
        | DataType::Time64(_)
        | DataType::Duration(_)
        | DataType::Interval(_)
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64 => Ok(FieldType::Int),
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
            Ok(FieldType::UInt)
        }
        DataType::Float16 | DataType::Float32 | DataType::Float64 => Ok(FieldType::Float),
        DataType::Timestamp(_, _) => Ok(FieldType::Timestamp),
        DataType::Date32 | DataType::Date64 => Ok(FieldType::Date),
        DataType::Binary | DataType::FixedSizeBinary(_) | DataType::LargeBinary => {
            Ok(FieldType::Binary)
        }
        DataType::Utf8 => Ok(FieldType::String),
        DataType::LargeUtf8 => Ok(FieldType::Text),
        // DataType::List(_) => {}
        // DataType::FixedSizeList(_, _) => {}
        // DataType::LargeList(_) => {}
        // DataType::Struct(_) => {}
        // DataType::Union(_, _, _) => {}
        // DataType::Dictionary(_, _) => {}
        // DataType::Decimal128(_, _) => {}
        // DataType::Decimal256(_, _) => {}
        _ => Err(FieldTypeNotSupported(format!("{dt:?}"))),
    }
}

pub fn map_value_to_dozer_field(
    column: &ArrayRef,
    row: usize,
    column_name: &str,
    schema: &Schema,
) -> Result<DozerField, FromArrowError> {
    match column.data_type() {
        DataType::Null => Ok(DozerField::Null),
        DataType::Boolean => Ok(make_from::<array::BooleanArray>(column, row)),
        DataType::Int8 => Ok(make_from::<array::Int8Array>(column, row)),
        DataType::Int16 => Ok(make_from::<array::Int16Array>(column, row)),
        DataType::Int32 => Ok(make_from::<array::Int32Array>(column, row)),
        DataType::Int64 => Ok(make_from::<array::Int64Array>(column, row)),
        DataType::UInt8 => Ok(make_from::<array::UInt8Array>(column, row)),
        DataType::UInt16 => Ok(make_from::<array::UInt16Array>(column, row)),
        DataType::UInt32 => Ok(make_from::<array::UInt32Array>(column, row)),
        DataType::UInt64 => Ok(make_from::<array::UInt64Array>(column, row)),
        DataType::Float16 => Ok(make_from::<array::Float32Array>(column, row)),
        DataType::Float32 => Ok(make_from::<array::Float32Array>(column, row)),
        DataType::Float64 => Ok(make_from::<array::Float64Array>(column, row)),
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
            for fd in schema.fields.clone().into_iter() {
                if fd.name == *column_name && fd.typ == FieldType::Json {
                    return make_json(column, row);
                }
            }
            Ok(make_from::<array::StringArray>(column, row))
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

pub fn map_record_batch_to_dozer_records(
    batch: arrow::record_batch::RecordBatch,
    schema: &DozerSchema,
) -> Result<Vec<Record>, FromArrowError> {
    if schema.fields.len() != batch.num_columns() {
        return Err(FromArrowError::SchemaMismatchError(
            schema.fields.len(),
            batch.num_columns(),
        ));
    }
    let mut records = Vec::new();
    let columns = batch.columns();
    let batch_schema = batch.schema();
    let dozer_schema = map_schema_to_dozer(&batch_schema)?;
    let mut sort_fields = vec![];
    for x in schema.fields.iter() {
        let dt = to_arrow::map_field_type(x.typ);
        sort_fields.push(SortField::new(dt));
    }
    let num_rows = batch.num_rows();

    for r in 0..num_rows {
        let mut values = vec![];
        for (c, x) in columns.iter().enumerate() {
            let field = schema.fields.get(c).unwrap();
            let value = map_value_to_dozer_field(x, r, &field.name, &dozer_schema)?;
            values.push(value);
        }
        records.push(Record {
            values,
            lifetime: None,
        });
    }

    Ok(records)
}

pub fn serialize_record_batch(record: &RecordBatch) -> Vec<u8> {
    let buffer: Vec<u8> = Vec::new();
    let mut stream_writer = StreamWriter::try_new(buffer, &record.schema()).unwrap();
    stream_writer.write(record).unwrap();
    stream_writer.finish().unwrap();
    stream_writer.into_inner().unwrap()
}
