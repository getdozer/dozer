use crate::types::{Field, FieldDefinition, FieldType, Record, Schema};
use arrow::datatypes::{self as arrow_types, DataType};
use arrow::{
    array::{self as arrow_array, ArrayRef},
    datatypes::i256,
    record_batch::RecordBatch,
};
use arrow_schema::{
    TimeUnit, DECIMAL128_MAX_PRECISION, DECIMAL128_MAX_SCALE, DECIMAL256_MAX_PRECISION,
};
use std::{collections::HashMap, sync::Arc};

pub const DOZER_SCHEMA_KEY: &str = "dozer_schema";

// Maps a Dozer Schema to an Arrow Schema
pub fn map_to_arrow_schema(
    schema: &crate::types::Schema,
) -> Result<arrow_types::Schema, arrow::error::ArrowError> {
    let mut fields = vec![];
    for fd in &schema.fields {
        let field = arrow_types::Field::from(fd.clone());
        fields.push(field);
    }
    Ok(arrow_types::Schema {
        fields: fields.into(),
        metadata: HashMap::from([(
            DOZER_SCHEMA_KEY.to_string(),
            serde_json::to_string(&schema).expect("Schema can always be serialized as JSON"),
        )]),
    })
}

// Maps a Dozer Record to an Arrow RecordBatch of size 1
// TODO: We can extend this as we build our micro batching support.
// In a micro batch we can send a record batch that is of size > 1
pub fn map_record_to_arrow(
    rec: Record,
    schema: &Schema,
) -> Result<RecordBatch, arrow::error::ArrowError> {
    let mut columns = vec![];

    for (idx, f) in rec.values.iter().enumerate() {
        let fd = schema.fields.get(idx).unwrap();
        let column = match (f, fd.typ) {
            (Field::UInt(v), FieldType::UInt) => {
                Arc::new(arrow_array::UInt64Array::from_iter_values([*v])) as ArrayRef
            }
            (Field::Null, FieldType::UInt) => {
                Arc::new(arrow_array::UInt64Array::from(vec![None as Option<u64>])) as ArrayRef
            }
            (Field::Int(v), FieldType::Int) => {
                Arc::new(arrow_array::Int64Array::from_iter_values([*v])) as ArrayRef
            }
            (Field::Null, FieldType::Int) => {
                Arc::new(arrow_array::Int64Array::from(vec![None as Option<i64>])) as ArrayRef
            }
            (Field::Float(v), FieldType::Float) => {
                Arc::new(arrow_array::Float64Array::from_iter_values([**v])) as ArrayRef
            }
            (Field::Null, FieldType::Float) => {
                Arc::new(arrow_array::Float64Array::from(vec![None as Option<f64>])) as ArrayRef
            }
            (Field::Boolean(v), FieldType::Boolean) => {
                Arc::new(arrow_array::BooleanArray::from(vec![*v])) as ArrayRef
            }
            (Field::Null, FieldType::Boolean) => {
                Arc::new(arrow_array::BooleanArray::from(vec![None as Option<bool>])) as ArrayRef
            }
            (Field::String(v), FieldType::String) => {
                Arc::new(arrow_array::StringArray::from_iter_values([v])) as ArrayRef
            }
            (Field::Null, FieldType::String) => {
                Arc::new(arrow_array::StringArray::from(vec![None as Option<String>])) as ArrayRef
            }
            (Field::Text(v), FieldType::Text) => {
                Arc::new(arrow_array::LargeStringArray::from_iter_values([v])) as ArrayRef
            }
            (Field::Null, FieldType::Text) => Arc::new(arrow_array::LargeStringArray::from(vec![
                None as Option<String>,
            ])) as ArrayRef,
            (Field::Decimal(v), FieldType::Decimal) => arrow_cast::cast(
                &arrow_array::Decimal128Array::from(vec![v.mantissa()])
                    .with_precision_and_scale(DECIMAL128_MAX_PRECISION, v.scale() as i8)?,
                &DataType::Decimal256(DECIMAL256_MAX_PRECISION, DECIMAL128_MAX_SCALE),
            )?,
            (Field::Null, FieldType::Decimal) => Arc::new(arrow_array::Decimal256Array::from(vec![
                None as Option<i256>,
            ])) as ArrayRef,
            (Field::Timestamp(v), FieldType::Timestamp) => {
                Arc::new(arrow_array::TimestampNanosecondArray::from_iter_values([{
                    v.timestamp_nanos_opt().expect(
                        "value can not be represented in a timestamp with nanosecond precision.",
                    )
                }])) as ArrayRef
            }
            (Field::Null, FieldType::Timestamp) => {
                Arc::new(arrow_array::TimestampNanosecondArray::from(vec![
                    None as Option<i64>,
                ])) as ArrayRef
            }
            (Field::Date(v), FieldType::Date) => {
                let d = v.and_hms_milli_opt(0, 0, 0, 0).unwrap();
                Arc::new(arrow_array::Date64Array::from_iter_values([
                    d.timestamp_millis()
                ])) as ArrayRef
            }
            (Field::Null, FieldType::Date) => {
                Arc::new(arrow_array::Date64Array::from(vec![None as Option<i64>])) as ArrayRef
            }
            (Field::Binary(v), FieldType::Binary) => {
                Arc::new(arrow_array::BinaryArray::from_iter_values([v])) as ArrayRef
            }
            (Field::Json(v), FieldType::Json) => Arc::new(
                arrow_array::StringArray::from_iter_values([format!("{v:?}")]),
            ) as ArrayRef,
            (Field::Null, FieldType::Json) => {
                Arc::new(arrow_array::StringArray::from(vec![None as Option<String>])) as ArrayRef
            }
            (Field::Point(v), FieldType::Point) => {
                Arc::new(arrow_array::BinaryArray::from_iter_values([v.to_bytes()])) as ArrayRef
            }
            (Field::Null, FieldType::Point) => {
                Arc::new(arrow_array::BinaryArray::from_opt_vec(vec![
                    None as Option<&[u8]>,
                ])) as ArrayRef
            }
            (Field::Duration(d), FieldType::Duration) => {
                Arc::new(arrow_array::DurationNanosecondArray::from_iter_values([
                    d.0.as_nanos() as i64,
                ])) as ArrayRef
            }
            (Field::Null, FieldType::Duration) => {
                Arc::new(arrow_array::BinaryArray::from_opt_vec(vec![
                    None as Option<&[u8]>,
                ])) as ArrayRef
            }
            (a, b) => Err(arrow::error::ArrowError::InvalidArgumentError(format!(
                "Invalid field type {b:?} for the field: {a:?}",
            )))?,
        };
        columns.push(column);
    }

    let schema = map_to_arrow_schema(schema).unwrap();
    RecordBatch::try_new(Arc::new(schema), columns)
}

// Maps the dozer field type to the arrow data type
// Optionally takes a metadata map to add additional metadata to the field

pub fn map_field_type(typ: FieldType) -> DataType {
    match typ {
        FieldType::UInt => DataType::UInt64,
        FieldType::U128 => DataType::Utf8,
        FieldType::Int => DataType::Int64,
        FieldType::Int8 => DataType::Int64,
        FieldType::I128 => DataType::Utf8,
        FieldType::Float => DataType::Float64,
        FieldType::Boolean => DataType::Boolean,
        FieldType::String => DataType::Utf8,
        FieldType::Text => DataType::LargeUtf8,
        FieldType::Decimal => DataType::Decimal256(DECIMAL256_MAX_PRECISION, DECIMAL128_MAX_SCALE),
        FieldType::Timestamp => DataType::Timestamp(arrow_types::TimeUnit::Nanosecond, None),
        FieldType::Date => DataType::Date64,
        FieldType::Binary => DataType::Binary,
        FieldType::Json => DataType::Utf8,
        FieldType::Point => DataType::Binary,
        FieldType::Duration => DataType::Duration(TimeUnit::Nanosecond),
    }
}

impl From<FieldDefinition> for arrow_types::Field {
    fn from(f: FieldDefinition) -> Self {
        let dt = map_field_type(f.typ);
        arrow_types::Field::new(f.name, dt, f.nullable)
    }
}
