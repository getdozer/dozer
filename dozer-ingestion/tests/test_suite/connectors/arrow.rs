use std::sync::Arc;

use dozer_types::arrow::array::{
    Time32MillisecondArray, Time32SecondArray, Time64MicrosecondArray, Time64NanosecondArray,
};

use dozer_types::{
    arrow,
    chrono::Datelike,
    types::{Field, FieldDefinition, FieldType},
};

use crate::test_suite::FieldsAndPk;

pub fn record_batch_with_all_supported_data_types() -> arrow::record_batch::RecordBatch {
    use arrow::datatypes::{DataType, Field, TimeUnit};

    let schema = arrow::datatypes::Schema::new(vec![
        Field::new("bool", DataType::Boolean, false),
        Field::new("bool_null", DataType::Boolean, true),
        Field::new("int8", DataType::Int8, false),
        Field::new("int8_null", DataType::Int8, true),
        Field::new("int16", DataType::Int16, false),
        Field::new("int16_null", DataType::Int16, true),
        Field::new("int32", DataType::Int32, false),
        Field::new("int32_null", DataType::Int32, true),
        Field::new("int64", DataType::Int64, false),
        Field::new("int64_null", DataType::Int64, true),
        Field::new("uint8", DataType::UInt8, false),
        Field::new("uint8_null", DataType::UInt8, true),
        Field::new("uint16", DataType::UInt16, false),
        Field::new("uint16_null", DataType::UInt16, true),
        Field::new("uint32", DataType::UInt32, false),
        Field::new("uint32_null", DataType::UInt32, true),
        Field::new("uint64", DataType::UInt64, false),
        Field::new("uint64_null", DataType::UInt64, true),
        // `Float16` not supported by parquet writer.
        // Field::new("float16", DataType::Float16, false),
        // Field::new("float16_null", DataType::Float16, true),
        Field::new("float32", DataType::Float32, false),
        Field::new("float32_null", DataType::Float32, true),
        Field::new("float64", DataType::Float64, false),
        Field::new("float64_null", DataType::Float64, true),
        Field::new(
            "timestamp_second",
            DataType::Timestamp(TimeUnit::Second, None),
            false,
        ),
        Field::new(
            "timestamp_second_null",
            DataType::Timestamp(TimeUnit::Second, None),
            true,
        ),
        Field::new(
            "timestamp_millisecond",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new(
            "timestamp_millisecond_null",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            true,
        ),
        Field::new(
            "timestamp_microsecond",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
        Field::new(
            "timestamp_microsecond_null",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
        Field::new(
            "timestamp_nanosecond",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new(
            "timestamp_nanosecond_null",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        ),
        Field::new("date32", DataType::Date32, false),
        Field::new("date32_null", DataType::Date32, true),
        Field::new("date64", DataType::Date64, false),
        Field::new("date64_null", DataType::Date64, true),
        Field::new("time32_second", DataType::Time32(TimeUnit::Second), false),
        Field::new(
            "time32_second_null",
            DataType::Time32(TimeUnit::Second),
            true,
        ),
        Field::new(
            "time32_millisecond",
            DataType::Time32(TimeUnit::Millisecond),
            false,
        ),
        Field::new(
            "time32_millisecond_null",
            DataType::Time32(TimeUnit::Millisecond),
            true,
        ),
        Field::new(
            "time64_microsecond",
            DataType::Time64(TimeUnit::Microsecond),
            false,
        ),
        Field::new(
            "time64_microsecond_null",
            DataType::Time64(TimeUnit::Microsecond),
            true,
        ),
        Field::new(
            "time64_nanosecond",
            DataType::Time64(TimeUnit::Nanosecond),
            false,
        ),
        Field::new(
            "time64_nanosecond_null",
            DataType::Time64(TimeUnit::Nanosecond),
            true,
        ),
        // `Duration` not supported by parquet writer.
        // Field::new(
        //     "duration_second",
        //     DataType::Duration(TimeUnit::Second),
        //     false,
        // ),
        // Field::new(
        //     "duration_second_null",
        //     DataType::Duration(TimeUnit::Second),
        //     true,
        // ),
        // Field::new(
        //     "duration_millisecond",
        //     DataType::Duration(TimeUnit::Millisecond),
        //     false,
        // ),
        // Field::new(
        //     "duration_millisecond_null",
        //     DataType::Duration(TimeUnit::Millisecond),
        //     true,
        // ),
        // Field::new(
        //     "duration_microsecond",
        //     DataType::Duration(TimeUnit::Microsecond),
        //     false,
        // ),
        // Field::new(
        //     "duration_microsecond_null",
        //     DataType::Duration(TimeUnit::Microsecond),
        //     true,
        // ),
        // Field::new(
        //     "duration_nanosecond",
        //     DataType::Duration(TimeUnit::Nanosecond),
        //     false,
        // ),
        // Field::new(
        //     "duration_nanosecond_null",
        //     DataType::Duration(TimeUnit::Nanosecond),
        //     true,
        // ),
        Field::new("binary", DataType::Binary, false),
        Field::new("binary_null", DataType::Binary, true),
        Field::new("fixed_size_binary_1", DataType::FixedSizeBinary(1), false),
        Field::new(
            "fixed_size_binary_1_null",
            DataType::FixedSizeBinary(1),
            true,
        ),
        Field::new("large_binary", DataType::LargeBinary, false),
        Field::new("large_binary_null", DataType::LargeBinary, true),
        Field::new("utf8", DataType::Utf8, false),
        Field::new("utf8_null", DataType::Utf8, true),
        Field::new("large_utf8", DataType::LargeUtf8, false),
        Field::new("large_utf8_null", DataType::LargeUtf8, true),
    ]);

    use arrow::array::{
        Array, BinaryArray, BooleanArray, Date32Array, Date64Array, FixedSizeBinaryArray,
        Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array,
        LargeBinaryArray, LargeStringArray, StringArray, TimestampMicrosecondArray,
        TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt16Array,
        UInt32Array, UInt64Array, UInt8Array,
    };

    let columns: Vec<Arc<dyn Array>> = vec![
        Arc::new(BooleanArray::from_iter([Some(true), Some(false)])),
        Arc::new(BooleanArray::from_iter([Some(true), None])),
        Arc::new(Int8Array::from_iter_values([0, 1])),
        Arc::new(Int8Array::from_iter([Some(0), None])),
        Arc::new(Int16Array::from_iter_values([0, 1])),
        Arc::new(Int16Array::from_iter([Some(0), None])),
        Arc::new(Int32Array::from_iter_values([0, 1])),
        Arc::new(Int32Array::from_iter([Some(0), None])),
        Arc::new(Int64Array::from_iter_values([0, 1])),
        Arc::new(Int64Array::from_iter([Some(0), None])),
        Arc::new(UInt8Array::from_iter_values([0, 1])),
        Arc::new(UInt8Array::from_iter([Some(0), None])),
        Arc::new(UInt16Array::from_iter_values([0, 1])),
        Arc::new(UInt16Array::from_iter([Some(0), None])),
        Arc::new(UInt32Array::from_iter_values([0, 1])),
        Arc::new(UInt32Array::from_iter([Some(0), None])),
        Arc::new(UInt64Array::from_iter_values([0, 1])),
        Arc::new(UInt64Array::from_iter([Some(0), None])),
        // Arc::new(Float16Array::from_iter_values([0i8.into(), 1i8.into()])),
        // Arc::new(Float16Array::from_iter([Some(0i8.into()), None])),
        Arc::new(Float32Array::from_iter_values([0.0, 1.0])),
        Arc::new(Float32Array::from_iter([Some(0.0), None])),
        Arc::new(Float64Array::from_iter_values([0.0, 1.0])),
        Arc::new(Float64Array::from_iter([Some(0.0), None])),
        Arc::new(TimestampSecondArray::from_iter_values([0, 1])),
        Arc::new(TimestampSecondArray::from_iter([Some(0), None])),
        Arc::new(TimestampMillisecondArray::from_iter_values([0, 1])),
        Arc::new(TimestampMillisecondArray::from_iter([Some(0), None])),
        Arc::new(TimestampMicrosecondArray::from_iter_values([0, 1])),
        Arc::new(TimestampMicrosecondArray::from_iter([Some(0), None])),
        Arc::new(TimestampNanosecondArray::from_iter_values([0, 1])),
        Arc::new(TimestampNanosecondArray::from_iter([Some(0), None])),
        Arc::new(Date32Array::from_iter_values([0, 1])),
        Arc::new(Date32Array::from_iter([Some(0), None])),
        Arc::new(Date64Array::from_iter_values([0, 1])),
        Arc::new(Date64Array::from_iter([Some(0), None])),
        Arc::new(Time32SecondArray::from_iter_values([0, 1])),
        Arc::new(Time32SecondArray::from_iter([Some(0), None])),
        Arc::new(Time32MillisecondArray::from_iter_values([0, 1])),
        Arc::new(Time32MillisecondArray::from_iter([Some(0), None])),
        Arc::new(Time64MicrosecondArray::from_iter_values([0, 1])),
        Arc::new(Time64MicrosecondArray::from_iter([Some(0), None])),
        Arc::new(Time64NanosecondArray::from_iter_values([0, 1])),
        Arc::new(Time64NanosecondArray::from_iter([Some(0), None])),
        // Arc::new(DurationSecondArray::from_iter_values([0, 1])),
        // Arc::new(DurationSecondArray::from_iter([Some(0), None])),
        // Arc::new(DurationMillisecondArray::from_iter_values([0, 1])),
        // Arc::new(DurationMillisecondArray::from_iter([Some(0), None])),
        // Arc::new(DurationMicrosecondArray::from_iter_values([0, 1])),
        // Arc::new(DurationMicrosecondArray::from_iter([Some(0), None])),
        // Arc::new(DurationNanosecondArray::from_iter_values([0, 1])),
        // Arc::new(DurationNanosecondArray::from_iter([Some(0), None])),
        // Arc::new(IntervalMonthDayNanoArray::from_iter_values([0, 1])),
        // Arc::new(IntervalMonthDayNanoArray::from_iter([Some(0), None])),
        Arc::new(BinaryArray::from_iter_values([
            [].as_slice(),
            [1].as_slice(),
        ])),
        Arc::new(BinaryArray::from_iter([Some([]), None])),
        Arc::new(FixedSizeBinaryArray::try_from_iter([[0], [1]].into_iter()).unwrap()),
        Arc::new(
            FixedSizeBinaryArray::try_from_sparse_iter_with_size([Some([0]), None].into_iter(), 1)
                .unwrap(),
        ),
        Arc::new(LargeBinaryArray::from_iter_values([
            [].as_slice(),
            [1].as_slice(),
        ])),
        Arc::new(LargeBinaryArray::from_iter([Some([]), None])),
        Arc::new(StringArray::from_iter_values(["", "1"])),
        Arc::new(StringArray::from_iter([Some(""), None])),
        Arc::new(LargeStringArray::from_iter_values(["", "1"])),
        Arc::new(LargeStringArray::from_iter([Some(""), None])),
    ];

    arrow::record_batch::RecordBatch::try_new(Arc::new(schema), columns)
        .expect("BUG in record_batch_with_all_supported_data_types")
}

fn field_type_to_arrow(field_type: FieldType) -> Option<arrow::datatypes::DataType> {
    match field_type {
        FieldType::UInt => Some(arrow::datatypes::DataType::UInt64),
        FieldType::U128 => None,
        FieldType::Int => Some(arrow::datatypes::DataType::Int64),
        FieldType::I128 => None,
        FieldType::Float => Some(arrow::datatypes::DataType::Float64),
        FieldType::Boolean => Some(arrow::datatypes::DataType::Boolean),
        FieldType::String => Some(arrow::datatypes::DataType::Utf8),
        FieldType::Text => Some(arrow::datatypes::DataType::LargeUtf8),
        FieldType::Binary => Some(arrow::datatypes::DataType::LargeBinary),
        FieldType::Decimal => None,
        FieldType::Timestamp => Some(arrow::datatypes::DataType::Timestamp(
            arrow::datatypes::TimeUnit::Nanosecond,
            None,
        )),
        FieldType::Date => Some(arrow::datatypes::DataType::Date32),
        FieldType::Bson => None,
        FieldType::Point => None,
        FieldType::Duration => Some(arrow::datatypes::DataType::Duration(
            arrow::datatypes::TimeUnit::Nanosecond,
        )),
    }
}

fn field_definition_to_arrow(field_definition: FieldDefinition) -> Option<arrow::datatypes::Field> {
    field_type_to_arrow(field_definition.typ).map(|data_type| {
        arrow::datatypes::Field::new(field_definition.name, data_type, field_definition.nullable)
    })
}

pub fn schema_to_arrow(fields: Vec<FieldDefinition>) -> (arrow::datatypes::Schema, FieldsAndPk) {
    let arrow_fields = fields
        .iter()
        .cloned()
        .filter_map(field_definition_to_arrow)
        .collect();
    let arrow_schema = arrow::datatypes::Schema::new(arrow_fields);

    let fields = fields
        .into_iter()
        .filter_map(|field| field_type_to_arrow(field.typ).map(|_| field))
        .collect();

    (arrow_schema, (fields, vec![]))
}

fn fields_to_arrow<'a, F: IntoIterator<Item = &'a Field>>(
    fields: F,
    count: usize,
    field_type: FieldType,
) -> Arc<dyn arrow::array::Array> {
    match field_type {
        FieldType::UInt => {
            let mut builder = arrow::array::UInt64Array::builder(count);
            for field in fields {
                match field {
                    Field::UInt(value) => builder.append_value(*value),
                    Field::Null => builder.append_null(),
                    _ => panic!("Unexpected field type"),
                }
            }
            Arc::new(builder.finish())
        }
        FieldType::U128 => panic!("Unexpected field type"),
        FieldType::Int => {
            let mut builder = arrow::array::Int64Array::builder(count);
            for field in fields {
                match field {
                    Field::Int(value) => builder.append_value(*value),
                    Field::Null => builder.append_null(),
                    _ => panic!("Unexpected field type"),
                }
            }
            Arc::new(builder.finish())
        }
        FieldType::I128 => panic!("Unexpected field type"),
        FieldType::Float => {
            let mut builder = arrow::array::Float64Array::builder(count);
            for field in fields {
                match field {
                    Field::Float(value) => builder.append_value(value.0),
                    Field::Null => builder.append_null(),
                    _ => panic!("Unexpected field type"),
                }
            }
            Arc::new(builder.finish())
        }
        FieldType::Boolean => {
            let mut builder = arrow::array::BooleanArray::builder(count);
            for field in fields {
                match field {
                    Field::Boolean(value) => builder.append_value(*value),
                    Field::Null => builder.append_null(),
                    _ => panic!("Unexpected field type"),
                }
            }
            Arc::new(builder.finish())
        }
        FieldType::String => {
            let mut builder = arrow::array::StringBuilder::new();
            for field in fields {
                match field {
                    Field::String(value) => builder.append_value(value),
                    Field::Null => builder.append_null(),
                    _ => panic!("Unexpected field type"),
                }
            }
            Arc::new(builder.finish())
        }
        FieldType::Text => {
            let mut builder = arrow::array::LargeStringBuilder::new();
            for field in fields {
                match field {
                    Field::Text(value) => builder.append_value(value),
                    Field::Null => builder.append_null(),
                    _ => panic!("Unexpected field type"),
                }
            }
            Arc::new(builder.finish())
        }
        FieldType::Binary => {
            let mut builder = arrow::array::LargeBinaryBuilder::new();
            for field in fields {
                match field {
                    Field::Binary(value) => builder.append_value(value),
                    Field::Null => builder.append_null(),
                    _ => panic!("Unexpected field type"),
                }
            }
            Arc::new(builder.finish())
        }
        FieldType::Decimal => panic!("Decimal not supported"),
        FieldType::Timestamp => {
            let mut builder = arrow::array::TimestampNanosecondArray::builder(count);
            for field in fields {
                match field {
                    Field::Timestamp(value) => builder.append_value(value.timestamp_nanos()),
                    Field::Null => builder.append_null(),
                    _ => panic!("Unexpected field type"),
                }
            }
            Arc::new(builder.finish())
        }
        FieldType::Date => {
            let mut builder = arrow::array::Date32Array::builder(count);
            for field in fields {
                match field {
                    Field::Date(value) => builder.append_value(value.num_days_from_ce()),
                    Field::Null => builder.append_null(),
                    _ => panic!("Unexpected field type"),
                }
            }
            Arc::new(builder.finish())
        }
        FieldType::Bson => panic!("Bson not supported"),
        FieldType::Point => panic!("Point not supported"),
        FieldType::Duration => {
            let mut builder = arrow::array::DurationNanosecondArray::builder(count);
            for field in fields {
                match field {
                    Field::Duration(value) => builder.append_value(value.0.as_nanos() as i64),
                    Field::Null => builder.append_null(),
                    _ => panic!("Unexpected field type"),
                }
            }
            Arc::new(builder.finish())
        }
    }
}

pub fn records_to_arrow(
    records: &[Vec<Field>],
    fields: Vec<FieldDefinition>,
) -> arrow::record_batch::RecordBatch {
    let mut columns = vec![];
    for (index, field) in fields.iter().enumerate() {
        if field_type_to_arrow(field.typ).is_some() {
            let fields = records.iter().map(|record| &record[index]);
            let column = fields_to_arrow(fields, records.len(), field.typ);
            columns.push(column);
        }
    }

    let (schema, _) = schema_to_arrow(fields);

    arrow::record_batch::RecordBatch::try_new(Arc::new(schema), columns)
        .expect("BUG in records_to_arrow")
}
