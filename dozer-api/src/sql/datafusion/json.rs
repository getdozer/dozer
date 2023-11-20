use std::str::FromStr;

use datafusion::arrow::{
    array::{
        as_largestring_array, as_string_array, Array, AsArray, BooleanArray, IntervalDayTimeArray,
        IntervalMonthDayNanoArray, IntervalYearMonthArray,
    },
    datatypes::{
        ArrowPrimitiveType, BinaryType, ByteArrayType, DataType, Decimal128Type, Decimal256Type,
        DecimalType, Float16Type, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type,
        Int8Type, IntervalUnit, LargeBinaryType, UInt16Type, UInt32Type, UInt64Type, UInt8Type,
    },
    record_batch::RecordBatch,
};
use dozer_types::arrow_cast::display::{ArrayFormatter, FormatOptions};
use serde_json::{map::Map, Number, Value};

use crate::sql::util::Iso8601Duration;

pub fn record_batches_to_json_rows(batches: &[RecordBatch]) -> Vec<Map<String, Value>> {
    let mut rows = Vec::new();

    if batches.is_empty() {
        return rows;
    }

    let schema = batches[0].schema();
    let schema = schema.as_ref();

    for batch in batches {
        for row_index in 0..batch.num_rows() {
            let mut row = Map::new();
            for (column_index, column) in batch.columns().iter().enumerate() {
                let value = field_to_json_value(&column, row_index);
                let column_name = schema.field(column_index).name();
                row.insert(column_name.clone(), value);
            }
            rows.push(row)
        }
    }

    rows
}

macro_rules! field {
    ($array:tt[$index:tt], $type:tt) => {
        $array
            .as_any()
            .downcast_ref::<$type>()
            .unwrap()
            .value($index)
    };
}

fn json_number_from_primitive<T>(array: &dyn Array, row_index: usize) -> Value
where
    T: ArrowPrimitiveType,
    T::Native: ToString,
{
    let array = array.as_primitive::<T>();
    Value::Number(Number::from_str(&array.value(row_index).to_string()).unwrap())
}

fn json_string_from_datetime(array: &dyn Array, row_index: usize) -> Value {
    let options = FormatOptions::default();
    let formatter =
        ArrayFormatter::try_new(array, &options).expect("datetime types should be supported");
    Value::String(formatter.value(row_index).to_string())
}

fn json_string_from_utf8(array: &dyn Array, row_index: usize) -> Value {
    let array = as_string_array(array);
    Value::String(array.value(row_index).to_string())
}

fn json_string_from_largeutf8(array: &dyn Array, row_index: usize) -> Value {
    let array = as_largestring_array(array);
    Value::String(array.value(row_index).to_string())
}

fn json_string_from_binary<T: ByteArrayType>(array: &dyn Array, row_index: usize) -> Value {
    let array = array.as_bytes::<T>();
    Value::String(format!("{:?}", array.value(row_index)))
}

fn json_string_from_fixedsizebinary(array: &dyn Array, row_index: usize) -> Value {
    let array = array.as_fixed_size_binary();
    Value::String(format!("{:?}", array.value(row_index)))
}

fn json_number_from_decimal<T>(
    array: &dyn Array,
    row_index: usize,
    precision: u8,
    scale: i8,
) -> Value
where
    T: ArrowPrimitiveType + DecimalType,
{
    let array = array.as_primitive::<T>();
    let value = array.value(row_index);
    Value::String(<T as DecimalType>::format_decimal(value, precision, scale))
}

fn field_to_json_value(column: &dyn Array, row_index: usize) -> Value {
    let i = row_index;
    let value = if column.is_null(i) {
        Value::Null
    } else {
        match column.data_type() {
            DataType::Null => Value::Null,
            DataType::Boolean => Value::Bool(field!(column[i], BooleanArray)),
            DataType::Int8 => json_number_from_primitive::<Int8Type>(column, i),
            DataType::Int16 => json_number_from_primitive::<Int16Type>(column, i),
            DataType::Int32 => json_number_from_primitive::<Int32Type>(column, i),
            DataType::Int64 => json_number_from_primitive::<Int64Type>(column, i),
            DataType::UInt8 => json_number_from_primitive::<UInt8Type>(column, i),
            DataType::UInt16 => json_number_from_primitive::<UInt16Type>(column, i),
            DataType::UInt32 => json_number_from_primitive::<UInt32Type>(column, i),
            DataType::UInt64 => json_number_from_primitive::<UInt64Type>(column, i),
            DataType::Float16 => json_number_from_primitive::<Float16Type>(column, i),
            DataType::Float32 => json_number_from_primitive::<Float32Type>(column, i),
            DataType::Float64 => json_number_from_primitive::<Float64Type>(column, i),

            DataType::Date32
            | DataType::Date64
            | DataType::Timestamp(_, _)
            | DataType::Time32(_)
            | DataType::Time64(_)
            | DataType::Duration(_) => json_string_from_datetime(column, i),

            DataType::Interval(IntervalUnit::DayTime) => Value::String({
                let value = field!(column[i], IntervalDayTimeArray);
                let (days, milliseconds) = (value as i32, (value >> 32) as i32);
                Iso8601Duration::DaysMilliseconds(days, milliseconds).to_string()
            }),
            DataType::Interval(IntervalUnit::MonthDayNano) => Value::String({
                let value = field!(column[i], IntervalMonthDayNanoArray);
                let (months, days, nanoseconds) =
                    (value as i32, (value >> 32) as i32, (value >> 64) as i64);
                Iso8601Duration::MonthsDaysNanoseconds(months, days, nanoseconds).to_string()
            }),
            DataType::Interval(IntervalUnit::YearMonth) => Value::String({
                let months = field!(column[i], IntervalYearMonthArray);
                Iso8601Duration::Months(months).to_string()
            }),

            DataType::Binary => json_string_from_binary::<BinaryType>(column, i),
            DataType::FixedSizeBinary(_) => json_string_from_fixedsizebinary(column, i),
            DataType::LargeBinary => json_string_from_binary::<LargeBinaryType>(column, i),
            DataType::Utf8 => json_string_from_utf8(column, i),
            DataType::LargeUtf8 => json_string_from_largeutf8(column, i),

            DataType::Decimal128(precision, scale) => {
                json_number_from_decimal::<Decimal128Type>(column, i, *precision, *scale)
            }
            DataType::Decimal256(precision, scale) => {
                json_number_from_decimal::<Decimal256Type>(column, i, *precision, *scale)
            }

            DataType::List(_)
            | DataType::FixedSizeList(_, _)
            | DataType::LargeList(_)
            | DataType::Struct(_)
            | DataType::Union(_, _)
            | DataType::Dictionary(_, _)
            | DataType::Map(_, _)
            | DataType::RunEndEncoded(_, _) => unimplemented!(),
        }
    };

    value
}
