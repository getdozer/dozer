#![allow(clippy::redundant_closure_call)]
use crate::errors::QueryError;

use chrono_tz::{Tz, UTC};
use clickhouse_rs::types::column::ColumnFrom;
use clickhouse_rs::{Block, ClientHandle};
use dozer_types::chrono::{DateTime, FixedOffset, Offset, TimeZone};
use dozer_types::ordered_float::OrderedFloat;
use dozer_types::rust_decimal::{self};
use dozer_types::serde_json;
use dozer_types::types::{Field, FieldDefinition, FieldType};
use either::Either;

use clickhouse_rs::types::{FromSql, Query, Value, ValueRef};

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

fn extract_last_column<Mapper, MapperReturn>(
    rows: &mut [Vec<Field>],
    mut mapper: Mapper,
) -> Result<Vec<MapperReturn>, QueryError>
where
    Mapper: FnMut(Field) -> Result<MapperReturn, QueryError>,
{
    rows.iter_mut()
        .map(|row| mapper(row.pop().expect("must still have column")))
        .collect()
}

fn make_nullable_mapper<Mapper, MapperReturn>(
    mut mapper: Mapper,
) -> impl FnMut(Field) -> Result<Option<MapperReturn>, QueryError>
where
    Mapper: FnMut(Field) -> Result<MapperReturn, QueryError>,
{
    move |field| {
        if matches!(field, Field::Null) {
            Ok(None)
        } else {
            mapper(field).map(Some)
        }
    }
}

/// This is a closure that takes a generic parameter,
/// like C++'s templated labmda, which Rust doesn't support.
///
/// Saves 4 parameters at every call site.
struct AddLastColumn<'a> {
    block: Block<clickhouse_rs::Simple>,
    name: &'a str,
    rows: &'a mut [Vec<Field>],
    nullable: bool,
}

impl<'a> AddLastColumn<'a> {
    fn call<Mapper, MapperReturn>(
        self,
        mapper: Mapper,
    ) -> Result<Block<clickhouse_rs::Simple>, QueryError>
    where
        Vec<MapperReturn>: ColumnFrom,
        Vec<Option<MapperReturn>>: ColumnFrom,
        Mapper: FnMut(Field) -> Result<MapperReturn, QueryError>,
    {
        Ok(if self.nullable {
            self.block.column(
                self.name,
                extract_last_column(self.rows, make_nullable_mapper(mapper))?,
            )
        } else {
            self.block
                .column(self.name, extract_last_column(self.rows, mapper)?)
        })
    }
}

fn add_last_column_to_block(
    block: Block<clickhouse_rs::Simple>,
    name: &str,
    rows: &mut [Vec<Field>],
    field_type: FieldType,
    nullable: bool,
) -> Result<Block<clickhouse_rs::Simple>, QueryError> {
    let make_error = || QueryError::TypeMismatch {
        field_name: name.to_string(),
        field_type,
    };

    macro_rules! trivial_mapper {
        ($field_type:path) => {
            |field| match field {
                $field_type(value) => Ok(value),
                _ => Err(make_error()),
            }
        };
    }

    let add_last_column = AddLastColumn {
        block,
        name,
        rows,
        nullable,
    };

    match field_type {
        FieldType::UInt => add_last_column.call(trivial_mapper!(Field::UInt)),
        FieldType::U128 => add_last_column.call(trivial_mapper!(Field::U128)),
        FieldType::Int => add_last_column.call(trivial_mapper!(Field::Int)),
        FieldType::Int8 => add_last_column.call(trivial_mapper!(Field::Int8)),
        FieldType::I128 => add_last_column.call(trivial_mapper!(Field::I128)),
        FieldType::Boolean => add_last_column.call(trivial_mapper!(Field::Boolean)),
        FieldType::Float => add_last_column.call(|field| match field {
            Field::Float(value) => Ok(value.0),
            _ => Err(make_error()),
        }),
        FieldType::String => add_last_column.call(trivial_mapper!(Field::String)),
        FieldType::Text => add_last_column.call(trivial_mapper!(Field::Text)),
        FieldType::Binary => add_last_column.call(trivial_mapper!(Field::Binary)),
        FieldType::Decimal => add_last_column.call(|field| match field {
            Field::Decimal(value) => {
                // This is hardcoded in `clickhouse-rs`.
                if value.scale() > 18 {
                    return Err(QueryError::DecimalOverflow);
                }
                let mantissa: i64 = value
                    .mantissa()
                    .try_into()
                    .map_err(|_| QueryError::DecimalOverflow)?;
                Ok(clickhouse_rs::types::Decimal::new(
                    mantissa,
                    value.scale() as u8,
                ))
            }
            _ => Err(make_error()),
        }),
        FieldType::Timestamp => add_last_column.call(|field| match field {
            Field::Timestamp(value) => Ok(value.with_timezone(&UTC)),
            _ => Err(make_error()),
        }),
        FieldType::Date => add_last_column.call(trivial_mapper!(Field::Date)),
        FieldType::Json => add_last_column.call(|field| match field {
            Field::Json(value) => Ok(dozer_types::json_types::json_to_bytes(&value)),
            _ => Err(make_error()),
        }),
        other => Err(QueryError::UnsupportedFieldType(other)),
    }
}

pub async fn insert_multi(
    mut client: ClientHandle,
    table_name: &str,
    fields: &[FieldDefinition],
    mut rows: Vec<Vec<Field>>,
    query_id: Option<String>,
) -> Result<(), QueryError> {
    let mut block = Block::<clickhouse_rs::Simple>::new();
    for field in fields.iter().rev() {
        block = add_last_column_to_block(block, &field.name, &mut rows, field.typ, field.nullable)?;
    }

    let query_id = query_id.unwrap_or("".to_string());

    let table = Query::new(table_name).id(query_id);

    // Insert the block into the table
    client.insert(table, block).await?;

    Ok(())
}

mod tests {
    #[test]
    fn test_add_last_column_to_block() {
        use super::*;
        use dozer_types::rust_decimal::prelude::ToPrimitive;
        let dozer_decimal = dozer_types::rust_decimal::Decimal::new(123, 10);
        let mut rows = vec![vec![
            Field::Null,
            Field::Text("text".to_string()),
            Field::Decimal(dozer_decimal),
        ]];
        let mut block = Block::<clickhouse_rs::Simple>::new();
        block = add_last_column_to_block(block, "decimal", &mut rows, FieldType::Decimal, false)
            .unwrap();
        block = add_last_column_to_block(block, "text", &mut rows, FieldType::Text, false).unwrap();
        block = add_last_column_to_block(block, "null", &mut rows, FieldType::UInt, true).unwrap();
        let decimal = block
            .get_column("decimal")
            .unwrap()
            .iter::<clickhouse_rs::types::Decimal>()
            .unwrap()
            .next()
            .unwrap();
        assert_eq!(Into::<f64>::into(decimal), dozer_decimal.to_f64().unwrap());
    }
}
