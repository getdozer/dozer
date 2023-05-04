use crate::errors::ObjectStoreSchemaError;
use crate::errors::ObjectStoreSchemaError::FieldTypeNotSupported;
use deltalake::arrow::datatypes::{DataType, Field};

use dozer_types::types::{FieldDefinition, FieldType, SourceDefinition};

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
