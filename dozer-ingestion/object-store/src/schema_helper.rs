use std::sync::Arc;

use deltalake::arrow::datatypes::{DataType, Field};
use dozer_ingestion_connector::dozer_types::types::{FieldDefinition, FieldType, SourceDefinition};

use crate::ObjectStoreSchemaError;

pub fn map_schema_to_dozer<'a, I: Iterator<Item = &'a Arc<Field>>>(
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
                _ => {
                    return Err(ObjectStoreSchemaError::FieldTypeNotSupported(
                        field.name().clone(),
                    ))
                }
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
