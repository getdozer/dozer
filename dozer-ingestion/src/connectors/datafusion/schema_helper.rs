use crate::errors::DataFusionSchemaError;
use crate::errors::DataFusionSchemaError::FieldTypeNotSupported;
use datafusion::arrow::datatypes::{DataType, Field};

use dozer_types::types::{FieldDefinition, FieldType, SourceDefinition};

pub fn map_schema_to_dozer(
    fields_list: &Vec<Field>,
) -> Result<Vec<FieldDefinition>, DataFusionSchemaError> {
    let mut fields = vec![];
    for field in fields_list {
        let mapped_field_type = match field.data_type() {
            // DataType::Null => ,
            DataType::Boolean => Ok(FieldType::Boolean),
            DataType::Int8 => Ok(FieldType::Int),
            DataType::Int16 => Ok(FieldType::Int),
            DataType::Int32 => Ok(FieldType::Int),
            DataType::Int64 => Ok(FieldType::Int),
            DataType::UInt8 => Ok(FieldType::UInt),
            DataType::UInt16 => Ok(FieldType::UInt),
            DataType::UInt32 => Ok(FieldType::UInt),
            DataType::UInt64 => Ok(FieldType::UInt),
            DataType::Float16 => Ok(FieldType::Float),
            DataType::Float32 => Ok(FieldType::Float),
            DataType::Float64 => Ok(FieldType::Float),
            DataType::Timestamp(_, _) => Ok(FieldType::Timestamp),
            DataType::Date32 => Ok(FieldType::Date),
            DataType::Date64 => Ok(FieldType::Date),
            DataType::Time32(_) => Ok(FieldType::Int),
            DataType::Time64(_) => Ok(FieldType::Int),
            DataType::Duration(_) => Ok(FieldType::Int),
            DataType::Interval(_) => Ok(FieldType::Int),
            DataType::Binary => Ok(FieldType::Binary),
            DataType::FixedSizeBinary(_) => Ok(FieldType::Binary),
            DataType::LargeBinary => Ok(FieldType::Binary),
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
            // DataType::Map(_, _) => {}
            _ => Err(FieldTypeNotSupported(field.name().clone())),
        }?;

        fields.push(FieldDefinition {
            name: field.name().clone(),
            typ: mapped_field_type,
            nullable: field.is_nullable(),
            source: SourceDefinition::Dynamic,
        });
    }

    Ok(fields)
}
