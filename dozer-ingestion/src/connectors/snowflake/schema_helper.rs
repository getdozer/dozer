use crate::errors::SnowflakeSchemaError;

use dozer_types::types::FieldType;

pub struct SchemaHelper {}

impl SchemaHelper {
    pub fn map_schema_type(
        type_name: &str,
        scale: Option<i64>,
    ) -> Result<FieldType, SnowflakeSchemaError> {
        match type_name {
            "NUMBER" => scale.map_or(Ok(FieldType::Int), |scale| {
                if scale > 0 {
                    Ok(FieldType::Decimal)
                } else {
                    Ok(FieldType::Int)
                }
            }),
            "FLOAT" => Ok(FieldType::Float),
            "TEXT" => Ok(FieldType::String),
            "BINARY" => Ok(FieldType::Binary),
            "BOOLEAN" => Ok(FieldType::Boolean),
            "DATE" => Ok(FieldType::Date),
            "TIMESTAMP_LTZ" | "TIMESTAMP_NTZ" | "TIMESTAMP_TZ" => Ok(FieldType::Timestamp),
            _ => Err(SnowflakeSchemaError::ColumnTypeNotSupported(format!(
                "{:?}",
                type_name
            ))),
        }
    }
}
