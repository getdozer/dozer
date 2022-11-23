use crate::errors::{SnowflakeError, SnowflakeSchemaError};

use dozer_types::types::{FieldDefinition, FieldType, Schema, SchemaIdentifier};
use odbc::ffi::SqlDataType;
use odbc::ColumnDescriptor;

pub struct SchemaHelper {}

impl SchemaHelper {
    pub fn map_type(
        column_descriptor: &ColumnDescriptor,
    ) -> Result<FieldType, SnowflakeSchemaError> {
        match column_descriptor.data_type {
            SqlDataType::SQL_CHAR => Ok(FieldType::String),
            SqlDataType::SQL_NUMERIC => Ok(FieldType::Int),
            SqlDataType::SQL_DECIMAL => Ok(FieldType::Int),
            SqlDataType::SQL_INTEGER => Ok(FieldType::Int),
            SqlDataType::SQL_SMALLINT => Ok(FieldType::Int),
            SqlDataType::SQL_FLOAT => Ok(FieldType::Float),
            SqlDataType::SQL_REAL => Ok(FieldType::Float),
            SqlDataType::SQL_DOUBLE => Ok(FieldType::Float),
            SqlDataType::SQL_DATETIME => Ok(FieldType::Timestamp),
            SqlDataType::SQL_VARCHAR => Ok(FieldType::String),
            SqlDataType::SQL_TIMESTAMP => Ok(FieldType::Timestamp),
            _ => Err(SnowflakeSchemaError::ColumnTypeNotSupported(format!(
                "{:?}",
                &column_descriptor.data_type
            ))),
        }
    }

    fn convert_column_to_field_definition(
        c: &ColumnDescriptor,
    ) -> Result<FieldDefinition, SnowflakeSchemaError> {
        let typ = Self::map_type(c)?;
        Ok(FieldDefinition {
            name: c.name.clone().to_lowercase(),
            typ,
            nullable: c.nullable.is_some(),
        })
    }

    pub fn map_schema(schema: Vec<ColumnDescriptor>) -> Result<Schema, SnowflakeError> {
        let fields: Result<Vec<FieldDefinition>, SnowflakeSchemaError> = schema
            .iter()
            .map(Self::convert_column_to_field_definition)
            .collect();

        let defined_fields = fields.map_err(SnowflakeError::SnowflakeSchemaError)?;

        Ok(Schema {
            identifier: Some(SchemaIdentifier { id: 1, version: 1 }),
            fields: defined_fields,
            values: vec![],
            primary_index: vec![0],
            secondary_indexes: vec![],
        })
    }
}
