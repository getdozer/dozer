use crate::errors::{ConnectorError, SnowflakeSchemaError};
use dozer_types::ingestion_types::SnowflakeConfig;
use odbc::create_environment_v3;
use std::collections::HashMap;

use crate::connectors::snowflake::connection::client::Client;
use crate::connectors::{TableInfo, ValidationResults};
use crate::errors::ConnectorError::TableNotFound;
use crate::errors::SnowflakeError::ConnectionError;
use dozer_types::types::{FieldType, SourceSchema};

pub struct SchemaHelper {}

impl SchemaHelper {
    pub fn get_schema(
        config: &SnowflakeConfig,
        table_names: Option<&Vec<TableInfo>>,
    ) -> Result<Vec<SourceSchema>, ConnectorError> {
        let client = Client::new(config);
        let env = create_environment_v3().map_err(|e| e.unwrap()).unwrap();
        let conn = env
            .connect_with_connection_string(&client.get_conn_string())
            .map_err(|e| ConnectionError(Box::new(e)))?;

        let keys = client
            .fetch_keys(&conn)
            .map_err(ConnectorError::SnowflakeError)?;

        let tables_indexes = table_names.map_or(HashMap::new(), |tables| {
            let mut result = HashMap::new();
            for (idx, table) in tables.iter().enumerate() {
                result.insert(table.name.clone(), idx);
            }

            result
        });

        client
            .fetch_tables(table_names, tables_indexes, keys, &conn)
            .map_err(ConnectorError::SnowflakeError)
    }

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
                "{type_name:?}"
            ))),
        }
    }

    pub fn validate_schemas(
        config: &SnowflakeConfig,
        tables: &[TableInfo],
    ) -> Result<ValidationResults, ConnectorError> {
        let schemas = Self::get_schema(config, Some(&tables.to_vec()))?;
        let mut validation_result = ValidationResults::new();

        let existing_schemas_names: Vec<String> = schemas.iter().map(|s| s.name.clone()).collect();
        for table in tables {
            let mut result = vec![];
            if !existing_schemas_names.contains(&table.name) {
                result.push((None, Err(TableNotFound(table.name.clone()))));
            }

            validation_result.insert(table.name.clone(), result);
        }

        Ok(validation_result)
    }
}
