use dozer_ingestion_connector::{
    dozer_types::{models::ingestion_types::SnowflakeConfig, types::FieldType},
    SourceSchema,
};
use odbc::create_environment_v3;
use std::collections::HashMap;

use crate::{connection::client::Client, SnowflakeError, SnowflakeSchemaError};

pub struct SchemaHelper {}

impl SchemaHelper {
    #[allow(clippy::type_complexity)]
    pub fn get_schema(
        config: SnowflakeConfig,
        table_names: Option<&[String]>,
    ) -> Result<Vec<Result<(String, SourceSchema), SnowflakeError>>, SnowflakeError> {
        let env = create_environment_v3().map_err(|e| e.unwrap()).unwrap();
        let client = Client::new(config.clone().into(), &env);

        let keys = client.fetch_keys()?;

        let tables_indexes = table_names.map(|table_names| {
            let mut result = HashMap::new();
            for (idx, table_name) in table_names.iter().enumerate() {
                result.insert(table_name.clone(), idx);
            }

            result
        });

        client.fetch_tables(tables_indexes, keys, config.schema)
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
            // TODO: proper type handling for VARIANT and TIME
            "VARIANT" => Ok(FieldType::String),
            "TIME" => Ok(FieldType::String),
            _ => Err(SnowflakeSchemaError::ColumnTypeNotSupported(format!(
                "{type_name:?}"
            ))),
        }
    }
}
