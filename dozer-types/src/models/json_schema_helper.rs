use schemars::{schema::RootSchema, schema_for};
use serde::{Deserialize, Serialize};

use crate::models::ingestion_types;

use super::{config::Config, connection::PostgresConfig};

#[derive(Debug, Serialize, Deserialize)]
pub struct Schema {
    pub name: String,
    pub schema: RootSchema,
}
pub fn get_dozer_schema() -> Result<String, serde_json::Error> {
    let schema = schema_for!(Config);
    let schema_json = serde_json::to_string_pretty(&schema)?;
    Ok(schema_json)
}

pub fn get_connection_schemas() -> Result<String, serde_json::Error> {
    let mut schemas = vec![];

    let configs = [
        ("postgres", schema_for!(PostgresConfig)),
        ("ethereum", schema_for!(ingestion_types::EthConfig)),
        ("grpc", schema_for!(ingestion_types::GrpcConfig)),
        ("snowflake", schema_for!(ingestion_types::SnowflakeConfig)),
        ("kafka", schema_for!(ingestion_types::KafkaConfig)),
        ("s3", schema_for!(ingestion_types::S3Storage)),
        ("local_storage", schema_for!(ingestion_types::LocalStorage)),
        ("deltalake", schema_for!(ingestion_types::DeltaLakeConfig)),
        ("mongodb", schema_for!(ingestion_types::MongodbConfig)),
        ("mysql", schema_for!(ingestion_types::MySQLConfig)),
        ("dozer", schema_for!(ingestion_types::NestedDozerConfig)),
    ];
    for (name, schema) in configs.iter() {
        schemas.push(Schema {
            name: name.to_string(),
            schema: schema.clone(),
        });
    }
    let schema_json = serde_json::to_string_pretty(&schemas)?;
    Ok(schema_json)
}

#[cfg(test)]
mod tests {
    use super::get_connection_schemas;

    #[test]
    fn get_schemas() {
        let schemas = get_connection_schemas();
        assert!(schemas.is_ok());
    }
}
