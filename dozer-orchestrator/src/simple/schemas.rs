use std::{collections::HashMap, path::PathBuf};

use dozer_cache::errors::CacheError;
use dozer_core::dag_schemas;
use dozer_sql::pipeline::builder::SchemaSQLContext;
use dozer_types::types::Schema;

use crate::errors::OrchestrationError;

pub const SCHEMA_FILE_NAME: &str = "schemas.json";

pub fn write_schemas(
    dag_schemas: &dag_schemas::DagSchemas<SchemaSQLContext>,
    path: PathBuf,
) -> Result<(), OrchestrationError> {
    let path = Path::new(&path).join(SCHEMA_FILE_NAME);

    let path = pipeline_dir.join(SCHEMA_FILE_NAME);
    let mut file = OpenOptions::new()
        .write(true)
        .append(true)
        .open(path)
        .map_err(|e| OrchestrationError::InternalError(Box::new(e)))?;

    let schemas = dag_schemas.get_sink_schemas();

    writeln!(file, "{}", serde_json::to_string(&schemas).unwrap())
        .map_err(|e| OrchestrationError::InternalError(Box::new(e)))?;
    Ok(())
}

pub fn load_schemas(path: &str) -> Result<HashMap<String, Schema>, OrchestrationError> {
    let path = Path::new(&path).join(SCHEMA_FILE_NAME);

    let schema_str = std::fs::read_to_string(path.clone())
        .map_err(|e| CacheError::SchemasNotInitializedPath(path.clone()))?;

    serde_json::from_str::<HashMap<String, Schema>>(&schema_str)
        .map_err(|e| CacheError::DeserializeSchemas(path))
}
