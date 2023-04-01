use std::{
    collections::HashMap,
    fs::OpenOptions,
    path::{Path, PathBuf},
};

use dozer_core::dag_schemas::DagSchemas;
use dozer_sql::pipeline::builder::SchemaSQLContext;
use dozer_types::types::Schema;
use std::io::Write;

use crate::errors::OrchestrationError;

pub const SCHEMA_FILE_NAME: &str = "schemas.json";

pub fn write_schemas(
    dag_schemas: &DagSchemas<SchemaSQLContext>,
    pipeline_dir: PathBuf,
) -> Result<HashMap<String, Schema>, OrchestrationError> {
    let path = pipeline_dir.join(SCHEMA_FILE_NAME);
    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .append(true)
        .open(path)
        .map_err(|e| OrchestrationError::InternalError(Box::new(e)))?;

    let schemas = dag_schemas.get_sink_schemas();

    writeln!(file, "{}", serde_json::to_string(&schemas).unwrap())
        .map_err(|e| OrchestrationError::InternalError(Box::new(e)))?;
    Ok(schemas)
}

pub fn load_schemas(path: &Path) -> Result<HashMap<String, Schema>, OrchestrationError> {
    let path = path.join(SCHEMA_FILE_NAME);

    let schema_str = std::fs::read_to_string(&path)
        .map_err(|_| OrchestrationError::SchemasNotInitializedPath(path.clone()))?;

    serde_json::from_str::<HashMap<String, Schema>>(&schema_str)
        .map_err(|_| OrchestrationError::DeserializeSchemas(path))
}
