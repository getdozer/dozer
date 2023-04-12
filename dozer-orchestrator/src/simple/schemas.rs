use std::{
    collections::HashMap,
    fs::OpenOptions,
    path::{Path, PathBuf},
};

use dozer_core::{dag_schemas::DagSchemas, DEFAULT_PORT_HANDLE};
use dozer_sql::pipeline::builder::SchemaSQLContext;
use dozer_types::{
    models::api_endpoint::{ApiEndpoint, ApiIndex},
    types::{Schema, SchemaIdentifier},
};
use std::io::Write;

use crate::{errors::OrchestrationError, utils::get_endpoint_schema_path};

pub fn write_schemas(
    dag_schemas: &DagSchemas<SchemaSQLContext>,
    pipeline_dir: PathBuf,
    api_endpoints: &[ApiEndpoint],
) -> Result<HashMap<String, Schema>, OrchestrationError> {
    let mut schemas = dag_schemas.get_sink_schemas();

    for api_endpoint in api_endpoints {
        let schema = schemas
            .get(&api_endpoint.name)
            .unwrap_or_else(|| panic!("Schema not found for a sink {}", api_endpoint.name));
        let schema = modify_schema(schema, api_endpoint)?;

        let schema_path = get_endpoint_schema_path(&pipeline_dir, &api_endpoint.name);
        if let Some(schema_dir) = schema_path.parent() {
            std::fs::create_dir_all(schema_dir)
                .map_err(|e| OrchestrationError::InternalError(Box::new(e)))?;
        }

        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .open(&schema_path)
            .map_err(|e| OrchestrationError::InternalError(Box::new(e)))?;
        writeln!(file, "{}", serde_json::to_string(&schema).unwrap())
            .map_err(|e| OrchestrationError::InternalError(Box::new(e)))?;

        schemas.insert(api_endpoint.name.clone(), schema);
    }

    Ok(schemas)
}

pub fn load_schema(pipeline_dir: &Path, endpoint_name: &str) -> Result<Schema, OrchestrationError> {
    let path = get_endpoint_schema_path(pipeline_dir, endpoint_name);

    let schema_str = std::fs::read_to_string(&path)
        .map_err(|_| OrchestrationError::SchemasNotInitializedPath(path.clone()))?;

    serde_json::from_str(&schema_str).map_err(|_| OrchestrationError::DeserializeSchema(path))
}

fn modify_schema(
    schema: &Schema,
    api_endpoint: &ApiEndpoint,
) -> Result<Schema, OrchestrationError> {
    let mut schema = schema.clone();
    // Generated Cache index based on api_index
    let configured_index =
        create_primary_indexes(&schema, &api_endpoint.index.to_owned().unwrap_or_default())?;
    // Generated schema in SQL
    let upstream_index = schema.primary_index.clone();

    let index = match (configured_index.is_empty(), upstream_index.is_empty()) {
        (true, true) => vec![],
        (true, false) => upstream_index,
        (false, true) => configured_index,
        (false, false) => {
            if !upstream_index.eq(&configured_index) {
                return Err(OrchestrationError::MismatchPrimaryKey {
                    endpoint_name: api_endpoint.name.clone(),
                    expected: get_field_names(&schema, &upstream_index),
                    actual: get_field_names(&schema, &configured_index),
                });
            }
            configured_index
        }
    };

    schema.primary_index = index;

    schema.identifier = Some(SchemaIdentifier {
        id: DEFAULT_PORT_HANDLE as u32,
        version: 1,
    });
    Ok(schema)
}

fn get_field_names(schema: &Schema, indexes: &[usize]) -> Vec<String> {
    indexes
        .iter()
        .map(|idx| schema.fields[*idx].name.to_owned())
        .collect()
}

fn create_primary_indexes(
    schema: &Schema,
    api_index: &ApiIndex,
) -> Result<Vec<usize>, OrchestrationError> {
    let mut primary_index = Vec::new();
    for name in api_index.primary_key.iter() {
        let idx = schema
            .fields
            .iter()
            .position(|fd| fd.name == name.clone())
            .map_or(Err(OrchestrationError::FieldNotFound(name.to_owned())), Ok)?;

        primary_index.push(idx);
    }
    Ok(primary_index)
}
