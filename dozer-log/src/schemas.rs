use std::{
    collections::HashMap,
    fs::OpenOptions,
    path::{Path, PathBuf},
};

use dozer_core::{dag_schemas::DagSchemas, DEFAULT_PORT_HANDLE};
use dozer_sql::pipeline::builder::SchemaSQLContext;
use dozer_types::{
    models::api_endpoint::{ApiEndpoint, ApiIndex},
    serde_json,
    types::{Schema, SchemaIdentifier},
};
use std::io::Write;

use crate::errors::SchemasError;

pub const SCHEMA_FILE_NAME: &str = "schemas.json";

pub fn write_schemas(
    dag_schemas: &DagSchemas<SchemaSQLContext>,
    pipeline_dir: PathBuf,
    api_endpoints: &[ApiEndpoint],
) -> Result<HashMap<String, Schema>, SchemasError> {
    let path = pipeline_dir.join(SCHEMA_FILE_NAME);
    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .append(true)
        .open(path)
        .map_err(|e| SchemasError::InternalError(Box::new(e)))?;

    let mut schemas = dag_schemas.get_sink_schemas();

    for api_endpoint in api_endpoints {
        let schema = schemas
            .get(&api_endpoint.name)
            .unwrap_or_else(|| panic!("Schema not found for a sink {}", api_endpoint.name));
        let schema = modify_schema(schema, api_endpoint)?;

        schemas.insert(api_endpoint.name.clone(), schema);
    }
    writeln!(file, "{}", serde_json::to_string(&schemas).unwrap())
        .map_err(|e| SchemasError::InternalError(Box::new(e)))?;
    Ok(schemas)
}

pub fn load_schemas(path: &Path) -> Result<HashMap<String, Schema>, SchemasError> {
    let path = path.join(SCHEMA_FILE_NAME);

    let schema_str = std::fs::read_to_string(&path)
        .map_err(|_| SchemasError::SchemasNotInitializedPath(path.clone()))?;

    serde_json::from_str::<HashMap<String, Schema>>(&schema_str)
        .map_err(|_| SchemasError::DeserializeSchemas(path))
}

fn modify_schema(schema: &Schema, api_endpoint: &ApiEndpoint) -> Result<Schema, SchemasError> {
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
                return Err(SchemasError::MismatchPrimaryKey {
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
) -> Result<Vec<usize>, SchemasError> {
    let mut primary_index = Vec::new();
    for name in api_index.primary_key.iter() {
        let idx = schema
            .fields
            .iter()
            .position(|fd| fd.name == name.clone())
            .map_or(Err(SchemasError::FieldNotFound(name.to_owned())), Ok)?;

        primary_index.push(idx);
    }
    Ok(primary_index)
}
