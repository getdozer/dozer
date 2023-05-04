use dozer_api::generator::protoc::generator::ProtoGenerator;
use dozer_cache::dozer_log::{
    home_dir::{HomeDir, MigrationId},
    schemas::{load_schema, write_schema, MigrationSchema},
};
use dozer_types::{
    models::api_endpoint::{ApiEndpoint, ApiIndex},
    types::{Schema, SchemaIdentifier},
};

use crate::errors::MigrationError;

pub fn needs_migration(
    home_dir: &HomeDir,
    endpoint_name: &str,
    schema: &MigrationSchema,
) -> Result<Option<MigrationId>, MigrationError> {
    let migration_path = home_dir
        .find_latest_migration_path(endpoint_name)
        .map_err(|(path, error)| MigrationError::FileSystem(path, error))?;
    let Some(migration_path) = migration_path else {
        return Ok(Some(MigrationId::first()));
    };

    if migration_path.log_path.exists() {
        return Ok(Some(migration_path.id.next()));
    }

    let existing_schema = load_schema(&migration_path.schema_path)
        .map_err(MigrationError::CannotLoadExistingSchema)?;
    if existing_schema == *schema {
        Ok(None)
    } else {
        Ok(Some(migration_path.id.next()))
    }
}

pub fn create_migration(
    home_dir: &HomeDir,
    endpoint_name: &str,
    migration_id: MigrationId,
    schema: &MigrationSchema,
) -> Result<(), MigrationError> {
    let migration_path = home_dir
        .create_migration_dir_all(endpoint_name, migration_id)
        .map_err(|(path, error)| MigrationError::FileSystem(path, error))?;

    write_schema(schema, &migration_path.schema_path).map_err(MigrationError::CannotWriteSchema)?;

    let proto_folder_path = &migration_path.api_dir;
    ProtoGenerator::generate(proto_folder_path, endpoint_name, schema)?;

    let mut resources = Vec::new();
    resources.push(endpoint_name);

    let common_resources = ProtoGenerator::copy_common(proto_folder_path)?;

    // Copy common service to be included in descriptor.
    resources.extend(common_resources.iter().map(|str| str.as_str()));

    // Generate a descriptor based on all proto files generated within sink.
    ProtoGenerator::generate_descriptor(
        proto_folder_path,
        &migration_path.descriptor_path,
        &resources,
    )?;

    Ok(())
}

pub fn modify_schema(
    schema: &Schema,
    api_endpoint: &ApiEndpoint,
) -> Result<Schema, MigrationError> {
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
                return Err(MigrationError::MismatchPrimaryKey {
                    endpoint_name: api_endpoint.name.clone(),
                    expected: get_field_names(&schema, &upstream_index),
                    actual: get_field_names(&schema, &configured_index),
                });
            }
            configured_index
        }
    };

    schema.primary_index = index;

    schema.identifier = Some(SchemaIdentifier { id: 0, version: 1 });
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
) -> Result<Vec<usize>, MigrationError> {
    let mut primary_index = Vec::new();
    for name in api_index.primary_key.iter() {
        let idx = schema
            .fields
            .iter()
            .position(|fd: &dozer_types::types::FieldDefinition| fd.name == name.clone())
            .map_or(Err(MigrationError::FieldNotFound(name.to_owned())), Ok)?;

        primary_index.push(idx);
    }
    Ok(primary_index)
}
