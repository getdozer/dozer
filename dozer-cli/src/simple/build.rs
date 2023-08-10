use std::{borrow::Cow, collections::BTreeMap, fs::OpenOptions, path::Path};

use dozer_api::generator::protoc::generator::ProtoGenerator;
use dozer_cache::dozer_log::{
    home_dir::{BuildId, BuildPath, HomeDir},
    replication::create_data_storage,
    schemas::EndpointSchema,
    storage::Storage,
};
use dozer_core::{
    dag_schemas::{DagSchemas, EdgeType},
    daggy,
    petgraph::{
        visit::{IntoEdgesDirected, IntoNodeReferences},
        Direction,
    },
};
use dozer_types::{
    log::info,
    models::{
        api_endpoint::{
            ApiEndpoint, FullText, SecondaryIndex, SecondaryIndexConfig, SortedInverted,
        },
        app_config::DataStorage,
    },
    node::NodeHandle,
    types::{FieldDefinition, FieldType, IndexDefinition, Schema, SchemaWithIndex},
};
use futures::future::try_join_all;
use serde::{de::DeserializeOwned, Serialize};

use crate::errors::BuildError;

#[derive(Debug)]
pub struct Contract {
    pub pipeline: daggy::Dag<NodeHandle, EdgeType>,
    pub endpoints: BTreeMap<String, EndpointSchema>,
}

impl Contract {
    pub fn new<T>(
        dag_schemas: DagSchemas<T>,
        endpoints: &[ApiEndpoint],
        enable_token: bool,
        enable_on_event: bool,
    ) -> Result<Self, BuildError> {
        let sink_schemas = dag_schemas.get_sink_schemas();
        let mut endpoint_schemas = BTreeMap::new();
        for (endpoint_name, (schema, connections)) in sink_schemas {
            let endpoint = endpoints
                .iter()
                .find(|e| e.name == *endpoint_name)
                .ok_or(BuildError::MissingEndpoint(endpoint_name.clone()))?;
            let (schema, secondary_indexes) = modify_schema(&schema, endpoint)?;
            let schema = EndpointSchema {
                schema,
                secondary_indexes,
                enable_token,
                enable_on_event,
                connections,
            };
            endpoint_schemas.insert(endpoint_name, schema);
        }

        let pipeline = dag_schemas
            .into_graph()
            .map_owned(|_, node| node.handle, |_, edge| edge);

        Ok(Self {
            pipeline,
            endpoints: endpoint_schemas,
        })
    }

    pub fn deserialize(build_path: &BuildPath) -> Result<Self, BuildError> {
        let pipeline: daggy::Dag<NodeHandle, EdgeType> =
            serde_json_from_path(&build_path.dag_path)?;

        let mut endpoints = BTreeMap::new();
        for (node_index, node) in pipeline.node_references() {
            // Endpoint must have zero out degree.
            if pipeline
                .edges_directed(node_index, Direction::Outgoing)
                .count()
                > 0
            {
                continue;
            }

            // `NodeHandle::id` is the endpoint name.
            let endpoint_name = node.id.clone();
            let endpoint_path = build_path.get_endpoint_path(&endpoint_name);
            let schema: EndpointSchema = serde_json_from_path(&endpoint_path.schema_path)?;
            endpoints.insert(endpoint_name, schema);
        }

        Ok(Self {
            pipeline,
            endpoints,
        })
    }
}

pub async fn build(
    home_dir: &HomeDir,
    contract: &Contract,
    storage_config: &DataStorage,
) -> Result<(), BuildError> {
    if let Some(build_id) = needs_build(home_dir, contract, storage_config).await? {
        let build_name = build_id.name().to_string();
        create_build(home_dir, build_id, contract)?;
        info!("Created new build {build_name}");
    } else {
        info!("Building not needed");
    }
    Ok(())
}

async fn needs_build(
    home_dir: &HomeDir,
    contract: &Contract,
    storage_config: &DataStorage,
) -> Result<Option<BuildId>, BuildError> {
    let build_path = home_dir
        .find_latest_build_path()
        .map_err(|(path, error)| BuildError::FileSystem(path.into(), error))?;
    let Some(build_path) = build_path else {
        return Ok(Some(BuildId::first()));
    };

    let mut futures = vec![];
    for endpoint in contract.endpoints.keys() {
        let endpoint_path = build_path.get_endpoint_path(endpoint);
        let (storage, prefix) =
            create_data_storage(storage_config.clone(), endpoint_path.log_dir.into()).await?;
        futures.push(is_empty(storage, prefix));
    }
    if !try_join_all(futures)
        .await?
        .into_iter()
        .all(|is_empty| is_empty)
    {
        return Ok(Some(build_path.id.next()));
    }

    let existing_contract = Contract::deserialize(&build_path)?;
    for (endpoint, schema) in &contract.endpoints {
        if let Some(existing_schema) = existing_contract.endpoints.get(endpoint) {
            if schema == existing_schema {
                continue;
            }
        } else {
            return Ok(Some(build_path.id.next()));
        }
    }
    Ok(None)
}

async fn is_empty(storage: Box<dyn Storage>, prefix: String) -> Result<bool, BuildError> {
    let objects = storage.list_objects(prefix, None).await?;
    Ok(objects.objects.is_empty())
}

fn create_build(
    home_dir: &HomeDir,
    build_id: BuildId,
    contract: &Contract,
) -> Result<(), BuildError> {
    let build_path = home_dir
        .create_build_dir_all(build_id)
        .map_err(|(path, error)| BuildError::FileSystem(path.into(), error))?;

    contract.serialize(&build_path)?;

    let mut resources = Vec::new();

    let proto_folder_path = build_path.contracts_dir.as_ref();
    for (endpoint_name, schema) in &contract.endpoints {
        ProtoGenerator::generate(proto_folder_path, endpoint_name, schema)?;
        resources.push(endpoint_name.clone());
    }

    let common_resources = ProtoGenerator::copy_common(proto_folder_path)?;

    // Copy common service to be included in descriptor.
    resources.extend(common_resources);

    // Generate a descriptor based on all proto files generated within sink.
    ProtoGenerator::generate_descriptor(
        proto_folder_path,
        build_path.descriptor_path.as_ref(),
        &resources,
    )?;

    Ok(())
}

fn modify_schema(
    schema: &Schema,
    api_endpoint: &ApiEndpoint,
) -> Result<SchemaWithIndex, BuildError> {
    let mut schema = schema.clone();
    // Generated Cache index based on api_index
    let configured_index = create_primary_indexes(
        &schema.fields,
        api_endpoint
            .index
            .as_ref()
            .map(|index| index.primary_key.as_slice()),
    )?;
    // Generated schema in SQL
    let upstream_index = schema.primary_index.clone();

    let index = match (configured_index.is_empty(), upstream_index.is_empty()) {
        (true, true) => vec![],
        (true, false) => upstream_index,
        (false, true) => configured_index,
        (false, false) => {
            if !upstream_index.eq(&configured_index) {
                return Err(BuildError::MismatchPrimaryKey {
                    endpoint_name: api_endpoint.name.clone(),
                    expected: get_field_names(&schema, &upstream_index),
                    actual: get_field_names(&schema, &configured_index),
                });
            }
            configured_index
        }
    };

    schema.primary_index = index;

    let secondary_index_config = get_secondary_index_config(api_endpoint);
    let secondary_indexes = generate_secondary_indexes(&schema.fields, &secondary_index_config)?;

    Ok((schema, secondary_indexes))
}

fn get_field_names(schema: &Schema, indexes: &[usize]) -> Vec<String> {
    indexes
        .iter()
        .map(|idx| schema.fields[*idx].name.to_owned())
        .collect()
}

fn get_secondary_index_config(api_endpoint: &ApiEndpoint) -> Cow<SecondaryIndexConfig> {
    if let Some(config) = api_endpoint
        .index
        .as_ref()
        .and_then(|index| index.secondary.as_ref())
    {
        Cow::Borrowed(config)
    } else {
        Cow::Owned(SecondaryIndexConfig::default())
    }
}

fn create_primary_indexes(
    field_definitions: &[FieldDefinition],
    primary_key: Option<&[String]>,
) -> Result<Vec<usize>, BuildError> {
    let mut primary_index = Vec::new();
    if let Some(primary_key) = primary_key {
        for name in primary_key {
            primary_index.push(field_index_from_field_name(field_definitions, name)?);
        }
    }
    Ok(primary_index)
}

fn generate_secondary_indexes(
    field_definitions: &[FieldDefinition],
    config: &SecondaryIndexConfig,
) -> Result<Vec<IndexDefinition>, BuildError> {
    let mut result = vec![];

    // Create default indexes unless skipped.
    for (index, field) in field_definitions.iter().enumerate() {
        if config.skip_default.contains(&field.name) {
            continue;
        }

        match field.typ {
            // Create sorted inverted indexes for these fields
            FieldType::UInt
            | FieldType::U128
            | FieldType::Int
            | FieldType::I128
            | FieldType::Float
            | FieldType::Boolean
            | FieldType::Decimal
            | FieldType::Timestamp
            | FieldType::Date
            | FieldType::Point
            | FieldType::Duration => result.push(IndexDefinition::SortedInverted(vec![index])),

            // Create sorted inverted and full text indexes for string fields.
            FieldType::String => {
                result.push(IndexDefinition::SortedInverted(vec![index]));
                result.push(IndexDefinition::FullText(index));
            }

            // Skip creating indexes
            FieldType::Text | FieldType::Binary | FieldType::Json => (),
        }
    }

    // Create requested indexes.
    for create in &config.create {
        if let Some(index) = &create.index {
            match index {
                SecondaryIndex::SortedInverted(SortedInverted { fields }) => {
                    let fields = fields
                        .iter()
                        .map(|field| field_index_from_field_name(field_definitions, field))
                        .collect::<Result<Vec<_>, _>>()?;
                    result.push(IndexDefinition::SortedInverted(fields));
                }
                SecondaryIndex::FullText(FullText { field }) => {
                    let field = field_index_from_field_name(field_definitions, field)?;
                    result.push(IndexDefinition::FullText(field));
                }
            }
        }
    }

    Ok(result)
}

fn field_index_from_field_name(
    fields: &[FieldDefinition],
    field_name: &str,
) -> Result<usize, BuildError> {
    fields
        .iter()
        .position(|field| field.name == field_name)
        .ok_or(BuildError::FieldNotFound(field_name.to_string()))
}

impl Contract {
    fn serialize(&self, build_path: &BuildPath) -> Result<(), BuildError> {
        serde_json_to_path(&build_path.dag_path, &self.pipeline)?;

        for (endpoint_name, schema) in &self.endpoints {
            let endpoint_path = build_path.get_endpoint_path(endpoint_name);
            serde_json_to_path(&endpoint_path.schema_path, schema)?;
        }

        Ok(())
    }
}

fn serde_json_to_path(path: impl AsRef<Path>, value: &impl Serialize) -> Result<(), BuildError> {
    let file = OpenOptions::new()
        .create(true)
        .write(true)
        .open(path.as_ref())
        .map_err(|e| BuildError::FileSystem(path.as_ref().into(), e))?;
    serde_json::to_writer_pretty(file, value)?;
    Ok(())
}

fn serde_json_from_path<T>(path: impl AsRef<Path>) -> Result<T, BuildError>
where
    T: DeserializeOwned,
{
    let file = OpenOptions::new()
        .read(true)
        .open(path.as_ref())
        .map_err(|e| BuildError::FileSystem(path.as_ref().into(), e))?;
    Ok(serde_json::from_reader(file)?)
}
