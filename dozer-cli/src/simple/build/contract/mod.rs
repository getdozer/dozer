use std::{collections::BTreeMap, fs::OpenOptions, path::Path};

use dozer_cache::dozer_log::{home_dir::BuildPath, schemas::EndpointSchema};
use dozer_core::{
    dag_schemas::{DagSchemas, EdgeType},
    daggy,
    petgraph::{
        visit::{IntoEdgesDirected, IntoNodeReferences},
        Direction,
    },
};
use dozer_types::{models::api_endpoint::ApiEndpoint, node::NodeHandle};
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
            let (schema, secondary_indexes) = modify_schema::modify_schema(&schema, endpoint)?;
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

    pub fn serialize(&self, build_path: &BuildPath) -> Result<(), BuildError> {
        serde_json_to_path(&build_path.dag_path, &self.pipeline)?;

        for (endpoint_name, schema) in &self.endpoints {
            let endpoint_path = build_path.get_endpoint_path(endpoint_name);
            serde_json_to_path(&endpoint_path.schema_path, schema)?;
        }

        Ok(())
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

mod modify_schema;
