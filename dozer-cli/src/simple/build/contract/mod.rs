use std::{
    collections::{BTreeMap, HashMap, HashSet},
    fs::OpenOptions,
    path::Path,
};

use dozer_cache::dozer_log::{home_dir::BuildPath, schemas::EndpointSchema};
use dozer_core::{
    dag_schemas::DagSchemas,
    daggy::{self, NodeIndex},
    node::PortHandle,
    petgraph::{
        visit::{EdgeRef, IntoEdgesDirected, IntoNodeReferences},
        Direction,
    },
};
use dozer_types::{
    models::{
        api_endpoint::ApiEndpoint,
        connection::{Connection, ConnectionConfig},
    },
    node::NodeHandle,
    types::Schema,
};
use dozer_types::{
    serde::{de::DeserializeOwned, Deserialize, Serialize},
    serde_json,
};

use crate::errors::BuildError;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(crate = "dozer_types::serde")]
pub struct NodeType {
    pub handle: NodeHandle,
    pub kind: NodeKind,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(crate = "dozer_types::serde")]
pub enum NodeKind {
    Source {
        typ: String,
        port_names: HashMap<PortHandle, String>,
    },
    Processor {
        typ: String,
    },
    Sink,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(crate = "dozer_types::serde")]
pub struct EdgeType {
    pub from_port: PortHandle,
    pub to_port: PortHandle,
    pub schema: Schema,
}

pub type PipelineContract = daggy::Dag<NodeType, EdgeType>;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(crate = "dozer_types::serde")]
pub struct Contract {
    pub version: usize,
    pub pipeline: PipelineContract,
    pub endpoints: BTreeMap<String, EndpointSchema>,
}

impl Contract {
    pub fn new(
        version: usize,
        dag_schemas: &DagSchemas,
        connections: &[Connection],
        endpoints: &[ApiEndpoint],
        enable_token: bool,
        enable_on_event: bool,
    ) -> Result<Self, BuildError> {
        let mut endpoint_schemas = BTreeMap::new();
        for endpoint in endpoints {
            let node_index = find_sink(dag_schemas, &endpoint.name)
                .ok_or(BuildError::MissingEndpoint(endpoint.name.clone()))?;

            let (schema, secondary_indexes) =
                modify_schema::modify_schema(sink_input_schema(dag_schemas, node_index), endpoint)?;

            let connections = collect_ancestor_sources(dag_schemas, node_index);

            let schema = EndpointSchema {
                schema,
                secondary_indexes,
                enable_token,
                enable_on_event,
                connections,
            };
            endpoint_schemas.insert(endpoint.name.clone(), schema);
        }

        let mut source_types = HashMap::new();
        for (node_index, node) in dag_schemas.graph().node_references() {
            if let dozer_core::NodeKind::Source(_) = &node.kind {
                let connection = connections
                    .iter()
                    .find(|connection| connection.name == node.handle.id)
                    .ok_or(BuildError::MissingConnection(node.handle.id.clone()))?;
                let typ = match &connection.config {
                    None => "None",
                    Some(ConnectionConfig::Postgres(_)) => "Postgres",
                    Some(ConnectionConfig::Ethereum(_)) => "Ethereum",
                    Some(ConnectionConfig::Grpc(_)) => "Grpc",
                    Some(ConnectionConfig::Snowflake(_)) => "Snowflake",
                    Some(ConnectionConfig::Kafka(_)) => "Kafka",
                    Some(ConnectionConfig::S3Storage(_)) => "S3Storage",
                    Some(ConnectionConfig::LocalStorage(_)) => "LocalStorage",
                    Some(ConnectionConfig::DeltaLake(_)) => "DeltaLake",
                    Some(ConnectionConfig::MongoDB(_)) => "MongoDB",
                    Some(ConnectionConfig::MySQL(_)) => "MySQL",
                    Some(ConnectionConfig::Dozer(_)) => "Dozer",
                };
                source_types.insert(node_index, typ);
            }
        }

        let graph = dag_schemas.graph();
        let pipeline = graph.map(
            |node_index, node| {
                let handle = node.handle.clone();
                let kind = match &node.kind {
                    dozer_core::NodeKind::Source(source) => {
                        let typ = source_types
                            .get(&node_index)
                            .expect("Source must have a type")
                            .to_string();
                        let port_names = source
                            .get_output_ports()
                            .iter()
                            .map(|port| {
                                let port_name = source.get_output_port_name(&port.handle);
                                (port.handle, port_name)
                            })
                            .collect();
                        NodeKind::Source { typ, port_names }
                    }
                    dozer_core::NodeKind::Processor(processor) => NodeKind::Processor {
                        typ: processor.type_name(),
                    },
                    dozer_core::NodeKind::Sink(_) => NodeKind::Sink,
                };
                NodeType { handle, kind }
            },
            |_, edge| EdgeType {
                from_port: edge.output_port,
                to_port: edge.input_port,
                schema: edge.schema.clone(),
            },
        );

        Ok(Self {
            version,
            pipeline,
            endpoints: endpoint_schemas,
        })
    }

    pub fn serialize(&self, build_path: &BuildPath) -> Result<(), BuildError> {
        serde_json_to_path(&build_path.dag_path, &self)?;
        Ok(())
    }

    pub fn deserialize(build_path: &BuildPath) -> Result<Self, BuildError> {
        serde_json_from_path(&build_path.dag_path)
    }
}

mod service;

/// Sink's `NodeHandle::id` must be `endpoint_name`.
fn find_sink(dag: &DagSchemas, endpoint_name: &str) -> Option<NodeIndex> {
    dag.graph()
        .node_references()
        .find(|(_node_index, node)| {
            if let dozer_core::NodeKind::Sink(_) = &node.kind {
                node.handle.id == endpoint_name
            } else {
                false
            }
        })
        .map(|(node_index, _)| node_index)
}

fn sink_input_schema(dag: &DagSchemas, node_index: NodeIndex) -> &Schema {
    let edge = dag
        .graph()
        .edges_directed(node_index, Direction::Incoming)
        .next()
        .expect("Sink must have one incoming edge");
    &edge.weight().schema
}

fn collect_ancestor_sources(dag: &DagSchemas, node_index: NodeIndex) -> HashSet<String> {
    let mut sources = HashSet::new();
    collect_ancestor_sources_recursive(dag, node_index, &mut sources);
    sources
}

fn collect_ancestor_sources_recursive(
    dag: &DagSchemas,
    node_index: NodeIndex,
    sources: &mut HashSet<String>,
) {
    for edge in dag.graph().edges_directed(node_index, Direction::Incoming) {
        let source_node_index = edge.source();
        let source_node = &dag.graph()[source_node_index];
        if matches!(source_node.kind, dozer_core::NodeKind::Source(_)) {
            sources.insert(source_node.handle.id.clone());
        }
        collect_ancestor_sources_recursive(dag, source_node_index, sources);
    }
}

fn serde_json_to_path(path: impl AsRef<Path>, value: &impl Serialize) -> Result<(), BuildError> {
    let file = OpenOptions::new()
        .create(true)
        .write(true)
        .open(path.as_ref())
        .map_err(|e| BuildError::FileSystem(path.as_ref().into(), e))?;
    serde_json::to_writer_pretty(file, value).map_err(BuildError::SerdeJson)
}

fn serde_json_from_path<T>(path: impl AsRef<Path>) -> Result<T, BuildError>
where
    T: DeserializeOwned,
{
    let file = OpenOptions::new()
        .read(true)
        .open(path.as_ref())
        .map_err(|e| BuildError::FileSystem(path.as_ref().into(), e))?;
    serde_json::from_reader(file).map_err(BuildError::FailedToLoadExistingContract)
}

mod modify_schema;
