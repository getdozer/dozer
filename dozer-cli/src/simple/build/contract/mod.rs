use std::{
    collections::{BTreeMap, HashMap},
    fs::OpenOptions,
    path::Path,
};

use dozer_cache::dozer_log::schemas::EndpointSchema;
use dozer_core::{
    dag_schemas::DagSchemas,
    daggy::{self, NodeIndex},
    node::PortHandle,
    petgraph::{
        algo::is_isomorphic_matching,
        visit::{IntoEdgesDirected, IntoNodeReferences},
        Direction,
    },
};
use dozer_types::{
    models::{
        connection::Connection,
        endpoint::{ApiEndpoint, Endpoint, EndpointKind},
    },
    node::NodeHandle,
    types::Schema,
};
use dozer_types::{
    serde::{de::DeserializeOwned, Deserialize, Serialize},
    serde_json,
};

use crate::errors::BuildError;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(crate = "dozer_types::serde")]
pub struct NodeType {
    pub handle: NodeHandle,
    pub kind: NodeKind,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(crate = "dozer_types::serde")]
pub enum NodeKind {
    Source {
        typ: String,
        port_names: HashMap<PortHandle, String>,
    },
    Processor {
        typ: String,
    },
    Sink {
        typ: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(crate = "dozer_types::serde")]
pub struct EdgeType {
    pub from_port: PortHandle,
    pub to_port: PortHandle,
    pub schema: Schema,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(crate = "dozer_types::serde")]
pub struct PipelineContract(daggy::Dag<NodeType, EdgeType>);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
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
        endpoints: &[Endpoint],
        enable_token: bool,
        enable_on_event: bool,
    ) -> Result<Self, BuildError> {
        let mut endpoint_schemas = BTreeMap::new();
        for endpoint in endpoints {
            let api: Option<&ApiEndpoint> = match &endpoint.kind {
                EndpointKind::Api(api) => Some(api),
                _ => None,
            };
            let path = match &endpoint.kind {
                EndpointKind::Api(api) => &api.path,
                EndpointKind::Aerospike(_aerospike) => "aerospike",
                EndpointKind::Dummy => "dummy",
            };

            let node_index = find_sink(dag_schemas, &endpoint.table_name)
                .ok_or(BuildError::MissingEndpoint(endpoint.table_name.clone()))?;

            let (schema, secondary_indexes) = modify_schema::modify_schema(
                &endpoint.table_name,
                sink_input_schema(dag_schemas, node_index),
                api,
            )?;

            let connections = dag_schemas
                .collect_ancestor_sources(node_index)
                .into_iter()
                .map(|handle| handle.id)
                .collect();

            let schema = EndpointSchema {
                path: path.to_string(),
                schema,
                secondary_indexes,
                enable_token,
                enable_on_event,
                connections,
            };
            endpoint_schemas.insert(endpoint.table_name.clone(), schema);
        }

        let mut source_types = HashMap::new();
        for (node_index, node) in dag_schemas.graph().node_references() {
            if let dozer_core::NodeKind::Source(_) = &node.kind {
                let connection = connections
                    .iter()
                    .find(|connection| connection.name == node.handle.id)
                    .ok_or(BuildError::MissingConnection(node.handle.id.clone()))?;
                let typ = connection.config.get_type_name();
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
                    dozer_core::NodeKind::Sink(sink) => NodeKind::Sink {
                        typ: sink.type_name(),
                    },
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
            pipeline: PipelineContract(pipeline),
            endpoints: endpoint_schemas,
        })
    }

    pub fn serialize(&self, path: &Path) -> Result<(), BuildError> {
        serde_json_to_path(path, &self)?;
        Ok(())
    }

    pub fn deserialize(path: &Path) -> Result<Self, BuildError> {
        serde_json_from_path(path)
    }
}

mod service;

/// Sink's `NodeHandle::id` must be `table_name`.
fn find_sink(dag: &DagSchemas, endpoint_table_name: &str) -> Option<NodeIndex> {
    dag.graph()
        .node_references()
        .find(|(_node_index, node)| {
            if let dozer_core::NodeKind::Sink(_) = &node.kind {
                node.handle.id == endpoint_table_name
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

fn serde_json_to_path(path: impl AsRef<Path>, value: &impl Serialize) -> Result<(), BuildError> {
    let file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
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

impl PartialEq<PipelineContract> for PipelineContract {
    fn eq(&self, other: &PipelineContract) -> bool {
        is_isomorphic_matching(
            self.0.graph(),
            other.0.graph(),
            |left, right| {
                left.handle == right.handle
                    && match (&left.kind, &right.kind) {
                        (
                            NodeKind::Source {
                                typ: left_typ,
                                port_names: left_portnames,
                            },
                            NodeKind::Source {
                                typ: right_typ,
                                port_names: right_portnames,
                            },
                        ) => {
                            if left_typ != right_typ {
                                false
                            } else {
                                let mut left: Vec<_> = left_portnames.values().collect();
                                left.sort();
                                let mut right: Vec<_> = right_portnames.values().collect();
                                right.sort();
                                left == right
                            }
                        }
                        (
                            NodeKind::Processor { typ: left_typ },
                            NodeKind::Processor { typ: right_typ },
                        ) => left_typ == right_typ,
                        (NodeKind::Sink { typ: _ }, NodeKind::Sink { typ: _ }) => true,
                        _ => false,
                    }
            },
            |left, right| left.schema == right.schema,
        )
    }
}

mod modify_schema;
