use std::{
    collections::{HashMap, HashSet},
    fmt::Display,
};

use dozer_api::generator::protoc::generator::ProtoGenerator;
use dozer_core::{
    daggy,
    petgraph::{
        dot,
        visit::{EdgeRef, IntoEdgeReferences, IntoEdgesDirected, IntoNodeReferences},
        Direction,
    },
};
use dozer_types::grpc_types::{contract::Schema, conversions::field_definition_to_grpc};

use crate::errors::BuildError;

use super::{Contract, NodeKind};

impl Contract {
    pub fn get_source_schemas(&self, connection_name: &str) -> Option<HashMap<String, Schema>> {
        // Find the source node.
        for (node_index, node) in self.pipeline.0.node_references() {
            if let NodeKind::Source { port_names, .. } = &node.kind {
                if node.handle.id == connection_name {
                    let mut result = HashMap::new();
                    for edge in self
                        .pipeline
                        .0
                        .edges_directed(node_index, Direction::Outgoing)
                    {
                        let edge = edge.weight();
                        let name = port_names
                            .get(&edge.from_port)
                            .expect("Every port name must have been added")
                            .clone();
                        let schema = edge.schema.clone();
                        result.insert(name, map_schema(schema));
                    }
                    return Some(result);
                }
            }
        }
        None
    }

    pub fn get_endpoints_schemas(&self) -> HashMap<String, Schema> {
        self.endpoints
            .iter()
            .map(|(name, endpoint)| (name.clone(), map_schema(endpoint.schema.clone())))
            .collect()
    }

    pub fn get_graph_schemas(&self) -> HashMap<String, Schema> {
        let graph = self.create_ui_graph();
        let nodes = graph.into_graph().into_nodes_edges().0;
        nodes
            .into_iter()
            .filter_map(|node| {
                let node = node.weight;
                node.output_schema
                    .map(|schema| (node.kind.to_string(), schema))
            })
            .collect()
    }

    pub fn generate_dot(&self) -> String {
        dot::Dot::new(&self.create_ui_graph()).to_string()
    }

    pub fn get_protos(&self) -> Result<(Vec<String>, Vec<String>), BuildError> {
        let mut protos = vec![];
        let mut set = HashSet::new();
        let mut libs = vec![];
        for (endpoint_name, schema) in &self.endpoints {
            let rendered_proto = ProtoGenerator::render(endpoint_name, schema)
                .map_err(BuildError::FailedToGenerateProtoFiles)?;
            for proto in rendered_proto.protos {
                if !set.contains(&proto.name) {
                    protos.push(proto.content);
                    set.insert(proto.name);
                }
            }
            libs.extend(rendered_proto.libraries);
        }
        libs.dedup();
        Ok((protos, libs))
    }

    fn create_ui_graph(&self) -> UiGraph {
        let mut ui_graph = UiGraph::new();
        let mut pipeline_node_index_to_ui_node_index = HashMap::new();
        let mut pipeline_source_to_ui_node_index = HashMap::new();

        // Create nodes.
        for (node_index, node) in self.pipeline.0.node_references() {
            match &node.kind {
                NodeKind::Source { typ, port_names } => {
                    // Create connection ui node.
                    let connection_node_index = ui_graph.add_node(UiNodeType {
                        kind: UiNodeKind::Connection {
                            typ: typ.clone(),
                            name: node.handle.id.clone(),
                        },
                        output_schema: None,
                    });
                    pipeline_node_index_to_ui_node_index.insert(node_index, connection_node_index);

                    // Create source ui node. Schema comes from connection's outgoing edge.
                    for edge in self
                        .pipeline
                        .0
                        .edges_directed(node_index, Direction::Outgoing)
                    {
                        if let std::collections::hash_map::Entry::Vacant(entry) =
                            pipeline_source_to_ui_node_index
                                .entry((node_index, edge.weight().from_port))
                        {
                            let edge = edge.weight();
                            let schema = edge.schema.clone();
                            let source_node_index = ui_graph.add_node(UiNodeType {
                                kind: UiNodeKind::Source {
                                    name: port_names[&edge.from_port].clone(),
                                },
                                output_schema: Some(map_schema(schema)),
                            });
                            entry.insert(source_node_index);
                        }
                    }
                }
                NodeKind::Processor { typ } => {
                    // Create processor ui node. Schema comes from the outgoing edge.
                    let mut edges = self
                        .pipeline
                        .0
                        .edges_directed(node_index, Direction::Outgoing)
                        .collect::<Vec<_>>();
                    assert!(
                        edges.len() == 1,
                        "We only support visualizing processors with one output port"
                    );
                    let edge = edges.remove(0);

                    let processor_node_index = ui_graph.add_node(UiNodeType {
                        kind: UiNodeKind::Processor {
                            typ: typ.clone(),
                            name: node.handle.id.clone(),
                        },
                        output_schema: Some(map_schema(edge.weight().schema.clone())),
                    });
                    pipeline_node_index_to_ui_node_index.insert(node_index, processor_node_index);
                }
                NodeKind::Sink => {
                    // Create sink ui node. Schema comes from endpoint.
                    let schema = self.endpoints[&node.handle.id].schema.clone();
                    let sink_node_index = ui_graph.add_node(UiNodeType {
                        kind: UiNodeKind::Sink {
                            name: node.handle.id.clone(),
                        },
                        output_schema: Some(map_schema(schema)),
                    });
                    pipeline_node_index_to_ui_node_index.insert(node_index, sink_node_index);
                }
            }
        }

        // Create edges.
        for edge in self.pipeline.0.edge_references() {
            let from_node_index = edge.source();
            let to_node_index = edge.target();
            let from_ui_node_index = pipeline_node_index_to_ui_node_index[&from_node_index];
            let to_ui_node_index = pipeline_node_index_to_ui_node_index[&to_node_index];

            let from_node = &self.pipeline.0[from_node_index];
            let kind = &from_node.kind;
            match &kind {
                NodeKind::Source { .. } => {
                    let ui_source_node_index = pipeline_source_to_ui_node_index
                        [&(from_node_index, edge.weight().from_port)];
                    // Connect ui connection node to ui source node.
                    ui_graph
                        .add_edge(from_ui_node_index, ui_source_node_index, UiEdgeType)
                        .unwrap();
                    // Connect ui source node to target node.
                    ui_graph
                        .add_edge(ui_source_node_index, to_ui_node_index, UiEdgeType)
                        .unwrap();
                }
                _ => {
                    // Connect ui node to target node.
                    ui_graph
                        .add_edge(from_ui_node_index, to_ui_node_index, UiEdgeType)
                        .unwrap();
                }
            }
        }

        remove_from_processor(&ui_graph)
    }
}

#[derive(Debug, Clone)]
struct UiNodeType {
    kind: UiNodeKind,
    output_schema: Option<Schema>,
}

impl Display for UiNodeType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.kind.fmt(f)
    }
}

#[derive(Debug, Clone)]
enum UiNodeKind {
    Connection { typ: String, name: String },
    Source { name: String },
    Processor { typ: String, name: String },
    Sink { name: String },
}

impl Display for UiNodeKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            UiNodeKind::Connection { typ, name } => write!(f, "connection::{typ}::{name}"),
            UiNodeKind::Source { name } => write!(f, "source::source::{name}"),
            UiNodeKind::Processor { typ, name } => write!(f, "processor::{typ}::{name}"),
            UiNodeKind::Sink { name } => write!(f, "sink::sink::{name}"),
        }
    }
}

#[derive(Debug)]
struct UiEdgeType;

impl Display for UiEdgeType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "")
    }
}

type UiGraph = daggy::Dag<UiNodeType, UiEdgeType>;

fn map_schema(schema: dozer_types::types::Schema) -> Schema {
    Schema {
        primary_index: schema.primary_index.into_iter().map(|i| i as i32).collect(),
        fields: field_definition_to_grpc(schema.fields),
    }
}

fn remove_from_processor(graph: &UiGraph) -> UiGraph {
    let mut output = UiGraph::new();

    // Create nodes that are not "from".
    let mut input_node_index_to_output_node_index = HashMap::new();
    for (node_index, node) in graph.node_references() {
        if !is_from(&node.kind) {
            let output_node_index = output.add_node(node.clone());
            input_node_index_to_output_node_index.insert(node_index, output_node_index);
        }
    }

    // Map "from" nodes to its only child.
    let mut input_from_node_index_to_output_node_index = HashMap::new();
    for (node_index, node) in graph.node_references() {
        if is_from(&node.kind) {
            let mut edges = graph
                .edges_directed(node_index, Direction::Outgoing)
                .collect::<Vec<_>>();
            assert!(
                edges.len() == 1,
                "We only support visualizing processors with one output port"
            );
            let edge = edges.remove(0);
            let output_node_index = input_node_index_to_output_node_index[&edge.target()];
            input_from_node_index_to_output_node_index.insert(node_index, output_node_index);
        }
    }

    // Create edges. Edges pointing to "from" nodes are pointed to its only child. Edges from "from" nodes are ignored.
    for edge in graph.edge_references() {
        let input_source_index = edge.source();
        let input_target_index = edge.target();
        if let Some(output_source_index) =
            input_node_index_to_output_node_index.get(&input_source_index)
        {
            if let Some(output_target_index) =
                input_node_index_to_output_node_index.get(&input_target_index)
            {
                output
                    .add_edge(*output_source_index, *output_target_index, UiEdgeType)
                    .unwrap();
            } else {
                output
                    .add_edge(
                        *output_source_index,
                        input_from_node_index_to_output_node_index[&input_target_index],
                        UiEdgeType,
                    )
                    .unwrap();
            }
        } else {
            // Ignore
        }
    }

    output
}

fn is_from(node_kind: &UiNodeKind) -> bool {
    if let UiNodeKind::Processor { typ, .. } = node_kind {
        typ == "Table"
    } else {
        false
    }
}
