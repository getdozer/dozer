use std::collections::HashMap;

use dozer_core::{
    daggy,
    petgraph::{
        dot,
        visit::{EdgeRef, IntoEdgeReferences, IntoEdgesDirected, IntoNodeReferences},
        Direction,
    },
};
use dozer_types::grpc_types::{contract::Schema, conversions::field_definition_to_grpc};

use super::{Contract, NodeKind};

impl Contract {
    pub fn get_source_schemas(&self, connection_name: &str) -> Option<HashMap<String, Schema>> {
        // Find the source node.
        for (node_index, node) in self.pipeline.node_references() {
            if let NodeKind::Source { port_names } = &node.kind {
                if node.handle.id == connection_name {
                    let mut result = HashMap::new();
                    for edge in self
                        .pipeline
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

    // Return all input schemas for a given node
    // Hence, source schemas are to be mapped separately.
    pub fn get_graph_schemas(&self) -> HashMap<String, Schema> {
        let mut schemas = HashMap::new();
        for (node_index, node) in self.pipeline.node_references() {
            // ignore source schemas

            match &node.kind {
                NodeKind::Sink => {
                    for edge in self
                        .pipeline
                        .edges_directed(node_index, Direction::Incoming)
                    {
                        let edge = edge.weight();
                        let schema = edge.schema.clone();
                        let id = node.handle.id.clone();
                        let label = get_label(&node.kind, false);
                        let key = format!("{}::{}", label, id);
                        schemas.insert(key, map_schema(schema.clone()));
                    }
                }
                _ => {
                    for edge in self
                        .pipeline
                        .edges_directed(node_index, Direction::Outgoing)
                    {
                        let edge = edge.weight();
                        let schema = edge.schema.clone();
                        let id = if let NodeKind::Source { port_names } = &node.kind {
                            port_names
                                .get(&edge.from_port)
                                .expect("All output port names must have been added")
                                .clone()
                        } else {
                            node.handle.id.clone()
                        };
                        let label = get_label(&node.kind, false);
                        let key = format!("{}::{}", label, id);
                        schemas.insert(key, map_schema(schema.clone()));
                    }
                }
            }
        }
        schemas
    }

    pub fn generate_dot(&self) -> String {
        let mut dag = daggy::Dag::<String, &'static str>::new();
        let mut node_index_map = HashMap::new();
        self.pipeline
            .node_references()
            .for_each(|(node_index, node)| {
                let label = get_label(&node.kind, true);
                let new_node_index = dag.add_node(format!("{}::{}", label, node.handle.id));
                node_index_map.insert(node_index, new_node_index);
            });
        self.pipeline.edge_references().for_each(|edge| {
            let source_node_index = edge.source();
            let source_node = &self.pipeline[source_node_index];
            let target_node_index = edge.target();

            let source_node_index = node_index_map[&source_node_index];
            let target_node_index = node_index_map[&target_node_index];

            let from_port = edge.weight().from_port;
            let kind = &source_node.kind;
            let label = get_label(kind, false);
            match &kind {
                NodeKind::Source { port_names } => {
                    let source_name = port_names
                        .get(&from_port)
                        .expect("All output port names must have been added to the source node");
                    let new_node_index = dag.add_node(format!("{}::{}", label, source_name));

                    dag.add_edge(source_node_index, new_node_index, "").unwrap();
                    dag.add_edge(new_node_index, target_node_index, "").unwrap();
                }
                _ => {
                    dag.add_edge(source_node_index, target_node_index, "")
                        .unwrap();
                }
            }
        });
        dot::Dot::new(&dag).to_string()
    }
}

fn map_schema(schema: dozer_types::types::Schema) -> Schema {
    Schema {
        primary_index: schema.primary_index.into_iter().map(|i| i as i32).collect(),
        fields: field_definition_to_grpc(schema.fields),
    }
}

fn get_label(kind: &NodeKind, match_connection: bool) -> &str {
    match kind {
        NodeKind::Source { .. } => match match_connection {
            true => "connection",
            false => "source",
        },
        NodeKind::Processor => "processor",
        NodeKind::Sink => "sink",
    }
}
