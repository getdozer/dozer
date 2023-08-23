use std::{
    collections::HashMap,
    fmt::{Display, Formatter},
};

use dozer_core::{
    dag_schemas::{DagSchemas, EdgeHaveSchema},
    daggy,
    petgraph::{
        self,
        visit::{EdgeRef, IntoEdgeReferences, IntoEdgesDirected, IntoNodeReferences},
        Direction,
    },
    EdgeHavePorts, NodeKind, NodeType,
};
use dozer_sql::pipeline::builder::SchemaSQLContext;
use dozer_types::grpc_types::contract::Schema;

use super::helper::map_schema;

pub struct Node {
    id: u32,
    label: String,
}
impl Display for Node {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.label)
    }
}

pub fn transform_dag_ui<T, E: EdgeHavePorts>(
    input_graph: &daggy::Dag<NodeType<T>, E>,
) -> daggy::Dag<Node, &str> {
    let mut dag = daggy::Dag::new();
    let mut sources: HashMap<String, daggy::NodeIndex> = HashMap::new();
    input_graph
        .node_references()
        .for_each(|(node_index, node)| {
            let node_index = node_index.index() as u32;
            let id = node.handle.id.to_string();
            let label = get_label(&node.kind, true);

            dag.add_node(Node {
                id: node_index,
                label: format!("{}::{}", label, id),
            });
        });
    input_graph.edge_references().for_each(|edge| {
        let source_node_index = edge.source();
        let target_node_index = edge.target();
        let source_index = find_node(source_node_index.index(), &dag);
        let target_index = find_node(target_node_index.index(), &dag);

        let from_port = edge.weight().output_port();
        let kind = &input_graph[source_node_index].kind;
        let label = get_label(kind, false);
        match &kind {
            dozer_core::NodeKind::Source(source) => {
                let source_name = source.get_output_port_name(&from_port);
                let new_node_idx = match sources.get(&source_name) {
                    Some(idx) => *idx,
                    None => {
                        let new_node_idx = dag.node_count();

                        let new_node_idx = dag.add_node(Node {
                            id: new_node_idx as u32,
                            label: format!("{}::{}", label, source_name),
                        });
                        sources.insert(source_name, new_node_idx);
                        new_node_idx
                    }
                };

                dag.add_edge(source_index, new_node_idx, "").unwrap();
                dag.add_edge(new_node_idx, target_index, "").unwrap();
            }
            _ => {
                dag.add_edge(source_index, target_index, "").unwrap();
            }
        }
    });
    dag
}

fn find_node(id: usize, dag: &daggy::Dag<Node, &str>) -> petgraph::graph::NodeIndex<u32> {
    dag.node_references()
        .find(|(_node_index, node)| node.id == id as u32)
        .map(|(node_index, _)| node_index)
        .unwrap()
}

// Return all input schemas for a given node
// Hence, source schemas are to be mapped separately.
pub fn map_dag_schemas(dag_schemas: &DagSchemas<SchemaSQLContext>) -> HashMap<String, Schema> {
    let mut schemas = HashMap::new();
    let graph = dag_schemas.graph();
    for (node_index, node) in graph.node_references() {
        // ignore source schemas

        match &node.kind {
            NodeKind::Sink(_) => {
                for edge in graph.edges_directed(node_index, Direction::Incoming) {
                    let edge = edge.weight();
                    let schema = edge.schema();
                    let id = node.handle.id.to_string();
                    let label = get_label(&node.kind, false);
                    let key = format!("{}::{}", label, id);
                    schemas.insert(key, map_schema(schema.clone()));
                }
            }
            _ => {
                for edge in graph.edges_directed(node_index, Direction::Outgoing) {
                    let edge = edge.weight();
                    let schema = edge.schema();
                    let id = if let NodeKind::Source(source) = &node.kind {
                        let to_port = edge.output_port;

                        source.get_output_port_name(&to_port)
                    } else {
                        node.handle.id.to_string()
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

fn get_label<T>(kind: &NodeKind<T>, match_connection: bool) -> &str {
    match kind {
        dozer_core::NodeKind::Source(_) => match match_connection {
            true => "connection",
            false => "source",
        },
        dozer_core::NodeKind::Processor(_processor) => "processor",
        dozer_core::NodeKind::Sink(_) => "sink",
    }
}
