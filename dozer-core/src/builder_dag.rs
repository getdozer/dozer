use std::{collections::HashMap, fmt::Debug};

use daggy::{
    petgraph::visit::{IntoNodeIdentifiers, IntoNodeReferences},
    NodeIndex,
};
use dozer_types::node::NodeHandle;

use crate::{
    checkpoint::OptionCheckpoint,
    dag_schemas::{DagHaveSchemas, DagSchemas, EdgeType},
    errors::ExecutionError,
    node::{Processor, Sink, Source, SourceState},
    NodeKind as DagNodeKind,
};

#[derive(Debug)]
/// Node in the builder DAG.
pub struct NodeType {
    /// The node handle.
    pub handle: NodeHandle,
    /// The node kind.
    pub kind: NodeKind,
}

#[derive(Debug)]
/// Node kind, source, processor or sink. Source has a checkpoint to start from.
pub enum NodeKind {
    Source {
        source: Box<dyn Source>,
        last_checkpoint: SourceState,
    },
    Processor(Box<dyn Processor>),
    Sink(Box<dyn Sink>),
}

/// Builder DAG builds all the sources, processors and sinks.
/// It also asks each source if its possible to start from the given checkpoint.
/// If not possible, it resets metadata and updates the checkpoint.
#[derive(Debug)]
pub struct BuilderDag {
    graph: daggy::Dag<NodeType, EdgeType>,
}

impl BuilderDag {
    pub async fn new(
        checkpoint: &OptionCheckpoint,
        dag_schemas: DagSchemas,
    ) -> Result<Self, ExecutionError> {
        // Collect input output schemas.
        let mut input_schemas = HashMap::new();
        let mut output_schemas = HashMap::new();
        for node_index in dag_schemas.graph().node_identifiers() {
            input_schemas.insert(node_index, dag_schemas.get_node_input_schemas(node_index));
            output_schemas.insert(node_index, dag_schemas.get_node_output_schemas(node_index));
        }

        // Load processor checkpoint data.
        let mut checkpoint_data = HashMap::new();
        for (node_index, node) in dag_schemas.graph().node_references() {
            if let DagNodeKind::Processor(_) = &node.kind {
                let processor_data = checkpoint.load_processor_data(&node.handle).await?;
                checkpoint_data.insert(node_index, processor_data);
            }
        }

        // Build the nodes.
        let mut graph = daggy::Dag::new();
        let (nodes, edges) = dag_schemas.into_graph().into_graph().into_nodes_edges();
        for (node_index, node) in nodes.into_iter().enumerate() {
            let node_index = NodeIndex::new(node_index);
            let node = node.weight;
            let node = match node.kind {
                DagNodeKind::Source(source) => {
                    let mut last_checkpoint_by_name = checkpoint.get_source_state(&node.handle)?;
                    let mut last_checkpoint = HashMap::new();
                    for port_def in source.get_output_ports() {
                        let port_name = source.get_output_port_name(&port_def.handle);
                        last_checkpoint.insert(
                            port_def.handle,
                            last_checkpoint_by_name
                                .as_mut()
                                .and_then(|last_checkpoint| {
                                    last_checkpoint.remove(&port_name).flatten()
                                }),
                        );
                    }

                    let source = source
                        .build(
                            output_schemas
                                .remove(&node_index)
                                .expect("we collected all output schemas"),
                        )
                        .map_err(ExecutionError::Factory)?;

                    NodeType {
                        handle: node.handle,
                        kind: NodeKind::Source {
                            source,
                            last_checkpoint,
                        },
                    }
                }
                DagNodeKind::Processor(processor) => {
                    let processor = processor
                        .build(
                            input_schemas
                                .remove(&node_index)
                                .expect("we collected all input schemas"),
                            output_schemas
                                .remove(&node_index)
                                .expect("we collected all output schemas"),
                            checkpoint.record_store(),
                            checkpoint_data
                                .remove(&node_index)
                                .expect("we collected all processor checkpoint data"),
                        )
                        .await
                        .map_err(ExecutionError::Factory)?;
                    NodeType {
                        handle: node.handle,
                        kind: NodeKind::Processor(processor),
                    }
                }
                DagNodeKind::Sink(sink) => {
                    let sink = sink
                        .build(
                            input_schemas
                                .remove(&node_index)
                                .expect("we collected all input schemas"),
                        )
                        .map_err(ExecutionError::Factory)?;
                    NodeType {
                        handle: node.handle,
                        kind: NodeKind::Sink(sink),
                    }
                }
            };
            graph.add_node(node);
        }

        // Connect the edges.
        for edge in edges {
            graph
                .add_edge(edge.source(), edge.target(), edge.weight)
                .expect("we know there's no loop");
        }

        Ok(BuilderDag { graph })
    }

    pub fn graph(&self) -> &daggy::Dag<NodeType, EdgeType> {
        &self.graph
    }

    pub fn into_graph(self) -> daggy::Dag<NodeType, EdgeType> {
        self.graph
    }
}
