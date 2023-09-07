use daggy::petgraph::visit::IntoNodeIdentifiers;
use dozer_types::node::{NodeHandle, OpIdentifier};

use crate::{
    checkpoint::OptionCheckpoint,
    dag_schemas::{DagHaveSchemas, DagSchemas, EdgeType},
    errors::ExecutionError,
    node::{ProcessorFactory, SinkFactory, Source},
    NodeKind as DagNodeKind,
};

#[derive(Debug)]
/// Node in the checkpoint DAG.
pub struct NodeType {
    /// The node handle.
    pub handle: NodeHandle,
    /// The node kind.
    pub kind: NodeKind,
}

#[derive(Debug)]
/// Node kind, source, processor or sink. Source has a checkpoint to start from.
pub enum NodeKind {
    Source((Box<dyn Source>, Option<OpIdentifier>)),
    Processor(Box<dyn ProcessorFactory>),
    Sink(Box<dyn SinkFactory>),
}

/// Checkpoint DAG checks if the sources can start from the specified checkpoint.
#[derive(Debug)]
pub struct DagCheckpoint {
    graph: daggy::Dag<NodeType, EdgeType>,
}

impl DagCheckpoint {
    pub fn new(
        dag_schemas: DagSchemas,
        checkpoint: &OptionCheckpoint,
    ) -> Result<Self, ExecutionError> {
        // Create node storages and sources.
        let mut sources = vec![];
        let node_indexes = dag_schemas.graph().node_identifiers().collect::<Vec<_>>();

        for node_index in node_indexes.iter().copied() {
            let node = &dag_schemas.graph()[node_index];
            match &node.kind {
                DagNodeKind::Source(source) => {
                    let output_schemas = dag_schemas.get_node_output_schemas(node_index);
                    let source = source
                        .build(output_schemas)
                        .map_err(ExecutionError::Factory)?;
                    if let Some(op) = checkpoint.get_source_state(&node.handle) {
                        if !source.can_start_from(op).map_err(ExecutionError::Source)? {
                            return Err(ExecutionError::SourceCannotStartFromCheckpoint {
                                source_name: node.handle.clone(),
                                checkpoint: op,
                            });
                        }
                    }
                    sources.push(Some(source));
                }
                DagNodeKind::Processor(_) | DagNodeKind::Sink(_) => {
                    sources.push(None);
                }
            }
        }

        // Create new graph.
        let graph = dag_schemas.into_graph().map_owned(
            |node_index, node| {
                if let Some(source) = sources[node_index.index()].take() {
                    NodeType {
                        handle: node.handle,
                        kind: NodeKind::Source((source, None)),
                    }
                } else {
                    NodeType {
                        handle: node.handle,
                        kind: match node.kind {
                            DagNodeKind::Processor(processor) => NodeKind::Processor(processor),
                            DagNodeKind::Sink(sink) => NodeKind::Sink(sink),
                            DagNodeKind::Source(_) => unreachable!("We created all sources"),
                        },
                    }
                }
            },
            |_, edge| edge,
        );
        Ok(Self { graph })
    }

    pub fn into_graph(self) -> daggy::Dag<NodeType, EdgeType> {
        self.graph
    }
}

impl DagHaveSchemas for DagCheckpoint {
    type NodeType = NodeType;
    type EdgeType = EdgeType;

    fn graph(&self) -> &daggy::Dag<Self::NodeType, Self::EdgeType> {
        &self.graph
    }
}
