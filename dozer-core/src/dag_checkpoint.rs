use daggy::petgraph::visit::IntoNodeIdentifiers;
use dozer_types::node::{NodeHandle, OpIdentifier};

use crate::{
    dag_schemas::{DagHaveSchemas, DagSchemas, EdgeType},
    errors::ExecutionError,
    node::{ProcessorFactory, SinkFactory, Source},
    NodeKind as DagNodeKind,
};

#[derive(Debug)]
/// Node in the checkpoint DAG.
pub struct NodeType<T> {
    /// The node handle.
    pub handle: NodeHandle,
    /// The node kind.
    pub kind: NodeKind<T>,
}

#[derive(Debug)]
/// Node kind, source, processor or sink. Source has a checkpoint to start from.
pub enum NodeKind<T> {
    Source((Box<dyn Source>, Option<OpIdentifier>)),
    Processor(Box<dyn ProcessorFactory<T>>),
    Sink(Box<dyn SinkFactory<T>>),
}

/// Checkpoint DAG determines the checkpoint to start the pipeline from.
///
/// This DAG is always consistent.
#[derive(Debug)]
pub struct DagCheckpoint<T> {
    graph: daggy::Dag<NodeType<T>, EdgeType>,
}

impl<T> DagCheckpoint<T> {
    pub fn new(dag_schemas: DagSchemas<T>) -> Result<Self, ExecutionError> {
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

    pub fn into_graph(self) -> daggy::Dag<NodeType<T>, EdgeType> {
        self.graph
    }
}

impl<T> DagHaveSchemas for DagCheckpoint<T> {
    type NodeType = NodeType<T>;
    type EdgeType = EdgeType;

    fn graph(&self) -> &daggy::Dag<Self::NodeType, Self::EdgeType> {
        &self.graph
    }
}
