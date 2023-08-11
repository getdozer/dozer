use std::{fmt::Debug, sync::Arc};

use daggy::petgraph::visit::IntoNodeIdentifiers;
use dozer_types::node::{NodeHandle, OpIdentifier};

use crate::{
    dag_checkpoint::{DagCheckpoint, NodeKind as CheckpointNodeKind},
    dag_schemas::{DagHaveSchemas, DagSchemas, EdgeType},
    errors::ExecutionError,
    node::{Processor, Sink, Source},
    processor_record::ProcessorRecordStore,
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
    Source(Box<dyn Source>, Option<OpIdentifier>),
    Processor(Box<dyn Processor>),
    Sink(Box<dyn Sink>),
}

/// Builder DAG builds all the sources, processors and sinks.
/// It also asks each source if its possible to start from the given checkpoint.
/// If not possible, it resets metadata and updates the checkpoint.
#[derive(Debug)]
pub struct BuilderDag {
    graph: daggy::Dag<NodeType, EdgeType>,
    record_store: Arc<ProcessorRecordStore>,
}

impl BuilderDag {
    pub fn new<T>(dag_schemas: DagSchemas<T>) -> Result<Self, ExecutionError> {
        // Decide the checkpoint to start from.
        let dag_checkpoint = DagCheckpoint::new(dag_schemas)?;

        // Create processors and sinks.
        let record_store = Arc::new(ProcessorRecordStore::new()?);
        let mut nodes = vec![];
        let node_indexes = dag_checkpoint
            .graph()
            .node_identifiers()
            .collect::<Vec<_>>();
        for node_index in node_indexes.iter().copied() {
            // Create and initialize source, processor or sink.
            let input_schemas = dag_checkpoint.get_node_input_schemas(node_index);
            let output_schemas = dag_checkpoint.get_node_output_schemas(node_index);

            let node = &dag_checkpoint.graph()[node_index];
            let kind = match &node.kind {
                CheckpointNodeKind::Source(_) => None,
                CheckpointNodeKind::Processor(processor) => {
                    let processor = processor
                        .build(input_schemas, output_schemas, &record_store)
                        .map_err(ExecutionError::Factory)?;
                    Some(NodeKind::Processor(processor))
                }
                CheckpointNodeKind::Sink(sink) => {
                    let sink = sink.build(input_schemas).map_err(ExecutionError::Factory)?;
                    Some(NodeKind::Sink(sink))
                }
            };

            nodes.push(kind);
        }

        // Create new graph.
        let graph = dag_checkpoint.into_graph().map_owned(
            |node_index, node| {
                if let Some(kind) = nodes[node_index.index()].take() {
                    NodeType {
                        handle: node.handle,
                        kind,
                    }
                } else {
                    NodeType {
                        handle: node.handle,
                        kind: match node.kind {
                            CheckpointNodeKind::Source((source, checkpoint)) => {
                                NodeKind::Source(source, checkpoint)
                            }
                            CheckpointNodeKind::Processor(_) | CheckpointNodeKind::Sink(_) => {
                                unreachable!("We created all processors and sinks")
                            }
                        },
                    }
                }
            },
            |_, edge| edge,
        );
        Ok(BuilderDag {
            graph,
            record_store,
        })
    }

    pub fn graph(&self) -> &daggy::Dag<NodeType, EdgeType> {
        &self.graph
    }

    pub fn record_store(&self) -> &Arc<ProcessorRecordStore> {
        &self.record_store
    }

    pub fn into_graph(self) -> daggy::Dag<NodeType, EdgeType> {
        self.graph
    }
}
