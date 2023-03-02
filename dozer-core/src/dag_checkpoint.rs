use std::{path::PathBuf, sync::Arc};

use daggy::petgraph::visit::IntoNodeIdentifiers;
use dozer_storage::lmdb_storage::LmdbEnvironmentOptions;
use dozer_types::{
    log::info,
    node::{NodeHandle, OpIdentifier, SourceStates},
};

use crate::{
    dag_metadata::{DagMetadata, NodeStorage},
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
    /// The node storage environment.
    pub storage: NodeStorage,
    /// Checkpoint read from the storage.
    pub commits: SourceStates,
    /// The node kind.
    pub kind: NodeKind<T>,
}

#[derive(Debug)]
/// Node kind, source, processor or sink. Source has a checkpoint to start from.
pub enum NodeKind<T> {
    Source((Box<dyn Source>, Option<OpIdentifier>)),
    Processor(Arc<dyn ProcessorFactory<T>>),
    Sink(Arc<dyn SinkFactory<T>>),
}

/// Checkpoint DAG determines the checkpoint to start the pipeline from.
///
/// This DAG is always consistent.
#[derive(Debug)]
pub struct DagCheckpoint<T> {
    graph: daggy::Dag<NodeType<T>, EdgeType>,
}

impl<T> DagCheckpoint<T> {
    pub fn new(
        dag_schemas: DagSchemas<T>,
        path: PathBuf,
        max_map_size: usize,
    ) -> Result<Self, ExecutionError> {
        // Initial consistency check.
        let mut dag_metadata = DagMetadata::new(dag_schemas, path)?;
        if !dag_metadata.check_consistency() {
            info!("[pipeline] Inconsistent, resetting");
            dag_metadata.clear();
            assert!(
                dag_metadata.check_consistency(),
                "We just deleted all metadata"
            );
        }

        // Create node storages and sources.
        let mut storage_and_sources = vec![];
        let node_indexes = dag_metadata.graph().node_identifiers().collect::<Vec<_>>();

        for node_index in node_indexes.iter().copied() {
            // Get or create node storage.
            let node_storage = if let Some(node_storage) =
                dag_metadata.node_weight_mut(node_index).storage.take()
            {
                node_storage
            } else {
                dag_metadata.initialize_node_storage(
                    node_index,
                    LmdbEnvironmentOptions {
                        max_map_sz: max_map_size,
                        ..LmdbEnvironmentOptions::default()
                    },
                )?
            };

            let node = &dag_metadata.graph()[node_index];
            match &node.kind {
                DagNodeKind::Source(source) => {
                    let output_schemas = dag_metadata.get_node_output_schemas(node_index);
                    let source = source.build(output_schemas)?;
                    // Check if source can start from the given checkpoint.
                    if let Some(checkpoint) = node.commits.get(&node.handle) {
                        if !source.can_start_from((checkpoint.txid, checkpoint.seq_in_tx))? {
                            info!(
                                "[pipeline] [{}] can not start from {:?}, resetting",
                                node.handle, checkpoint
                            );
                            dag_metadata.clear();
                        }
                    }
                    storage_and_sources.push(Some((node_storage, Some(source))));
                }
                DagNodeKind::Processor(_) | DagNodeKind::Sink(_) => {
                    storage_and_sources.push(Some((node_storage, None)));
                }
            }
        }

        // Create new graph.
        let graph = dag_metadata.into_graph().map_owned(
            |node_index, node| {
                let (storage, source) = storage_and_sources[node_index.index()]
                    .take()
                    .expect("We created all storages");
                if let Some(source) = source {
                    let checkpoint = node.commits.get(&node.handle).copied();
                    NodeType {
                        handle: node.handle,
                        storage,
                        commits: node.commits,
                        kind: NodeKind::Source((source, checkpoint)),
                    }
                } else {
                    NodeType {
                        handle: node.handle,
                        storage,
                        commits: node.commits,
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
