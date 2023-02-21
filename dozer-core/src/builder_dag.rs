use std::{fmt::Debug, path::PathBuf};

use daggy::petgraph::visit::IntoNodeIdentifiers;
use dozer_storage::lmdb_storage::LmdbEnvironmentOptions;
use dozer_types::{
    node::{NodeHandle, OpIdentifier},
    types::Schema,
};

use crate::{
    dag_metadata::{DagMetadata, NodeStorage},
    dag_schemas::{DagHaveSchemas, DagSchemas},
    errors::ExecutionError,
    node::{OutputPortType, PortHandle, Processor, Sink, Source},
    NodeKind as DagNodeKind,
};

#[derive(Debug)]
/// Node in the builder DAG.
pub struct NodeType {
    /// The node handle.
    pub handle: NodeHandle,
    /// The node storage environment.
    pub storage: NodeStorage,
    /// The node kind.
    pub kind: Option<NodeKind>,
}

#[derive(Debug)]
/// Node kind, source, processor or sink. Source has a checkpoint to start from.
pub enum NodeKind {
    Source(Box<dyn Source>, Option<OpIdentifier>),
    Processor(Box<dyn Processor>),
    Sink(Box<dyn Sink>),
}

#[derive(Debug)]
pub struct EdgeType {
    pub output_port: PortHandle,
    pub output_port_type: OutputPortType,
    pub input_port: PortHandle,
    pub schema: Schema,
}

/// Builder DAG builds all the sources, processors and sinks.
/// It also asks each source if its possible to start from the given checkpoint.
/// If not possible, it resets metadata and updates the checkpoint.
#[derive(Debug)]
pub struct BuilderDag {
    /// Node weights will be moved to execution dag.
    graph: daggy::Dag<Option<NodeType>, EdgeType>,
}

impl BuilderDag {
    pub fn new<T: Clone>(
        dag_schemas: &DagSchemas<T>,
        path: PathBuf,
        max_map_size: usize,
    ) -> Result<Self, ExecutionError> {
        // Initial consistency check.
        let mut dag_metadata = DagMetadata::new(dag_schemas, path)?;
        if !dag_metadata.check_consistency() {
            dag_metadata.clear();
            assert!(
                dag_metadata.check_consistency(),
                "We just deleted all metadata"
            );
        }

        // Create new nodes.
        let mut nodes = vec![];
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

            // Create and initialize source, processor or sink.
            let input_schemas = dag_metadata.get_node_input_schemas(node_index);
            let input_schemas = input_schemas
                .into_iter()
                .map(|(key, value)| (key, value.0))
                .collect();
            let output_schemas = dag_metadata.get_node_output_schemas(node_index);
            let output_schemas = output_schemas
                .into_iter()
                .map(|(key, value)| (key, value.0))
                .collect();

            let node = &dag_metadata.graph()[node_index];
            let temp_node_kind = match &node.kind {
                DagNodeKind::Source(source) => {
                    let source = source.build(output_schemas)?;
                    TempNodeKind::Source(source)
                }
                DagNodeKind::Processor(processor) => {
                    let processor = processor.build(
                        input_schemas,
                        output_schemas,
                        &mut node_storage.master_txn.write(),
                    )?;
                    TempNodeKind::Processor(processor)
                }
                DagNodeKind::Sink(sink) => {
                    let sink = sink.build(input_schemas)?;
                    TempNodeKind::Sink(sink)
                }
            };

            nodes.push(Some((temp_node_kind, node_storage)));
        }

        // Check if sources can start from the given checkpoint.
        for node_index in node_indexes {
            if let Some((TempNodeKind::Source(source), _)) = &nodes[node_index.index()] {
                let node = &dag_metadata.graph()[node_index];
                if let Some(checkpoint) = node.commits.get(&node.handle) {
                    if !source.can_start_from((checkpoint.txid, checkpoint.seq_in_tx))? {
                        dag_metadata.clear();
                    }
                }
            }
        }

        // Create new graph.
        let graph = dag_schemas.graph().map(
            |node_index, _| {
                let node = &dag_metadata.graph()[node_index];
                let (temp_node_kind, storage) = nodes[node_index.index()]
                    .take()
                    .expect("We created all nodes");
                let node_kind = match temp_node_kind {
                    TempNodeKind::Source(source) => {
                        let checkpoint = node.commits.get(&node.handle).copied();
                        NodeKind::Source(source, checkpoint)
                    }
                    TempNodeKind::Processor(processor) => NodeKind::Processor(processor),
                    TempNodeKind::Sink(sink) => NodeKind::Sink(sink),
                };
                Some(NodeType {
                    handle: node.handle.clone(),
                    storage,
                    kind: Some(node_kind),
                })
            },
            |_, edge| EdgeType {
                output_port: edge.output_port,
                output_port_type: edge.output_port_type,
                schema: edge.schema.clone(),
                input_port: edge.input_port,
            },
        );
        Ok(BuilderDag { graph })
    }

    pub fn graph(&self) -> &daggy::Dag<Option<NodeType>, EdgeType> {
        &self.graph
    }

    pub fn node_weight_mut(&mut self, node_index: daggy::NodeIndex) -> &mut Option<NodeType> {
        self.graph
            .node_weight_mut(node_index)
            .expect("Invalid node index")
    }
}

enum TempNodeKind {
    Source(Box<dyn Source>),
    Processor(Box<dyn Processor>),
    Sink(Box<dyn Sink>),
}
