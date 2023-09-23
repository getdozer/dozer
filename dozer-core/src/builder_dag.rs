use std::{collections::HashMap, fmt::Debug, sync::Arc};

use daggy::petgraph::visit::{IntoNodeIdentifiers, IntoNodeReferences};
use dozer_types::node::NodeHandle;

use crate::{
    checkpoint::{CheckpointFactory, CheckpointFactoryOptions, OptionCheckpoint},
    dag_schemas::{DagHaveSchemas, DagSchemas, EdgeType},
    errors::ExecutionError,
    node::{PortHandle, Processor, Sink, Source, SourceState},
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
        port_names: HashMap<PortHandle, String>,
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
    checkpoint_factory: Arc<CheckpointFactory>,
    initial_epoch_id: u64,
}

impl BuilderDag {
    pub async fn new(
        checkpoint: OptionCheckpoint,
        options: CheckpointFactoryOptions,
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
        let graph = dag_schemas.into_graph().try_map(
            |node_index, node| match node.kind {
                DagNodeKind::Source(source) => {
                    let mut port_names = HashMap::default();
                    for port in source.get_output_ports() {
                        let port_name = source.get_output_port_name(&port.handle);
                        port_names.insert(port.handle, port_name);
                    }

                    let mut last_checkpoint_by_name = checkpoint.get_source_state(&node.handle)?;
                    let mut last_checkpoint = HashMap::new();
                    for (port_handle, port_name) in port_names.iter() {
                        last_checkpoint.insert(
                            *port_handle,
                            last_checkpoint_by_name
                                .as_mut()
                                .and_then(|last_checkpoint| {
                                    last_checkpoint.remove(port_name).flatten()
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

                    Ok::<_, ExecutionError>(NodeType {
                        handle: node.handle,
                        kind: NodeKind::Source {
                            source,
                            port_names,
                            last_checkpoint,
                        },
                    })
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
                        .map_err(ExecutionError::Factory)?;
                    Ok(NodeType {
                        handle: node.handle,
                        kind: NodeKind::Processor(processor),
                    })
                }
                DagNodeKind::Sink(sink) => {
                    let sink = sink
                        .build(
                            input_schemas
                                .remove(&node_index)
                                .expect("we collected all input schemas"),
                        )
                        .map_err(ExecutionError::Factory)?;
                    Ok(NodeType {
                        handle: node.handle,
                        kind: NodeKind::Sink(sink),
                    })
                }
            },
            |_, edge| Ok(edge),
        )?;

        let initial_epoch_id = checkpoint.next_epoch_id();
        let (checkpoint_factory, _) = CheckpointFactory::new(checkpoint, options).await?;

        Ok(BuilderDag {
            graph,
            initial_epoch_id,
            checkpoint_factory: Arc::new(checkpoint_factory),
        })
    }

    pub fn graph(&self) -> &daggy::Dag<NodeType, EdgeType> {
        &self.graph
    }

    pub fn checkpoint_factory(&self) -> &Arc<CheckpointFactory> {
        &self.checkpoint_factory
    }

    pub fn initial_epoch_id(&self) -> u64 {
        self.initial_epoch_id
    }

    pub fn into_graph(self) -> daggy::Dag<NodeType, EdgeType> {
        self.graph
    }
}
