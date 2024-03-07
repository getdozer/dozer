use std::{
    collections::{hash_map::Entry, HashMap},
    fmt::Debug,
};

use daggy::{petgraph::visit::IntoNodeIdentifiers, NodeIndex};
use dozer_types::{
    log::warn,
    node::{NodeHandle, OpIdentifier},
};

use crate::{
    dag_schemas::{DagHaveSchemas, DagSchemas, EdgeType},
    errors::ExecutionError,
    event::EventHub,
    node::{Processor, Sink, SinkFactory, Source},
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
        last_checkpoint: Option<OpIdentifier>,
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
    event_hub: EventHub,
}

impl BuilderDag {
    pub async fn new(
        dag_schemas: DagSchemas,
        event_hub_capacity: usize,
    ) -> Result<Self, ExecutionError> {
        // Collect input output schemas.
        let mut input_schemas = HashMap::new();
        let mut output_schemas = HashMap::new();
        for node_index in dag_schemas.graph().node_identifiers() {
            input_schemas.insert(node_index, dag_schemas.get_node_input_schemas(node_index));
            output_schemas.insert(node_index, dag_schemas.get_node_output_schemas(node_index));
        }

        // Collect sources that may affect a node.
        let mut affecting_sources = dag_schemas
            .graph()
            .node_identifiers()
            .map(|node_index| dag_schemas.collect_ancestor_sources(node_index))
            .collect::<Vec<_>>();

        // Prepare nodes and edges for consuming.
        let (nodes, edges) = dag_schemas.into_graph().into_graph().into_nodes_edges();
        let mut nodes = nodes
            .into_iter()
            .map(|node| Some(node.weight))
            .collect::<Vec<_>>();

        // Build the sinks and load checkpoint.
        let event_hub = EventHub::new(event_hub_capacity);
        let mut graph = daggy::Dag::new();
        let mut source_states = HashMap::new();
        let mut source_op_ids = HashMap::new();
        let mut source_id_to_sinks = HashMap::<NodeHandle, Vec<NodeIndex>>::new();
        let mut node_index_map: HashMap<NodeIndex, NodeIndex> = HashMap::new();
        for (node_index, node) in nodes.iter_mut().enumerate() {
            if let Some((handle, sink)) = take_sink(node) {
                let sources = std::mem::take(&mut affecting_sources[node_index]);
                if sources.len() > 1 {
                    warn!("Multiple sources ({sources:?}) connected to same sink: {handle}");
                }
                let source = sources.into_iter().next().expect("sink must have a source");

                let node_index = NodeIndex::new(node_index);
                let mut sink = sink
                    .build(
                        input_schemas
                            .remove(&node_index)
                            .expect("we collected all input schemas"),
                        event_hub.clone(),
                    )
                    .await
                    .map_err(ExecutionError::Factory)?;

                let state = sink.get_source_state().map_err(ExecutionError::Sink)?;
                if let Some(state) = state {
                    match source_states.entry(source.clone()) {
                        Entry::Occupied(entry) => {
                            if entry.get() != &state {
                                return Err(ExecutionError::SourceStateConflict(source));
                            }
                        }
                        Entry::Vacant(entry) => {
                            entry.insert(state);
                        }
                    }
                }

                let op_id = sink.get_latest_op_id().map_err(ExecutionError::Sink)?;
                if let Some(op_id) = op_id {
                    match source_op_ids.entry(source.clone()) {
                        Entry::Occupied(mut entry) => {
                            *entry.get_mut() = op_id.min(*entry.get());
                        }
                        Entry::Vacant(entry) => {
                            entry.insert(op_id);
                        }
                    }
                }

                let new_node_index = graph.add_node(NodeType {
                    handle,
                    kind: NodeKind::Sink(sink),
                });
                node_index_map.insert(node_index, new_node_index);
                source_id_to_sinks
                    .entry(source)
                    .or_default()
                    .push(new_node_index);
            }
        }

        // Build sources, processors, and collect source states.
        for (node_index, node) in nodes.iter_mut().enumerate() {
            let Some(node) = node.take() else {
                continue;
            };
            let node_index = NodeIndex::new(node_index);
            let node = match node.kind {
                DagNodeKind::Source(source) => {
                    let source = source
                        .build(
                            output_schemas
                                .remove(&node_index)
                                .expect("we collected all output schemas"),
                            event_hub.clone(),
                            source_states.remove(&node.handle),
                        )
                        .map_err(ExecutionError::Factory)?;

                    // Write state to relevant sink.
                    let state = source
                        .serialize_state()
                        .await
                        .map_err(ExecutionError::Source)?;
                    let mut checkpoint = None;
                    for sink in source_id_to_sinks.remove(&node.handle).unwrap_or_default() {
                        let sink = &mut graph[sink];
                        let sink_handle = &sink.handle;
                        let NodeKind::Sink(sink) = &mut sink.kind else {
                            unreachable!()
                        };
                        sink.set_source_state(&state)
                            .map_err(ExecutionError::Sink)?;
                        if let Some(sink_checkpoint) = source_op_ids.remove(sink_handle) {
                            checkpoint =
                                Some(checkpoint.unwrap_or(sink_checkpoint).min(sink_checkpoint));
                        }
                    }

                    NodeType {
                        handle: node.handle,
                        kind: NodeKind::Source {
                            source,
                            last_checkpoint: checkpoint,
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
                            event_hub.clone(),
                        )
                        .await
                        .map_err(ExecutionError::Factory)?;
                    NodeType {
                        handle: node.handle,
                        kind: NodeKind::Processor(processor),
                    }
                }
                DagNodeKind::Sink(_) => unreachable!(),
            };
            let new_node_index = graph.add_node(node);
            node_index_map.insert(node_index, new_node_index);
        }

        // Connect the edges.
        for edge in edges {
            graph
                .add_edge(
                    node_index_map[&edge.source()],
                    node_index_map[&edge.target()],
                    edge.weight,
                )
                .expect("we know there's no loop");
        }

        Ok(BuilderDag { graph, event_hub })
    }

    pub fn graph(&self) -> &daggy::Dag<NodeType, EdgeType> {
        &self.graph
    }

    pub fn into_graph_and_event_hub(self) -> (daggy::Dag<NodeType, EdgeType>, EventHub) {
        (self.graph, self.event_hub)
    }
}

fn take_sink(node: &mut Option<super::NodeType>) -> Option<(NodeHandle, Box<dyn SinkFactory>)> {
    let super::NodeType { handle, kind } = node.take()?;
    if let super::NodeKind::Sink(sink) = kind {
        Some((handle, sink))
    } else {
        *node = Some(super::NodeType { handle, kind });
        None
    }
}
