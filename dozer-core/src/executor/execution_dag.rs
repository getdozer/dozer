use std::{
    collections::{hash_map::Entry, HashMap},
    fmt::Debug,
    sync::Arc,
};

use crate::{
    builder_dag::{BuilderDag, NodeKind},
    checkpoint::OptionCheckpoint,
    dag_schemas::EdgeKind,
    error_manager::ErrorManager,
    errors::ExecutionError,
    executor_operation::ExecutorOperation,
    forwarder::SenderWithPortMapping,
    hash_map_to_vec::insert_vec_element,
    node::{OutputPortType, PortHandle},
    record_store::{create_record_writer, RecordWriter},
};
use crossbeam::channel::{bounded, Receiver, Sender};
use daggy::petgraph::{
    visit::{EdgeRef, IntoEdges, IntoEdgesDirected},
    Direction,
};
use dozer_log::tokio::sync::Mutex;
use dozer_tracing::LabelsAndProgress;
use dozer_types::node::NodeHandle;

#[derive(Debug)]
pub struct NodeType {
    pub handle: NodeHandle,
    pub kind: Option<NodeKind>,
}

type SharedRecordWriter = Arc<Mutex<Option<Box<dyn RecordWriter>>>>;

#[derive(Debug, Clone)]
pub struct EdgeType {
    /// Output port handle.
    pub output_port: PortHandle,
    /// Edge kind.
    pub edge_kind: EdgeKind,
    /// The sender for data flowing downstream. Edges that have same source and target node share the same sender.
    pub sender: Sender<ExecutorOperation>,
    /// The record writer for persisting data for downstream queries, if persistency is needed. Different edges with the same output port share the same record writer.
    pub record_writer: SharedRecordWriter,
    /// Input port handle.
    pub input_port: PortHandle,
    /// The receiver from receiving data from upstream. Edges that have same source and target node share the same receiver.
    pub receiver: Receiver<ExecutorOperation>,
}

#[derive(Debug)]
pub struct ExecutionDag {
    /// Nodes will be moved into execution threads.
    graph: daggy::Dag<NodeType, EdgeType>,
    initial_epoch_id: u64,
    error_manager: Arc<ErrorManager>,
    labels: LabelsAndProgress,
}

impl ExecutionDag {
    pub async fn new(
        builder_dag: BuilderDag,
        checkpoint: OptionCheckpoint,
        labels: LabelsAndProgress,
        channel_buffer_sz: usize,
        error_threshold: Option<u32>,
    ) -> Result<Self, ExecutionError> {
        // We only create record writer once for every output port. Every `HashMap` in this `Vec` tracks if a node's output ports already have the record writer created.
        let mut all_record_writers = vec![
            HashMap::<PortHandle, SharedRecordWriter>::new();
            builder_dag.graph().node_count()
        ];
        // We only create channel once for every pair of nodes.
        let mut channels = HashMap::<
            (daggy::NodeIndex, daggy::NodeIndex),
            (Sender<ExecutorOperation>, Receiver<ExecutorOperation>),
        >::new();

        // Create new edges.
        let mut edges = vec![];
        for builder_dag_edge in builder_dag.graph().raw_edges().iter() {
            let source_node_index = builder_dag_edge.source();
            let target_node_index = builder_dag_edge.target();
            let edge = &builder_dag_edge.weight;
            let output_port = edge.output_port;
            let edge_kind = edge.edge_kind.clone();

            // Create or get record writer.
            let record_writer =
                match all_record_writers[source_node_index.index()].entry(output_port) {
                    Entry::Vacant(entry) => {
                        let record_writer = match &edge_kind {
                            EdgeKind::FromSource {
                                port_type: OutputPortType::StatefulWithPrimaryKeyLookup,
                                port_name,
                            } => {
                                let record_writer_data = checkpoint
                                    .load_record_writer_data(
                                        &builder_dag.graph()[source_node_index].handle,
                                        port_name,
                                    )
                                    .await?;
                                Some(
                                    create_record_writer(edge.schema.clone(), record_writer_data)
                                        .map_err(ExecutionError::RestoreRecordWriter)?,
                                )
                            }
                            _ => None,
                        };
                        let record_writer = Arc::new(Mutex::new(record_writer));
                        entry.insert(record_writer).clone()
                    }
                    Entry::Occupied(entry) => entry.get().clone(),
                };

            // Create or get channel.
            let (sender, receiver) = match channels.entry((source_node_index, target_node_index)) {
                Entry::Vacant(entry) => {
                    let (sender, receiver) = bounded(channel_buffer_sz);
                    entry.insert((sender.clone(), receiver.clone()));
                    (sender, receiver)
                }
                Entry::Occupied(entry) => entry.get().clone(),
            };

            // Create edge.
            let edge = EdgeType {
                output_port,
                edge_kind,
                sender,
                record_writer,
                input_port: edge.input_port,
                receiver,
            };
            edges.push(Some(edge));
        }

        // Create new graph.
        let initial_epoch_id = checkpoint.next_epoch_id();
        let graph = builder_dag.into_graph().map_owned(
            |_, node| NodeType {
                handle: node.handle,
                kind: Some(node.kind),
            },
            |edge_index, _| {
                edges[edge_index.index()]
                    .take()
                    .expect("We created all edges")
            },
        );
        Ok(ExecutionDag {
            graph,
            initial_epoch_id,
            error_manager: Arc::new(if let Some(threshold) = error_threshold {
                ErrorManager::new_threshold(threshold)
            } else {
                ErrorManager::new_unlimited()
            }),
            labels,
        })
    }

    pub fn graph(&self) -> &daggy::Dag<NodeType, EdgeType> {
        &self.graph
    }

    pub fn node_weight_mut(&mut self, node_index: daggy::NodeIndex) -> &mut NodeType {
        &mut self.graph[node_index]
    }

    pub fn initial_epoch_id(&self) -> u64 {
        self.initial_epoch_id
    }

    pub fn error_manager(&self) -> &Arc<ErrorManager> {
        &self.error_manager
    }

    pub fn labels(&self) -> &LabelsAndProgress {
        &self.labels
    }

    pub fn collect_senders(&self, node_index: daggy::NodeIndex) -> Vec<SenderWithPortMapping> {
        // Map from target node index to `SenderWithPortMapping`.
        let mut senders = HashMap::<daggy::NodeIndex, SenderWithPortMapping>::new();
        for edge in self.graph.edges(node_index) {
            match senders.entry(edge.target()) {
                Entry::Vacant(entry) => {
                    let port_mapping =
                        [(edge.weight().output_port, vec![edge.weight().input_port])]
                            .into_iter()
                            .collect();
                    entry.insert(SenderWithPortMapping {
                        sender: edge.weight().sender.clone(),
                        port_mapping,
                    });
                }
                Entry::Occupied(mut entry) => {
                    insert_vec_element(
                        &mut entry.get_mut().port_mapping,
                        edge.weight().output_port,
                        edge.weight().input_port,
                    );
                }
            }
        }
        senders.into_values().collect()
    }

    pub async fn collect_record_writers(
        &mut self,
        node_index: daggy::NodeIndex,
    ) -> HashMap<PortHandle, Box<dyn RecordWriter>> {
        let edge_indexes = self
            .graph
            .edges(node_index)
            .map(|edge| edge.id())
            .collect::<Vec<_>>();

        let mut record_writers = HashMap::new();
        for edge_index in edge_indexes {
            let edge = self
                .graph
                .edge_weight_mut(edge_index)
                .expect("We don't modify graph structure, only modify the edge weight");

            if let Entry::Vacant(entry) = record_writers.entry(edge.output_port) {
                // This interior mutability is to work around `Arc`. Other parts of this function is correctly marked `mut`.
                if let Some(record_writer) = edge.record_writer.lock().await.take() {
                    entry.insert(record_writer);
                }
            }
        }

        record_writers
    }

    pub fn collect_receivers(
        &self,
        node_index: daggy::NodeIndex,
    ) -> (Vec<NodeHandle>, Vec<Receiver<ExecutorOperation>>) {
        // Map from source node index to source node handle and the receiver to receiver from source.
        let mut handles_and_receivers =
            HashMap::<daggy::NodeIndex, (NodeHandle, Receiver<ExecutorOperation>)>::new();
        for edge in self.graph.edges_directed(node_index, Direction::Incoming) {
            let source_node_index = edge.source();
            if let Entry::Vacant(entry) = handles_and_receivers.entry(source_node_index) {
                entry.insert((
                    self.graph[source_node_index].handle.clone(),
                    edge.weight().receiver.clone(),
                ));
            }
        }
        handles_and_receivers.into_values().unzip()
    }
}
