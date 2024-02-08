use std::{
    collections::{hash_map::Entry, HashMap},
    fmt::Debug,
    sync::Arc,
};

use crate::{
    builder_dag::{BuilderDag, NodeType},
    checkpoint::{CheckpointFactory, CheckpointFactoryOptions, OptionCheckpoint},
    dag_schemas::EdgeKind,
    error_manager::ErrorManager,
    errors::ExecutionError,
    executor_operation::ExecutorOperation,
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
use dozer_recordstore::ProcessorRecordStore;
use dozer_tracing::LabelsAndProgress;

pub type SharedRecordWriter = Arc<Mutex<Option<Box<dyn RecordWriter>>>>;

#[derive(Debug, Clone)]
pub struct EdgeType {
    /// Output port handle.
    pub output_port: PortHandle,
    /// Edge kind.
    pub edge_kind: EdgeKind,
    /// The sender for data flowing downstream.
    pub sender: Sender<ExecutorOperation>,
    /// The record writer for persisting data for downstream queries, if persistency is needed. Different edges with the same output port share the same record writer.
    pub record_writer: SharedRecordWriter,
    /// Input port handle.
    pub input_port: PortHandle,
    /// The receiver from receiving data from upstream.
    pub receiver: Receiver<ExecutorOperation>,
}

#[derive(Debug)]
pub struct ExecutionDag {
    /// Nodes will be moved into execution threads.
    graph: daggy::Dag<Option<NodeType>, EdgeType>,
    initial_epoch_id: u64,
    record_store: Arc<ProcessorRecordStore>,
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
        checkpoint_factory_options: CheckpointFactoryOptions,
    ) -> Result<Self, ExecutionError> {
        // We only create record stored once for every output port. Every `HashMap` in this `Vec` tracks if a node's output ports already have the record store created.
        let mut all_record_writers = vec![
            HashMap::<PortHandle, SharedRecordWriter>::new();
            builder_dag.graph().node_count()
        ];

        // Create new edges.
        let mut edges = vec![];
        for builder_dag_edge in builder_dag.graph().raw_edges().iter() {
            let source_node_index = builder_dag_edge.source();
            let edge = &builder_dag_edge.weight;
            let output_port = edge.output_port;
            let edge_kind = edge.edge_kind.clone();

            // Create or get record store.
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
                                    create_record_writer(
                                        edge.schema.clone(),
                                        checkpoint.record_store(),
                                        record_writer_data,
                                    )
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

            // Create channel.
            let (sender, receiver) = bounded(channel_buffer_sz);

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
        let (checkpoint_factory, _) =
            CheckpointFactory::new(checkpoint, checkpoint_factory_options).await?;
        let graph = builder_dag.into_graph().map_owned(
            |_, node| Some(node),
            |edge_index, _| {
                edges[edge_index.index()]
                    .take()
                    .expect("We created all edges")
            },
        );
        Ok(ExecutionDag {
            graph,
            initial_epoch_id,
            record_store: checkpoint_factory.record_store().clone(),
            error_manager: Arc::new(if let Some(threshold) = error_threshold {
                ErrorManager::new_threshold(threshold)
            } else {
                ErrorManager::new_unlimited()
            }),
            labels,
        })
    }

    pub fn graph(&self) -> &daggy::Dag<Option<NodeType>, EdgeType> {
        &self.graph
    }

    pub fn node_weight_mut(&mut self, node_index: daggy::NodeIndex) -> &mut Option<NodeType> {
        &mut self.graph[node_index]
    }

    pub fn record_store(&self) -> &Arc<ProcessorRecordStore> {
        &self.record_store
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

    #[allow(clippy::type_complexity)]
    pub async fn collect_senders_and_record_writers(
        &mut self,
        node_index: daggy::NodeIndex,
    ) -> (
        HashMap<PortHandle, Vec<Sender<ExecutorOperation>>>,
        HashMap<PortHandle, Box<dyn RecordWriter>>,
    ) {
        let edge_indexes = self
            .graph
            .edges(node_index)
            .map(|edge| edge.id())
            .collect::<Vec<_>>();

        let mut senders = HashMap::new();
        let mut record_writers = HashMap::new();
        for edge_index in edge_indexes {
            let edge = self
                .graph
                .edge_weight_mut(edge_index)
                .expect("We don't modify graph structure, only modify the edge weight");
            insert_vec_element(&mut senders, edge.output_port, edge.sender.clone());
            if let Entry::Vacant(entry) = record_writers.entry(edge.output_port) {
                // This interior mutability is to word around `Arc`. Other parts of this function is correctly marked `mut`.
                if let Some(record_writer) = edge.record_writer.lock().await.take() {
                    entry.insert(record_writer);
                }
            }
        }

        (senders, record_writers)
    }

    pub fn collect_receivers(
        &mut self,
        node_index: daggy::NodeIndex,
    ) -> (Vec<PortHandle>, Vec<Receiver<ExecutorOperation>>) {
        let edge_indexes = self
            .graph
            .edges_directed(node_index, Direction::Incoming)
            .map(|edge| edge.id())
            .collect::<Vec<_>>();

        let mut input_ports = Vec::new();
        let mut receivers = Vec::new();
        for edge_index in edge_indexes {
            let edge = self
                .graph
                .edge_weight_mut(edge_index)
                .expect("We don't modify graph structure, only modify the edge weight");
            input_ports.push(edge.input_port);
            receivers.push(edge.receiver.clone());
        }
        (input_ports, receivers)
    }
}
