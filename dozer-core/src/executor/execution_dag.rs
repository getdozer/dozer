use std::{
    borrow::BorrowMut,
    cell::RefCell,
    collections::{hash_map::Entry, HashMap},
    fmt::Debug,
    rc::Rc,
    sync::Arc,
};

use crate::{
    builder_dag::{BuilderDag, NodeKind, NodeType},
    epoch::EpochManager,
    error_manager::ErrorManager,
    errors::ExecutionError,
    hash_map_to_vec::insert_vec_element,
    node::PortHandle,
    record_store::{create_record_writer, RecordWriter},
};
use crossbeam::channel::{bounded, Receiver, Sender};
use daggy::petgraph::{
    visit::{EdgeRef, IntoEdges, IntoEdgesDirected, IntoNodeIdentifiers},
    Direction,
};
use dozer_types::epoch::ExecutorOperation;

pub type SharedRecordWriter = Rc<RefCell<Option<Box<dyn RecordWriter>>>>;

#[derive(Debug, Clone)]
pub struct EdgeType {
    /// Output port handle.
    pub output_port: PortHandle,
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
    epoch_manager: Arc<EpochManager>,
    error_manager: Arc<ErrorManager>,
}

impl ExecutionDag {
    pub fn new(
        builder_dag: BuilderDag,
        channel_buffer_sz: usize,
        error_threshold: Option<u32>,
    ) -> Result<Self, ExecutionError> {
        // Count number of sources.
        let num_sources = builder_dag
            .graph()
            .node_identifiers()
            .filter(|node_index| {
                matches!(
                    builder_dag.graph()[*node_index].kind,
                    NodeKind::Source(_, _)
                )
            })
            .count();

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
            let output_port = builder_dag_edge.weight.output_port;

            // Create or get record store.
            let record_writer =
                match all_record_writers[source_node_index.index()].entry(output_port) {
                    Entry::Vacant(entry) => {
                        let record_writer = create_record_writer(
                            output_port,
                            edge.output_port_type,
                            edge.schema.clone(),
                        );
                        let record_writer = if let Some(record_writer) = record_writer {
                            Rc::new(RefCell::new(Some(record_writer)))
                        } else {
                            Rc::new(RefCell::new(None))
                        };
                        entry.insert(record_writer).clone()
                    }
                    Entry::Occupied(entry) => entry.get().clone(),
                };

            // Create channel.
            let (sender, receiver) = bounded(channel_buffer_sz);

            // Create edge.
            let edge = EdgeType {
                output_port,
                sender,
                record_writer,
                input_port: edge.input_port,
                receiver,
            };
            edges.push(Some(edge));
        }

        // Create new graph.
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
            epoch_manager: Arc::new(EpochManager::new(num_sources)),
            error_manager: Arc::new(if let Some(threshold) = error_threshold {
                ErrorManager::new_threshold(threshold)
            } else {
                ErrorManager::new_unlimited()
            }),
        })
    }

    pub fn graph(&self) -> &daggy::Dag<Option<NodeType>, EdgeType> {
        &self.graph
    }

    pub fn node_weight_mut(&mut self, node_index: daggy::NodeIndex) -> &mut Option<NodeType> {
        &mut self.graph[node_index]
    }

    pub fn epoch_manager(&self) -> &Arc<EpochManager> {
        &self.epoch_manager
    }

    pub fn error_manager(&self) -> &Arc<ErrorManager> {
        &self.error_manager
    }

    #[allow(clippy::type_complexity)]
    pub fn collect_senders_and_record_writers(
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
                // This interior mutability is to word around `Rc`. Other parts of this function is correctly marked `mut`.
                if let Some(record_writer) = edge.record_writer.borrow_mut().take() {
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
