use std::{
    borrow::BorrowMut,
    cell::RefCell,
    collections::{hash_map::Entry, HashMap},
    fmt::Debug,
    path::Path,
    rc::Rc,
    sync::Arc,
};

use crossbeam::channel::{bounded, Receiver, Sender};
use daggy::petgraph::{
    visit::{EdgeRef, IntoEdges, IntoEdgesDirected, IntoNodeReferences},
    Direction,
};
use dozer_storage::{lmdb::Database, lmdb_storage::SharedTransaction};

use crate::{
    dag_schemas::{DagHaveSchemas, DagSchemas},
    epoch::EpochManager,
    errors::ExecutionError,
    executor_utils::init_component,
    hash_map_to_vec::insert_vec_element,
    node::{NodeHandle, PortHandle, Processor, Sink, Source},
    record_store::{create_record_store, RecordReader, RecordWriter},
    NodeKind as DagNodeKind,
};

use super::ExecutorOperation;

#[derive(Debug)]
/// Node in the execution DAG.
pub struct NodeType {
    /// The node storage environment.
    pub storage: NodeStorage,
    /// The node kind. Will be moved to execution node.
    pub kind: Option<NodeKind>,
}

#[derive(Debug, Clone)]
/// A node's storage environment.
pub struct NodeStorage {
    /// Name of the node.
    pub handle: NodeHandle,
    pub master_tx: SharedTransaction,
    pub meta_db: Database,
}

#[derive(Debug)]
/// Node kind, source, processor or sink.
pub enum NodeKind {
    Source(Box<dyn Source>),
    Processor(Box<dyn Processor>),
    Sink(Box<dyn Sink>),
}

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
    /// The record reader for reading persisted data of corresponding output port, if there's some.
    pub record_reader: Option<Box<dyn RecordReader>>,
}

#[derive(Debug)]
pub struct ExecutionDag {
    graph: daggy::Dag<NodeType, EdgeType>,
    epoch_manager: Arc<EpochManager>,
}

impl ExecutionDag {
    pub fn new<T: Clone>(
        dag_schemas: &DagSchemas<T>,
        base_path: &Path,
        channel_buf_sz: usize,
    ) -> Result<Self, ExecutionError> {
        let dag = &dag_schemas.graph;

        // Create new nodes.
        let mut nodes = vec![];
        let mut num_sources = 0;
        for (node_index, node) in dag.node_references() {
            let input_schemas = dag_schemas.get_node_input_schemas(node_index);
            let input_schemas = input_schemas
                .into_iter()
                .map(|(key, value)| (key, value.0))
                .collect();
            let output_schemas = dag_schemas.get_node_output_schemas(node_index);
            let output_schemas = output_schemas
                .into_iter()
                .map(|(key, value)| (key, value.0))
                .collect();

            let (storage_metadata, node_kind) = match &node.kind {
                DagNodeKind::Source(source) => {
                    num_sources += 1;
                    let source = source.build(output_schemas)?;
                    (
                        init_component(&node.handle, base_path, |_| Ok(()))?,
                        NodeKind::Source(source),
                    )
                }
                DagNodeKind::Processor(processor) => {
                    let mut processor = processor.build(input_schemas, output_schemas)?;
                    (
                        init_component(&node.handle, base_path, |state| processor.init(state))?,
                        NodeKind::Processor(processor),
                    )
                }
                DagNodeKind::Sink(sink) => {
                    let mut sink = sink.build(input_schemas)?;
                    (
                        init_component(&node.handle, base_path, |state| sink.init(state))?,
                        NodeKind::Sink(sink),
                    )
                }
            };

            let master_tx = storage_metadata.env.create_txn()?;

            let node_storage = NodeStorage {
                handle: node.handle.clone(),
                master_tx,
                meta_db: storage_metadata.meta_db,
            };

            nodes.push(Some(NodeType {
                storage: node_storage,
                kind: Some(node_kind),
            }));
        }

        // We only create record stored once for every output port. Every `HashMap` in this `Vec` tracks if a node's output ports already have the record store created.
        let mut all_record_stores =
            vec![
                HashMap::<PortHandle, (SharedRecordWriter, Option<Box<dyn RecordReader>>)>::new();
                dag.node_count()
            ];

        // Create new edges.
        let mut edges = vec![];
        for dag_schema_edge in dag.raw_edges().iter() {
            let source_node_index = dag_schema_edge.source().index();
            let edge = &dag_schema_edge.weight;
            let output_port = dag_schema_edge.weight.output_port;

            // Create or get record store.
            let (record_writer, record_reader) =
                match all_record_stores[source_node_index].entry(output_port) {
                    Entry::Vacant(entry) => {
                        let record_store = create_record_store(
                            &nodes[source_node_index]
                                .as_ref()
                                .expect("We created all nodes")
                                .storage
                                .master_tx,
                            output_port,
                            edge.output_port_type,
                            edge.schema.clone(),
                            channel_buf_sz + 1,
                        )?;
                        let (record_writer, record_reader) =
                            if let Some((record_writer, record_reader)) = record_store {
                                (
                                    Rc::new(RefCell::new(Some(record_writer))),
                                    Some(record_reader),
                                )
                            } else {
                                (Rc::new(RefCell::new(None)), None)
                            };
                        entry.insert((record_writer, record_reader)).clone()
                    }
                    Entry::Occupied(entry) => entry.get().clone(),
                };

            // Create channel.
            let (sender, receiver) = bounded(channel_buf_sz);

            // Create edge.
            let edge = EdgeType {
                output_port,
                sender,
                record_writer,
                input_port: edge.input_port,
                receiver,
                record_reader,
            };
            edges.push(Some(edge));
        }

        // Create new graph.
        let graph = dag.map(
            |node_index, _| {
                nodes[node_index.index()]
                    .take()
                    .expect("We created all nodes")
            },
            |edge_index, _| {
                edges[edge_index.index()]
                    .take()
                    .expect("We created all edges")
            },
        );
        Ok(ExecutionDag {
            graph,
            epoch_manager: Arc::new(EpochManager::new(num_sources)),
        })
    }

    pub fn graph_mut(&mut self) -> &mut daggy::Dag<NodeType, EdgeType> {
        &mut self.graph
    }

    pub fn epoch_manager(&self) -> &Arc<EpochManager> {
        &self.epoch_manager
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

    #[allow(clippy::type_complexity)]
    pub fn collect_receivers_and_record_readers(
        &mut self,
        node_index: daggy::NodeIndex,
    ) -> (
        Vec<PortHandle>,
        Vec<Receiver<ExecutorOperation>>,
        HashMap<PortHandle, Box<dyn RecordReader>>,
    ) {
        let edge_indexes = self
            .graph
            .edges_directed(node_index, Direction::Incoming)
            .map(|edge| edge.id())
            .collect::<Vec<_>>();

        let mut input_ports = Vec::new();
        let mut receivers = Vec::new();
        let mut record_readers = HashMap::new();
        for edge_index in edge_indexes {
            let edge = self
                .graph
                .edge_weight_mut(edge_index)
                .expect("We don't modify graph structure, only modify the edge weight");
            input_ports.push(edge.input_port);
            receivers.push(edge.receiver.clone());
            if let Some(record_reader) = edge.record_reader.take() {
                debug_assert!(
                    record_readers
                        .insert(edge.input_port, record_reader)
                        .is_none(),
                    "More than one output connect to a input port"
                );
            }
        }
        (input_ports, receivers, record_readers)
    }
}
