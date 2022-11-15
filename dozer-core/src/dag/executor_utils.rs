#![allow(clippy::type_complexity)]
use crate::dag::dag::{Dag, Edge, Endpoint, NodeType};
use crate::dag::errors::ExecutionError;
use crate::dag::errors::ExecutionError::InvalidOperation;
use crate::dag::executor_local::ExecutorOperation;
use crate::dag::node::{
    NodeHandle, PortHandle, StatefulPortHandle, StatefulProcessorFactory, StatefulSinkFactory,
    StatefulSourceFactory, StatelessProcessorFactory, StatelessSinkFactory, StatelessSourceFactory,
};
use crate::dag::record_store::RecordReader;
use crate::storage::common::{Database, Environment, EnvironmentManager, RenewableRwTransaction};
use crate::storage::errors::StorageError;
use crate::storage::lmdb_storage::LmdbEnvironmentManager;
use crossbeam::channel::{bounded, Receiver, Select, Sender};
use dozer_types::parking_lot::RwLock;
use dozer_types::types::{Operation, Schema};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

const CHECKPOINT_DB_NAME: &str = "__CHECKPOINT_META";

pub(crate) struct StorageMetadata {
    pub env: Box<dyn EnvironmentManager>,
    pub meta_db: Database,
}

impl StorageMetadata {
    pub fn new(env: Box<dyn EnvironmentManager>, meta_db: Database) -> Self {
        Self { env, meta_db }
    }
}

pub(crate) fn init_component<F>(
    node_handle: &NodeHandle,
    base_path: &Path,
    mut init_f: F,
) -> Result<StorageMetadata, ExecutionError>
where
    F: FnMut(&mut dyn Environment) -> Result<(), ExecutionError>,
{
    let mut env = LmdbEnvironmentManager::create(base_path, node_handle.as_str())?;
    let db = env.open_database(CHECKPOINT_DB_NAME, false)?;
    init_f(env.as_environment())?;
    Ok(StorageMetadata::new(env, db))
}
#[inline]
pub(crate) fn init_select(receivers: &Vec<Receiver<ExecutorOperation>>) -> Select {
    let mut sel = Select::new();
    for r in receivers {
        sel.recv(r);
    }
    sel
}

pub(crate) fn requires_schema_update(
    new: Schema,
    port_handle: &PortHandle,
    input_schemas: &mut HashMap<PortHandle, Schema>,
    input_ports: &[PortHandle],
) -> bool {
    input_schemas.insert(*port_handle, new);
    let count = input_ports
        .iter()
        .filter(|e| !input_schemas.contains_key(*e))
        .count();
    count == 0
}

pub(crate) fn map_to_op(op: ExecutorOperation) -> Result<(u64, Operation), ExecutionError> {
    match op {
        ExecutorOperation::Delete { seq, old } => Ok((seq, Operation::Delete { old })),
        ExecutorOperation::Insert { seq, new } => Ok((seq, Operation::Insert { new })),
        ExecutorOperation::Update { seq, old, new } => Ok((seq, Operation::Update { old, new })),
        _ => Err(InvalidOperation(op.to_string())),
    }
}

pub(crate) fn map_to_exec_op(seq: u64, op: Operation) -> ExecutorOperation {
    match op {
        Operation::Update { old, new } => ExecutorOperation::Update { old, new, seq },
        Operation::Delete { old } => ExecutorOperation::Delete { old, seq },
        Operation::Insert { new } => ExecutorOperation::Insert { new, seq },
    }
}

pub(crate) fn index_edges(
    dag: &Dag,
    channel_buf_sz: usize,
) -> (
    HashMap<NodeHandle, HashMap<PortHandle, Vec<Sender<ExecutorOperation>>>>,
    HashMap<NodeHandle, HashMap<PortHandle, Vec<Receiver<ExecutorOperation>>>>,
) {
    let mut senders: HashMap<NodeHandle, HashMap<PortHandle, Vec<Sender<ExecutorOperation>>>> =
        HashMap::new();
    let mut receivers: HashMap<NodeHandle, HashMap<PortHandle, Vec<Receiver<ExecutorOperation>>>> =
        HashMap::new();

    for edge in dag.edges.iter() {
        if !senders.contains_key(&edge.from.node) {
            senders.insert(edge.from.node.clone(), HashMap::new());
        }
        if !receivers.contains_key(&edge.to.node) {
            receivers.insert(edge.to.node.clone(), HashMap::new());
        }

        let (tx, rx) = bounded(channel_buf_sz);

        let rcv_port: PortHandle = edge.to.port;
        if receivers
            .get(&edge.to.node)
            .unwrap()
            .contains_key(&rcv_port)
        {
            receivers
                .get_mut(&edge.to.node)
                .unwrap()
                .get_mut(&rcv_port)
                .unwrap()
                .push(rx);
        } else {
            receivers
                .get_mut(&edge.to.node)
                .unwrap()
                .insert(rcv_port, vec![rx]);
        }

        let snd_port: PortHandle = edge.from.port;
        if senders
            .get(&edge.from.node)
            .unwrap()
            .contains_key(&snd_port)
        {
            senders
                .get_mut(&edge.from.node)
                .unwrap()
                .get_mut(&snd_port)
                .unwrap()
                .push(tx);
        } else {
            senders
                .get_mut(&edge.from.node)
                .unwrap()
                .insert(snd_port, vec![tx]);
        }
    }

    (senders, receivers)
}

pub(crate) enum SourceHolder {
    Stateful(Box<dyn StatefulSourceFactory>),
    Stateless(Box<dyn StatelessSourceFactory>),
}

pub(crate) enum ProcessorHolder {
    Stateful(Box<dyn StatefulProcessorFactory>),
    Stateless(Box<dyn StatelessProcessorFactory>),
}

pub(crate) enum SinkHolder {
    Stateful(Box<dyn StatefulSinkFactory>),
    Stateless(Box<dyn StatelessSinkFactory>),
}

pub(crate) fn get_node_types_and_edges(
    dag: Dag,
) -> (
    Vec<(NodeHandle, SourceHolder)>,
    Vec<(NodeHandle, ProcessorHolder)>,
    Vec<(NodeHandle, SinkHolder)>,
    Vec<Edge>,
) {
    let mut sources = Vec::new();
    let mut processors = Vec::new();
    let mut sinks = Vec::new();

    for node in dag.nodes.into_iter() {
        match node.1 {
            NodeType::StatefulSource(s) => sources.push((node.0, SourceHolder::Stateful(s))),
            NodeType::StatelessSource(s) => sources.push((node.0, SourceHolder::Stateless(s))),

            NodeType::StatefulProcessor(s) => {
                processors.push((node.0, ProcessorHolder::Stateful(s)))
            }
            NodeType::StatelessProcessor(s) => {
                processors.push((node.0, ProcessorHolder::Stateless(s)))
            }

            NodeType::StatefulSink(s) => sinks.push((node.0, SinkHolder::Stateful(s))),
            NodeType::StatelessSink(s) => sinks.push((node.0, SinkHolder::Stateless(s))),
        }
    }
    (sources, processors, sinks, dag.edges)
}

pub(crate) fn build_receivers_lists(
    receivers: HashMap<PortHandle, Vec<Receiver<ExecutorOperation>>>,
) -> (Vec<PortHandle>, Vec<Receiver<ExecutorOperation>>) {
    let mut handles_ls: Vec<PortHandle> = Vec::new();
    let mut receivers_ls: Vec<Receiver<ExecutorOperation>> = Vec::new();
    for e in receivers {
        for r in e.1 {
            receivers_ls.push(r);
            handles_ls.push(e.0);
        }
    }
    (handles_ls, receivers_ls)
}

pub(crate) fn get_inputs_for_output(
    edges: &[Edge],
    node: &NodeHandle,
    port: &PortHandle,
) -> Vec<Endpoint> {
    edges
        .iter()
        .filter(|e| e.from.node == *node && e.from.port == *port)
        .map(|e| e.to.clone())
        .collect()
}

const PORT_STATE_KEY: &str = "__PORT_STATE_";

pub(crate) fn create_ports_databases(
    env: &mut dyn Environment,
    ports: &Vec<StatefulPortHandle>,
) -> Result<HashMap<PortHandle, Database>, StorageError> {
    let mut port_databases = HashMap::<PortHandle, Database>::new();
    for out_port in ports {
        if out_port.stateful {
            let db = env.open_database(
                format!("{}_{}", PORT_STATE_KEY, out_port.handle).as_str(),
                false,
            )?;
            port_databases.insert(out_port.handle, db);
        }
    }
    Ok(port_databases)
}

pub(crate) fn fill_ports_record_readers(
    handle: &NodeHandle,
    edges: &[Edge],
    port_databases: &HashMap<PortHandle, Database>,
    master_tx: &Arc<RwLock<Box<dyn RenewableRwTransaction>>>,
    record_stores: &Arc<RwLock<HashMap<NodeHandle, HashMap<PortHandle, RecordReader>>>>,
    output_ports: &Vec<StatefulPortHandle>,
) {
    for out_port in output_ports {
        if out_port.stateful {
            for r in get_inputs_for_output(edges, handle, &out_port.handle) {
                let mut writer = record_stores.write();
                writer.get_mut(&r.node).unwrap().insert(
                    r.port,
                    RecordReader::new(
                        master_tx.clone(),
                        port_databases.get(&out_port.handle).unwrap().clone(),
                    ),
                );
            }
        }
    }
}
