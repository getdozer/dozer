#![allow(clippy::type_complexity)]
use crate::dag::dag::{Dag, Edge, Endpoint};
use crate::dag::dag_metadata::METADATA_DB_NAME;
use crate::dag::errors::ExecutionError;
use crate::dag::executor::ExecutorOperation;
use crate::dag::node::{NodeHandle, OutputPortDef, OutputPortType, PortHandle};
use crate::dag::record_store::{
    AutogenRowKeyLookupRecordReader, PrimaryKeyValueLookupRecordReader, RecordReader,
};
use crate::storage::common::Database;
use crate::storage::lmdb_storage::{LmdbEnvironmentManager, SharedTransaction};
use crossbeam::channel::{bounded, Receiver, Select, Sender};
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::path::Path;

use super::hash_map_to_vec::insert_vec_element;

pub(crate) struct StorageMetadata {
    pub env: LmdbEnvironmentManager,
    pub meta_db: Database,
}

impl StorageMetadata {
    pub fn new(env: LmdbEnvironmentManager, meta_db: Database) -> Self {
        Self { env, meta_db }
    }
}

pub(crate) fn init_component<F>(
    node_handle: &NodeHandle,
    base_path: &Path,
    mut init_f: F,
) -> Result<StorageMetadata, ExecutionError>
where
    F: FnMut(&mut LmdbEnvironmentManager) -> Result<(), ExecutionError>,
{
    let mut env = LmdbEnvironmentManager::create(base_path, format!("{node_handle}").as_str())?;
    let db = env.open_database(METADATA_DB_NAME, false)?;
    init_f(&mut env)?;
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

pub(crate) fn index_edges<T: Clone>(
    dag: &Dag<T>,
    channel_buf_sz: usize,
) -> (
    HashMap<NodeHandle, HashMap<PortHandle, Vec<Sender<ExecutorOperation>>>>,
    HashMap<NodeHandle, HashMap<PortHandle, Vec<Receiver<ExecutorOperation>>>>,
) {
    let mut senders = HashMap::new();
    let mut receivers = HashMap::new();

    for edge in dag.edges() {
        let (tx, rx) = bounded(channel_buf_sz);
        // let (tx, rx) = match dag.nodes.get(&edge.from.node).unwrap() {
        //     NodeType::Source(_) => bounded(1),
        //     _ => bounded(channel_buf_sz),
        // };

        insert_sender_or_receiver(&mut senders, edge.from.clone(), tx);
        insert_sender_or_receiver(&mut receivers, edge.to.clone(), rx);
    }

    (senders, receivers)
}

fn insert_sender_or_receiver<T>(
    map: &mut HashMap<NodeHandle, HashMap<PortHandle, Vec<T>>>,
    endpoint: Endpoint,
    value: T,
) {
    match map.entry(endpoint.node) {
        Entry::Occupied(mut entry) => {
            insert_vec_element(entry.get_mut(), endpoint.port, value);
        }
        Entry::Vacant(entry) => {
            let mut port_map = HashMap::new();
            port_map.insert(endpoint.port, vec![value]);
            entry.insert(port_map);
        }
    }
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

fn get_inputs_for_output(edges: &[Edge], node: &NodeHandle, port: &PortHandle) -> Vec<Endpoint> {
    edges
        .iter()
        .filter(|e| e.from.node == *node && e.from.port == *port)
        .map(|e| e.to.clone())
        .collect()
}

const PORT_STATE_KEY: &str = "__PORT_STATE_";

#[derive(Debug)]
pub(crate) struct StateOptions {
    pub(crate) db: Database,
    pub(crate) meta_db: Database,
    pub(crate) typ: OutputPortType,
}

pub(crate) fn create_ports_databases_and_fill_downstream_record_readers(
    handle: &NodeHandle,
    edges: &[Edge],
    mut env: LmdbEnvironmentManager,
    output_ports: &[OutputPortDef],
    record_stores: &mut HashMap<NodeHandle, HashMap<PortHandle, Box<dyn RecordReader>>>,
) -> Result<(SharedTransaction, HashMap<PortHandle, StateOptions>), ExecutionError> {
    let mut port_databases: Vec<Option<StateOptions>> = Vec::new();
    for port in output_ports {
        let opt = match &port.typ {
            OutputPortType::Stateless => None,
            typ => {
                let db =
                    env.open_database(&format!("{}_{}", PORT_STATE_KEY, port.handle), false)?;
                let meta_db =
                    env.open_database(&format!("{}_{}_META", PORT_STATE_KEY, port.handle), false)?;
                Some(StateOptions {
                    db,
                    meta_db,
                    typ: typ.clone(),
                })
            }
        };
        port_databases.push(opt);
    }

    let master_tx = env.create_txn()?;

    for (state_options, port) in port_databases.iter().zip(output_ports.iter()) {
        if let Some(state_options) = state_options {
            for endpoint in get_inputs_for_output(edges, handle, &port.handle) {
                let record_reader: Box<dyn RecordReader> = match port.typ {
                    OutputPortType::AutogenRowKeyLookup => Box::new(
                        AutogenRowKeyLookupRecordReader::new(master_tx.clone(), state_options.db),
                    ),
                    OutputPortType::StatefulWithPrimaryKeyLookup { .. } => Box::new(
                        PrimaryKeyValueLookupRecordReader::new(master_tx.clone(), state_options.db),
                    ),
                    OutputPortType::Stateless => panic!("Internal error: Invalid port type"),
                };

                record_stores
                    .get_mut(&endpoint.node)
                    .expect("Record store HashMap must be created for every node upfront")
                    .insert(endpoint.port, record_reader);
            }
        }
    }

    let port_databases = output_ports
        .iter()
        .zip(port_databases.into_iter())
        .flat_map(|(output_port, state_option)| {
            state_option.map(|state_option| (output_port.handle, state_option))
        })
        .collect();

    Ok((master_tx, port_databases))
}
