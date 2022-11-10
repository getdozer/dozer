#![allow(clippy::type_complexity)]
use crate::dag::channels::SourceChannelForwarder;
use crate::dag::dag::{Dag, Edge, Endpoint, NodeType, PortDirection};
use crate::dag::errors::ExecutionError;
use crate::dag::errors::ExecutionError::{
    InvalidOperation, MissingNodeInput, MissingNodeOutput, SchemaNotInitialized,
};
use crate::dag::executor_processor::start_processor;
use crate::dag::executor_sink::start_sink;
use crate::dag::executor_source::start_source;
use crate::dag::executor_utils::{
    build_receivers_lists, create_ports_databases, fill_ports_record_readers,
    get_inputs_for_output, get_node_types, index_edges, init_component, init_select, map_to_op,
    requires_schema_update,
};
use crate::dag::forwarder::{LocalChannelForwarder, PortRecordStoreWriter};
use crate::dag::node::{
    NodeHandle, PortHandle, Processor, ProcessorFactory, SinkFactory, SourceFactory,
};
use crate::dag::record_store::RecordReader;
use crate::storage::common::{Database, RenewableRwTransaction};
use crate::storage::errors::StorageError;
use crate::storage::transactions::{ExclusiveTransaction, SharedTransaction};
use crossbeam::channel::{bounded, Receiver, Select, Sender};
use dozer_types::parking_lot::RwLock;
use dozer_types::types::{Operation, Record, Schema};
use fp_rust::sync::CountDownLatch;
use libc::size_t;
use log::{error, info, warn};
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::path::{Path, PathBuf};
use std::string::ToString;
use std::sync::Arc;
use std::thread;
use std::thread::JoinHandle;
use std::time::Duration;

#[derive(Clone, Debug, PartialEq)]
pub enum ExecutorOperation {
    Delete { seq: u64, old: Record },
    Insert { seq: u64, new: Record },
    Update { seq: u64, old: Record, new: Record },
    SchemaUpdate { new: Schema },
    Commit { source: NodeHandle, epoch: u64 },
    Terminate,
}

impl Display for ExecutorOperation {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let type_str = match self {
            ExecutorOperation::Delete { .. } => "Delete",
            ExecutorOperation::Update { .. } => "Update",
            ExecutorOperation::Insert { .. } => "Insert",
            ExecutorOperation::SchemaUpdate { .. } => "SchemaUpdate",
            ExecutorOperation::Terminate { .. } => "Terminate",
            ExecutorOperation::Commit { .. } => "Commit",
        };
        f.write_str(type_str)
    }
}

pub const DEFAULT_PORT_HANDLE: u16 = 0xffff_u16;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct SchemaKey {
    pub node_handle: NodeHandle,
    pub port_handle: PortHandle,
    pub direction: PortDirection,
}

impl SchemaKey {
    pub fn new(node_handle: NodeHandle, port_handle: PortHandle, direction: PortDirection) -> Self {
        Self {
            node_handle,
            port_handle,
            direction,
        }
    }
}

pub struct MultiThreadedDagExecutor {
    channel_buf_sz: usize,
    commit_size: u16,
}

impl MultiThreadedDagExecutor {
    pub fn new(channel_buf_sz: usize, commit_size: u16) -> Self {
        Self {
            channel_buf_sz,
            commit_size,
        }
    }

    pub fn start(&self, dag: Dag, path: PathBuf) -> Result<(), ExecutionError> {
        let (mut senders, mut receivers) = index_edges(&dag, self.channel_buf_sz);
        let mut handles: Vec<JoinHandle<Result<(), ExecutionError>>> = Vec::new();

        let mut record_stores = Arc::new(RwLock::new(
            dag.nodes
                .iter()
                .map(|e| (e.0.clone(), HashMap::<PortHandle, RecordReader>::new()))
                .collect(),
        ));

        let (sources, processors, sinks, edges) = get_node_types(dag);

        let latch = Arc::new(CountDownLatch::new((processors.len() + sinks.len()) as u64));

        for snk in sinks {
            let snk_receivers = receivers.remove(&snk.0.clone());
            let snk_handle = start_sink(
                snk.0.clone(),
                snk.1,
                snk_receivers.map_or(Err(MissingNodeInput(snk.0.clone())), Ok)?,
                path.clone(),
                latch.clone(),
            );
            handles.push(snk_handle);
        }

        for processor in processors {
            let proc_receivers = receivers.remove(&processor.0.clone());
            if proc_receivers.is_none() {
                return Err(MissingNodeInput(processor.0));
            }

            let proc_senders = senders.remove(&processor.0.clone());
            if proc_senders.is_none() {
                return Err(MissingNodeOutput(processor.0));
            }

            let proc_handle = start_processor(
                edges.clone(),
                processor.0.clone(),
                processor.1,
                proc_senders.unwrap(),
                proc_receivers.unwrap(),
                path.clone(),
                record_stores.clone(),
                latch.clone(),
            );
            handles.push(proc_handle);
        }

        latch.wait();

        for source in sources {
            handles.push(start_source(
                source.0.clone(),
                source.1,
                senders.remove(&source.0.clone()).unwrap(),
                self.commit_size,
                path.clone(),
            ));
        }

        for sh in handles {
            let r = sh.join().unwrap()?;
        }

        Ok(())
    }
}
