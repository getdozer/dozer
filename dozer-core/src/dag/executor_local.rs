#![allow(clippy::type_complexity)]
#![allow(clippy::ptr_arg)]
use crate::dag::dag::{Dag, Edge, PortDirection};
use crate::dag::errors::ExecutionError;
use crate::dag::errors::ExecutionError::{MissingNodeInput, MissingNodeOutput};
use crate::dag::executor_processor::{start_stateful_processor, start_stateless_processor};
use crate::dag::executor_sink::{start_stateful_sink, start_stateless_sink};
use crate::dag::executor_source::start_stateless_source;
use crate::dag::executor_utils::{
    get_node_types_and_edges, index_edges, ProcessorHolder, SinkHolder, SourceHolder,
};
use crate::dag::node::{NodeHandle, PortHandle};
use crate::dag::record_store::RecordReader;
use crossbeam::channel::{Receiver, Sender};
use dozer_types::parking_lot::RwLock;
use dozer_types::types::{Record, Schema};
use fp_rust::sync::CountDownLatch;
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::path::PathBuf;
use std::sync::Arc;
use std::thread::JoinHandle;

#[derive(Clone, Debug, PartialEq, Eq)]
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
    commit_size: u32,
}

impl MultiThreadedDagExecutor {
    pub fn new(channel_buf_sz: usize, commit_size: u32) -> Self {
        Self {
            channel_buf_sz,
            commit_size,
        }
    }

    fn start_sinks(
        sinks: Vec<(NodeHandle, SinkHolder)>,
        receivers: &mut HashMap<NodeHandle, HashMap<PortHandle, Vec<Receiver<ExecutorOperation>>>>,
        path: PathBuf,
        latch: &Arc<CountDownLatch>,
    ) -> Result<Vec<JoinHandle<Result<(), ExecutionError>>>, ExecutionError> {
        let mut handles: Vec<JoinHandle<Result<(), ExecutionError>>> = Vec::new();

        for holder in sinks {
            let snk_receivers = receivers.remove(&holder.0.clone());
            match holder.1 {
                SinkHolder::Stateful(s) => {
                    handles.push(start_stateful_sink(
                        holder.0.clone(),
                        s,
                        snk_receivers.map_or(Err(MissingNodeInput(holder.0)), Ok)?,
                        path.clone(),
                        latch.clone(),
                    ));
                }
                SinkHolder::Stateless(s) => {
                    handles.push(start_stateless_sink(
                        holder.0.clone(),
                        s,
                        snk_receivers.map_or(Err(MissingNodeInput(holder.0)), Ok)?,
                        latch.clone(),
                    ));
                }
            }
        }

        Ok(handles)
    }

    fn start_sources(
        sources: Vec<(NodeHandle, SourceHolder)>,
        senders: &mut HashMap<NodeHandle, HashMap<PortHandle, Vec<Sender<ExecutorOperation>>>>,
        path: PathBuf,
        commit_size: u32,
        channel_buffer: usize,
    ) -> Result<Vec<JoinHandle<Result<(), ExecutionError>>>, ExecutionError> {
        let mut handles: Vec<JoinHandle<Result<(), ExecutionError>>> = Vec::new();

        for holder in sources {
            match holder.1 {
                SourceHolder::Stateful(_s) => {
                    todo!()
                }
                SourceHolder::Stateless(s) => {
                    handles.push(start_stateless_source(
                        holder.0.clone(),
                        s,
                        senders.remove(&holder.0).unwrap(),
                        commit_size,
                        channel_buffer,
                        path.clone(),
                    ));
                }
            }
        }
        Ok(handles)
    }

    fn start_processors(
        processors: Vec<(NodeHandle, ProcessorHolder)>,
        senders: &mut HashMap<NodeHandle, HashMap<PortHandle, Vec<Sender<ExecutorOperation>>>>,
        receivers: &mut HashMap<NodeHandle, HashMap<PortHandle, Vec<Receiver<ExecutorOperation>>>>,
        path: PathBuf,
        edges: &Vec<Edge>,
        latch: &Arc<CountDownLatch>,
        record_stores: &Arc<RwLock<HashMap<NodeHandle, HashMap<PortHandle, RecordReader>>>>,
    ) -> Result<Vec<JoinHandle<Result<(), ExecutionError>>>, ExecutionError> {
        let mut handles: Vec<JoinHandle<Result<(), ExecutionError>>> = Vec::new();

        for holder in processors {
            let proc_receivers = receivers.remove(&holder.0.clone());
            if proc_receivers.is_none() {
                return Err(MissingNodeInput(holder.0));
            }
            let proc_senders = senders.remove(&holder.0.clone());
            if proc_senders.is_none() {
                return Err(MissingNodeOutput(holder.0));
            }

            match holder.1 {
                ProcessorHolder::Stateful(s) => {
                    handles.push(start_stateful_processor(
                        edges.clone(),
                        holder.0,
                        s,
                        proc_senders.unwrap(),
                        proc_receivers.unwrap(),
                        path.clone(),
                        record_stores.clone(),
                        latch.clone(),
                    ));
                }
                ProcessorHolder::Stateless(s) => {
                    handles.push(start_stateless_processor(
                        holder.0,
                        s,
                        proc_senders.unwrap(),
                        proc_receivers.unwrap(),
                        record_stores.clone(),
                        latch.clone(),
                    ));
                }
            }
        }

        Ok(handles)
    }

    pub fn start(&self, dag: Dag, path: PathBuf) -> Result<(), ExecutionError> {
        let (mut senders, mut receivers) = index_edges(&dag, self.channel_buf_sz);

        let record_stores = Arc::new(RwLock::new(
            dag.nodes
                .iter()
                .map(|e| (e.0.clone(), HashMap::<PortHandle, RecordReader>::new()))
                .collect(),
        ));

        let (sources, processors, sinks, edges) = get_node_types_and_edges(dag);
        let latch = Arc::new(CountDownLatch::new((processors.len() + sinks.len()) as u64));

        let mut all_handles = Vec::<JoinHandle<Result<(), ExecutionError>>>::new();

        all_handles.extend(Self::start_sinks(
            sinks,
            &mut receivers,
            path.clone(),
            &latch,
        )?);
        all_handles.extend(Self::start_processors(
            processors,
            &mut senders,
            &mut receivers,
            path.clone(),
            &edges,
            &latch,
            &record_stores,
        )?);

        latch.wait();

        all_handles.extend(Self::start_sources(
            sources,
            &mut senders,
            path,
            self.commit_size,
            self.channel_buf_sz,
        )?);

        for sh in all_handles {
            sh.join().unwrap()?;
        }

        Ok(())
    }
}
