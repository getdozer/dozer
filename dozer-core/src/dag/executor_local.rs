#![allow(clippy::type_complexity)]
use crate::dag::channels::SourceChannelForwarder;
use crate::dag::dag::{Dag, NodeType, PortDirection};
use crate::dag::errors::ExecutionError;
use crate::dag::errors::ExecutionError::{
    InvalidOperation, MissingNodeInput, MissingNodeOutput, SchemaNotInitialized,
};
use crate::dag::executor_utils::init_component;
use crate::dag::forwarder::LocalChannelForwarder;
use crate::dag::node::{NodeHandle, PortHandle, ProcessorFactory, SinkFactory, SourceFactory};
use crossbeam::channel::{bounded, Receiver, Select, Sender};
use dozer_types::parking_lot::RwLock;
use dozer_types::types::{Operation, Record, Schema};
use libc::size_t;
use log::{error, warn};
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::path::{Path, PathBuf};
use std::string::ToString;
use std::thread;
use std::thread::JoinHandle;

const DEFAULT_COMMIT_SZ: u16 = 10_000;

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
}

impl MultiThreadedDagExecutor {
    pub fn new(channel_buf_sz: usize) -> Self {
        Self { channel_buf_sz }
    }

    fn map_to_op(op: ExecutorOperation) -> Result<(u64, Operation), ExecutionError> {
        match op {
            ExecutorOperation::Delete { seq, old } => Ok((seq, Operation::Delete { old })),
            ExecutorOperation::Insert { seq, new } => Ok((seq, Operation::Insert { new })),
            ExecutorOperation::Update { seq, old, new } => {
                Ok((seq, Operation::Update { old, new }))
            }
            _ => Err(InvalidOperation(op.to_string())),
        }
    }

    fn index_edges(
        &self,
        dag: &Dag,
    ) -> (
        HashMap<NodeHandle, HashMap<PortHandle, Vec<Sender<ExecutorOperation>>>>,
        HashMap<NodeHandle, HashMap<PortHandle, Vec<Receiver<ExecutorOperation>>>>,
    ) {
        let mut senders: HashMap<NodeHandle, HashMap<PortHandle, Vec<Sender<ExecutorOperation>>>> =
            HashMap::new();
        let mut receivers: HashMap<
            NodeHandle,
            HashMap<PortHandle, Vec<Receiver<ExecutorOperation>>>,
        > = HashMap::new();

        for edge in dag.edges.iter() {
            if !senders.contains_key(&edge.from.node) {
                senders.insert(edge.from.node.clone(), HashMap::new());
            }
            if !receivers.contains_key(&edge.to.node) {
                receivers.insert(edge.to.node.clone(), HashMap::new());
            }

            let (tx, rx) = bounded(self.channel_buf_sz);

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

    fn get_node_types(
        &self,
        dag: Dag,
    ) -> (
        Vec<(NodeHandle, Box<dyn SourceFactory>)>,
        Vec<(NodeHandle, Box<dyn ProcessorFactory>)>,
        Vec<(NodeHandle, Box<dyn SinkFactory>)>,
    ) {
        let mut sources = Vec::new();
        let mut processors = Vec::new();
        let mut sinks = Vec::new();

        for node in dag.nodes.into_iter() {
            match node.1 {
                NodeType::Source(s) => sources.push((node.0, s)),
                NodeType::Processor(p) => {
                    processors.push((node.0, p));
                }
                NodeType::Sink(s) => {
                    sinks.push((node.0, s));
                }
            }
        }
        (sources, processors, sinks)
    }

    fn start_source(
        &self,
        handle: NodeHandle,
        src_factory: Box<dyn SourceFactory>,
        senders: HashMap<PortHandle, Vec<Sender<ExecutorOperation>>>,
        base_path: PathBuf,
    ) -> JoinHandle<Result<(), ExecutionError>> {
        let mut fw = LocalChannelForwarder::new(handle.clone(), senders, DEFAULT_COMMIT_SZ);

        thread::spawn(move || -> Result<(), ExecutionError> {
            let src = src_factory.build();
            for p in src_factory.get_output_ports() {
                if let Some(schema) = src.get_output_schema(p) {
                    fw.update_schema(schema, p)?
                }
            }

            src.start(&mut fw, None)
        })
    }

    fn build_receivers_lists(
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

    fn start_sink(
        &self,
        handle: NodeHandle,
        snk_factory: Box<dyn SinkFactory>,
        receivers: HashMap<PortHandle, Vec<Receiver<ExecutorOperation>>>,
        base_path: PathBuf,
    ) -> JoinHandle<Result<(), ExecutionError>> {
        thread::spawn(move || -> Result<(), ExecutionError> {
            let mut snk = snk_factory.build();

            let (handles_ls, receivers_ls) =
                MultiThreadedDagExecutor::build_receivers_lists(receivers);

            let mut state_meta =
                init_component(handle, base_path, snk_factory.is_stateful(), false, |e| {
                    snk.init(e)
                })?;

            let mut input_schemas = HashMap::<PortHandle, Schema>::new();
            let mut schema_initialized = false;

            let mut sel = Select::new();
            for r in &receivers_ls {
                sel.recv(r);
            }
            loop {
                let index = sel.ready();
                let op = receivers_ls[index]
                    .recv()
                    .map_err(|e| ExecutionError::SinkReceiverError(index, Box::new(e)))?;
                match op {
                    ExecutorOperation::SchemaUpdate { new } => {
                        input_schemas.insert(handles_ls[index], new);
                        let input_ports = snk_factory.get_input_ports();
                        let count = input_ports
                            .iter()
                            .filter(|e| !input_schemas.contains_key(*e))
                            .count();
                        if count == 0 {
                            let r = snk.update_schema(&input_schemas);
                            if let Err(e) = r {
                                warn!("Schema Update Failed...");
                                return Err(e);
                            } else {
                                schema_initialized = true;
                            }
                        }
                    }

                    ExecutorOperation::Terminate => {
                        return Ok(());
                    }

                    ExecutorOperation::Commit { epoch, source } => {
                        if let Some(s) = state_meta.as_mut() {
                            s.tx.put(&s.meta_db, source.as_bytes(), &epoch.to_be_bytes())?;
                            s.tx.commit_and_renew()?;
                        }
                    }

                    _ => {
                        if !schema_initialized {
                            return Err(SchemaNotInitialized);
                        }

                        let data_op = MultiThreadedDagExecutor::map_to_op(op)?;
                        snk.process(
                            handles_ls[index],
                            data_op.0,
                            data_op.1,
                            state_meta.as_mut().map(|e| e.tx.as_rw_transaction()),
                        )?;
                    }
                }
            }
        })
    }

    fn start_processor(
        &self,
        handle: NodeHandle,
        proc_factory: Box<dyn ProcessorFactory>,
        senders: HashMap<PortHandle, Vec<Sender<ExecutorOperation>>>,
        receivers: HashMap<PortHandle, Vec<Receiver<ExecutorOperation>>>,
        base_path: PathBuf,
    ) -> JoinHandle<Result<(), ExecutionError>> {
        thread::spawn(move || -> Result<(), ExecutionError> {
            let mut proc = proc_factory.build();

            let (handles_ls, receivers_ls) =
                MultiThreadedDagExecutor::build_receivers_lists(receivers);

            let mut fw = LocalChannelForwarder::new(handle.clone(), senders, 0);
            let mut sel = Select::new();
            for r in &receivers_ls {
                sel.recv(r);
            }

            let mut input_schemas = HashMap::<PortHandle, Schema>::new();
            let mut output_schemas = HashMap::<PortHandle, Schema>::new();
            let mut schema_initialized = false;

            let mut state_meta =
                init_component(handle, base_path, proc_factory.is_stateful(), false, |e| {
                    proc.init(e)
                })?;

            loop {
                let index = sel.ready();
                let op = receivers_ls[index]
                    .recv()
                    .map_err(|e| ExecutionError::ProcessorReceiverError(index, Box::new(e)))?;
                match op {
                    ExecutorOperation::SchemaUpdate { new } => {
                        input_schemas.insert(handles_ls[index], new);
                        let input_ports = proc_factory.get_input_ports();
                        let count = input_ports
                            .iter()
                            .filter(|e| !input_schemas.contains_key(*e))
                            .count();
                        if count == 0 {
                            for out_port in proc_factory.get_output_ports() {
                                let r = proc.update_schema(out_port, &input_schemas);
                                match r {
                                    Ok(out_schema) => {
                                        output_schemas.insert(out_port, out_schema.clone());
                                        fw.update_schema(out_schema, out_port)?;
                                        schema_initialized = true;
                                    }
                                    Err(e) => {
                                        warn!("New schema is not compatible with older version. Handling it. {:?}", e);
                                        todo!("Schema is not compatible with order version. Handle it!")
                                    }
                                }
                            }
                        }
                    }

                    ExecutorOperation::Terminate => {
                        fw.send_term()?;
                        return Ok(());
                    }

                    ExecutorOperation::Commit { epoch, source } => {
                        if let Some(s) = state_meta.as_mut() {
                            s.tx.put(&s.meta_db, source.as_bytes(), &epoch.to_be_bytes())?;
                            s.tx.commit_and_renew()?;
                        }
                    }

                    _ => {
                        if !schema_initialized {
                            error!("Received a CDC before schema initialization. Exiting from SNK message loop.");
                            return Err(SchemaNotInitialized);
                        }

                        let data_op = MultiThreadedDagExecutor::map_to_op(op)?;
                        fw.update_seq_no(data_op.0);
                        proc.process(
                            handles_ls[index],
                            data_op.1,
                            &mut fw,
                            state_meta.as_mut().map(|e| e.tx.as_rw_transaction()),
                        )?;
                    }
                }
            }
        })
    }

    pub fn start(&self, dag: Dag, path: PathBuf) -> Result<(), ExecutionError> {
        let (mut senders, mut receivers) = self.index_edges(&dag);
        let (sources, processors, sinks) = self.get_node_types(dag);
        let mut handles: Vec<JoinHandle<Result<(), ExecutionError>>> = Vec::new();

        for snk in sinks {
            let snk_receivers = receivers.remove(&snk.0.clone());
            let snk_handle = self.start_sink(
                snk.0.clone(),
                snk.1,
                snk_receivers.map_or(Err(MissingNodeInput(snk.0.clone())), Ok)?,
                path.clone(),
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

            let proc_handle = self.start_processor(
                processor.0.clone(),
                processor.1,
                proc_senders.unwrap(),
                proc_receivers.unwrap(),
                path.clone(),
            );
            handles.push(proc_handle);
        }

        for source in sources {
            handles.push(self.start_source(
                source.0.clone(),
                source.1,
                senders.remove(&source.0.clone()).unwrap(),
                path.clone(),
            ));
        }

        for sh in handles {
            sh.join().unwrap()?;
        }

        Ok(())
    }
}
