#![allow(clippy::type_complexity)]
use crate::dag::channels::SourceChannelForwarder;
use crate::dag::dag::{Dag, Edge, Endpoint, NodeType, PortDirection};
use crate::dag::errors::ExecutionError;
use crate::dag::errors::ExecutionError::{
    InvalidOperation, MissingNodeInput, MissingNodeOutput, SchemaNotInitialized,
};
use crate::dag::executor_utils::{
    get_inputs_for_output, get_node_types, index_edges, init_component, init_select, map_to_op,
    requires_schema_update,
};
use crate::dag::forwarder::LocalChannelForwarder;
use crate::dag::node::{NodeHandle, PortHandle, ProcessorFactory, SinkFactory, SourceFactory};
use crate::dag::record_store::RecordReader;
use crate::storage::common::{Database, RenewableRwTransaction, RoTransaction};
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
const PORT_STATE_KEY: &str = "__PORT_STATE_";

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

            let mut input_schemas = HashMap::<PortHandle, Schema>::new();
            let mut schema_initialized = false;

            let mut state_meta =
                init_component(&handle, base_path, snk_factory.is_stateful(), |e| {
                    snk.init(e)
                })?;
            let mut master_tx = match state_meta.as_mut() {
                Some(s) => Some(s.env.create_txn(false)?),
                _ => None,
            };

            let (handles_ls, receivers_ls) =
                MultiThreadedDagExecutor::build_receivers_lists(receivers);

            let mut sel = init_select(&receivers_ls);
            loop {
                let index = sel.ready();
                let op = receivers_ls[index]
                    .recv()
                    .map_err(|e| ExecutionError::SinkReceiverError(index, Box::new(e)))?;

                match op {
                    ExecutorOperation::SchemaUpdate { new } => {
                        if requires_schema_update(
                            new,
                            &handles_ls[index],
                            &mut input_schemas,
                            snk_factory.get_input_ports(),
                        ) {
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
                        if let Some(tx) = master_tx.as_mut() {
                            tx.put(
                                &state_meta.as_ref().unwrap().meta_db,
                                source.as_bytes(),
                                &epoch.to_be_bytes(),
                            )?;
                            tx.commit_and_renew()?;
                        }
                    }

                    _ => {
                        if !schema_initialized {
                            return Err(SchemaNotInitialized);
                        }

                        let data_op = map_to_op(op)?;
                        snk.process(
                            handles_ls[index],
                            data_op.0,
                            data_op.1,
                            master_tx.as_mut().map(|e| e.as_rw_transaction()),
                        )?;
                    }
                }
            }
        })
    }

    fn start_processor(
        &self,
        edges: Vec<Edge>,
        handle: NodeHandle,
        proc_factory: Box<dyn ProcessorFactory>,
        senders: HashMap<PortHandle, Vec<Sender<ExecutorOperation>>>,
        receivers: HashMap<PortHandle, Vec<Receiver<ExecutorOperation>>>,
        base_path: PathBuf,
        record_stores: &mut HashMap<NodeHandle, HashMap<PortHandle, RecordReader>>,
    ) -> JoinHandle<Result<(), ExecutionError>> {
        thread::spawn(move || -> Result<(), ExecutionError> {
            let mut proc = proc_factory.build();

            let mut input_schemas = HashMap::<PortHandle, Schema>::new();
            let mut output_schemas = HashMap::<PortHandle, Schema>::new();
            let mut schema_initialized = false;

            let mut state_meta =
                init_component(&handle, base_path, proc_factory.is_stateful(), |e| {
                    proc.init(e)
                })?;
            let mut master_tx: Option<Box<dyn RenewableRwTransaction>> = None;

            let mut out_port_databases = HashMap::<PortHandle, Database>::new();
            if let Some(m) = state_meta.as_mut() {
                for out_port in proc_factory.get_output_ports() {
                    let db = m.env.open_database(
                        format!("{}_{}", PORT_STATE_KEY, out_port).as_str(),
                        false,
                    )?;
                    let tx = m.env.create_txn(false)?;
                    out_port_databases.insert(out_port, db);
                    for r in get_inputs_for_output(&edges, &handle, &out_port) {
                        // record_stores.get_mut(&r.node).unwrap().insert(
                        //     r.port,
                        //     RecordReader::new(
                        //         tx.as_ro_transaction(),
                        //         out_port_databases.get(&out_port).unwrap(),
                        //     ),
                        // );
                    }
                    master_tx = Some(tx);
                }
            }

            let (handles_ls, receivers_ls) =
                MultiThreadedDagExecutor::build_receivers_lists(receivers);

            let mut fw = LocalChannelForwarder::new(handle.clone(), senders, 0);

            let mut sel = init_select(&receivers_ls);
            loop {
                let index = sel.ready();
                let op = receivers_ls[index]
                    .recv()
                    .map_err(|e| ExecutionError::ProcessorReceiverError(index, Box::new(e)))?;

                match op {
                    ExecutorOperation::SchemaUpdate { new } => {
                        if requires_schema_update(
                            new,
                            &handles_ls[index],
                            &mut input_schemas,
                            proc_factory.get_input_ports(),
                        ) {
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
                        if let Some(tx) = master_tx.as_mut() {
                            tx.put(
                                &state_meta.as_ref().unwrap().meta_db,
                                source.as_bytes(),
                                &epoch.to_be_bytes(),
                            )?;
                            tx.commit_and_renew()?;
                        }
                    }

                    _ => {
                        if !schema_initialized {
                            error!("Received a CDC before schema initialization. Exiting from SNK message loop.");
                            return Err(SchemaNotInitialized);
                        }

                        let data_op = map_to_op(op)?;
                        fw.update_seq_no(data_op.0);
                        proc.process(
                            handles_ls[index],
                            data_op.1,
                            &mut fw,
                            master_tx.as_mut().map(|e| e.as_rw_transaction()),
                        )?;
                    }
                }
            }
        })
    }

    pub fn start(&self, dag: Dag, path: PathBuf) -> Result<(), ExecutionError> {
        let (mut senders, mut receivers) = index_edges(&dag, self.channel_buf_sz);
        let mut handles: Vec<JoinHandle<Result<(), ExecutionError>>> = Vec::new();

        let mut record_stores: HashMap<NodeHandle, HashMap<PortHandle, RecordReader>> = dag
            .nodes
            .iter()
            .map(|e| (e.0.clone(), HashMap::new()))
            .collect();

        let (sources, processors, sinks, edges) = get_node_types(dag);

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
                edges.clone(),
                processor.0.clone(),
                processor.1,
                proc_senders.unwrap(),
                proc_receivers.unwrap(),
                path.clone(),
                &mut record_stores,
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
