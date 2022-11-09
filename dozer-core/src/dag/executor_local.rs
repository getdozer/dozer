#![allow(clippy::type_complexity)]
use crate::dag::channels::SourceChannelForwarder;
use crate::dag::dag::{Dag, Edge, Endpoint, NodeType, PortDirection};
use crate::dag::errors::ExecutionError;
use crate::dag::errors::ExecutionError::{
    InvalidOperation, MissingNodeInput, MissingNodeOutput, SchemaNotInitialized,
};
use crate::dag::executor_utils::{
    build_receivers_lists, create_ports_databases, fill_ports_record_readers,
    get_inputs_for_output, get_node_types, index_edges, init_component, init_select, map_to_op,
    requires_schema_update,
};
use crate::dag::forwarder::{LocalChannelForwarder, PortRecordStoreWriter};
use crate::dag::node::{NodeHandle, PortHandle, ProcessorFactory, SinkFactory, SourceFactory};
use crate::dag::record_store::RecordReader;
use crate::storage::common::{Database, RenewableRwTransaction, RoTransaction};
use crate::storage::errors::StorageError;
use crossbeam::channel::{bounded, Receiver, Select, Sender};
use dozer_types::types::{Operation, Record, Schema};
use fp_rust::sync::CountDownLatch;
use libc::size_t;
use log::{error, info, warn};
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::path::{Path, PathBuf};
use std::string::ToString;
use std::sync::{Arc, RwLock};
use std::thread;
use std::thread::JoinHandle;
use std::time::Duration;

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

    fn start_source(
        &self,
        handle: NodeHandle,
        src_factory: Box<dyn SourceFactory>,
        senders: HashMap<PortHandle, Vec<Sender<ExecutorOperation>>>,
        base_path: PathBuf,
    ) -> JoinHandle<Result<(), ExecutionError>> {
        let mut fw = LocalChannelForwarder::new_source_forwarder(
            handle.clone(),
            senders,
            DEFAULT_COMMIT_SZ,
            None,
        );

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

    fn start_sink(
        &self,
        handle: NodeHandle,
        snk_factory: Box<dyn SinkFactory>,
        receivers: HashMap<PortHandle, Vec<Receiver<ExecutorOperation>>>,
        base_path: PathBuf,
        latch: Arc<CountDownLatch>,
    ) -> JoinHandle<Result<(), ExecutionError>> {
        thread::spawn(move || -> Result<(), ExecutionError> {
            let mut snk = snk_factory.build();

            let mut input_schemas = HashMap::<PortHandle, Schema>::new();
            let mut schema_initialized = false;

            let mut state_meta = init_component(&handle, base_path, |e| snk.init(e))?;
            let mut master_tx = state_meta.env.create_txn()?;

            let (handles_ls, receivers_ls) = build_receivers_lists(receivers);

            latch.countdown();

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
                        master_tx.put(
                            &state_meta.meta_db,
                            source.as_bytes(),
                            &epoch.to_be_bytes(),
                        )?;
                        master_tx.commit_and_renew()?;
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
                            Some(master_tx.as_rw_transaction()),
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
        record_stores: Arc<RwLock<HashMap<NodeHandle, HashMap<PortHandle, RecordReader>>>>,
        latch: Arc<CountDownLatch>,
    ) -> JoinHandle<Result<(), ExecutionError>> {
        thread::spawn(move || -> Result<(), ExecutionError> {
            let mut proc = proc_factory.build();

            let mut input_schemas = HashMap::<PortHandle, Schema>::new();
            let mut output_schemas = HashMap::<PortHandle, Schema>::new();
            let mut schema_initialized = false;

            let mut state_meta = init_component(&handle, base_path, |e| proc.init(e))?;
            let mut port_databases = create_ports_databases(
                state_meta.env.as_environment(),
                proc_factory.get_output_ports(),
            )?;
            let mut master_tx: Arc<RwLock<Box<dyn RenewableRwTransaction>>> =
                Arc::new(RwLock::new(state_meta.env.create_txn()?));

            fill_ports_record_readers(
                &handle,
                &edges,
                &port_databases,
                &master_tx,
                &record_stores,
                proc_factory.get_output_ports(),
            );

            let (handles_ls, receivers_ls) = build_receivers_lists(receivers);
            let mut fw = LocalChannelForwarder::new_processor_forwarder(
                handle.clone(),
                senders,
                Some(PortRecordStoreWriter::new(
                    port_databases.clone(),
                    output_schemas.clone(),
                )),
            );

            info!(
                "Processor {} initialization complete. Ready to start...",
                handle
            );
            latch.countdown();

            // let guard = record_stores.read();
            // let reader = guard
            //     .get(&handle)
            //     .ok_or(ExecutionError::InvalidNodeHandle(handle.clone()))?;

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
                        master_tx.write().unwrap().put(
                            &state_meta.meta_db,
                            source.as_bytes(),
                            &epoch.to_be_bytes(),
                        )?;
                        master_tx.write().unwrap().commit_and_renew()?;
                    }

                    _ => {
                        if !schema_initialized {
                            error!("Received a CDC before schema initialization. Exiting from SNK message loop.");
                            return Err(SchemaNotInitialized);
                        }

                        let guard = record_stores.read().unwrap();
                        let reader = guard
                            .get(&handle)
                            .ok_or(ExecutionError::InvalidNodeHandle(handle.clone()))?;

                        let data_op = map_to_op(op)?;
                        fw.update_seq_no(data_op.0);
                        proc.process(
                            handles_ls[index],
                            data_op.1,
                            &mut fw,
                            Some(master_tx.write().unwrap().as_rw_transaction()),
                            reader,
                        )?;
                    }
                }
            }
        })
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
            let snk_handle = self.start_sink(
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

            let proc_handle = self.start_processor(
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
            handles.push(self.start_source(
                source.0.clone(),
                source.1,
                senders.remove(&source.0.clone()).unwrap(),
                path.clone(),
            ));
        }

        for sh in handles {
            let r = sh.join().unwrap()?;
        }

        Ok(())
    }
}
