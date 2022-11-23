#![allow(clippy::too_many_arguments)]
use crate::dag::channels::SourceChannelForwarder;
use crate::dag::dag::Edge;
use crate::dag::errors::ExecutionError;
use crate::dag::errors::ExecutionError::SchemaNotInitialized;
use crate::dag::executor_local::ExecutorOperation;
use crate::dag::executor_utils::ProcessorHolder::Stateful;
use crate::dag::executor_utils::{
    build_receivers_lists, create_ports_databases, fill_ports_record_readers, init_component,
    init_select, map_to_op, requires_schema_update,
};
use crate::dag::forwarder::{LocalChannelForwarder, StateWriter};
use crate::dag::node::{
    NodeHandle, PortHandle, StatefulProcessor, StatefulProcessorFactory, StatelessProcessor,
    StatelessProcessorFactory,
};
use crate::dag::record_store::RecordReader;
use crate::storage::common::RenewableRwTransaction;
use crate::storage::transactions::SharedTransaction;
use crossbeam::channel::{Receiver, Sender};
use dozer_types::parking_lot::RwLock;
use dozer_types::types::Schema;
use fp_rust::sync::CountDownLatch;
use log::{error, info, warn};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::thread;
use std::thread::JoinHandle;

pub(crate) enum ProcessorFactoryHolder {
    Stateful(Box<dyn StatefulProcessorFactory>),
    Stateless(Box<dyn StatelessProcessorFactory>),
}

pub(crate) enum ProcessorHolder {
    Stateful(Box<dyn StatefulProcessor>),
    Stateless(Box<dyn StatelessProcessor>),
}

// fn update_stateless_processor_schema(
//     new: Schema,
//     out_handle: &PortHandle,
//     input_schemas: &mut HashMap<PortHandle, Schema>,
//     output_schemas: &mut HashMap<PortHandle, Schema>,
//     input_ports: &[PortHandle],
//     output_ports: &[PortHandle],
//     proc: &mut Box<dyn StatelessProcessor>,
//     fw: &mut LocalChannelForwarder,
// ) -> Result<bool, ExecutionError> {
//     if requires_schema_update(new, out_handle, input_schemas, input_ports) {
//         for out_port in output_ports {
//             let r = proc.update_schema(*out_port, input_schemas);
//             match r {
//                 Ok(out_schema) => {
//                     output_schemas.insert(*out_port, out_schema.clone());
//                     fw.update_schema(out_schema, *out_port)?;
//                 }
//                 Err(e) => {
//                     warn!(
//                         "New schema is not compatible with older version. Handling it. {:?}",
//                         e
//                     );
//                     return Ok(false);
//                 }
//             }
//         }
//         Ok(true)
//     } else {
//         Ok(true)
//     }
// }
//
// pub(crate) fn start_stateless_processor(
//     handle: NodeHandle,
//     proc_factory: Box<dyn StatelessProcessorFactory>,
//     senders: HashMap<PortHandle, Vec<Sender<ExecutorOperation>>>,
//     receivers: HashMap<PortHandle, Vec<Receiver<ExecutorOperation>>>,
//     base_path: PathBuf,
//     record_stores: Arc<RwLock<HashMap<NodeHandle, HashMap<PortHandle, RecordReader>>>>,
//     latch: Arc<CountDownLatch>,
// ) -> JoinHandle<Result<(), ExecutionError>> {
//     thread::spawn(move || -> Result<(), ExecutionError> {
//         let mut proc = proc_factory.build();
//         let input_port_handles = proc_factory.get_input_ports();
//         let output_port_handles = proc_factory.get_output_ports();
//
//         let mut input_schemas = HashMap::<PortHandle, Schema>::new();
//         let mut output_schemas = HashMap::<PortHandle, Schema>::new();
//         let mut schema_initialized = false;
//
//         let mut state_meta = init_component(&handle, base_path.as_path(), |_e| Ok(()))?;
//         let master_tx: Box<dyn RenewableRwTransaction> = state_meta.env.create_txn()?;
//
//         let (handles_ls, receivers_ls) = build_receivers_lists(receivers);
//         let mut fw = LocalChannelForwarder::new_processor_forwarder(
//             handle.clone(),
//             senders,
//             StateWriter::new(
//                 state_meta.meta_db,
//                 HashMap::new(),
//                 TransactionHolder::Exclusive(master_tx),
//                 Some(proc_factory.get_input_ports()),
//             ),
//             false,
//         );
//
//         info!("[{}] Initialization complete. Ready to start...", handle);
//         latch.countdown();
//
//         let mut sel = init_select(&receivers_ls);
//         loop {
//             let index = sel.ready();
//             let op = receivers_ls[index]
//                 .recv()
//                 .map_err(|e| ExecutionError::ProcessorReceiverError(index, Box::new(e)))?;
//
//             match op {
//                 ExecutorOperation::SchemaUpdate { new } => {
//                     schema_initialized = update_stateless_processor_schema(
//                         new,
//                         &handles_ls[index],
//                         &mut input_schemas,
//                         &mut output_schemas,
//                         &input_port_handles,
//                         &output_port_handles,
//                         &mut proc,
//                         &mut fw,
//                     )?;
//                 }
//
//                 ExecutorOperation::Terminate => {
//                     fw.send_term_and_wait()?;
//                     return Ok(());
//                 }
//
//                 ExecutorOperation::Commit { epoch, source } => {
//                     fw.store_and_send_commit(source, epoch)?;
//                 }
//
//                 _ => {
//                     if !schema_initialized {
//                         error!("Received a CDC before schema initialization. Exiting from SNK message loop.");
//                         return Err(SchemaNotInitialized);
//                     }
//
//                     let guard = record_stores.read();
//                     let reader = guard
//                         .get(&handle)
//                         .ok_or_else(|| ExecutionError::InvalidNodeHandle(handle.clone()))?;
//
//                     let data_op = map_to_op(op)?;
//                     fw.update_seq_no(data_op.0);
//                     proc.process(handles_ls[index], data_op.1, &mut fw, reader)?;
//                 }
//             }
//         }
//     })
// }
//
// fn update_stateful_processor_schema(
//     new: Schema,
//     out_handle: &PortHandle,
//     output_ports: &[PortHandle],
//     proc: &mut Box<dyn StatefulProcessor>,
//     fw: &mut LocalChannelForwarder,
// ) -> Result<bool, ExecutionError> {
//     if let Some(input_schemas) = fw.update_input_schema(*out_handle, new)? {
//         for out_port in output_ports {
//             let r = proc.update_schema(*out_port, &input_schemas);
//             match r {
//                 Ok(out_schema) => {
//                     fw.send_and_update_output_schema(out_schema, *out_port)?;
//                 }
//                 Err(e) => {
//                     warn!(
//                         "New schema is not compatible with older version. Handling it. {:?}",
//                         e
//                     );
//                     return Ok(false);
//                 }
//             }
//         }
//         Ok(true)
//     } else {
//         Ok(true)
//     }
// }
//
// pub(crate) fn start_stateful_processor(
//     edges: Vec<Edge>,
//     handle: NodeHandle,
//     proc_factory: Box<dyn StatefulProcessorFactory>,
//     senders: HashMap<PortHandle, Vec<Sender<ExecutorOperation>>>,
//     receivers: HashMap<PortHandle, Vec<Receiver<ExecutorOperation>>>,
//     base_path: PathBuf,
//     record_stores: Arc<RwLock<HashMap<NodeHandle, HashMap<PortHandle, RecordReader>>>>,
//     latch: Arc<CountDownLatch>,
// ) -> JoinHandle<Result<(), ExecutionError>> {
//     thread::spawn(move || -> Result<(), ExecutionError> {
//         let mut proc = proc_factory.build();
//         let input_port_handles = proc_factory.get_input_ports();
//         let output_port_handles: Vec<PortHandle> = proc_factory
//             .get_output_ports()
//             .iter()
//             .map(|e| e.handle)
//             .collect();
//
//         let mut input_schemas = HashMap::<PortHandle, Schema>::new();
//         let mut schema_initialized = false;
//
//         let mut state_meta = init_component(&handle, base_path.as_path(), |e| proc.init(e))?;
//
//         let port_databases = create_ports_databases(
//             state_meta.env.as_environment(),
//             &proc_factory.get_output_ports(),
//         )?;
//
//         let master_tx: Arc<RwLock<Box<dyn RenewableRwTransaction>>> =
//             Arc::new(RwLock::new(state_meta.env.create_txn()?));
//
//         fill_ports_record_readers(
//             &handle,
//             &edges,
//             &port_databases,
//             &master_tx,
//             &record_stores,
//             &proc_factory.get_output_ports(),
//         );
//
//         let (handles_ls, receivers_ls) = build_receivers_lists(receivers);
//         let mut fw = LocalChannelForwarder::new_processor_forwarder(
//             handle.clone(),
//             senders,
//             StateWriter::new(
//                 state_meta.meta_db,
//                 port_databases,
//                 TransactionHolder::Shared(master_tx.clone()),
//                 Some(proc_factory.get_input_ports()),
//             ),
//             true,
//         );
//
//         info!("[{}] Initialization complete. Ready to start...", handle);
//         latch.countdown();
//
//         let mut sel = init_select(&receivers_ls);
//         loop {
//             let index = sel.ready();
//             let op = receivers_ls[index]
//                 .recv()
//                 .map_err(|e| ExecutionError::ProcessorReceiverError(index, Box::new(e)))?;
//
//             match op {
//                 ExecutorOperation::SchemaUpdate { new } => {
//                     schema_initialized = update_stateful_processor_schema(
//                         new,
//                         &handles_ls[index],
//                         &mut input_schemas,
//                         &input_port_handles,
//                         &output_port_handles,
//                         &mut proc,
//                         &mut fw,
//                     )?;
//                 }
//
//                 ExecutorOperation::Terminate => {
//                     fw.send_term_and_wait()?;
//                     return Ok(());
//                 }
//
//                 ExecutorOperation::Commit { epoch, source } => {
//                     fw.store_and_send_commit(source, epoch)?;
//                 }
//
//                 _ => {
//                     if !schema_initialized {
//                         error!("Received a CDC before schema initialization. Exiting from SNK message loop.");
//                         return Err(SchemaNotInitialized);
//                     }
//
//                     let guard = record_stores.read();
//                     let reader = guard
//                         .get(&handle)
//                         .ok_or_else(|| ExecutionError::InvalidNodeHandle(handle.clone()))?;
//
//                     let data_op = map_to_op(op)?;
//                     let mut rw_txn = SharedTransaction::new(&master_tx);
//                     fw.update_seq_no(data_op.0);
//                     proc.process(handles_ls[index], data_op.1, &mut fw, &mut rw_txn, reader)?;
//                 }
//             }
//         }
//     })
// }

struct ProcessorDetails {
    proc: ProcessorHolder,
    input_ports: Vec<PortHandle>,
    output_ports: Vec<PortHandle>,
}

impl ProcessorDetails {
    pub fn new(
        proc: ProcessorHolder,
        input_ports: Vec<PortHandle>,
        output_ports: Vec<PortHandle>,
    ) -> Self {
        Self {
            proc,
            input_ports,
            output_ports,
        }
    }
}

fn update_processor_schema(
    new: Schema,
    out_handle: &PortHandle,
    output_ports: &[PortHandle],
    proc: &mut ProcessorHolder,
    fw: &mut LocalChannelForwarder,
) -> Result<bool, ExecutionError> {
    let all_input_schemas = fw.update_input_schema(*out_handle, new)?;
    if let Some(input_schemas) = all_input_schemas {
        for out_port in output_ports {
            let r = match proc {
                ProcessorHolder::Stateful(p) => p.update_schema(*out_port, &input_schemas),
                ProcessorHolder::Stateless(p) => p.update_schema(*out_port, &input_schemas),
            };
            match r {
                Ok(out_schema) => {
                    fw.send_and_update_output_schema(out_schema, *out_port)?;
                }
                Err(e) => {
                    warn!(
                        "New schema is not compatible with older version. Handling it. {:?}",
                        e
                    );
                    return Ok(false);
                }
            }
        }
        Ok(true)
    } else {
        Ok(true)
    }
}

pub(crate) fn start_processor(
    edges: Vec<Edge>,
    handle: NodeHandle,
    proc_factory: ProcessorFactoryHolder,
    senders: HashMap<PortHandle, Vec<Sender<ExecutorOperation>>>,
    receivers: HashMap<PortHandle, Vec<Receiver<ExecutorOperation>>>,
    base_path: PathBuf,
    record_stores: Arc<RwLock<HashMap<NodeHandle, HashMap<PortHandle, RecordReader>>>>,
    latch: Arc<CountDownLatch>,
) -> JoinHandle<Result<(), ExecutionError>> {
    thread::spawn(move || -> Result<(), ExecutionError> {
        let mut proc_details = match &proc_factory {
            ProcessorFactoryHolder::Stateful(f) => ProcessorDetails::new(
                ProcessorHolder::Stateful(f.build()),
                f.get_input_ports(),
                f.get_output_ports().iter().map(|e| e.handle).collect(),
            ),
            ProcessorFactoryHolder::Stateless(f) => ProcessorDetails::new(
                ProcessorHolder::Stateless(f.build()),
                f.get_input_ports(),
                f.get_output_ports(),
            ),
        };

        let mut schema_initialized = false;
        let mut state_meta = init_component(&handle, base_path.as_path(), |e| {
            match &mut proc_details.proc {
                ProcessorHolder::Stateful(p) => p.init(e),
                ProcessorHolder::Stateless(p) => p.init(),
            }
        })?;

        let port_databases = match &proc_factory {
            ProcessorFactoryHolder::Stateful(f) => {
                create_ports_databases(state_meta.env.as_environment(), &f.get_output_ports())?
            }
            _ => HashMap::new(),
        };

        let master_tx: Arc<RwLock<Box<dyn RenewableRwTransaction>>> =
            Arc::new(RwLock::new(state_meta.env.create_txn()?));

        if let ProcessorFactoryHolder::Stateful(proc_factory) = &proc_factory {
            fill_ports_record_readers(
                &handle,
                &edges,
                &port_databases,
                &master_tx,
                &record_stores,
                &proc_factory.get_output_ports(),
            );
        }

        let (handles_ls, receivers_ls) = build_receivers_lists(receivers);
        let mut fw = LocalChannelForwarder::new_processor_forwarder(
            handle.clone(),
            senders,
            StateWriter::new(
                state_meta.meta_db,
                port_databases,
                master_tx.clone(),
                Some(proc_details.input_ports),
            ),
            true,
        );

        info!("[{}] Initialization complete. Ready to start...", handle);
        latch.countdown();

        let mut sel = init_select(&receivers_ls);
        loop {
            let index = sel.ready();
            let op = receivers_ls[index]
                .recv()
                .map_err(|e| ExecutionError::ProcessorReceiverError(index, Box::new(e)))?;

            match op {
                ExecutorOperation::SchemaUpdate { new } => {
                    schema_initialized = update_processor_schema(
                        new,
                        &handles_ls[index],
                        &proc_details.output_ports,
                        &mut proc_details.proc,
                        &mut fw,
                    )?;
                }

                ExecutorOperation::Terminate => {
                    fw.send_term_and_wait()?;
                    return Ok(());
                }

                ExecutorOperation::Commit { epoch, source } => {
                    fw.store_and_send_commit(source, epoch)?;
                }

                _ => {
                    if !schema_initialized {
                        error!("Received a CDC before schema initialization. Exiting from SNK message loop.");
                        return Err(SchemaNotInitialized);
                    }

                    let guard = record_stores.read();
                    let reader = guard
                        .get(&handle)
                        .ok_or_else(|| ExecutionError::InvalidNodeHandle(handle.clone()))?;

                    let data_op = map_to_op(op)?;
                    fw.update_seq_no(data_op.0);

                    match proc_details.proc {
                        ProcessorHolder::Stateful(ref mut p) => p.process(
                            handles_ls[index],
                            data_op.1,
                            &mut fw,
                            &mut SharedTransaction::new(&master_tx),
                            reader,
                        )?,

                        ProcessorHolder::Stateless(ref mut p) => {
                            p.process(handles_ls[index], data_op.1, &mut fw, reader)?
                        }

                        _ => {
                            return Err(ExecutionError::InvalidOperation(
                                "Invalid Transaction for Processor".to_string(),
                            ))
                        }
                    }
                }
            }
        }
    })
}
