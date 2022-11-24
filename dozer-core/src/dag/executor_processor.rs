#![allow(clippy::too_many_arguments)]
use crate::dag::dag::Edge;
use crate::dag::errors::ExecutionError;
use crate::dag::errors::ExecutionError::SchemaNotInitialized;
use crate::dag::executor_local::{ExecutorOperation, InputPortState};
use crate::dag::executor_utils::{
    build_receivers_lists, create_ports_databases, fill_ports_record_readers, init_component,
    init_select, map_to_op,
};
use crate::dag::forwarder::{LocalChannelForwarder, StateWriter};
use crate::dag::node::{NodeHandle, OutputPortDef, PortHandle, Processor, ProcessorFactory};
use crate::dag::record_store::RecordReader;
use crate::storage::common::RenewableRwTransaction;
use crate::storage::transactions::SharedTransaction;
use crossbeam::channel::{Receiver, Sender};
use dozer_types::parking_lot::RwLock;
use dozer_types::types::Schema;
use fp_rust::sync::CountDownLatch;
use log::{error, info, warn};
use std::collections::HashMap;
use std::ops::Add;
use std::path::PathBuf;
use std::sync::Arc;
use std::thread;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

fn update_processor_schema(
    new: Schema,
    out_handle: &PortHandle,
    output_ports: &[OutputPortDef],
    proc: &mut Box<dyn Processor>,
    fw: &mut LocalChannelForwarder,
) -> Result<bool, ExecutionError> {
    match fw.update_input_schema(*out_handle, new)? {
        Some(input_schemas) => {
            for out_port in output_ports {
                match proc.update_schema(out_port.handle, &input_schemas) {
                    Ok(out_schema) => {
                        fw.send_and_update_output_schema(out_schema, out_port.handle)?;
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
        }
        None => Ok(true),
    }
}

pub(crate) fn start_processor(
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

        let mut schema_initialized = false;
        let mut state_meta = init_component(&handle, base_path.as_path(), |e| proc.init(e))?;

        let port_databases = create_ports_databases(
            state_meta.env.as_environment(),
            &proc_factory.get_output_ports(),
        )?;

        let master_tx: Arc<RwLock<Box<dyn RenewableRwTransaction>>> =
            Arc::new(RwLock::new(state_meta.env.create_txn()?));

        fill_ports_record_readers(
            &handle,
            &edges,
            &port_databases,
            &master_tx,
            &record_stores,
            &proc_factory.get_output_ports(),
        );

        let (handles_ls, receivers_ls) = build_receivers_lists(receivers);
        let mut fw = LocalChannelForwarder::new_processor_forwarder(
            handle.clone(),
            senders,
            StateWriter::new(
                state_meta.meta_db,
                port_databases,
                master_tx.clone(),
                Some(proc_factory.get_input_ports()),
            ),
            true,
        );

        info!(
            "[{}] Initialization started. Waiting for schema definitions...",
            handle
        );
        latch.countdown();

        for rcv in receivers_ls.iter().enumerate() {
            let op = rcv
                .1
                .recv()
                .map_err(|e| ExecutionError::ProcessorReceiverError(rcv.0, Box::new(e)))?;
            match op {
                ExecutorOperation::SchemaUpdate { new } => {
                    info!(
                        "[{}] Received Schema configuration on port {}",
                        handle, &handles_ls[rcv.0]
                    );
                    update_processor_schema(
                        new,
                        &handles_ls[rcv.0],
                        &proc_factory.get_output_ports(),
                        &mut proc,
                        &mut fw,
                    )?;
                }
                _ => {
                    return {
                        error!(
                            "[{}] Invalid message received. Expected a SchemaUpdate",
                            handle
                        );
                        Err(ExecutionError::SchemaNotInitialized)
                    }
                }
            }
        }

        info!(
            "[{}] Schema definition complete. Waiting for data...",
            handle
        );

        let mut port_states: Vec<InputPortState> =
            handles_ls.iter().map(|h| InputPortState::Open).collect();

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
                        &proc_factory.get_output_ports(),
                        &mut proc,
                        &mut fw,
                    )?;
                }

                ExecutorOperation::Terminate => {
                    port_states[index] = InputPortState::Terminated;
                    info!(
                        "[{}] Received Terminate request on port {}",
                        handle, &handles_ls[index]
                    );
                    if port_states.iter().all(|v| v == &InputPortState::Terminated) {
                        fw.send_term_and_wait()?;
                        return Ok(());
                    }
                }

                ExecutorOperation::Commit { epoch, source } => {
                    fw.store_and_send_commit(source, epoch)?;
                }

                _ => {
                    // if !schema_initialized {
                    //     error!("Received a CDC before schema initialization. Exiting from SNK message loop.");
                    //     return Err(SchemaNotInitialized);
                    // }

                    let guard = record_stores.read();
                    let reader = guard
                        .get(&handle)
                        .ok_or_else(|| ExecutionError::InvalidNodeHandle(handle.clone()))?;

                    let data_op = map_to_op(op)?;
                    fw.update_seq_no(data_op.0);

                    proc.process(
                        handles_ls[index],
                        data_op.1,
                        &mut fw,
                        &mut SharedTransaction::new(&master_tx),
                        reader,
                    )?;
                }
            }
        }
    })
}
