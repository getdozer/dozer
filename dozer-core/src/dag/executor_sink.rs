use crate::dag::errors::ExecutionError;
use crate::dag::errors::ExecutionError::SchemaNotInitialized;
use crate::dag::executor_local::ExecutorOperation;
use crate::dag::executor_utils::{build_receivers_lists, init_component, init_select, map_to_op};
use crate::dag::forwarder::StateWriter;
use crate::dag::node::{NodeHandle, PortHandle, SinkFactory};
use crate::dag::record_store::RecordReader;
use crate::storage::common::RenewableRwTransaction;
use crate::storage::transactions::SharedTransaction;
use crossbeam::channel::{Receiver, RecvTimeoutError};
use dozer_types::parking_lot::RwLock;

use fp_rust::sync::CountDownLatch;
use log::{error, info};
use std::collections::HashMap;
use std::ops::Add;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

pub(crate) fn start_sink(
    stop_req: Arc<AtomicBool>,
    handle: NodeHandle,
    snk_factory: Box<dyn SinkFactory>,
    receivers: HashMap<PortHandle, Vec<Receiver<ExecutorOperation>>>,
    base_path: PathBuf,
    latch: Arc<CountDownLatch>,
    record_stores: Arc<RwLock<HashMap<NodeHandle, HashMap<PortHandle, RecordReader>>>>,
    term_barrier: Arc<Barrier>,
) -> JoinHandle<Result<(), ExecutionError>> {
    thread::spawn(move || -> Result<(), ExecutionError> {
        let mut snk = snk_factory.build();
        let mut schema_initialized = false;

        let mut state_meta = init_component(&handle, base_path.as_path(), |e| snk.init(e))?;

        let master_tx: Arc<RwLock<Box<dyn RenewableRwTransaction>>> =
            Arc::new(RwLock::new(state_meta.env.create_txn()?));

        let mut state_writer = StateWriter::new(
            state_meta.meta_db,
            HashMap::new(),
            master_tx.clone(),
            Some(snk_factory.get_input_ports()),
        );

        let (handles_ls, receivers_ls) = build_receivers_lists(receivers);
        latch.countdown();

        for rcv in receivers_ls.iter().enumerate() {
            let op = rcv
                .1
                .recv_deadline(Instant::now().add(Duration::from_millis(50)));
            match op {
                Err(RecvTimeoutError::Timeout) => {
                    if stop_req.load(Ordering::Relaxed) {
                        stop_req.store(true, Ordering::Relaxed);
                        term_barrier.wait();
                        return Ok(());
                    }
                    continue;
                }
                Err(RecvTimeoutError::Disconnected) => {
                    stop_req.store(true, Ordering::Relaxed);
                    term_barrier.wait();
                    return Ok(());
                }
                Ok(ExecutorOperation::SchemaUpdate { new }) => {
                    info!(
                        "PRC [{}] Received Schema configuration on port {}",
                        handle, &handles_ls[rcv.0]
                    );
                    let all_input_schemas =
                        state_writer.update_input_schema(handles_ls[rcv.0], new)?;
                    if let Some(input_schemas) = all_input_schemas {
                        match snk.update_schema(&input_schemas) {
                            Err(e) => {
                                error!(
                                    "SNK [{}] Error during update_schema(). Unsupported new schema.",
                                    handle
                                );
                                return Err(e);
                            }
                            _ => schema_initialized = true,
                        }
                    }
                }
                _ => {
                    return {
                        error!(
                            "PRC [{}] Invalid message received. Expected a SchemaUpdate",
                            handle
                        );
                        Err(ExecutionError::SchemaNotInitialized)
                    }
                }
            }
        }

        let mut sel = init_select(&receivers_ls);
        loop {
            let index = sel.ready();
            let op = receivers_ls[index]
                .recv()
                .map_err(|e| ExecutionError::SinkReceiverError(index, Box::new(e)))?;

            match op {
                ExecutorOperation::SchemaUpdate { new } => {
                    let all_input_schemas =
                        state_writer.update_input_schema(handles_ls[index], new)?;
                    if let Some(input_schemas) = all_input_schemas {
                        match snk.update_schema(&input_schemas) {
                            Err(e) => {
                                error!(
                                    "SNK [{}] Error during update_schema(). Unsupported new schema.",
                                    handle
                                );
                                return Err(e);
                            }
                            _ => schema_initialized = true,
                        }
                    }
                }

                ExecutorOperation::Terminate => {
                    info!(
                        "SNK [{}] TERM request received on port {}",
                        handle, handles_ls[index]
                    );
                    term_barrier.wait();
                    return Ok(());
                }
                ExecutorOperation::Commit { epoch, source } => {
                    state_writer.store_commit_info(&source, epoch)?
                }

                _ => {
                    if !schema_initialized {
                        return Err(SchemaNotInitialized);
                    }

                    let data_op = map_to_op(op)?;

                    let guard = record_stores.read();
                    let reader = guard
                        .get(&handle)
                        .ok_or_else(|| ExecutionError::InvalidNodeHandle(handle.clone()))?;

                    snk.process(
                        handles_ls[index],
                        data_op.0,
                        data_op.1,
                        &mut SharedTransaction::new(&master_tx),
                        reader,
                    )?;
                }
            }
        }
    })
}
