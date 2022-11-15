use crate::dag::errors::ExecutionError;
use crate::dag::errors::ExecutionError::SchemaNotInitialized;
use crate::dag::executor_local::ExecutorOperation;
use crate::dag::executor_utils::{
    build_receivers_lists, create_ports_databases, fill_ports_record_readers, init_component,
    init_select, map_to_op, requires_schema_update,
};
use crate::dag::node::{NodeHandle, PortHandle, StatefulSinkFactory, StatelessSinkFactory};
use crate::storage::transactions::ExclusiveTransaction;
use crossbeam::channel::Receiver;
use dozer_types::types::Schema;
use fp_rust::sync::CountDownLatch;
use log::{error, info, warn};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::thread;
use std::thread::JoinHandle;

pub(crate) fn start_stateful_sink(
    handle: NodeHandle,
    snk_factory: Box<dyn StatefulSinkFactory>,
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
                        &snk_factory.get_input_ports(),
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
                    master_tx.put(&state_meta.meta_db, source.as_bytes(), &epoch.to_be_bytes())?;
                    master_tx.commit_and_renew()?;
                }

                _ => {
                    if !schema_initialized {
                        return Err(SchemaNotInitialized);
                    }

                    let data_op = map_to_op(op)?;
                    let mut rw_txn = ExclusiveTransaction::new(&mut master_tx);
                    snk.process(handles_ls[index], data_op.0, data_op.1, &mut rw_txn)?;
                }
            }
        }
    })
}

pub(crate) fn start_stateless_sink(
    handle: NodeHandle,
    snk_factory: Box<dyn StatelessSinkFactory>,
    receivers: HashMap<PortHandle, Vec<Receiver<ExecutorOperation>>>,
    base_path: PathBuf,
    latch: Arc<CountDownLatch>,
) -> JoinHandle<Result<(), ExecutionError>> {
    thread::spawn(move || -> Result<(), ExecutionError> {
        let mut snk = snk_factory.build();

        let mut input_schemas = HashMap::<PortHandle, Schema>::new();
        let mut schema_initialized = false;
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
                        &snk_factory.get_input_ports(),
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

                ExecutorOperation::Commit { epoch, source } => {}

                _ => {
                    if !schema_initialized {
                        return Err(SchemaNotInitialized);
                    }

                    let data_op = map_to_op(op)?;
                    snk.process(handles_ls[index], data_op.0, data_op.1)?;
                }
            }
        }
    })
}
