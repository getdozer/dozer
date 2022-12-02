// use crate::dag::errors::ExecutionError;
// use crate::dag::errors::ExecutionError::SchemaNotInitialized;
// use crate::dag::executor_local::ExecutorOperation;
// use crate::dag::executor_utils::{build_receivers_lists, init_component, init_select, map_to_op};
// use crate::dag::forwarder::StateWriter;
// use crate::dag::node::{NodeHandle, PortHandle, SinkFactory};
// use crate::dag::record_store::RecordReader;
// use crate::storage::common::RenewableRwTransaction;
// use crate::storage::transactions::SharedTransaction;
// use crossbeam::channel::Receiver;
// use dozer_types::parking_lot::RwLock;
//
// use crate::dag::dag_schemas::DagSchemaManager;
// use dozer_types::types::Schema;
// use fp_rust::sync::CountDownLatch;
// use log::{error, info};
// use std::collections::HashMap;
// use std::path::PathBuf;
// use std::sync::{Arc, Barrier};
// use std::thread;
// use std::thread::JoinHandle;
//
// pub(crate) fn start_sink(
//     input_schemas: &HashMap<PortHandle, Schema>,
//     handle: NodeHandle,
//     snk_factory: Box<dyn SinkFactory>,
//     receivers: HashMap<PortHandle, Vec<Receiver<ExecutorOperation>>>,
//     base_path: PathBuf,
//     latch: Arc<CountDownLatch>,
//     record_stores: Arc<RwLock<HashMap<NodeHandle, HashMap<PortHandle, RecordReader>>>>,
//     _term_barrier: Arc<Barrier>,
// ) -> JoinHandle<Result<(), ExecutionError>> {
//     thread::spawn(move || -> Result<(), ExecutionError> {
//         let mut snk = snk_factory.build();
//         let mut state_meta = init_component(&handle, base_path.as_path(), |e| snk.init(e))?;
//
//         let master_tx: Arc<RwLock<Box<dyn RenewableRwTransaction>>> =
//             Arc::new(RwLock::new(state_meta.env.create_txn()?));
//
//         let mut state_writer = StateWriter::new(
//             state_meta.meta_db,
//             HashMap::new(),
//             master_tx.clone(),
//             Some(snk_factory.get_input_ports()),
//         );
//
//         let (handles_ls, receivers_ls) = build_receivers_lists(receivers);
//         latch.countdown();
//
//         let mut sel = init_select(&receivers_ls);
//         loop {
//             let index = sel.ready();
//             let op = receivers_ls[index]
//                 .recv()
//                 .map_err(|e| ExecutionError::SinkReceiverError(index, Box::new(e)))?;
//
//             match op {
//                 ExecutorOperation::Terminate => {
//                     info!("[{}] Terminating: Exiting message loop", handle);
//                     return Ok(());
//                 }
//                 ExecutorOperation::Commit { epoch, source } => {
//                     snk.commit(&mut SharedTransaction::new(&master_tx))?;
//                     state_writer.store_commit_info(&source, epoch)?
//                 }
//
//                 _ => {
//                     let data_op = map_to_op(op)?;
//
//                     let guard = record_stores.read();
//                     let reader = guard
//                         .get(&handle)
//                         .ok_or_else(|| ExecutionError::InvalidNodeHandle(handle.clone()))?;
//
//                     snk.process(
//                         handles_ls[index],
//                         data_op.0,
//                         data_op.1,
//                         &mut SharedTransaction::new(&master_tx),
//                         reader,
//                     )?;
//                 }
//             }
//         }
//     })
// }
