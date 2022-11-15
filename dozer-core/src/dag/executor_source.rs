use crate::dag::channels::SourceChannelForwarder;
use crate::dag::dag::Edge;
use crate::dag::errors::ExecutionError;
use crate::dag::errors::ExecutionError::SchemaNotInitialized;
use crate::dag::executor_local::ExecutorOperation;
use crate::dag::executor_utils::{
    build_receivers_lists, create_ports_databases, fill_ports_record_readers, init_component,
    init_select, map_to_op, requires_schema_update,
};
use crate::dag::forwarder::{LocalChannelForwarder, PortRecordStoreWriter};
use crate::dag::node::{NodeHandle, PortHandle, StatelessSourceFactory};
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

pub(crate) fn start_stateless_source(
    handle: NodeHandle,
    src_factory: Box<dyn StatelessSourceFactory>,
    senders: HashMap<PortHandle, Vec<Sender<ExecutorOperation>>>,
    commit_size: u32,
    channel_buffer: usize,
    base_path: PathBuf,
) -> JoinHandle<Result<(), ExecutionError>> {
    let mut fw =
        LocalChannelForwarder::new_source_forwarder(handle.clone(), senders, commit_size, None);

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
