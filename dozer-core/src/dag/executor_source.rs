use crate::dag::channels::SourceChannelForwarder;
use crate::dag::errors::ExecutionError;
use crate::dag::executor_local::ExecutorOperation;
use crate::dag::forwarder::LocalChannelForwarder;
use crate::dag::node::{NodeHandle, PortHandle, StatelessSourceFactory};
use crossbeam::channel::Sender;
use std::collections::HashMap;
use std::path::PathBuf;
use std::thread;
use std::thread::JoinHandle;

pub(crate) fn start_stateless_source(
    handle: NodeHandle,
    src_factory: Box<dyn StatelessSourceFactory>,
    senders: HashMap<PortHandle, Vec<Sender<ExecutorOperation>>>,
    commit_size: u32,
    _channel_buffer: usize,
    _base_path: PathBuf,
) -> JoinHandle<Result<(), ExecutionError>> {
    let mut fw = LocalChannelForwarder::new_source_forwarder(handle, senders, commit_size, None);

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
