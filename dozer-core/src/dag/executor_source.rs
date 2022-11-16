use crate::dag::channels::SourceChannelForwarder;
use crate::dag::errors::ExecutionError;
use crate::dag::errors::ExecutionError::InternalError;
use crate::dag::executor_local::ExecutorOperation;
use crate::dag::executor_utils::{init_select, map_to_exec_op};
use crate::dag::forwarder::LocalChannelForwarder;
use crate::dag::node::{NodeHandle, PortHandle, StatelessSourceFactory};
use crossbeam::channel::{bounded, Receiver, RecvTimeoutError, Sender};
use dozer_types::internal_err;
use dozer_types::types::{Operation, Schema};
use log::{error, info};
use std::collections::HashMap;
use std::ops::Add;
use std::path::PathBuf;
use std::thread;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

struct InternalChannelSourceForwarder {
    senders: HashMap<PortHandle, Sender<ExecutorOperation>>,
}

impl InternalChannelSourceForwarder {
    pub fn new(senders: HashMap<PortHandle, Sender<ExecutorOperation>>) -> Self {
        Self { senders }
    }
}

impl SourceChannelForwarder for InternalChannelSourceForwarder {
    fn send(&mut self, seq: u64, op: Operation, port: PortHandle) -> Result<(), ExecutionError> {
        let sender = self
            .senders
            .get(&port)
            .ok_or(ExecutionError::InvalidPortHandle(port))?;
        let exec_op = map_to_exec_op(seq, op);
        internal_err!(sender.send(exec_op))
    }

    fn update_schema(&mut self, schema: Schema, port: PortHandle) -> Result<(), ExecutionError> {
        let sender = self
            .senders
            .get(&port)
            .ok_or(ExecutionError::InvalidPortHandle(port))?;
        internal_err!(sender.send(ExecutorOperation::SchemaUpdate { new: schema }))
    }

    fn terminate(&mut self) -> Result<(), ExecutionError> {
        for sender in &self.senders {
            let _ = sender.1.send(ExecutorOperation::Terminate);
        }
        Ok(())
    }
}

pub(crate) fn start_stateless_source(
    handle: NodeHandle,
    src_factory: Box<dyn StatelessSourceFactory>,
    mut out_channels: HashMap<PortHandle, Vec<Sender<ExecutorOperation>>>,
    commit_size: u32,
    channel_buffer: usize,
    base_path: PathBuf,
) -> JoinHandle<Result<(), ExecutionError>> {
    let mut internal_receivers: Vec<Receiver<ExecutorOperation>> = Vec::new();
    let mut internal_senders: HashMap<PortHandle, Sender<ExecutorOperation>> = HashMap::new();
    let output_ports = src_factory.get_output_ports();

    for port in &output_ports {
        let channels = bounded::<ExecutorOperation>(channel_buffer.clone());
        internal_receivers.push(channels.1);
        internal_senders.insert(port.clone(), channels.0);
    }

    let mut fw = InternalChannelSourceForwarder::new(internal_senders);
    thread::spawn(move || -> Result<(), ExecutionError> {
        let src = src_factory.build();
        for p in src_factory.get_output_ports() {
            if let Some(schema) = src.get_output_schema(p) {
                fw.update_schema(schema, p)?
            }
        }
        src.start(&mut fw, None)
    });

    let listener_handle = handle.clone();
    let listener = thread::spawn(move || -> Result<(), ExecutionError> {
        let mut dag_fw =
            LocalChannelForwarder::new_source_forwarder(handle, out_channels, commit_size, None);

        let mut sel = init_select(&internal_receivers);
        loop {
            let port_index = sel.ready();
            let r = internal_receivers[port_index]
                .recv_deadline(Instant::now().add(Duration::from_millis(500)));
            match r {
                Err(RecvTimeoutError::Timeout) => {
                    // Check for graceful shutdown request
                }
                Err(RecvTimeoutError::Disconnected) => {
                    info!("[{}] Source exited. Shutting down...", listener_handle);
                    return Ok(());
                }
                Ok(ExecutorOperation::Insert { seq, new }) => {
                    dag_fw.send(seq, Operation::Insert { new }, output_ports[port_index])?;
                }
                Ok(ExecutorOperation::Delete { seq, old }) => {
                    dag_fw.send(seq, Operation::Delete { old }, output_ports[port_index])?;
                }
                Ok(ExecutorOperation::Update { seq, old, new }) => {
                    dag_fw.send(
                        seq,
                        Operation::Update { old, new },
                        output_ports[port_index],
                    )?;
                }
                Ok(ExecutorOperation::SchemaUpdate { new }) => {
                    dag_fw.update_schema(new, output_ports[port_index])?;
                }
                Ok(ExecutorOperation::Terminate) => {
                    dag_fw.terminate()?;
                    return Ok(());
                }
                _ => {}
            }
        }
    });

    listener
}
