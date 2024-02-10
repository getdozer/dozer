use crate::channels::ProcessorChannelForwarder;
use crate::epoch::Epoch;
use crate::error_manager::ErrorManager;
use crate::errors::ExecutionError;
use crate::executor_operation::ExecutorOperation;
use crate::node::PortHandle;
use crate::record_store::RecordWriter;

use crossbeam::channel::Sender;
use dozer_types::log::debug;
use dozer_types::node::{NodeHandle, OpIdentifier};
use dozer_types::types::TableOperation;
use std::collections::HashMap;
use std::ops::Deref;
use std::sync::Arc;

#[derive(Debug)]
pub struct SenderWithPortMapping {
    pub sender: Sender<ExecutorOperation>,
    /// From output port to input port.
    pub port_mapping: HashMap<PortHandle, Vec<PortHandle>>,
}

impl SenderWithPortMapping {
    pub fn send_op(&self, mut op: TableOperation) -> Result<(), ExecutionError> {
        let Some(ports) = self.port_mapping.get(&op.port) else {
            // Downstream node is not interested in data from this port.
            return Ok(());
        };

        if let Some((last_port, ports)) = ports.split_last() {
            for port in ports {
                let mut op = op.clone();
                op.port = *port;
                self.sender.send(ExecutorOperation::Op { op })?;
            }
            op.port = *last_port;
            self.sender.send(ExecutorOperation::Op { op })?;
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct ChannelManager {
    owner: NodeHandle,
    record_writers: HashMap<PortHandle, Box<dyn RecordWriter>>,
    senders: Vec<SenderWithPortMapping>,
    error_manager: Arc<ErrorManager>,
}

impl ChannelManager {
    #[inline]
    pub fn send_op(&mut self, mut op: TableOperation) -> Result<(), ExecutionError> {
        if let Some(writer) = self.record_writers.get_mut(&op.port) {
            match writer.write(op.op) {
                Ok(new_op) => op.op = new_op,
                Err(e) => {
                    self.error_manager.report(e.into());
                    return Ok(());
                }
            }
        }

        if let Some((last_sender, senders)) = self.senders.split_last() {
            for sender in senders {
                sender.send_op(op.clone())?;
            }
            last_sender.send_op(op)?;
        }

        Ok(())
    }

    /// Send anything that's not an `ExecutorOperation::Op`.
    pub fn send_non_op(&self, op: ExecutorOperation) -> Result<(), ExecutionError> {
        assert!(!matches!(op, ExecutorOperation::Op { .. }));
        if let Some((last_sender, senders)) = self.senders.split_last() {
            for sender in senders {
                sender.sender.send(op.clone())?;
            }
            last_sender.sender.send(op)?;
        }

        Ok(())
    }

    pub fn send_terminate(&self) -> Result<(), ExecutionError> {
        self.send_non_op(ExecutorOperation::Terminate)
    }

    pub fn send_snapshotting_started(&self, connection_name: String) -> Result<(), ExecutionError> {
        self.send_non_op(ExecutorOperation::SnapshottingStarted { connection_name })
    }

    pub fn send_snapshotting_done(
        &self,
        connection_name: String,
        id: Option<OpIdentifier>,
    ) -> Result<(), ExecutionError> {
        self.send_non_op(ExecutorOperation::SnapshottingDone {
            connection_name,
            id,
        })
    }

    pub fn send_commit(&mut self, epoch: Epoch) -> Result<(), ExecutionError> {
        debug!(
            "[{}] Checkpointing - {}: {:?}",
            self.owner,
            epoch.common_info.id,
            epoch.common_info.source_states.deref()
        );

        self.send_non_op(ExecutorOperation::Commit { epoch })
    }

    pub fn owner(&self) -> &NodeHandle {
        &self.owner
    }

    pub fn new(
        owner: NodeHandle,
        record_writers: HashMap<PortHandle, Box<dyn RecordWriter>>,
        senders: Vec<SenderWithPortMapping>,
        error_manager: Arc<ErrorManager>,
    ) -> Self {
        Self {
            owner,
            record_writers,
            senders,
            error_manager,
        }
    }
}

impl ProcessorChannelForwarder for ChannelManager {
    fn send(&mut self, op: TableOperation) {
        self.send_op(op)
            .unwrap_or_else(|e| panic!("Failed to send operation: {e}"))
    }
}
