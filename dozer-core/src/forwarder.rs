use crate::channels::ProcessorChannelForwarder;
use crate::epoch::Epoch;
use crate::error_manager::ErrorManager;
use crate::errors::ExecutionError;
use crate::errors::ExecutionError::InvalidPortHandle;
use crate::executor_operation::ExecutorOperation;
use crate::node::PortHandle;
use crate::record_store::RecordWriter;

use crossbeam::channel::Sender;
use dozer_types::log::debug;
use dozer_types::node::{NodeHandle, OpIdentifier};
use dozer_types::types::OperationWithId;
use std::collections::HashMap;
use std::ops::Deref;
use std::sync::Arc;

#[derive(Debug)]
pub struct ChannelManager {
    owner: NodeHandle,
    record_writers: HashMap<PortHandle, Box<dyn RecordWriter>>,
    senders: HashMap<PortHandle, Vec<Sender<ExecutorOperation>>>,
    error_manager: Arc<ErrorManager>,
}

impl ChannelManager {
    #[inline]
    pub fn send_op(
        &mut self,
        mut op: OperationWithId,
        port_id: PortHandle,
    ) -> Result<(), ExecutionError> {
        if let Some(writer) = self.record_writers.get_mut(&port_id) {
            match writer.write(op.op) {
                Ok(new_op) => op.op = new_op,
                Err(e) => {
                    self.error_manager.report(e.into());
                    return Ok(());
                }
            }
        }

        let senders = self
            .senders
            .get(&port_id)
            .ok_or(InvalidPortHandle(port_id))?;

        let exec_op = ExecutorOperation::Op { op };

        if let Some((last_sender, senders)) = senders.split_last() {
            for sender in senders {
                sender.send(exec_op.clone())?;
            }
            last_sender.send(exec_op)?;
        }

        Ok(())
    }

    pub fn send_to_all_ports(&self, op: ExecutorOperation) -> Result<(), ExecutionError> {
        for senders in self.senders.values() {
            for sender in senders {
                sender.send(op.clone())?;
            }
        }

        Ok(())
    }

    pub fn send_terminate(&self) -> Result<(), ExecutionError> {
        self.send_to_all_ports(ExecutorOperation::Terminate)
    }

    pub fn send_snapshotting_started(&self, connection_name: String) -> Result<(), ExecutionError> {
        self.send_to_all_ports(ExecutorOperation::SnapshottingStarted { connection_name })
    }

    pub fn send_snapshotting_done(
        &self,
        connection_name: String,
        id: Option<OpIdentifier>,
    ) -> Result<(), ExecutionError> {
        self.send_to_all_ports(ExecutorOperation::SnapshottingDone {
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

        self.send_to_all_ports(ExecutorOperation::Commit { epoch })
    }

    pub fn owner(&self) -> &NodeHandle {
        &self.owner
    }

    pub fn new(
        owner: NodeHandle,
        record_writers: HashMap<PortHandle, Box<dyn RecordWriter>>,
        senders: HashMap<PortHandle, Vec<Sender<ExecutorOperation>>>,
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
    fn send(&mut self, op: OperationWithId, port: PortHandle) {
        self.send_op(op, port)
            .unwrap_or_else(|e| panic!("Failed to send operation: {e}"))
    }
}
