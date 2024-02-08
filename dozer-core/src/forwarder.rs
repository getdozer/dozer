use crate::channels::ProcessorChannelForwarder;
use crate::epoch::{Epoch, EpochManager};
use crate::error_manager::ErrorManager;
use crate::errors::ExecutionError;
use crate::errors::ExecutionError::InvalidPortHandle;
use crate::executor_operation::ExecutorOperation;
use crate::node::PortHandle;
use crate::record_store::RecordWriter;

use crossbeam::channel::Sender;
use dozer_types::log::debug;
use dozer_types::models::ingestion_types::IngestionMessage;
use dozer_types::node::{NodeHandle, OpIdentifier, SourceState};
use dozer_types::types::OperationWithId;
use std::collections::HashMap;
use std::ops::Deref;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

#[derive(Debug)]
pub struct ChannelManager {
    owner: NodeHandle,
    record_writers: HashMap<PortHandle, Box<dyn RecordWriter>>,
    senders: HashMap<PortHandle, Vec<Sender<ExecutorOperation>>>,
    error_manager: Arc<ErrorManager>,
}

impl ChannelManager {
    #[inline]
    fn send_op(
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

    pub fn send_terminate(&self) -> Result<(), ExecutionError> {
        for senders in self.senders.values() {
            for sender in senders {
                sender.send(ExecutorOperation::Terminate)?;
            }
        }

        Ok(())
    }

    pub fn send_snapshotting_started(&self, connection_name: String) -> Result<(), ExecutionError> {
        for senders in self.senders.values() {
            for sender in senders {
                sender.send(ExecutorOperation::SnapshottingStarted {
                    connection_name: connection_name.clone(),
                })?;
            }
        }

        Ok(())
    }

    pub fn send_snapshotting_done(
        &self,
        connection_name: String,
        id: Option<OpIdentifier>,
    ) -> Result<(), ExecutionError> {
        for senders in self.senders.values() {
            for sender in senders {
                sender.send(ExecutorOperation::SnapshottingDone {
                    connection_name: connection_name.clone(),
                    id,
                })?;
            }
        }

        Ok(())
    }

    pub fn send_commit(&mut self, epoch: &Epoch) -> Result<(), ExecutionError> {
        debug!(
            "[{}] Checkpointing - {}: {:?}",
            self.owner,
            epoch.common_info.id,
            epoch.common_info.source_states.deref()
        );

        for senders in &self.senders {
            for sender in senders.1 {
                sender.send(ExecutorOperation::Commit {
                    epoch: epoch.clone(),
                })?;
            }
        }

        Ok(())
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

#[derive(Debug)]
pub(crate) struct SourceChannelManager {
    port_names: HashMap<PortHandle, String>,
    manager: ChannelManager,
    source_level_state: Vec<u8>,
    source_state: SourceState,
    commit_sz: u32,
    num_uncommitted_ops: u32,
    max_duration_between_commits: Duration,
    last_commit_instant: SystemTime,
    epoch_manager: Arc<EpochManager>,
}

impl SourceChannelManager {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        owner: NodeHandle,
        source_state: Vec<u8>,
        port_names: HashMap<PortHandle, String>,
        record_writers: HashMap<PortHandle, Box<dyn RecordWriter>>,
        senders: HashMap<PortHandle, Vec<Sender<ExecutorOperation>>>,
        commit_sz: u32,
        max_duration_between_commits: Duration,
        epoch_manager: Arc<EpochManager>,
        error_manager: Arc<ErrorManager>,
    ) -> Self {
        let source_level_state = source_state;
        // FIXME: Read current_op_id from persisted state.
        let source_state = SourceState::NotStarted;

        Self {
            manager: ChannelManager::new(owner, record_writers, senders, error_manager),
            port_names,
            source_level_state,
            source_state,
            commit_sz,
            num_uncommitted_ops: 0,
            max_duration_between_commits,
            last_commit_instant: SystemTime::now(),
            epoch_manager,
        }
    }

    fn should_participate_in_commit(&self) -> bool {
        self.num_uncommitted_ops >= self.commit_sz
            || self
                .last_commit_instant
                .elapsed()
                .unwrap_or(self.max_duration_between_commits) // In case of system time drift, we just commit
                >= self.max_duration_between_commits
    }

    fn commit(&mut self, request_termination: bool) -> Result<bool, ExecutionError> {
        let epoch = self.epoch_manager.wait_for_epoch_close(
            (self.manager.owner.clone(), self.source_state.clone()),
            request_termination,
            self.num_uncommitted_ops > 0,
        );

        if let Some(common_info) = epoch.common_info {
            if let Some(checkpoint_writer) = common_info.checkpoint_writer.as_ref() {
                for (port, port_name) in &self.port_names {
                    if let Some(record_writer) = self.manager.record_writers.get(port) {
                        let object = checkpoint_writer
                            .create_record_writer_object(&self.manager.owner, port_name)?;
                        record_writer
                            .serialize(object)
                            .map_err(ExecutionError::SerializeRecordWriter)?;
                    }
                }
            }

            self.manager
                .send_commit(&Epoch::from(common_info, epoch.decision_instant))?;
        }

        self.num_uncommitted_ops = 0;
        self.last_commit_instant = epoch.decision_instant;
        Ok(epoch.should_terminate)
    }

    pub fn trigger_commit_if_needed(
        &mut self,
        request_termination: bool,
    ) -> Result<bool, ExecutionError> {
        if request_termination || self.should_participate_in_commit() {
            self.commit(request_termination)
        } else {
            Ok(false)
        }
    }

    pub fn send_and_trigger_commit_if_needed(
        &mut self,
        message: IngestionMessage,
        port: PortHandle,
        request_termination: bool,
    ) -> Result<bool, ExecutionError> {
        match message {
            IngestionMessage::OperationEvent { op, id, .. } => {
                self.source_state = if let Some(id) = id {
                    SourceState::Restartable {
                        state: self.source_level_state.clone(),
                        checkpoint: id,
                    }
                } else {
                    SourceState::NonRestartable
                };

                self.manager.send_op(OperationWithId { id, op }, port)?;
                self.num_uncommitted_ops += 1;
                self.trigger_commit_if_needed(request_termination)
            }
            IngestionMessage::SnapshottingStarted => {
                self.num_uncommitted_ops += 1;
                self.manager
                    .send_snapshotting_started(self.manager.owner.id.clone())?;
                self.trigger_commit_if_needed(request_termination)
            }
            IngestionMessage::SnapshottingDone { id } => {
                self.num_uncommitted_ops += 1;
                self.manager
                    .send_snapshotting_done(self.manager.owner.id.clone(), id)?;
                self.commit(request_termination)
            }
        }
    }

    pub fn terminate(&mut self) -> Result<(), ExecutionError> {
        self.manager.send_terminate()
    }
}

impl ProcessorChannelForwarder for ChannelManager {
    fn send(&mut self, op: OperationWithId, port: PortHandle) {
        self.send_op(op, port)
            .unwrap_or_else(|e| panic!("Failed to send operation: {e}"))
    }
}
