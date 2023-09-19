use crate::channels::ProcessorChannelForwarder;
use crate::epoch::{Epoch, EpochManager};
use crate::error_manager::ErrorManager;
use crate::errors::ExecutionError;
use crate::errors::ExecutionError::InvalidPortHandle;
use crate::executor_operation::{ExecutorOperation, ProcessorOperation};
use crate::node::PortHandle;
use crate::record_store::{RecordWriter, RecordWriterError};

use crossbeam::channel::Sender;
use dozer_types::ingestion_types::IngestionMessage;
use dozer_types::log::debug;
use dozer_types::node::{NodeHandle, TableState};
use std::collections::HashMap;
use std::ops::Deref;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

#[derive(Debug)]
pub(crate) struct StateWriter {
    record_writers: HashMap<PortHandle, Box<dyn RecordWriter>>,
}

impl StateWriter {
    pub fn new(record_writers: HashMap<PortHandle, Box<dyn RecordWriter>>) -> Self {
        Self { record_writers }
    }

    fn store_op(
        &mut self,
        op: ProcessorOperation,
        port: &PortHandle,
    ) -> Result<ProcessorOperation, RecordWriterError> {
        if let Some(writer) = self.record_writers.get_mut(port) {
            writer.write(op)
        } else {
            Ok(op)
        }
    }
}

#[derive(Debug)]
struct ChannelManager {
    owner: NodeHandle,
    senders: HashMap<PortHandle, Vec<Sender<ExecutorOperation>>>,
    state_writer: Option<StateWriter>,
    error_manager: Arc<ErrorManager>,
}

impl ChannelManager {
    #[inline]
    fn send_op(
        &mut self,
        mut op: ProcessorOperation,
        port_id: PortHandle,
    ) -> Result<(), ExecutionError> {
        if let Some(state_writer) = self.state_writer.as_mut() {
            match state_writer.store_op(op, &port_id) {
                Ok(new_op) => op = new_op,
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

    fn send_terminate(&self) -> Result<(), ExecutionError> {
        for senders in self.senders.values() {
            for sender in senders {
                sender.send(ExecutorOperation::Terminate)?;
            }
        }

        Ok(())
    }

    fn send_snapshotting_done(&self, connection_name: String) -> Result<(), ExecutionError> {
        for senders in self.senders.values() {
            for sender in senders {
                sender.send(ExecutorOperation::SnapshottingDone {
                    connection_name: connection_name.clone(),
                })?;
            }
        }

        Ok(())
    }

    fn send_commit(&mut self, epoch: &Epoch) -> Result<(), ExecutionError> {
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
    fn new(
        owner: NodeHandle,
        senders: HashMap<PortHandle, Vec<Sender<ExecutorOperation>>>,
        state_writer: Option<StateWriter>,
        error_manager: Arc<ErrorManager>,
    ) -> Self {
        Self {
            owner,
            senders,
            state_writer,
            error_manager,
        }
    }
}

#[derive(Debug)]
pub(crate) struct SourceChannelManager {
    source_handle: NodeHandle,
    port_names: HashMap<PortHandle, String>,
    manager: ChannelManager,
    current_op_ids: HashMap<String, TableState>,
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
        port_names: HashMap<PortHandle, String>,
        senders: HashMap<PortHandle, Vec<Sender<ExecutorOperation>>>,
        state_writer: Option<StateWriter>,
        commit_sz: u32,
        max_duration_between_commits: Duration,
        epoch_manager: Arc<EpochManager>,
        error_manager: Arc<ErrorManager>,
    ) -> Self {
        // FIXME: Read current_op_id from persisted state.
        let current_op_ids = port_names
            .values()
            .map(|n| (n.clone(), TableState::NotStarted))
            .collect();

        Self {
            manager: ChannelManager::new(owner.clone(), senders, state_writer, error_manager),
            port_names,
            current_op_ids,
            source_handle: owner,
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
            (self.source_handle.clone(), self.current_op_ids.clone()),
            request_termination,
            self.num_uncommitted_ops > 0,
        );
        if let Some(common_info) = epoch.common_info {
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
                let port_name = self.port_names[&port].clone();
                self.current_op_ids.insert(
                    port_name,
                    if let Some(id) = id {
                        TableState::Restartable(id)
                    } else {
                        TableState::NonRestartable
                    },
                );

                self.manager.send_op(
                    ProcessorOperation::new(&op, self.epoch_manager.record_store())?,
                    port,
                )?;
                self.num_uncommitted_ops += 1;
                self.trigger_commit_if_needed(request_termination)
            }
            IngestionMessage::SnapshottingDone => {
                self.num_uncommitted_ops += 1;
                self.manager
                    .send_snapshotting_done(self.source_handle.id.clone())?;
                self.commit(request_termination)
            }
            IngestionMessage::SnapshottingStarted => {
                // TODO "implement handle for snapshotting started"
                Ok(false)
            }
        }
    }

    pub fn terminate(&mut self) -> Result<(), ExecutionError> {
        self.manager.send_terminate()
    }
}

#[derive(Debug)]
pub(crate) struct ProcessorChannelManager {
    manager: ChannelManager,
}

impl ProcessorChannelManager {
    pub fn new(
        owner: NodeHandle,
        senders: HashMap<PortHandle, Vec<Sender<ExecutorOperation>>>,
        state_writer: Option<StateWriter>,
        error_manager: Arc<ErrorManager>,
    ) -> Self {
        Self {
            manager: ChannelManager::new(owner, senders, state_writer, error_manager),
        }
    }

    pub fn store_and_send_commit(&mut self, epoch: &Epoch) -> Result<(), ExecutionError> {
        self.manager.send_commit(epoch)
    }

    pub fn send_terminate(&self) -> Result<(), ExecutionError> {
        self.manager.send_terminate()
    }

    pub fn send_snapshotting_done(&self, connection_name: String) -> Result<(), ExecutionError> {
        self.manager.send_snapshotting_done(connection_name)
    }
}

impl ProcessorChannelForwarder for ProcessorChannelManager {
    fn send(&mut self, op: ProcessorOperation, port: PortHandle) {
        self.manager
            .send_op(op, port)
            .unwrap_or_else(|e| panic!("Failed to send operation: {e}"))
    }
}
