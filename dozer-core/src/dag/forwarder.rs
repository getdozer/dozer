use crate::dag::channels::ProcessorChannelForwarder;
use crate::dag::dag_metadata::SOURCE_ID_IDENTIFIER;
use crate::dag::epoch::{Epoch, EpochManager};
use crate::dag::errors::ExecutionError;
use crate::dag::errors::ExecutionError::{InternalError, InvalidPortHandle};
use crate::dag::executor::ExecutorOperation;
use crate::dag::executor_utils::StateOptions;
use crate::dag::node::{NodeHandle, PortHandle};
use crate::storage::common::Database;

use crate::dag::record_store::common::{RecordWriter, RecordWriterUtils};
use crate::storage::lmdb_storage::SharedTransaction;
use crossbeam::channel::Sender;
use dozer_types::internal_err;
use dozer_types::types::{Operation, Schema};
use log::debug;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Debug)]
pub(crate) struct StateWriter {
    meta_db: Database,
    record_writers: HashMap<PortHandle, Box<dyn RecordWriter>>,
    tx: SharedTransaction,
}

impl StateWriter {
    pub fn new(
        meta_db: Database,
        dbs: HashMap<PortHandle, StateOptions>,
        tx: SharedTransaction,
        output_schemas: HashMap<PortHandle, Schema>,
    ) -> Result<Self, ExecutionError> {
        let mut record_writers = HashMap::<PortHandle, Box<dyn RecordWriter>>::new();
        for (port, options) in dbs {
            let schema = output_schemas
                .get(&port)
                .ok_or(ExecutionError::InvalidPortHandle(port))?
                .clone();

            let writer =
                RecordWriterUtils::create_writer(options.typ, options.db, options.meta_db, schema)?;
            record_writers.insert(port, writer);
        }

        Ok(Self {
            meta_db,
            record_writers,
            tx,
        })
    }

    fn store_op(&mut self, op: Operation, port: &PortHandle) -> Result<Operation, ExecutionError> {
        if let Some(writer) = self.record_writers.get_mut(port) {
            writer.write(op, &self.tx)
        } else {
            Ok(op)
        }
    }

    pub fn store_commit_info(&mut self, epoch_details: &Epoch) -> Result<(), ExecutionError> {
        //
        for (source, (txid, seq_in_tx)) in &epoch_details.details {
            let mut full_key = vec![SOURCE_ID_IDENTIFIER];
            full_key.extend(source.to_bytes());

            let mut value: Vec<u8> = Vec::with_capacity(16);
            value.extend(txid.to_be_bytes());
            value.extend(seq_in_tx.to_be_bytes());

            self.tx
                .write()
                .put(self.meta_db, full_key.as_slice(), value.as_slice())?;
        }
        self.tx.write().commit_and_renew()?;
        Ok(())
    }
}

#[derive(Debug)]
struct ChannelManager {
    owner: NodeHandle,
    senders: HashMap<PortHandle, Vec<Sender<ExecutorOperation>>>,
    state_writer: StateWriter,
    stateful: bool,
}

impl ChannelManager {
    #[inline]
    fn send_op(&mut self, mut op: Operation, port_id: PortHandle) -> Result<(), ExecutionError> {
        if self.stateful {
            op = self.state_writer.store_op(op, &port_id)?;
        }

        let senders = self
            .senders
            .get(&port_id)
            .ok_or(InvalidPortHandle(port_id))?;

        let exec_op = match op {
            Operation::Insert { new } => ExecutorOperation::Insert { new },
            Operation::Update { old, new } => ExecutorOperation::Update { old, new },
            Operation::Delete { old } => ExecutorOperation::Delete { old },
        };

        if let Some((last_sender, senders)) = senders.split_last() {
            for sender in senders {
                internal_err!(sender.send(exec_op.clone()))?;
            }
            internal_err!(last_sender.send(exec_op))?;
        }

        Ok(())
    }

    fn send_terminate(&self) -> Result<(), ExecutionError> {
        for senders in self.senders.values() {
            for sender in senders {
                internal_err!(sender.send(ExecutorOperation::Terminate))?;
            }
        }

        Ok(())
    }

    fn store_and_send_commit(&mut self, epoch: &Epoch) -> Result<(), ExecutionError> {
        debug!("[{}] Checkpointing - {}", self.owner, &epoch);
        self.state_writer.store_commit_info(epoch)?;

        for senders in &self.senders {
            for sender in senders.1 {
                internal_err!(sender.send(ExecutorOperation::Commit {
                    epoch: epoch.clone()
                }))?;
            }
        }

        Ok(())
    }
    fn new(
        owner: NodeHandle,
        senders: HashMap<PortHandle, Vec<Sender<ExecutorOperation>>>,
        state_writer: StateWriter,
        stateful: bool,
    ) -> Self {
        Self {
            owner,
            senders,
            state_writer,
            stateful,
        }
    }
}

#[derive(Debug)]
pub(crate) struct SourceChannelManager {
    source_handle: NodeHandle,
    manager: ChannelManager,
    curr_txid: u64,
    curr_seq_in_tx: u64,
    commit_sz: u32,
    num_uncommited_ops: u32,
    max_duration_between_commits: Duration,
    last_commit_instant: Instant,
    epoch_manager: Arc<EpochManager>,
}

impl SourceChannelManager {
    pub fn new(
        owner: NodeHandle,
        senders: HashMap<PortHandle, Vec<Sender<ExecutorOperation>>>,
        state_writer: StateWriter,
        stateful: bool,
        commit_sz: u32,
        max_duration_between_commits: Duration,
        epoch_manager: Arc<EpochManager>,
    ) -> Self {
        Self {
            manager: ChannelManager::new(owner.clone(), senders, state_writer, stateful),
            curr_txid: 0,
            curr_seq_in_tx: 0,
            source_handle: owner,
            commit_sz,
            num_uncommited_ops: 0,
            max_duration_between_commits,
            last_commit_instant: Instant::now(),
            epoch_manager,
        }
    }

    fn should_commit(&self) -> bool {
        self.num_uncommited_ops >= self.commit_sz
            || self.last_commit_instant.elapsed() >= self.max_duration_between_commits
    }

    pub fn trigger_commit_if_needed(
        &mut self,
        request_termination: bool,
    ) -> Result<bool, ExecutionError> {
        if request_termination || self.should_commit() {
            let epoch = self.epoch_manager.wait_for_epoch_close(
                self.source_handle.clone(),
                (self.curr_txid, self.curr_seq_in_tx),
                request_termination,
            );
            self.manager
                .store_and_send_commit(&Epoch::new(epoch.id, epoch.details))?;
            self.num_uncommited_ops = 0;
            self.last_commit_instant = Instant::now();
            Ok(epoch.terminating)
        } else {
            Ok(false)
        }
    }

    pub fn send_and_trigger_commit_if_needed(
        &mut self,
        txid: u64,
        seq_in_tx: u64,
        op: Operation,
        port: PortHandle,
        request_termination: bool,
    ) -> Result<bool, ExecutionError> {
        //
        self.curr_txid = txid;
        self.curr_seq_in_tx = seq_in_tx;
        self.manager.send_op(op, port)?;
        self.num_uncommited_ops += 1;
        self.trigger_commit_if_needed(request_termination)
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
        state_writer: StateWriter,
        stateful: bool,
    ) -> Self {
        Self {
            manager: ChannelManager::new(owner, senders, state_writer, stateful),
        }
    }

    pub fn store_and_send_commit(&mut self, epoch: &Epoch) -> Result<(), ExecutionError> {
        self.manager.store_and_send_commit(epoch)
    }

    pub fn send_terminate(&self) -> Result<(), ExecutionError> {
        self.manager.send_terminate()
    }
}

impl ProcessorChannelForwarder for ProcessorChannelManager {
    fn send(&mut self, op: Operation, port: PortHandle) -> Result<(), ExecutionError> {
        self.manager.send_op(op, port)
    }
}
