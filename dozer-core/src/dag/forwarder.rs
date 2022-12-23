use crate::dag::channels::{ProcessorChannelForwarder, SourceChannelForwarder};

use crate::dag::dag_metadata::SOURCE_ID_IDENTIFIER;
use crate::dag::epoch_manager::{Epoch, EpochManager};
use crate::dag::errors::ExecutionError;
use crate::dag::errors::ExecutionError::{InternalError, InvalidPortHandle};
use crate::dag::executor::ExecutorOperation;
use crate::dag::executor_utils::StateOptions;
use crate::dag::node::{NodeHandle, PortHandle};
use crate::storage::common::{Database, RenewableRwTransaction};
use crate::storage::errors::StorageError::SerializationError;
use crossbeam::channel::Sender;
use dozer_types::internal_err;
use dozer_types::parking_lot::RwLock;
use dozer_types::types::{Operation, Record, Schema};
use log::info;
use std::collections::HashMap;
use std::sync::Arc;
use std::thread::sleep;
use std::time::{Duration, Instant};

pub(crate) struct StateWriter {
    meta_db: Database,
    dbs: HashMap<PortHandle, StateOptions>,
    output_schemas: HashMap<PortHandle, Schema>,
    input_schemas: HashMap<PortHandle, Schema>,
    input_ports: Option<Vec<PortHandle>>,
    tx: Arc<RwLock<Box<dyn RenewableRwTransaction>>>,
}

impl StateWriter {
    pub fn new(
        meta_db: Database,
        dbs: HashMap<PortHandle, StateOptions>,
        tx: Arc<RwLock<Box<dyn RenewableRwTransaction>>>,
        input_ports: Option<Vec<PortHandle>>,
        output_schemas: HashMap<PortHandle, Schema>,
        input_schemas: HashMap<PortHandle, Schema>,
    ) -> Self {
        Self {
            meta_db,
            dbs,
            output_schemas,
            input_schemas,
            tx,
            input_ports,
        }
    }

    fn write_record(
        db: &Database,
        rec: &Record,
        schema: &Schema,
        tx: &mut Arc<RwLock<Box<dyn RenewableRwTransaction>>>,
    ) -> Result<(), ExecutionError> {
        let key = rec.get_key(&schema.primary_index);
        let value = bincode::serialize(&rec).map_err(|e| SerializationError {
            typ: "Record".to_string(),
            reason: Box::new(e),
        })?;
        tx.write().put(db, key.as_slice(), value.as_slice())?;
        Ok(())
    }

    fn retr_record(
        db: &Database,
        key: &[u8],
        tx: &Arc<RwLock<Box<dyn RenewableRwTransaction>>>,
    ) -> Result<Record, ExecutionError> {
        let curr = tx
            .read()
            .get(db, key)?
            .ok_or_else(ExecutionError::RecordNotFound)?;

        let r: Record = bincode::deserialize(&curr).map_err(|e| SerializationError {
            typ: "Record".to_string(),
            reason: Box::new(e),
        })?;
        Ok(r)
    }

    fn store_op(&mut self, op: Operation, port: &PortHandle) -> Result<Operation, ExecutionError> {
        if let Some(opts) = self.dbs.get(port) {
            let schema = self
                .output_schemas
                .get(port)
                .ok_or(ExecutionError::InvalidPortHandle(*port))?;

            match op {
                Operation::Insert { new } => {
                    StateWriter::write_record(&opts.db, &new, schema, &mut self.tx)?;
                    Ok(Operation::Insert { new })
                }
                Operation::Delete { mut old } => {
                    let key = old.get_key(&schema.primary_index);
                    if opts.options.retrieve_old_record_for_deletes {
                        old = StateWriter::retr_record(&opts.db, &key, &self.tx)?;
                    }
                    self.tx.write().del(&opts.db, &key, None)?;
                    Ok(Operation::Delete { old })
                }
                Operation::Update { mut old, new } => {
                    let key = old.get_key(&schema.primary_index);
                    if opts.options.retrieve_old_record_for_updates {
                        old = StateWriter::retr_record(&opts.db, &key, &self.tx)?;
                    }
                    StateWriter::write_record(&opts.db, &new, schema, &mut self.tx)?;
                    Ok(Operation::Update { old, new })
                }
            }
        } else {
            Ok(op)
        }
    }

    pub fn store_commit_info(
        &mut self,
        states: &HashMap<NodeHandle, (u64, u64)>,
    ) -> Result<(), ExecutionError> {
        //

        for (source, seq) in states.iter() {
            let mut full_key = vec![SOURCE_ID_IDENTIFIER];
            full_key.extend(source.to_bytes());

            let mut value: Vec<u8> = Vec::with_capacity(16);
            value.extend(seq.0.to_be_bytes());
            value.extend(seq.1.to_be_bytes());

            self.tx
                .write()
                .put(&self.meta_db, full_key.as_slice(), value.as_slice())?;
        }

        self.tx.write().commit_and_renew()?;
        Ok(())
    }

    pub(crate) fn get_all_input_schemas(&self) -> Option<HashMap<PortHandle, Schema>> {
        match &self.input_ports {
            Some(input_ports) => {
                let count = input_ports
                    .iter()
                    .filter(|e| !self.input_schemas.contains_key(*e))
                    .count();
                if count == 0 {
                    Some(self.input_schemas.clone())
                } else {
                    None
                }
            }
            _ => None,
        }
    }
}

pub struct LocalChannelForwarder {
    senders: HashMap<PortHandle, Vec<Sender<ExecutorOperation>>>,
    curr_txid: u64,
    curr_seq_in_tx: u64,
    owner: NodeHandle,
    state_writer: StateWriter,
    stateful: bool,
    epoch_manager: Arc<RwLock<EpochManager>>,
    curr_epoch: u64,
}

impl LocalChannelForwarder {
    pub(crate) fn new_source_forwarder(
        owner: NodeHandle,
        senders: HashMap<PortHandle, Vec<Sender<ExecutorOperation>>>,
        state_writer: StateWriter,
        stateful: bool,
        epoch_manager: Arc<RwLock<EpochManager>>,
    ) -> Self {
        Self {
            senders,
            curr_txid: 0,
            curr_seq_in_tx: 0,
            owner,
            state_writer,
            stateful,
            epoch_manager,
            curr_epoch: 0,
        }
    }

    pub(crate) fn new_processor_forwarder(
        owner: NodeHandle,
        senders: HashMap<PortHandle, Vec<Sender<ExecutorOperation>>>,
        rec_store_writer: StateWriter,
        stateful: bool,
        epoch_manager: Arc<RwLock<EpochManager>>,
    ) -> Self {
        Self {
            senders,
            curr_txid: 0,
            curr_seq_in_tx: 0,
            owner,
            state_writer: rec_store_writer,
            stateful,
            epoch_manager,
            curr_epoch: 0,
        }
    }

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

        if senders.len() == 1 {
            internal_err!(senders[0].send(exec_op))?;
        } else {
            for sender in senders {
                internal_err!(sender.send(exec_op.clone()))?;
            }
        }

        Ok(())
    }

    pub fn send_term_and_wait(&self) -> Result<(), ExecutionError> {
        for senders in &self.senders {
            for sender in senders.1 {
                internal_err!(sender.send(ExecutorOperation::Terminate))?;
            }

            loop {
                let mut count = 0_usize;
                for senders in &self.senders {
                    for sender in senders.1 {
                        count += sender.len();
                    }
                }

                if count > 0 {
                    info!(
                        "[{}] Terminating: waiting for {} messages to be flushed",
                        self.owner, count
                    );
                    sleep(Duration::from_millis(500));
                } else {
                    info!(
                        "[{}] Terminating: all messages flushed. Exiting message loop.",
                        self.owner
                    );
                    break;
                }
            }
        }

        Ok(())
    }

    pub fn store_and_send_commit(&mut self, epoch: &Epoch) -> Result<(), ExecutionError> {
        info!("[{}] Checkpointing ({})", self.owner, epoch);
        self.state_writer.store_commit_info(&epoch.details)?;

        for senders in &self.senders {
            for sender in senders.1 {
                internal_err!(sender.send(ExecutorOperation::Commit {
                    epoch: epoch.clone()
                }))?;
            }
        }

        Ok(())
    }

    pub fn commit_and_terminate(&mut self) -> Result<(), ExecutionError> {
        let closed_epoch = self.epoch_manager.write().close_and_poll_epoch(&self.owner);

        if let Some(closed_epoch) = closed_epoch {
            self.store_and_send_commit(&closed_epoch)?;
        }

        self.send_term_and_wait()
    }

    pub fn trigger_commit_if_needed(&mut self) -> Result<(), ExecutionError> {
        let closed_epoch = self.epoch_manager.write().poll_epoch(&self.owner);
        if let Some(closed_epoch) = closed_epoch {
            self.store_and_send_commit(&closed_epoch)?;
        }
        Ok(())
    }
}

impl SourceChannelForwarder for LocalChannelForwarder {
    fn send(
        &mut self,
        txid: u64,
        seq_in_tx: u64,
        op: Operation,
        port: PortHandle,
    ) -> Result<(), ExecutionError> {
        //
        self.send_op(op, port)?;
        let closed_epoch =
            self.epoch_manager
                .write()
                .add_op_to_epoch(&self.owner, txid, seq_in_tx)?;

        if let Some(closed_epoch) = closed_epoch {
            self.store_and_send_commit(&closed_epoch)?;
        }
        Ok(())
    }
}

impl ProcessorChannelForwarder for LocalChannelForwarder {
    fn send(&mut self, op: Operation, port: PortHandle) -> Result<(), ExecutionError> {
        self.send_op(op, port)
    }
}
