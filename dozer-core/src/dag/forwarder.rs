use crate::dag::channels::{ProcessorChannelForwarder, SourceChannelForwarder};
use crate::dag::errors::ExecutionError;
use crate::dag::errors::ExecutionError::{InternalError, InvalidPortHandle};
use crate::dag::executor_local::ExecutorOperation;
use crate::dag::node::{NodeHandle, PortHandle};
use crate::storage::common::{Database, RenewableRwTransaction};
use crate::storage::errors::StorageError::SerializationError;
use crossbeam::channel::Sender;
use dozer_types::internal_err;
use dozer_types::parking_lot::RwLock;
use dozer_types::types::{Operation, Schema};
use log::info;
use std::collections::HashMap;
use std::sync::Arc;
use std::thread::sleep;
use std::time::Duration;

pub struct StateWriter {
    meta_db: Database,
    dbs: HashMap<PortHandle, Database>,
    schemas: HashMap<PortHandle, Schema>,
    tx: Arc<RwLock<Box<dyn RenewableRwTransaction>>>,
}

impl StateWriter {
    pub fn new(
        meta_db: Database,
        dbs: HashMap<PortHandle, Database>,
        schemas: HashMap<PortHandle, Schema>,
        tx: Arc<RwLock<Box<dyn RenewableRwTransaction>>>,
    ) -> Self {
        Self {
            meta_db,
            dbs,
            schemas,
            tx,
        }
    }

    fn store_op(
        &mut self,
        _seq_no: u64,
        op: &Operation,
        port: &PortHandle,
    ) -> Result<(), ExecutionError> {
        if let Some(db) = self.dbs.get(port) {
            let schema = self
                .schemas
                .get(port)
                .ok_or(ExecutionError::InvalidPortHandle(*port))?;

            match op {
                Operation::Insert { new } => {
                    let key = new.get_key(&schema.primary_index)?;
                    let value = bincode::serialize(&new).map_err(|e| SerializationError {
                        typ: "Record".to_string(),
                        reason: Box::new(e),
                    })?;
                    self.tx.write().put(db, key.as_slice(), value.as_slice())?;
                }
                Operation::Delete { old: _ } => {}
                Operation::Update { old: _, new: _ } => {}
            }
        }
        Ok(())
    }

    fn store_commit_info(&mut self, source: &NodeHandle, seq: u64) -> Result<(), ExecutionError> {
        self.tx
            .write()
            .put(&self.meta_db, source.as_bytes(), &seq.to_be_bytes())?;
        self.tx.write().commit_and_renew()?;
        Ok(())
    }
}

pub struct LocalChannelForwarder {
    senders: HashMap<PortHandle, Vec<Sender<ExecutorOperation>>>,
    curr_seq_no: u64,
    commit_size: u32,
    commit_counter: u32,
    owner: NodeHandle,
    state_writer: Option<StateWriter>,
}

impl LocalChannelForwarder {
    pub fn new_source_forwarder(
        owner: NodeHandle,
        senders: HashMap<PortHandle, Vec<Sender<ExecutorOperation>>>,
        commit_size: u32,
        state_writer: Option<StateWriter>,
    ) -> Self {
        Self {
            senders,
            curr_seq_no: 0,
            commit_size,
            commit_counter: 0,
            owner,
            state_writer,
        }
    }

    pub fn new_processor_forwarder(
        owner: NodeHandle,
        senders: HashMap<PortHandle, Vec<Sender<ExecutorOperation>>>,
        rec_store_writer: Option<StateWriter>,
    ) -> Self {
        Self {
            senders,
            curr_seq_no: 0,
            commit_size: 0,
            commit_counter: 0,
            owner,
            state_writer: rec_store_writer,
        }
    }

    fn update_output_schema(&mut self, port: PortHandle, schema: Schema) {
        if let Some(w) = &mut self.state_writer {
            w.schemas.insert(port, schema);
        }
    }

    pub fn update_seq_no(&mut self, seq: u64) {
        self.curr_seq_no = seq;
    }

    fn send_op(
        &mut self,
        seq: u64,
        op: Operation,
        port_id: PortHandle,
    ) -> Result<(), ExecutionError> {
        if let Some(rs) = self.state_writer.as_mut() {
            rs.store_op(seq, &op, &port_id)?;
        }

        let senders = self
            .senders
            .get(&port_id)
            .ok_or(InvalidPortHandle(port_id))?;

        let exec_op = match op {
            Operation::Insert { new } => ExecutorOperation::Insert { seq, new },
            Operation::Update { old, new } => ExecutorOperation::Update { seq, old, new },
            Operation::Delete { old } => ExecutorOperation::Delete { seq, old },
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

    pub fn store_and_send_commit(
        &mut self,
        source_node: NodeHandle,
        seq: u64,
    ) -> Result<(), ExecutionError> {
        if let Some(ref mut s) = self.state_writer {
            info!(
                "[{}] Checkpointing (source: {}, epoch: {})",
                self.owner, source_node, seq
            );
            s.store_commit_info(&source_node, seq)?;
        }

        for senders in &self.senders {
            for sender in senders.1 {
                internal_err!(sender.send(ExecutorOperation::Commit {
                    source: source_node.clone(),
                    epoch: seq
                }))?;
            }
        }

        Ok(())
    }

    pub fn send_update_schema(
        &mut self,
        schema: Schema,
        port_id: PortHandle,
    ) -> Result<(), ExecutionError> {
        self.update_output_schema(port_id, schema.clone());
        let senders = self
            .senders
            .get(&port_id)
            .ok_or(InvalidPortHandle(port_id))?;

        for s in senders {
            internal_err!(s.send(ExecutorOperation::SchemaUpdate {
                new: schema.clone(),
            }))?;
        }

        Ok(())
    }
}

impl SourceChannelForwarder for LocalChannelForwarder {
    fn send(&mut self, seq: u64, op: Operation, port: PortHandle) -> Result<(), ExecutionError> {
        self.curr_seq_no = seq;
        if self.commit_counter >= self.commit_size {
            self.store_and_send_commit(self.owner.clone(), seq)?;
            self.commit_counter = 0;
        }
        self.send_op(seq, op, port)?;
        self.commit_counter += 1;
        Ok(())
    }

    fn update_schema(&mut self, schema: Schema, port: PortHandle) -> Result<(), ExecutionError> {
        self.send_update_schema(schema, port)
    }

    fn terminate(&mut self) -> Result<(), ExecutionError> {
        self.store_and_send_commit(self.owner.clone(), self.curr_seq_no)?;
        self.send_term_and_wait()
    }
}

impl ProcessorChannelForwarder for LocalChannelForwarder {
    fn send(&mut self, op: Operation, port: PortHandle) -> Result<(), ExecutionError> {
        self.send_op(self.curr_seq_no, op, port)
    }
}
