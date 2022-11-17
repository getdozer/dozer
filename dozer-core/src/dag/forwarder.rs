use crate::dag::channels::{ProcessorChannelForwarder, SourceChannelForwarder};
use crate::dag::errors::ExecutionError;
use crate::dag::errors::ExecutionError::{InternalError, InvalidPortHandle};
use crate::dag::executor_local::ExecutorOperation;
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
use std::time::Duration;

const SOURCE_ID_IDENTIFIER: u8 = 0_u8;
const SCHEMA_IDENTIFIER: u8 = 1_u8;

pub(crate) struct StateWriter {
    meta_db: Database,
    dbs: HashMap<PortHandle, StateOptions>,
    schemas: HashMap<PortHandle, Schema>,
    tx: Arc<RwLock<Box<dyn RenewableRwTransaction>>>,
}

impl StateWriter {
    pub fn new(
        meta_db: Database,
        dbs: HashMap<PortHandle, StateOptions>,
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

    fn write_record(
        &self,
        db: &Database,
        rec: &Record,
        schema: &Schema,
    ) -> Result<(), ExecutionError> {
        let key = rec.get_key(&schema.primary_index)?;
        let value = bincode::serialize(&rec).map_err(|e| SerializationError {
            typ: "Record".to_string(),
            reason: Box::new(e),
        })?;
        self.tx.write().put(db, key.as_slice(), value.as_slice())?;
        Ok(())
    }

    fn retr_record(&self, db: &Database, key: &[u8]) -> Result<Record, ExecutionError> {
        let curr = self
            .tx
            .read()
            .get(db, key)?
            .ok_or_else(ExecutionError::RecordNotFound)?;
        let r: Record = bincode::deserialize(&curr).map_err(|e| SerializationError {
            typ: "Record".to_string(),
            reason: Box::new(e),
        })?;
        Ok(r)
    }

    fn store_op(
        &mut self,
        _seq_no: u64,
        op: Operation,
        port: &PortHandle,
    ) -> Result<Operation, ExecutionError> {
        if let Some(opts) = self.dbs.get(port) {
            let schema = self
                .schemas
                .get(port)
                .ok_or(ExecutionError::InvalidPortHandle(*port))?;

            match op {
                Operation::Insert { new } => {
                    self.write_record(&opts.db, &new, schema)?;
                    Ok(Operation::Insert { new })
                }
                Operation::Delete { mut old } => {
                    let key = old.get_key(&schema.primary_index)?;
                    if opts.options.retrieve_old_record_for_deletes {
                        old = self.retr_record(&opts.db, &key)?;
                    }
                    self.tx.write().del(&opts.db, &key, None)?;
                    Ok(Operation::Delete { old })
                }
                Operation::Update { mut old, new } => {
                    let key = old.get_key(&schema.primary_index)?;
                    if opts.options.retrieve_old_record_for_updates {
                        old = self.retr_record(&opts.db, &key)?;
                    }
                    self.write_record(&opts.db, &new, schema)?;
                    Ok(Operation::Update { old, new })
                }
            }
        } else {
            Ok(op)
        }
    }

    fn store_commit_info(&mut self, source: &NodeHandle, seq: u64) -> Result<(), ExecutionError> {
        let mut full_key = vec![SOURCE_ID_IDENTIFIER];
        full_key.extend(source.as_bytes());

        self.tx
            .write()
            .put(&self.meta_db, full_key.as_slice(), &seq.to_be_bytes())?;
        self.tx.write().commit_and_renew()?;
        Ok(())
    }

    fn store_schema(&mut self, port: PortHandle, schema: Schema) -> Result<(), ExecutionError> {
        self.schemas.insert(port, schema);

        let schemas_value = bincode::serialize(&self.schemas).map_err(|e| SerializationError {
            typ: "HashMap<PortHandle, Schema>".to_string(),
            reason: Box::new(e),
        })?;
        self.tx.write().put(
            &self.meta_db,
            vec![SCHEMA_IDENTIFIER].as_slice(),
            schemas_value.as_slice(),
        )?;
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
    pub(crate) fn new_source_forwarder(
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

    pub(crate) fn new_processor_forwarder(
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

    fn update_output_schema(
        &mut self,
        port: PortHandle,
        schema: Schema,
    ) -> Result<(), ExecutionError> {
        if let Some(w) = &mut self.state_writer {
            w.store_schema(port, schema)?;
        }
        Ok(())
    }

    pub fn update_seq_no(&mut self, seq: u64) {
        self.curr_seq_no = seq;
    }

    fn send_op(
        &mut self,
        seq: u64,
        mut op: Operation,
        port_id: PortHandle,
    ) -> Result<(), ExecutionError> {
        if let Some(rs) = self.state_writer.as_mut() {
            op = rs.store_op(seq, op, &port_id)?;
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
        self.update_output_schema(port_id, schema.clone())?;
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
