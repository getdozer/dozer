use crate::dag::channels::{ProcessorChannelForwarder, SourceChannelForwarder};
use crate::dag::errors::ExecutionError;
use crate::dag::errors::ExecutionError::{InternalError, InvalidPortHandle};
use crate::dag::executor_local::ExecutorOperation;
use crate::dag::node::{NodeHandle, PortHandle};
use crate::storage::common::{Database, RenewableRwTransaction, RwTransaction};
use crate::storage::errors::StorageError::SerializationError;
use crossbeam::channel::Sender;
use dozer_types::internal_err;
use dozer_types::parking_lot::RwLock;
use dozer_types::types::{Operation, Schema};
use std::collections::HashMap;
use std::sync::Arc;
use std::thread::sleep;
use std::time::Duration;

pub struct PortRecordStoreWriter {
    dbs: HashMap<PortHandle, Database>,
    schemas: HashMap<PortHandle, Schema>,
    tx: Arc<RwLock<Box<dyn RenewableRwTransaction>>>,
}

impl PortRecordStoreWriter {
    pub fn new(
        dbs: HashMap<PortHandle, Database>,
        schemas: HashMap<PortHandle, Schema>,
        tx: Arc<RwLock<Box<dyn RenewableRwTransaction>>>,
    ) -> Self {
        Self { dbs, schemas, tx }
    }

    fn store_op(
        &mut self,
        seq_no: u64,
        op: &Operation,
        port: &PortHandle,
    ) -> Result<(), ExecutionError> {
        match op {
            Operation::Insert { new } => {
                let schema = self
                    .schemas
                    .get(port)
                    .ok_or(ExecutionError::InvalidPortHandle(port.clone()))?;
                let db = self
                    .dbs
                    .get(port)
                    .ok_or(ExecutionError::InvalidPortHandle(port.clone()))?;
                let key = new.get_key(&schema.primary_index)?;
                let value = bincode::serialize(&new).map_err(|e| SerializationError {
                    typ: "Record".to_string(),
                    reason: Box::new(e),
                })?;
                self.tx.write().put(db, key.as_slice(), value.as_slice())?;
                Ok(())
            }
            Operation::Delete { old } => Ok(()),
            Operation::Update { old, new } => Ok(()),
        }
    }
}

pub struct LocalChannelForwarder {
    senders: HashMap<PortHandle, Vec<Sender<ExecutorOperation>>>,
    curr_seq_no: u64,
    commit_size: u16,
    commit_counter: u16,
    source_handle: NodeHandle,
    rec_store_writer: Option<PortRecordStoreWriter>,
}

impl LocalChannelForwarder {
    pub fn new_source_forwarder(
        source_handle: NodeHandle,
        senders: HashMap<PortHandle, Vec<Sender<ExecutorOperation>>>,
        commit_size: u16,
        rec_store_writer: Option<PortRecordStoreWriter>,
    ) -> Self {
        Self {
            senders,
            curr_seq_no: 0,
            commit_size,
            commit_counter: 0,
            source_handle,
            rec_store_writer,
        }
    }

    pub fn new_processor_forwarder(
        source_handle: NodeHandle,
        senders: HashMap<PortHandle, Vec<Sender<ExecutorOperation>>>,
        rec_store_writer: Option<PortRecordStoreWriter>,
    ) -> Self {
        Self {
            senders,
            curr_seq_no: 0,
            commit_size: 0,
            commit_counter: 0,
            source_handle,
            rec_store_writer,
        }
    }

    fn update_output_schema(&mut self, port: PortHandle, schema: Schema) {
        if let Some(w) = &mut self.rec_store_writer {
            w.schemas.insert(port, schema);
        }
    }

    pub fn update_seq_no(&mut self, seq: u64) {
        self.curr_seq_no = seq;
    }

    fn send_op(
        &mut self,
        seq_opt: Option<u64>,
        op: Operation,
        port_id: PortHandle,
    ) -> Result<(), ExecutionError> {
        let seq = seq_opt.unwrap_or(self.curr_seq_no);
        if let Some(rs) = self.rec_store_writer.as_mut() {
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

    pub fn send_term(&self) -> Result<(), ExecutionError> {
        for senders in &self.senders {
            for sender in senders.1 {
                internal_err!(sender.send(ExecutorOperation::Terminate))?;
            }

            loop {
                let mut is_empty = true;
                for senders in &self.senders {
                    for sender in senders.1 {
                        is_empty |= sender.is_empty();
                    }
                }

                if !is_empty {
                    sleep(Duration::from_millis(250));
                } else {
                    break;
                }
            }
        }

        Ok(())
    }

    pub fn send_commit(&self, seq: u64) -> Result<(), ExecutionError> {
        for senders in &self.senders {
            for sender in senders.1 {
                internal_err!(sender.send(ExecutorOperation::Commit {
                    source: self.source_handle.clone(),
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
        if self.commit_counter >= self.commit_size {
            self.send_commit(seq)?;
            self.commit_counter = 0;
        }
        self.send_op(Some(seq), op, port)?;
        self.commit_counter += 1;
        Ok(())
    }

    fn update_schema(&mut self, schema: Schema, port: PortHandle) -> Result<(), ExecutionError> {
        self.send_update_schema(schema, port)
    }

    fn terminate(&mut self) -> Result<(), ExecutionError> {
        self.send_term()
    }
}

impl ProcessorChannelForwarder for LocalChannelForwarder {
    fn send(&mut self, op: Operation, port: PortHandle) -> Result<(), ExecutionError> {
        self.send_op(None, op, port)
    }
}
