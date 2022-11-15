use crate::dag::errors::ExecutionError;
use crate::dag::errors::ExecutionError::{InternalError, InvalidPortHandle};
use crate::dag::executor_local::ExecutorOperation;
use crate::dag::node::{NodeHandle, PortHandle};
use crate::storage::common::{Database, RenewableRwTransaction};
use crate::storage::errors::StorageError::SerializationError;
use crossbeam::channel::Sender;
use dozer_types::internal_err;
use dozer_types::parking_lot::RwLock;
use dozer_types::types::{Operation, Record, Schema};
use libc::thread_info;
use log::info;
use std::collections::HashMap;
use std::slice::SliceIndex;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

pub struct PortOperationStore {
    db: Database,
    schema: Schema,
    tx: Arc<RwLock<Box<dyn RenewableRwTransaction>>>,
}

impl PortOperationStore {
    pub fn new(
        db: Database,
        schema: Schema,
        tx: Arc<RwLock<Box<dyn RenewableRwTransaction>>>,
    ) -> Self {
        Self { db, schema, tx }
    }

    fn store_operation(&mut self, seq_no: u64, op: &Operation) -> Result<(), ExecutionError> {
        match op {
            Operation::Insert { new } => {
                let key = new.get_key(&self.schema.primary_index)?;
                let value = bincode::serialize(&new).map_err(|e| SerializationError {
                    typ: "Record".to_string(),
                    reason: Box::new(e),
                })?;
                self.tx
                    .write()
                    .put(&self.db, key.as_slice(), value.as_slice())?;
            }
            Operation::Delete { old } => {}
            Operation::Update { old, new } => {}
        }

        Ok(())
    }

    #[inline]
    fn update_schema(&mut self, new: Schema) {
        self.schema = new;
    }
}

pub struct PortOperationManager {
    handle: PortHandle,
    senders: Vec<Sender<ExecutorOperation>>,
    store: Option<PortOperationStore>,
}

impl PortOperationManager {
    pub fn new(
        handle: PortHandle,
        senders: Vec<Sender<ExecutorOperation>>,
        store: Option<PortOperationStore>,
    ) -> Self {
        Self {
            handle,
            senders,
            store,
        }
    }

    #[inline]
    fn send(&self, op: ExecutorOperation) -> Result<(), ExecutionError> {
        match self.senders.len() {
            0 => Ok(()),
            1 => self.senders[0].send(op),
            _ => {
                for sender in &self.senders {
                    internal_err!(sender.send(op.clone()))?;
                }
                Ok(())
            }
        }
        .map_err(|e| ExecutionError::InternalError(Box::new(e)))
    }

    #[inline]
    fn store_and_send(&mut self, seq: u64, op: Operation) -> Result<(), ExecutionError> {
        if let Some(rs) = self.store.as_mut() {
            rs.store_operation(seq, &op)?;
        }

        let exec_op = match op {
            Operation::Insert { new } => ExecutorOperation::Insert { seq, new },
            Operation::Update { old, new } => ExecutorOperation::Update { seq, old, new },
            Operation::Delete { old } => ExecutorOperation::Delete { seq, old },
        };

        self.send(exec_op)
    }

    fn pending(&self) -> usize {
        self.senders.iter().map(|e| e.len()).sum()
    }

    #[inline]
    fn update_schema(&mut self, new: Schema) {
        if let Some(mut s) = self.store.as_mut() {
            s.update_schema(new);
        }
    }
}

pub struct OperationManager {
    handle: NodeHandle,
    ports: HashMap<PortHandle, PortOperationManager>,
    meta_db: Database,
    tx: Arc<RwLock<Box<dyn RenewableRwTransaction>>>,
}

impl OperationManager {
    pub fn new(
        handle: NodeHandle,
        senders: HashMap<PortHandle, Vec<Sender<ExecutorOperation>>>,
        mut schemas: HashMap<PortHandle, Schema>,
        mut databases: HashMap<PortHandle, Database>,
        meta_db: Database,
        tx: Arc<RwLock<Box<dyn RenewableRwTransaction>>>,
    ) -> Result<OperationManager, ExecutionError> {
        let mut ports = HashMap::<PortHandle, PortOperationManager>::new();

        for sender in senders {
            let schema = schemas
                .remove(&sender.0)
                .ok_or(InvalidPortHandle(sender.0))?;
            let store = databases
                .remove(&sender.0)
                .map(|db| PortOperationStore::new(db, schema.clone(), tx.clone()));
            ports.insert(
                sender.0,
                PortOperationManager::new(sender.0.clone(), sender.1, store),
            );
        }

        Ok(Self {
            handle,
            ports,
            meta_db,
            tx,
        })
    }

    fn store_commit_metadata(
        &mut self,
        seq: u64,
        src_node_handle: NodeHandle,
    ) -> Result<(), ExecutionError> {
        self.tx.write().put(
            &self.meta_db,
            src_node_handle.as_bytes(),
            &seq.to_be_bytes(),
        )?;
        self.tx.write().commit_and_renew()?;
        Ok(())
    }

    #[inline]
    fn get_port(&self, port_id: &PortHandle) -> Result<&PortOperationManager, ExecutionError> {
        self.ports.get(port_id).ok_or(InvalidPortHandle(*port_id))
    }

    #[inline]
    fn send_to_all_ports(&self, op: ExecutorOperation) -> Result<(), ExecutionError> {
        for p in &self.ports {
            self.get_port(p.0)?.send(op.clone())?
        }
        Ok(())
    }

    pub fn send_term_and_wait(&self) -> Result<(), ExecutionError> {
        info!(
            "[{}] Sending TERMINATE message to {} output ports...",
            self.handle,
            self.ports.len()
        );
        self.send_to_all_ports(ExecutorOperation::Terminate)?;

        loop {
            let pending: usize = self.ports.iter().map(|p| p.1.pending()).sum();

            if pending > 0 {
                info!(
                    "[{}] Waiting for {} messages to be flushed...",
                    self.handle, pending
                );
                thread::sleep(Duration::from_millis(1000));
            } else {
                info!(
                    "[{}] All messages flushed. Exiting message loop.",
                    self.handle
                );
                break;
            }
        }
        Ok(())
    }

    pub fn store_and_send_commit(
        &mut self,
        source_handle: NodeHandle,
        seq: u64,
    ) -> Result<(), ExecutionError> {
        info!("[{}] Committing epoch {}", self.handle, seq);
        self.store_commit_metadata(seq, source_handle.clone())?;
        self.send_to_all_ports(ExecutorOperation::Commit {
            source: source_handle.clone(),
            epoch: seq,
        })
    }

    pub fn send_update_schema(
        &mut self,
        schema: Schema,
        port_id: PortHandle,
    ) -> Result<(), ExecutionError> {
        info!("[{}] Updating and forwarding new schema", self.handle);

        let mut p = self
            .ports
            .get_mut(&port_id)
            .ok_or(InvalidPortHandle(port_id))?;

        p.update_schema(schema.clone());
        p.send(ExecutorOperation::SchemaUpdate { new: schema })?;

        Ok(())
    }
}
