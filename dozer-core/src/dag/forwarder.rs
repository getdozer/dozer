use crate::dag::channels::{ProcessorChannelForwarder, SourceChannelForwarder};
use crate::dag::errors::ExecutionError;
use crate::dag::errors::ExecutionError::{InternalError, InvalidPortHandle};
use crate::dag::mt_executor::ExecutorOperation;
use crate::dag::node::{NodeHandle, PortHandle};
use crossbeam::channel::Sender;
use dozer_types::internal_err;
use dozer_types::types::{Operation, Schema};
use std::collections::HashMap;
use std::thread::sleep;
use std::time::Duration;

pub struct LocalChannelForwarder {
    senders: HashMap<PortHandle, Vec<Sender<ExecutorOperation>>>,
    curr_seq_no: u64,
    commit_size: u16,
    commit_counter: u16,
    source_handle: NodeHandle,
}

impl LocalChannelForwarder {
    pub fn new(
        source_handle: NodeHandle,
        senders: HashMap<PortHandle, Vec<Sender<ExecutorOperation>>>,
        commit_size: u16,
    ) -> Self {
        Self {
            senders,
            curr_seq_no: 0,
            commit_size,
            commit_counter: 0,
            source_handle,
        }
    }

    pub fn update_seq_no(&mut self, seq: u64) {
        self.curr_seq_no = seq;
    }

    fn send_op(
        &self,
        seq_opt: Option<u64>,
        op: Operation,
        port_id: PortHandle,
    ) -> Result<(), ExecutionError> {
        let senders = self
            .senders
            .get(&port_id)
            .ok_or(InvalidPortHandle(port_id))?;

        let seq = seq_opt.unwrap_or(self.curr_seq_no);
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

    pub fn send_commit(&self) -> Result<(), ExecutionError> {
        for senders in &self.senders {
            for sender in senders.1 {
                internal_err!(sender.send(ExecutorOperation::Commit {
                    source: self.source_handle.clone(),
                    epoch: self.curr_seq_no
                }))?;
            }
        }

        Ok(())
    }

    pub fn send_update_schema(
        &self,
        schema: Schema,
        port_id: PortHandle,
    ) -> Result<(), ExecutionError> {
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
            self.send_commit()?;
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
