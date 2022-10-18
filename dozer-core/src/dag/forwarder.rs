use crate::dag::dag::PortHandle;
use crate::dag::mt_executor::ExecutorOperation;
use anyhow::{anyhow, Context};
use crossbeam::channel::Sender;
use dozer_types::types::{Operation, Schema};
use std::collections::HashMap;
use std::thread::sleep;
use std::time::Duration;

pub trait SourceChannelForwarder: Send + Sync {
    fn send(&self, seq: u64, op: Operation, port: PortHandle) -> anyhow::Result<()>;
    fn update_schema(&self, schema: Schema, port: PortHandle) -> anyhow::Result<()>;
}

pub trait ProcessorChannelForwarder {
    fn send(&self, op: Operation, port: PortHandle) -> anyhow::Result<()>;
}

pub trait ChannelManager {
    fn terminate(&self) -> anyhow::Result<()>;
}

pub struct LocalChannelForwarder {
    senders: HashMap<PortHandle, Vec<Sender<ExecutorOperation>>>,
    curr_seq_no: u64,
}

impl LocalChannelForwarder {
    pub fn new(senders: HashMap<PortHandle, Vec<Sender<ExecutorOperation>>>) -> Self {
        Self {
            senders,
            curr_seq_no: 0,
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
    ) -> anyhow::Result<()> {
        let senders = self
            .senders
            .get(&port_id)
            .context(anyhow!("Unable to find port id {}", port_id))?;

        let seq = seq_opt.unwrap_or(self.curr_seq_no);
        let exec_op = match op {
            Operation::Insert { new } => ExecutorOperation::Insert { seq, new },
            Operation::Update { old, new } => ExecutorOperation::Update { seq, old, new },
            Operation::Delete { old } => ExecutorOperation::Delete { seq, old },
            Operation::Lookup { curr } => ExecutorOperation::Lookup { seq, curr },
        };

        if senders.len() == 1 {
            senders[0].send(exec_op)?;
        } else {
            for sender in senders {
                sender.send(exec_op.clone())?;
            }
        }

        Ok(())
    }

    pub fn send_term(&self) -> anyhow::Result<()> {
        for senders in &self.senders {
            for sender in senders.1 {
                sender.send(ExecutorOperation::Terminate)?;
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

    pub fn send_update_schema(&self, schema: Schema, port_id: PortHandle) -> anyhow::Result<()> {
        let senders = self
            .senders
            .get(&port_id)
            .context(anyhow!("Unable to find port id {}", port_id))?;

        for s in senders {
            s.send(ExecutorOperation::SchemaUpdate {
                new: schema.clone(),
            })?;
        }

        Ok(())
    }
}

impl SourceChannelForwarder for LocalChannelForwarder {
    fn send(&self, seq: u64, op: Operation, port: PortHandle) -> anyhow::Result<()> {
        self.send_op(Some(seq), op, port)
    }

    fn update_schema(&self, schema: Schema, port: PortHandle) -> anyhow::Result<()> {
        self.send_update_schema(schema, port)
    }
}

impl ProcessorChannelForwarder for LocalChannelForwarder {
    fn send(&self, op: Operation, port: PortHandle) -> anyhow::Result<()> {
        self.send_op(None, op, port)
    }
}

impl ChannelManager for LocalChannelForwarder {
    fn terminate(&self) -> anyhow::Result<()> {
        self.send_term()
    }
}
