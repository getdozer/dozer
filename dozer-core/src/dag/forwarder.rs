use crate::dag::dag::PortHandle;
use crate::dag::node::NodeOperation;
use anyhow::anyhow;
use crossbeam::channel::Sender;
use dozer_types::types::{Operation, OperationEvent, Schema};
use std::collections::HashMap;
use std::thread::sleep;
use std::time::Duration;

pub trait SourceChannelForwarder: Send + Sync {
    fn send(&self, seq_no: u64, op: NodeOperation, port: PortHandle) -> anyhow::Result<()>;
    fn update_schema(&self, schema: Schema, port: PortHandle) -> anyhow::Result<()>;
}

pub trait ProcessorChannelForwarder {
    fn send(&self, op: NodeOperation, port: PortHandle) -> anyhow::Result<()>;
}

pub trait ChannelManager {
    fn terminate(&self) -> anyhow::Result<()>;
}

pub struct LocalChannelForwarder {
    senders: HashMap<PortHandle, Vec<Sender<OperationEvent>>>,
    curr_seq_no: u64,
}

impl LocalChannelForwarder {
    pub fn new(senders: HashMap<PortHandle, Vec<Sender<OperationEvent>>>) -> Self {
        Self {
            senders,
            curr_seq_no: 0,
        }
    }

    pub fn update_seq_no(&mut self, seq: u64) {
        self.curr_seq_no = seq;
    }

    fn send_node_op(
        &self,
        seq_no: u64,
        op: NodeOperation,
        port_id: PortHandle,
    ) -> anyhow::Result<()> {
        match op {
            NodeOperation::Insert { new } => self.send_op_event(
                OperationEvent::new(seq_no, Operation::Insert { new }),
                port_id,
            ),
            NodeOperation::Delete { old } => self.send_op_event(
                OperationEvent::new(seq_no, Operation::Delete { old }),
                port_id,
            ),
            NodeOperation::Update { old, new } => self.send_op_event(
                OperationEvent::new(seq_no, Operation::Update { old, new }),
                port_id,
            ),
        }
    }

    fn send_op_event(&self, op: OperationEvent, port_id: PortHandle) -> anyhow::Result<()> {
        let senders = self.senders.get(&port_id);
        if senders.is_none() {
            return Err(anyhow!("Invalid output port".to_string()));
        }

        if senders.unwrap().len() == 1 {
            senders.unwrap()[0].send(op)?;
        } else {
            for sender in senders.unwrap() {
                sender.send(op.clone())?;
            }
        }

        Ok(())
    }

    pub fn terminate(&self) -> anyhow::Result<()> {
        for senders in &self.senders {
            for sender in senders.1 {
                sender.send(OperationEvent::new(0, Operation::Terminate))?;
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
}

impl SourceChannelForwarder for LocalChannelForwarder {
    fn send(&self, seq_no: u64, op: NodeOperation, port: PortHandle) -> anyhow::Result<()> {
        self.send_node_op(seq_no, op, port)
    }

    fn update_schema(&self, schema: Schema, port: PortHandle) -> anyhow::Result<()> {
        self.send_op_event(
            OperationEvent::new(0, Operation::SchemaUpdate { new: schema }),
            port,
        )
    }
}

impl ProcessorChannelForwarder for LocalChannelForwarder {
    fn send(&self, op: NodeOperation, port: PortHandle) -> anyhow::Result<()> {
        self.send_node_op(self.curr_seq_no, op, port)
    }
}

impl ChannelManager for LocalChannelForwarder {
    fn terminate(&self) -> anyhow::Result<()> {
        self.terminate()
    }
}
