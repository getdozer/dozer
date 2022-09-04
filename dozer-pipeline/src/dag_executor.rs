use std::collections::HashMap;
use std::ops::Deref;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use crate::executor::{Executor, Pipe};
use crate::{ExecutionContext, Processor, Record};


struct DagExecutor {
    processors: HashMap<u16, Box<dyn Processor>>,
    pipes: HashMap<u32, u16>,
    counter: u16,
    inputs: Vec<InputPipe>,
    outputs: HashMap<u32, UnboundedSender<Record>>,
    internal_senders: HashMap<u32, UnboundedSender<Record>>,
    internal_receivers: Vec<InputPipe>
}

struct InputPipe {
    receiver: UnboundedReceiver<Record>,
    to_node: u16,
    to_port: u8
}

impl DagExecutor {
    pub fn new() -> DagExecutor {
        DagExecutor {
            processors: HashMap::new(),
            pipes: HashMap::new(),
            counter: 0,
            inputs: Vec::new(),
            outputs: HashMap::new(),
            internal_senders: HashMap::new(),
            internal_receivers: Vec::new()
        }
    }
}


impl Executor for DagExecutor {

    fn register_processor(&mut self, processor: Box<dyn Processor>) -> u16 {
        self.counter +=1;
        self.processors.insert(self.counter, processor);
        self.counter
    }

    fn register_pipe(&mut self, pipe: Pipe) {
        let key : u32 = u32::from(pipe.from) << 16 | u32::from(pipe.from_port);
        self.pipes.insert(key, pipe.to);
    }

    fn register_input(&mut self, rx: UnboundedReceiver<Record>, node: u16, port: u8) {
        self.inputs.push(InputPipe{receiver: rx, to_node: node, to_port: port})
    }

    fn register_output(&mut self, node: u16, port: u8, tx: UnboundedSender<Record>) {
        let key : u32 = u32::from(node) << 16 | u32::from(port);
        self.outputs.insert(key, tx);
    }

    fn prepare(&self, context: &ExecutionContext) {
        for p in self.processors.values() {
            p.prepare(context);
        }
    }

    fn start(&self, context: &ExecutionContext) {
        todo!()
    }

    fn stop(&self, context: &ExecutionContext) {
        todo!()
    }
}