use std::collections::HashMap;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use crate::{ExecutionContext, Processor, Record};

pub struct Pipe {
    pub from: u16,
    pub from_port: u8,
    pub to: u16,
    pub to_port: u8
}


pub trait Executor {
    fn register_processor(&mut self, processor: Box<dyn Processor>) -> u16;
    fn register_pipe(&mut self, pipe: Pipe);
    fn register_input(&mut self, rx: UnboundedReceiver<Record>, node: u16, port: u8);
    fn register_output(&mut self, node: u16, port: u8, tx: UnboundedSender<Record>);
    fn prepare(&self, context: &ExecutionContext);
    fn start(&self, context: &ExecutionContext);
    fn stop(&self, context: &ExecutionContext);
}

