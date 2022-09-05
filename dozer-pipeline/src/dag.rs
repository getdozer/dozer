use std::ops::DerefMut;
use std::sync::Arc;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::sync::Mutex;
use crate::Record;

pub struct InternalEdge {
    pub from_node: u16,
    pub from_port: u8,
    pub to_node: u16,
    pub to_port: u8
}

impl InternalEdge {
    pub fn new(from_node: u16, from_port: u8, to_node: u16, to_port: u8) -> Self {
        Self { from_node, from_port, to_node, to_port }
    }
}

pub struct InputEdge {
    pub input_id: u8,
    pub input: UnboundedReceiver<Record>,
    pub to_node: u16,
    pub to_port: u8
}

impl InputEdge {
    pub fn new(input_id: u8, input: UnboundedReceiver<Record>, to_node: u16, to_port: u8) -> Self {
        Self { input_id, input, to_node, to_port }
    }
}

pub struct OutputEdge {
    pub output_id: u8,
    pub output: UnboundedSender<Record>,
    pub from_node: u16,
    pub from_port: u8
}

impl OutputEdge {
    pub fn new(output_id: u8, output: UnboundedSender<Record>, from_node: u16, from_port: u8) -> Self {
        Self { output_id, output, from_node, from_port }
    }
}


pub enum Edge {
    internal(InternalEdge),
    input(InputEdge),
    output(OutputEdge)
}


pub struct Node {
    pub id: u16,
    pub processor: Box<dyn Processor>
}

impl Node {
    pub fn new(id: u16, processor: Box<dyn Processor>) -> Node {
        Node{id, processor}
    }

}

pub trait Processor : Send {
    fn process(&mut self, data: (u8, Record)) -> Vec<(u8, Record)>;
}



pub struct Where {

}

impl Where {
    pub fn new() -> Where {
        Where {}
    }
}

impl Processor for Where {
    fn process(&mut self, data: (u8, Record)) -> Vec<(u8, Record)> {
        println!("processing");
        vec![(1, Record::new(0, vec![]))]
    }
}





