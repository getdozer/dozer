use std::collections::HashMap;
use std::future::Future;
use crate::record::Record;

pub struct NodeConfig {
    output_ports: Vec<u8>

}

impl NodeConfig {
    pub fn new(input_ports: Vec<u8>, output_ports: Vec<u8>) -> NodeConfig {
        NodeConfig {
            output_ports
        }
    }
}


pub trait Processor {
    fn get_config(&self) -> &NodeConfig;
    fn prepare(&self, ctx: &ExecutionContext);
    fn process(&self, port: u8, data: Record, ctx: &ExecutionContext) -> (u8, Vec<Record>);
}

pub trait Storage {

}

pub struct ExecutionContext {
   // storage: Box<dyn Storage>
}

impl ExecutionContext {
    pub fn new() -> ExecutionContext {
        ExecutionContext {
           // storage
        }
    }
}





