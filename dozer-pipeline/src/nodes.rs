use std::collections::HashMap;
use std::future::Future;
use crate::record::Record;

pub struct NodeConfig {
    output_ports: Vec<u8>,
    input_ports: Vec<u8>

}

impl NodeConfig {
    pub fn new(input_ports: Vec<u8>, output_ports: Vec<u8>) -> NodeConfig {
        NodeConfig {
            input_ports, output_ports
        }
    }
}


pub trait Processor {
    fn get_config(&self) -> &NodeConfig;
    fn process(&self, data: HashMap<u8,Vec<Record>>, ctx: &ExecutionContext) -> HashMap<u8,Vec<Record>>;
}

pub struct ExecutionContext {

}

impl ExecutionContext {
    pub fn new() -> ExecutionContext {
        ExecutionContext {}
    }
}





