use std::collections::HashMap;
use crate::nodes::{ExecutionContext, NodeConfig, Processor};
use crate::Record;

pub struct FilterNodeConfig {


}

impl FilterNodeConfig {
    pub fn new() -> FilterNodeConfig {
        FilterNodeConfig {}
    }
}

pub struct FilterNode {
    config: NodeConfig,
    filter_config: FilterNodeConfig
}

impl FilterNode {
    pub fn new(config: NodeConfig, filter_config: FilterNodeConfig) -> FilterNode {
        FilterNode { config, filter_config }
    }
}

impl Processor for FilterNode {
    fn get_config(&self) -> &NodeConfig {
        &self.config
    }

    fn process(&self, data: HashMap<u8,Vec<Record>>, ctx: &ExecutionContext) -> HashMap<u8,Vec<Record>> {
        println!("test");
        return HashMap::new();
    }
}
