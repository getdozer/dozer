use dozer_core::dag::{
    executor_local::DEFAULT_PORT_HANDLE,
    node::{PortHandle, StatefulPortHandle, StatefulProcessor, StatefulProcessorFactory},
};
use sqlparser::ast::TableWithJoins;

use super::processor::ProductProcessor;

pub struct ProductProcessorFactory {
    statement: Vec<TableWithJoins>,
}

impl ProductProcessorFactory {
    /// Creates a new [`ProductProcessorFactory`].
    pub fn new(statement: Vec<TableWithJoins>) -> Self {
        Self { statement }
    }
}

impl StatefulProcessorFactory for ProductProcessorFactory {
    fn get_input_ports(&self) -> Vec<PortHandle> {
        vec![DEFAULT_PORT_HANDLE]
    }

    fn get_output_ports(&self) -> Vec<StatefulPortHandle> {
        vec![StatefulPortHandle::new(DEFAULT_PORT_HANDLE, false)]
    }

    fn build(&self) -> Box<dyn StatefulProcessor> {
        Box::new(ProductProcessor::new(self.statement.clone()))
    }
}
