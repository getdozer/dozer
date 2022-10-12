use std::collections::HashMap;

use dozer_core::dag::dag::PortHandle;
use dozer_core::dag::forwarder::ProcessorChannelForwarder;
use dozer_core::dag::mt_executor::DefaultPortHandle;
use dozer_core::dag::node::{Processor, ProcessorFactory};
use dozer_core::state::StateStore;
use dozer_types::types::{Field, Operation, Schema};
use log::info;

use crate::pipeline::expression::execution::{Expression, ExpressionExecutor};

pub struct SelectionProcessorFactory {
    input_ports: Vec<PortHandle>,
    output_ports: Vec<PortHandle>,
    expression: Box<Expression>,
}

impl SelectionProcessorFactory {
    pub fn new(
        input_ports: Vec<PortHandle>,
        output_ports: Vec<PortHandle>,
        expression: Box<Expression>,
    ) -> Self {
        Self {
            input_ports,
            output_ports,
            expression,
        }
    }
}

impl ProcessorFactory for SelectionProcessorFactory {
    fn get_input_ports(&self) -> Vec<PortHandle> {
        self.input_ports.clone()
    }

    fn get_output_ports(&self) -> Vec<PortHandle> {
        self.output_ports.clone()
    }

    fn build(&self) -> Box<dyn Processor> {
        Box::new(SelectionProcessor {
            expression: self.expression.clone(),
        })
    }
}

pub struct SelectionProcessor {
    expression: Box<Expression>,
}

impl SelectionProcessor {
    fn delete(&self, record: &dozer_types::types::Record) -> Operation {
        Operation::Delete {
            old: record.clone(),
        }
    }

    fn insert(&self, record: &dozer_types::types::Record) -> Operation {
        Operation::Insert {
            new: record.clone(),
        }
    }
}

impl Processor for SelectionProcessor {
    fn update_schema(
        &self,
        _output_port: PortHandle,
        input_schemas: &HashMap<PortHandle, Schema>,
    ) -> anyhow::Result<Schema> {
        Ok(input_schemas.get(&0).unwrap().clone())
    }

    fn init<'a>(&'_ mut self, _state_store: &mut dyn StateStore) -> anyhow::Result<()> {
        info!("{:?}", "Initialising Selection Processor");
        Ok(())
    }

    fn process(
        &mut self,
        _from_port: PortHandle,
        op: Operation,
        fw: &dyn ProcessorChannelForwarder,
        _state_store: &mut dyn StateStore,
    ) -> anyhow::Result<()> {
        match op {
            Operation::Delete { ref old } => {
                if self.expression.evaluate(old) == Field::Boolean(true) {
                    let _ = fw.send(op, DefaultPortHandle);
                }
            }
            Operation::Insert { ref new } => {
                if self.expression.evaluate(new) == Field::Boolean(true) {
                    let _ = fw.send(op, DefaultPortHandle);
                }
            }
            Operation::Update { ref old, ref new } => {
                let old_fulfilled = self.expression.evaluate(old) == Field::Boolean(true);
                let new_fulfilled = self.expression.evaluate(new) == Field::Boolean(true);
                match (old_fulfilled, new_fulfilled) {
                    (true, true) => {
                        // both records fulfills the WHERE condition, forward the operation
                        let _ = fw.send(op, DefaultPortHandle);
                    }
                    (true, false) => {
                        // the old record fulfills the WHERE condition while then new one doesn't, forward a delete operation
                        let _ = fw.send(self.delete(old), DefaultPortHandle);
                    }
                    (false, true) => {
                        // the old record doesn't fulfill the WHERE condition while then new one does, forward an insert operation
                        let _ = fw.send(self.insert(new), DefaultPortHandle);
                    }
                    (false, false) => {
                        // both records doesn't fulfill the WHERE condition, don't forward the operation
                    }
                }
            }
        }
        Ok(())
    }
}
