use log::info;
use rand::Rng;
use std::collections::HashMap;

use dozer_core::dag::dag::PortHandle;
use dozer_core::dag::forwarder::ProcessorChannelForwarder;
use dozer_core::dag::mt_executor::DefaultPortHandle;
use dozer_core::dag::node::NextStep;
use dozer_core::dag::node::{Processor, ProcessorFactory};
use dozer_core::state::StateStore;
use dozer_types::types::{FieldDefinition, Operation, Record, Schema, SchemaIdentifier};

use crate::pipeline::expression::execution::{Expression, ExpressionExecutor};

pub struct ProjectionProcessorFactory {
    input_ports: Vec<PortHandle>,
    output_ports: Vec<PortHandle>,
    expressions: Vec<Expression>,
    names: Vec<String>,
}

impl ProjectionProcessorFactory {
    pub fn new(
        input_ports: Vec<PortHandle>,
        output_ports: Vec<PortHandle>,
        expressions: Vec<Expression>,
        names: Vec<String>,
    ) -> Self {
        Self {
            input_ports,
            output_ports,
            expressions,
            names,
        }
    }
}

impl ProcessorFactory for ProjectionProcessorFactory {
    fn get_input_ports(&self) -> Vec<PortHandle> {
        self.input_ports.clone()
    }

    fn get_output_ports(&self) -> Vec<PortHandle> {
        self.output_ports.clone()
    }

    fn build(&self) -> Box<dyn Processor> {
        Box::new(ProjectionProcessor {
            expressions: self.expressions.clone(),
            names: self.names.clone(),
        })
    }
}

pub struct ProjectionProcessor {
    expressions: Vec<Expression>,
    names: Vec<String>,
}

impl ProjectionProcessor {
    fn delete(&mut self, record: &Record) -> Operation {
        let mut results = vec![];
        for expr in &self.expressions {
            results.push(expr.evaluate(record));
        }
        Operation::Delete {
            old: Record::new(None, results),
        }
    }

    fn insert(&mut self, record: &Record) -> Operation {
        let mut results = vec![];
        for expr in &self.expressions {
            results.push(expr.evaluate(record));
        }
        Operation::Insert {
            new: Record::new(None, results),
        }
    }

    fn update(&self, old: &Record, new: &Record) -> Operation {
        let mut old_results = vec![];
        let mut new_results = vec![];
        for expr in &self.expressions {
            old_results.push(expr.evaluate(old));
            new_results.push(expr.evaluate(new));
        }

        Operation::Update {
            old: Record::new(None, old_results),
            new: Record::new(None, new_results),
        }
    }
}

impl Processor for ProjectionProcessor {
    fn update_schema(
        &self,
        _output_port: PortHandle,
        input_schemas: &HashMap<PortHandle, Schema>,
    ) -> anyhow::Result<Schema> {
        let input_schema = input_schemas.get(&DefaultPortHandle).unwrap();
        let mut output_schema = Schema::empty();

        let mut rng = rand::thread_rng();
        output_schema.identifier = Option::from(SchemaIdentifier {
            id: rng.gen(),
            version: 1,
        });

        for (counter, e) in self.expressions.iter().enumerate() {
            let field_name = self.names.get(counter).unwrap().clone();
            let field_type = e.get_type(input_schema);
            let field_nullable = true;
            output_schema
                .fields
                .push(FieldDefinition::new(field_name, field_type, field_nullable));
        }

        Ok(output_schema)
    }

    fn init<'a>(&'_ mut self, _: &mut dyn StateStore) -> anyhow::Result<()> {
        info!("{:?}", "Initialising Projection Processor");
        Ok(())
    }

    fn process(
        &mut self,
        _from_port: PortHandle,
        op: Operation,
        fw: &dyn ProcessorChannelForwarder,
        _state_store: &mut dyn StateStore,
    ) -> anyhow::Result<NextStep> {
        let _ = match op {
            Operation::Delete { ref old } => fw.send(self.delete(old), DefaultPortHandle),
            Operation::Insert { ref new } => fw.send(self.insert(new), DefaultPortHandle),
            Operation::Update { ref old, ref new } => {
                fw.send(self.update(old, new), DefaultPortHandle)
            }
            Operation::SchemaUpdate { new: _ } => todo!(),
            Operation::Terminate => todo!(),
        };
        Ok(NextStep::Continue)
    }
}
