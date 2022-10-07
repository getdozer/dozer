use rand::Rng;
use std::collections::HashMap;

use anyhow::bail;
use anyhow::Context;
use sqlparser::ast::SelectItem;

use dozer_core::dag::dag::PortHandle;
use dozer_core::dag::forwarder::ProcessorChannelForwarder;
use dozer_core::dag::mt_executor::DefaultPortHandle;
use dozer_core::dag::node::NextStep;
use dozer_core::dag::node::{Processor, ProcessorFactory};
use dozer_core::state::StateStore;
use dozer_types::types::{FieldDefinition, Operation, Record, Schema, SchemaIdentifier};

use crate::pipeline::expression::expression::{Expression, ExpressionExecutor};

pub struct ProjectionProcessorFactory {
    id: i32,
    input_ports: Vec<PortHandle>,
    output_ports: Vec<PortHandle>,
    expressions: Vec<Box<Expression>>,
    names: Vec<String>,
}

impl ProjectionProcessorFactory {
    pub fn new(
        id: i32,
        input_ports: Vec<PortHandle>,
        output_ports: Vec<PortHandle>,
        expressions: Vec<Box<Expression>>,
        names: Vec<String>,
    ) -> Self {
        Self {
            id,
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
            id: self.id,
            expressions: self.expressions.clone(),
            names: self.names.clone(),
            ctr: 0,
        })
    }
}

pub struct ProjectionProcessor {
    id: i32,
    expressions: Vec<Box<Expression>>,
    names: Vec<String>,
    ctr: u64,
}

impl Processor for ProjectionProcessor {
    fn update_schema(
        &self,
        output_port: PortHandle,
        input_schemas: &HashMap<PortHandle, Schema>,
    ) -> anyhow::Result<Schema> {
        let input_schema = input_schemas.get(&DefaultPortHandle).unwrap();
        let mut output_schema = Schema::empty();

        let mut rng = rand::thread_rng();
        output_schema.identifier = Option::from(SchemaIdentifier {
            id: rng.gen(),
            version: 1,
        });

        let mut counter = 0;
        for e in self.expressions.iter().enumerate() {
            let field_name = self.names.get(counter).unwrap().clone();
            let field_type = e.1.get_type(input_schema);
            let field_nullable = true;
            output_schema
                .fields
                .push(FieldDefinition::new(field_name, field_type, field_nullable));
            counter = counter + 1;
        }

        Ok(output_schema)
    }

    fn init<'a>(&'a mut self, _: &mut dyn StateStore) -> anyhow::Result<()> {
        println!("PROC {}: Initialising TestProcessor", self.id);
        //   self.state = Some(state_manager.init_state_store("pippo".to_string()).unwrap());
        Ok(())
    }

    fn process(
        &mut self,
        _from_port: PortHandle,
        op: Operation,
        fw: &dyn ProcessorChannelForwarder,
        _state_store: &mut dyn StateStore,
    ) -> anyhow::Result<NextStep> {
        match op {
            Operation::Delete { old: _ } => {
                bail!("DELETE Operation not supported.")
            }
            Operation::Insert { ref new } => {
                // println!("PROC {}: Message {} received", self.id, self.ctr);
                self.ctr += 1;
                let mut results = vec![];
                for expr in &self.expressions {
                    results.push(expr.evaluate(&new));
                }
                let _ = fw.send(
                    Operation::Insert {
                        new: Record::new(None, results),
                    },
                    DefaultPortHandle,
                );

                Ok(NextStep::Continue)
            }
            Operation::Update { old: _, new: _ } => bail!("UPDATE Operation not supported."),
            _ => Ok(NextStep::Continue),
        }
    }
}
