use std::collections::HashMap;

use sqlparser::ast::Expr as SqlExpr;

use anyhow::bail;
use dozer_core::dag::dag::PortHandle;
use dozer_core::dag::forwarder::ProcessorChannelForwarder;
use dozer_core::dag::mt_executor::DefaultPortHandle;
use dozer_core::dag::node::{Processor, ProcessorFactory};
use dozer_core::dag::node::NextStep;
use dozer_core::state::StateStore;
use dozer_types::types::{Field, Operation, Schema};

use crate::common::error::{DozerSqlError, Result};
use crate::pipeline::expression::builder::ExpressionBuilder;
use crate::pipeline::expression::expression::{Expression, PhysicalExpression};

pub struct SelectionProcessorFactory {
    id: i32,
    input_ports: Vec<PortHandle>,
    output_ports: Vec<PortHandle>,
    expression: Box<Expression>,
}

impl SelectionProcessorFactory {
    pub fn new(id: i32, input_ports: Vec<PortHandle>, output_ports: Vec<PortHandle>, expression: Box<Expression>) -> Self {
        Self { id, input_ports, output_ports, expression }
    }
}


impl ProcessorFactory for SelectionProcessorFactory {
    fn get_input_ports(&self) -> Vec<PortHandle> {
        self.input_ports.clone()
    }

    fn get_output_ports(&self) -> Vec<PortHandle> {
        self.output_ports.clone()
    }

    fn get_output_schema(&self, output_port: PortHandle, input_schemas: HashMap<PortHandle, Schema>) -> anyhow::Result<Schema> {
        Ok(input_schemas.get(&DefaultPortHandle).unwrap().clone())
    }

    fn build(&self) -> Box<dyn Processor> {
        Box::new(SelectionProcessor { id: self.id, expression: self.expression.clone() })
    }
}

pub struct SelectionProcessor {
    id: i32,
    expression: Box<Expression>,
}

impl Processor for SelectionProcessor {
    fn init<'a>(&'a mut self, state_store: &mut dyn StateStore, input_schemas: HashMap<PortHandle, Schema>) -> anyhow::Result<()> {
        println!("PROC {}: Initialising TestProcessor", self.id);
        //   self.state = Some(state_manager.init_state_store("pippo".to_string()).unwrap());
        Ok(())
    }

    fn process(
        &mut self,
        _from_port: PortHandle,
        op: Operation,
        fw: &dyn ProcessorChannelForwarder,
        state_store: &mut dyn StateStore,
    ) -> anyhow::Result<NextStep> {
        match op {
            Operation::Delete { old } => {
                bail!("DELETE Operation not supported.")
            }
            Operation::Insert { ref new } => {
                if self.expression.evaluate(&new) == Field::Boolean(true) {
                    let _ = fw.send(op, DefaultPortHandle);
                }
                Ok(NextStep::Continue)
            }
            Operation::Update { old, new } => bail!("UPDATE Operation not supported."),
            Operation::Terminate => bail!("TERMINATE Operation not supported."),
        }
    }
}

pub struct SelectionBuilder {
    expression_builder: ExpressionBuilder,
}

impl SelectionBuilder {
    pub fn new(schema: &Schema) -> SelectionBuilder {
        Self {
            expression_builder: ExpressionBuilder::new(schema.clone())
        }
    }

    pub fn get_processor(&self, selection: Option<SqlExpr>) -> Result<SelectionProcessorFactory> {
        match selection {
            Some(expression) => {
                let expression = self.expression_builder.parse_sql_expression(&expression)?;
                Ok(SelectionProcessorFactory::new(1, vec![DefaultPortHandle], vec![DefaultPortHandle], expression))
            }
            _ => Err(DozerSqlError::NotImplemented(
                "Unsupported WHERE clause.".to_string(),
            )),
        }
    }
}
