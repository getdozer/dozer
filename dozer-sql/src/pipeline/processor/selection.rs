use crate::common::error::{DozerSqlError, Result};
use std::sync::Arc;
use crate::pipeline::expression::expression::PhysicalExpression;
use dozer_core::dag::dag::PortHandle;
use dozer_core::dag::node::NextStep;
use dozer_core::dag::node::{ChannelForwarder, ExecutionContext, Processor};
use dozer_types::types::{Field, Operation, OperationEvent, Schema};
use num_traits::FloatErrorKind::Invalid;
use sqlparser::ast::{Expr as SqlExpr, SelectItem};
use crate::pipeline::expression::builder::ExpressionBuilder;

pub struct SelectionProcessor {
    id: i32,
    input_ports: Option<Vec<PortHandle>>,
    output_ports: Option<Vec<PortHandle>>,
    operator: Box<dyn PhysicalExpression>,
}

impl SelectionProcessor {
    pub fn new(
        id: i32,
        input_ports: Option<Vec<PortHandle>>,
        output_ports: Option<Vec<PortHandle>>,
        operator: Box<dyn PhysicalExpression>,
    ) -> Self {
        Self {
            id,
            input_ports,
            output_ports,
            operator,
        }
    }
}

impl Processor for SelectionProcessor {
    fn get_input_ports(&self) -> Option<Vec<PortHandle>> {
        self.input_ports.clone()
    }

    fn get_output_ports(&self) -> Option<Vec<PortHandle>> {
        self.output_ports.clone()
    }

    fn init(&self) -> core::result::Result<(), String> {
        println!("PROC {}: Initialising SelectionProcessor", self.id);
        Ok(())
    }

    fn process(
        &self,
        from_port: Option<PortHandle>,
        op: OperationEvent,
        ctx: &dyn ExecutionContext,
        fw: &ChannelForwarder,
    ) -> core::result::Result<NextStep, String> {
        //println!("PROC {}: Message {} received", self.id, op.id);

        match op.operation {
            Operation::Delete { old } => {
                Err("DELETE Operation not supported.".to_string())
            }
            Operation::Insert { ref new} => {
                if self.operator.evaluate(&new) == Field::Boolean(true) {
                    fw.send(op, None);
                }
                Ok(NextStep::Continue)
            }
            Operation::Update { old, new} => Err("UPDATE Operation not supported.".to_string()),
            Operation::Terminate => Err("TERMINATE Operation not supported.".to_string()),
            _ => Err("TERMINATE Operation not supported.".to_string()),
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

    pub fn get_processor(&self, selection: Option<SqlExpr>) -> Result<Arc<dyn Processor>> {
        match selection {
            Some(expression) => {
                let operator = self.expression_builder.parse_sql_expression(&expression)?;
                Ok(Arc::new(SelectionProcessor::new(0, None, None, operator)))
            }
            _ => Err(DozerSqlError::NotImplemented(
                "Unsupported WHERE clause.".to_string(),
            )),
        }
    }


}
