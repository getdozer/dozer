use std::sync::Arc;
use dozer_core::dag::node::NextStep;
use dozer_core::dag::dag::PortHandle;
use dozer_core::dag::node::{Processor, ExecutionContext, ChannelForwarder};
use dozer_types::types::{OperationEvent, Schema};
use sqlparser::ast::SelectItem;
use crate::common::error::{DozerSqlError, Result};
use crate::pipeline::expression::operator::Expression;
use crate::pipeline::expression::builder::ExpressionBuilder;

pub struct ProjectionProcessor {
    id: i32,
    input_ports: Option<Vec<PortHandle>>,
    output_ports: Option<Vec<PortHandle>>,
}

impl ProjectionProcessor {
    pub fn new(id: i32, input_ports: Option<Vec<PortHandle>>, output_ports: Option<Vec<PortHandle>>) -> Self {
        Self { id, input_ports, output_ports }
    }
}

impl Processor for ProjectionProcessor {
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

    fn process(&self, from_port: Option<PortHandle>, op: OperationEvent, ctx: & dyn ExecutionContext, fw: &ChannelForwarder) -> core::result::Result<NextStep, String> {

        //  println!("PROC {}: Message {} received", self.id, op.id);
        fw.send(op, None);
        Ok(NextStep::Continue)
    }
}

pub struct ProjectionBuilder {
    expression_builder: ExpressionBuilder,
}

impl ProjectionBuilder {

    pub fn new(schema: &Schema) -> ProjectionBuilder {
        Self {
            expression_builder: ExpressionBuilder::new(schema.clone())
        }
    }

    pub fn get_processor(&self, projection: Vec<SelectItem>) -> Result<Arc<dyn Processor>> {
        let expression = projection.into_iter()
            .map(|expr| {
                self.sql_select_to_expression(&expr)
            })
            .flat_map(|result| match result {
                Ok(vec) => vec.into_iter().map(Ok).collect(),
                Err(err) => vec![Err(err)],
            })
            .collect::<Result<Vec<Box<dyn Expression>>>>();
        Ok(Arc::new(ProjectionProcessor::new(0, None, None)))
    }

    fn sql_select_to_expression(
        &self,
        sql: &SelectItem,
    ) -> Result<Vec<Box<dyn Expression>>> {

        match sql {
            SelectItem::UnnamedExpr(sql_expr) => {
                let expr = self.expression_builder.parse_sql_expression(sql_expr);
                Ok(vec![expr.unwrap()])
            },
            SelectItem::ExprWithAlias { expr, alias } => Err(DozerSqlError::NotImplemented(
                format!("Unsupported Expression {}", expr)
            )),
            SelectItem::Wildcard => Err(DozerSqlError::NotImplemented(
                format!("Unsupported Wildcard")
            )),
            SelectItem::QualifiedWildcard(ref object_name) => Err(DozerSqlError::NotImplemented(
                format!("Unsupported Qualified Wildcard {}", object_name)
            ))
        }
    }

}