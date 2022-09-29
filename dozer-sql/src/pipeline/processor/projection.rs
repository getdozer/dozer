use std::collections::HashMap;
use std::sync::Arc;
use dozer_core::dag::node::NextStep;
use dozer_core::dag::dag::PortHandle;
use dozer_core::dag::node::{ChannelForwarder, ExecutionContext, Processor};
use dozer_types::types::{Field, OperationEvent, Schema};
use sqlparser::ast::{BinaryOperator, Expr as SqlExpr, FunctionArg, FunctionArgExpr, SelectItem, Value as SqlValue};
use crate::common::error::{DozerSqlError, Result};
use crate::pipeline::expression::aggregate::AggregateFunctionType;
use crate::pipeline::expression::expression::{Expression, PhysicalExpression};
use crate::pipeline::expression::builder::ExpressionBuilder;
use crate::pipeline::expression::expression::Expression::{AggregateFunction, ScalarFunction};
use crate::pipeline::expression::operator::OperatorType;
use crate::pipeline::expression::scalar::ScalarFunctionType;

pub struct ProjectionProcessor {
    id: i32,
    input_ports: Option<Vec<PortHandle>>,
    output_ports: Option<Vec<PortHandle>>,
    expressions: Vec<Box<Expression>>,
}

impl ProjectionProcessor {
    pub fn new(id: i32, input_ports: Option<Vec<PortHandle>>, output_ports: Option<Vec<PortHandle>>, expressions: Vec<Box<Expression>>) -> Self {
        Self { id, input_ports, output_ports, expressions }
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
        println!("PROC {}: Initialising ProjectionProcessor", self.id);
        Ok(())
    }

    fn process(&self, from_port: Option<PortHandle>, op: OperationEvent, ctx: & dyn ExecutionContext, fw: &ChannelForwarder) -> core::result::Result<NextStep, String> {

        fw.send(op, None);
        Ok(NextStep::Continue)
    }
}