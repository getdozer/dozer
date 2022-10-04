use std::collections::HashMap;
use std::sync::Arc;

use sqlparser::ast::{
    BinaryOperator, Expr as SqlExpr, FunctionArg, FunctionArgExpr, SelectItem, Value as SqlValue,
};

use anyhow::bail;
use dozer_core::dag::dag::PortHandle;
use dozer_core::dag::forwarder::ProcessorChannelForwarder;
use dozer_core::dag::mt_executor::DefaultPortHandle;
use dozer_core::dag::node::{ExecutionContext, Processor, ProcessorFactory};
use dozer_core::dag::node::NextStep;
use dozer_core::state::StateStore;
use dozer_types::types::{Field, Operation, OperationEvent, Record, Schema};

use crate::common::error::{DozerSqlError, Result};
use crate::pipeline::expression::aggregate::AggregateFunctionType;
use crate::pipeline::expression::builder::ExpressionBuilder;
use crate::pipeline::expression::expression::{Expression, PhysicalExpression};
use crate::pipeline::expression::expression::Expression::{AggregateFunction, ScalarFunction};
use crate::pipeline::expression::operator::BinaryOperatorType;
use crate::pipeline::expression::scalar::ScalarFunctionType;

pub struct ProjectionProcessorFactory {
    id: i32,
    input_ports: Vec<PortHandle>,
    output_ports: Vec<PortHandle>,
    expressions: Vec<Box<Expression>>,
}

impl ProjectionProcessorFactory {
    pub fn new(
        id: i32,
        input_ports: Vec<PortHandle>,
        output_ports: Vec<PortHandle>,
        expressions: Vec<Box<Expression>>,
    ) -> Self {
        Self {
            id,
            input_ports,
            output_ports,
            expressions,
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

    fn get_output_schema(
        &self,
        output_port: PortHandle,
        input_schemas: HashMap<PortHandle, Schema>,
    ) -> anyhow::Result<Schema> {
        Ok(input_schemas.get(&DefaultPortHandle).unwrap().clone())
    }

    fn build(&self) -> Box<dyn Processor> {
        Box::new(ProjectionProcessor {
            id: self.id,
            expressions: self.expressions.clone(),
            ctr: 0,
        })
    }
}

pub struct ProjectionProcessor {
    id: i32,
    expressions: Vec<Box<Expression>>,
    ctr: u64,
}

impl Processor for ProjectionProcessor {
    fn init<'a>(
        &'a mut self,
        state_store: &mut dyn StateStore,
        input_schemas: HashMap<PortHandle, Schema>,
    ) -> anyhow::Result<()> {
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
            Operation::Update { old, new } => bail!("UPDATE Operation not supported."),
            Operation::Terminate => bail!("TERMINATE Operation not supported."),
            _ => bail!("TERMINATE Operation not supported."),
        }
    }
}
