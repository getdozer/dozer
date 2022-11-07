use crate::pipeline::expression::builder::{ExpressionBuilder, ExpressionType};
use crate::pipeline::expression::execution::{Expression, ExpressionExecutor};
use dozer_core::dag::channels::ProcessorChannelForwarder;
use dozer_core::dag::errors::ExecutionError;
use dozer_core::dag::errors::ExecutionError::InternalError;
use dozer_core::dag::mt_executor::DEFAULT_PORT_HANDLE;
use dozer_core::dag::node::PortHandle;
use dozer_core::dag::node::{Processor, ProcessorFactory};
use dozer_core::storage::lmdb_sys::Transaction;
use dozer_types::types::{Field, Operation, Schema};
use log::info;
use sqlparser::ast::Expr as SqlExpr;
use std::collections::HashMap;

pub struct SelectionProcessorFactory {
    statement: SqlExpr,
}

impl SelectionProcessorFactory {
    /// Creates a new [`SelectionProcessorFactory`].
    pub fn new(statement: SqlExpr) -> Self {
        Self { statement }
    }
}

impl ProcessorFactory for SelectionProcessorFactory {
    fn is_stateful(&self) -> bool {
        false
    }

    fn get_input_ports(&self) -> Vec<PortHandle> {
        vec![DEFAULT_PORT_HANDLE]
    }

    fn get_output_ports(&self) -> Vec<PortHandle> {
        vec![DEFAULT_PORT_HANDLE]
    }

    fn build(&self) -> Box<dyn Processor> {
        Box::new(SelectionProcessor {
            statement: self.statement.clone(),
            expression: Box::new(Expression::Literal(Field::Boolean(true))),
            builder: ExpressionBuilder {},
        })
    }
}

pub struct SelectionProcessor {
    statement: SqlExpr,
    expression: Box<Expression>,
    builder: ExpressionBuilder,
}

impl SelectionProcessor {
    fn build_expression(
        &self,
        sql_expression: &SqlExpr,
        schema: &Schema,
    ) -> Result<Box<Expression>, ExecutionError> {
        self.builder
            .build(&ExpressionType::FullExpression, sql_expression, schema)
            .map_err(|e| InternalError(Box::new(e)))
    }

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
        &mut self,
        _output_port: PortHandle,
        input_schemas: &HashMap<PortHandle, Schema>,
    ) -> Result<Schema, ExecutionError> {
        let schema = input_schemas.get(&DEFAULT_PORT_HANDLE).unwrap();
        self.expression = self.build_expression(&self.statement, schema)?;
        Ok(schema.clone())
    }

    fn init<'a>(&'_ mut self, _state: Option<&mut Transaction>) -> Result<(), ExecutionError> {
        info!("{:?}", "Initialising Selection Processor");
        Ok(())
    }

    fn process(
        &mut self,
        _from_port: PortHandle,
        op: Operation,
        fw: &dyn ProcessorChannelForwarder,
        _state: Option<&mut Transaction>,
    ) -> Result<(), ExecutionError> {
        match op {
            Operation::Delete { ref old } => {
                if self
                    .expression
                    .evaluate(old)
                    .map_err(|e| InternalError(Box::new(e)))?
                    == Field::Boolean(true)
                {
                    let _ = fw.send(op, DEFAULT_PORT_HANDLE);
                }
            }
            Operation::Insert { ref new } => {
                if self
                    .expression
                    .evaluate(new)
                    .map_err(|e| InternalError(Box::new(e)))?
                    == Field::Boolean(true)
                {
                    let _ = fw.send(op, DEFAULT_PORT_HANDLE);
                }
            }
            Operation::Update { ref old, ref new } => {
                let old_fulfilled = self
                    .expression
                    .evaluate(old)
                    .map_err(|e| InternalError(Box::new(e)))?
                    == Field::Boolean(true);
                let new_fulfilled = self
                    .expression
                    .evaluate(new)
                    .map_err(|e| InternalError(Box::new(e)))?
                    == Field::Boolean(true);
                match (old_fulfilled, new_fulfilled) {
                    (true, true) => {
                        // both records fulfills the WHERE condition, forward the operation
                        let _ = fw.send(op, DEFAULT_PORT_HANDLE);
                    }
                    (true, false) => {
                        // the old record fulfills the WHERE condition while then new one doesn't, forward a delete operation
                        let _ = fw.send(self.delete(old), DEFAULT_PORT_HANDLE);
                    }
                    (false, true) => {
                        // the old record doesn't fulfill the WHERE condition while then new one does, forward an insert operation
                        let _ = fw.send(self.insert(new), DEFAULT_PORT_HANDLE);
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
