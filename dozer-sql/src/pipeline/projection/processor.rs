use crate::pipeline::expression::execution::{Expression, ExpressionExecutor};

use dozer_core::channels::ProcessorChannelForwarder;
use dozer_core::epoch::Epoch;
use dozer_core::errors::ExecutionError;
use dozer_core::errors::ExecutionError::InternalError;
use dozer_core::node::{PortHandle, Processor};
use dozer_core::DEFAULT_PORT_HANDLE;
use dozer_types::types::{Operation, Record, Schema};

#[derive(Debug)]
pub struct ProjectionProcessor {
    expressions: Vec<Expression>,
    input_schema: Schema,
}

impl ProjectionProcessor {
    pub fn new(input_schema: Schema, expressions: Vec<Expression>) -> Self {
        Self {
            input_schema,
            expressions,
        }
    }

    fn delete(&mut self, record: &Record) -> Result<Operation, ExecutionError> {
        let mut results = vec![];

        for expr in &self.expressions {
            results.push(
                expr.evaluate(record, &self.input_schema)
                    .map_err(|e| InternalError(Box::new(e)))?,
            );
        }
        Ok(Operation::Delete {
            old: Record::new(None, results, None),
        })
    }

    fn insert(&mut self, record: &Record) -> Result<Operation, ExecutionError> {
        let mut results = vec![];

        for expr in self.expressions.clone() {
            results.push(
                expr.evaluate(record, &self.input_schema)
                    .map_err(|e| InternalError(Box::new(e)))?,
            );
        }
        Ok(Operation::Insert {
            new: Record::new(None, results, None),
        })
    }

    fn update(&self, old: &Record, new: &Record) -> Result<Operation, ExecutionError> {
        let mut old_results = vec![];
        let mut new_results = vec![];

        for expr in &self.expressions {
            old_results.push(
                expr.evaluate(old, &self.input_schema)
                    .map_err(|e| InternalError(Box::new(e)))?,
            );
            new_results.push(
                expr.evaluate(new, &self.input_schema)
                    .map_err(|e| InternalError(Box::new(e)))?,
            );
        }

        Ok(Operation::Update {
            old: Record::new(None, old_results, None),
            new: Record::new(None, new_results, None),
        })
    }
}

impl Processor for ProjectionProcessor {
    fn process(
        &mut self,
        _from_port: PortHandle,
        op: Operation,
        fw: &mut dyn ProcessorChannelForwarder,
    ) -> Result<(), ExecutionError> {
        let _ = match op {
            Operation::Delete { ref old } => fw.send(self.delete(old)?, DEFAULT_PORT_HANDLE),
            Operation::Insert { ref new } => fw.send(self.insert(new)?, DEFAULT_PORT_HANDLE),
            Operation::Update { ref old, ref new } => {
                fw.send(self.update(old, new)?, DEFAULT_PORT_HANDLE)
            }
        };
        Ok(())
    }

    fn commit(&self, _epoch: &Epoch) -> Result<(), ExecutionError> {
        Ok(())
    }
}
