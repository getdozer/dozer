use crate::pipeline::expression::execution::{Expression, ExpressionExecutor};

use dozer_core::dag::channels::ProcessorChannelForwarder;
use dozer_core::dag::dag::DEFAULT_PORT_HANDLE;
use dozer_core::dag::errors::ExecutionError;
use dozer_core::dag::errors::ExecutionError::InternalError;
use dozer_core::dag::node::{NodeHandle, PortHandle, Processor};
use dozer_core::dag::record_store::RecordReader;
use dozer_core::storage::lmdb_storage::{LmdbEnvironmentManager, SharedTransaction};
use dozer_types::types::{Operation, Record};
use log::info;
use std::collections::HashMap;

pub struct ProjectionProcessor {
    expressions: Vec<(String, Expression)>,
}

impl ProjectionProcessor {
    pub fn new(expressions: Vec<(String, Expression)>) -> Self {
        Self { expressions }
    }

    fn delete(&mut self, record: &Record) -> Result<Operation, ExecutionError> {
        let mut results = vec![];

        for expr in &self.expressions {
            results.push(
                expr.1
                    .evaluate(record)
                    .map_err(|e| InternalError(Box::new(e)))?,
            );
        }
        Ok(Operation::Delete {
            old: Record::new(None, results),
        })
    }

    fn insert(&mut self, record: &Record) -> Result<Operation, ExecutionError> {
        let mut results = vec![];

        for expr in self.expressions.clone() {
            results.push(
                expr.1
                    .evaluate(record)
                    .map_err(|e| InternalError(Box::new(e)))?,
            );
        }
        Ok(Operation::Insert {
            new: Record::new(None, results),
        })
    }

    fn update(&self, old: &Record, new: &Record) -> Result<Operation, ExecutionError> {
        let mut old_results = vec![];
        let mut new_results = vec![];

        for expr in &self.expressions {
            old_results.push(
                expr.1
                    .evaluate(old)
                    .map_err(|e| InternalError(Box::new(e)))?,
            );
            new_results.push(
                expr.1
                    .evaluate(new)
                    .map_err(|e| InternalError(Box::new(e)))?,
            );
        }

        Ok(Operation::Update {
            old: Record::new(None, old_results),
            new: Record::new(None, new_results),
        })
    }
}

impl Processor for ProjectionProcessor {
    fn init(&mut self, _env: &mut LmdbEnvironmentManager) -> Result<(), ExecutionError> {
        info!("{:?}", "Initialising Projection Processor");
        Ok(())
    }

    fn process(
        &mut self,
        _from_port: PortHandle,
        op: Operation,
        fw: &mut dyn ProcessorChannelForwarder,
        _tx: &SharedTransaction,
        _reader: &HashMap<PortHandle, RecordReader>,
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

    fn commit(
        &self,
        _source: &NodeHandle,
        _txid: u64,
        _seq_in_tx: u64,
        _tx: &SharedTransaction,
    ) -> Result<(), ExecutionError> {
        Ok(())
    }
}
