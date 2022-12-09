use crate::pipeline::expression::execution::{Expression, ExpressionExecutor};
use dozer_core::dag::channels::ProcessorChannelForwarder;
use dozer_core::dag::dag::DEFAULT_PORT_HANDLE;
use dozer_core::dag::errors::ExecutionError;
use dozer_core::dag::errors::ExecutionError::InternalError;
use dozer_core::dag::node::{PortHandle, Processor};
use dozer_core::dag::record_store::RecordReader;
use dozer_core::storage::common::{Environment, RwTransaction};
use dozer_types::types::{Field, Operation};
use log::info;
use std::collections::HashMap;

pub struct SelectionProcessor {
    expression: Box<Expression>,
}

impl SelectionProcessor {
    pub fn new(expression: Box<Expression>) -> Self {
        Self { expression }
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
    fn init(&mut self, _env: &mut dyn Environment) -> Result<(), ExecutionError> {
        info!("{:?}", "Initialising Selection Processor");
        Ok(())
    }

    fn commit(&self, _tx: &mut dyn RwTransaction) -> Result<(), ExecutionError> {
        Ok(())
    }

    fn process(
        &mut self,
        _from_port: PortHandle,
        op: Operation,
        fw: &mut dyn ProcessorChannelForwarder,
        _tx: &mut dyn RwTransaction,
        _reader: &HashMap<PortHandle, RecordReader>,
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
