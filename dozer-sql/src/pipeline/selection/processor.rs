use crate::pipeline::expression::execution::Expression;
use dozer_core::channels::ProcessorChannelForwarder;
use dozer_core::epoch::Epoch;
use dozer_core::executor_operation::ProcessorOperation;
use dozer_core::node::{PortHandle, Processor};
use dozer_core::processor_record::ProcessorRecord;
use dozer_core::DEFAULT_PORT_HANDLE;
use dozer_types::errors::internal::BoxedError;
use dozer_types::types::{Field, Schema};

#[derive(Debug)]
pub struct SelectionProcessor {
    expression: Expression,
    input_schema: Schema,
}

impl SelectionProcessor {
    pub fn new(input_schema: Schema, expression: Expression) -> Self {
        Self {
            input_schema,
            expression,
        }
    }

    fn delete(&self, record: &ProcessorRecord) -> ProcessorOperation {
        ProcessorOperation::Delete {
            old: record.clone(),
        }
    }

    fn insert(&self, record: &ProcessorRecord) -> ProcessorOperation {
        ProcessorOperation::Insert {
            new: record.clone(),
        }
    }
}

impl Processor for SelectionProcessor {
    fn commit(&self, _epoch: &Epoch) -> Result<(), BoxedError> {
        Ok(())
    }

    fn process(
        &mut self,
        _from_port: PortHandle,
        op: ProcessorOperation,
        fw: &mut dyn ProcessorChannelForwarder,
    ) -> Result<(), BoxedError> {
        match op {
            ProcessorOperation::Delete { ref old } => {
                if self.expression.evaluate(old, &self.input_schema)? == Field::Boolean(true) {
                    fw.send(op, DEFAULT_PORT_HANDLE);
                }
            }
            ProcessorOperation::Insert { ref new } => {
                if self.expression.evaluate(new, &self.input_schema)? == Field::Boolean(true) {
                    fw.send(op, DEFAULT_PORT_HANDLE);
                }
            }
            ProcessorOperation::Update { ref old, ref new } => {
                let old_fulfilled =
                    self.expression.evaluate(old, &self.input_schema)? == Field::Boolean(true);
                let new_fulfilled =
                    self.expression.evaluate(new, &self.input_schema)? == Field::Boolean(true);
                match (old_fulfilled, new_fulfilled) {
                    (true, true) => {
                        // both records fulfills the WHERE condition, forward the operation
                        fw.send(op, DEFAULT_PORT_HANDLE);
                    }
                    (true, false) => {
                        // the old record fulfills the WHERE condition while then new one doesn't, forward a delete operation
                        fw.send(self.delete(old), DEFAULT_PORT_HANDLE);
                    }
                    (false, true) => {
                        // the old record doesn't fulfill the WHERE condition while then new one does, forward an insert operation
                        fw.send(self.insert(new), DEFAULT_PORT_HANDLE);
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
