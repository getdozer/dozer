use crate::pipeline::expression::execution::Expression;
use dozer_core::channels::ProcessorChannelForwarder;
use dozer_core::dozer_log::storage::Object;
use dozer_core::epoch::Epoch;
use dozer_core::executor_operation::ProcessorOperation;
use dozer_core::node::{PortHandle, Processor};
use dozer_core::processor_record::ProcessorRecordStore;
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
}

impl Processor for SelectionProcessor {
    fn commit(&self, _epoch: &Epoch) -> Result<(), BoxedError> {
        Ok(())
    }

    fn process(
        &mut self,
        _from_port: PortHandle,
        record_store: &ProcessorRecordStore,
        op: ProcessorOperation,
        fw: &mut dyn ProcessorChannelForwarder,
    ) -> Result<(), BoxedError> {
        match op {
            ProcessorOperation::Delete { ref old } => {
                let old = record_store.load_record(old)?;
                if self.expression.evaluate(&old, &self.input_schema)? == Field::Boolean(true) {
                    fw.send(op, DEFAULT_PORT_HANDLE);
                }
            }
            ProcessorOperation::Insert { ref new } => {
                let new = record_store.load_record(new)?;
                if self.expression.evaluate(&new, &self.input_schema)? == Field::Boolean(true) {
                    fw.send(op, DEFAULT_PORT_HANDLE);
                }
            }
            ProcessorOperation::Update { old, new } => {
                let old_decoded = record_store.load_record(&old)?;
                let old_fulfilled = self.expression.evaluate(&old_decoded, &self.input_schema)?
                    == Field::Boolean(true);
                let new_decoded = record_store.load_record(&new)?;
                let new_fulfilled = self.expression.evaluate(&new_decoded, &self.input_schema)?
                    == Field::Boolean(true);
                match (old_fulfilled, new_fulfilled) {
                    (true, true) => {
                        // both records fulfills the WHERE condition, forward the operation
                        fw.send(ProcessorOperation::Update { old, new }, DEFAULT_PORT_HANDLE);
                    }
                    (true, false) => {
                        // the old record fulfills the WHERE condition while then new one doesn't, forward a delete operation
                        fw.send(ProcessorOperation::Delete { old }, DEFAULT_PORT_HANDLE);
                    }
                    (false, true) => {
                        // the old record doesn't fulfill the WHERE condition while then new one does, forward an insert operation
                        fw.send(ProcessorOperation::Insert { new }, DEFAULT_PORT_HANDLE);
                    }
                    (false, false) => {
                        // both records doesn't fulfill the WHERE condition, don't forward the operation
                    }
                }
            }
        }
        Ok(())
    }

    fn serialize(
        &mut self,
        _record_store: &ProcessorRecordStore,
        _object: Object,
    ) -> Result<(), BoxedError> {
        Ok(())
    }
}
