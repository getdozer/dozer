use dozer_core::channels::ProcessorChannelForwarder;
use dozer_core::checkpoint::serialize::Cursor;
use dozer_core::dozer_log::storage::Object;
use dozer_core::epoch::Epoch;
use dozer_core::node::{PortHandle, Processor};
use dozer_core::DEFAULT_PORT_HANDLE;
use dozer_recordstore::ProcessorRecordStore;
use dozer_sql_expression::execution::Expression;
use dozer_types::errors::internal::BoxedError;
use dozer_types::types::{Field, Operation, Schema};

use crate::errors::PipelineError;

#[derive(Debug)]
pub struct SelectionProcessor {
    expression: Expression,
    input_schema: Schema,
}

impl SelectionProcessor {
    pub fn new(
        input_schema: Schema,
        mut expression: Expression,
        checkpoint_data: Option<Vec<u8>>,
    ) -> Result<Self, PipelineError> {
        if let Some(data) = checkpoint_data {
            let mut cursor = Cursor::new(&data);
            expression.deserialize_state(&mut cursor)?;
        }

        Ok(Self {
            input_schema,
            expression,
        })
    }
}

impl Processor for SelectionProcessor {
    fn commit(&self, _epoch: &Epoch) -> Result<(), BoxedError> {
        Ok(())
    }

    fn process(
        &mut self,
        _from_port: PortHandle,
        _record_store: &ProcessorRecordStore,
        op: Operation,
        fw: &mut dyn ProcessorChannelForwarder,
    ) -> Result<(), BoxedError> {
        match op {
            Operation::Delete { ref old } => {
                if self.expression.evaluate(old, &self.input_schema)? == Field::Boolean(true) {
                    fw.send(op, DEFAULT_PORT_HANDLE);
                }
            }
            Operation::Insert { ref new } => {
                if self.expression.evaluate(new, &self.input_schema)? == Field::Boolean(true) {
                    fw.send(op, DEFAULT_PORT_HANDLE);
                }
            }
            Operation::Update { old, new } => {
                let old_fulfilled =
                    self.expression.evaluate(&old, &self.input_schema)? == Field::Boolean(true);
                let new_fulfilled =
                    self.expression.evaluate(&new, &self.input_schema)? == Field::Boolean(true);
                match (old_fulfilled, new_fulfilled) {
                    (true, true) => {
                        // both records fulfills the WHERE condition, forward the operation
                        fw.send(Operation::Update { old, new }, DEFAULT_PORT_HANDLE);
                    }
                    (true, false) => {
                        // the old record fulfills the WHERE condition while then new one doesn't, forward a delete operation
                        fw.send(Operation::Delete { old }, DEFAULT_PORT_HANDLE);
                    }
                    (false, true) => {
                        // the old record doesn't fulfill the WHERE condition while then new one does, forward an insert operation
                        fw.send(Operation::Insert { new }, DEFAULT_PORT_HANDLE);
                    }
                    (false, false) => {
                        // both records doesn't fulfill the WHERE condition, don't forward the operation
                    }
                }
            }
            Operation::BatchInsert { new } => {
                for record in new {
                    self.process(
                        _from_port,
                        _record_store,
                        Operation::Insert { new: record },
                        fw,
                    )?;
                }
            }
        }
        Ok(())
    }

    fn serialize(
        &mut self,
        _record_store: &ProcessorRecordStore,
        mut object: Object,
    ) -> Result<(), BoxedError> {
        self.expression.serialize_state(&mut object)?;
        Ok(())
    }
}
