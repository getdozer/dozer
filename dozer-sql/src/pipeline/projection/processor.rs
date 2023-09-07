use crate::pipeline::errors::PipelineError;
use crate::pipeline::expression::execution::Expression;

use dozer_core::channels::ProcessorChannelForwarder;
use dozer_core::dozer_log::storage::Object;
use dozer_core::epoch::Epoch;
use dozer_core::executor_operation::ProcessorOperation;
use dozer_core::node::{PortHandle, Processor};
use dozer_core::processor_record::ProcessorRecordStore;
use dozer_core::DEFAULT_PORT_HANDLE;
use dozer_types::errors::internal::BoxedError;
use dozer_types::types::{Operation, Record, Schema};

#[derive(Debug)]
pub struct ProjectionProcessor {
    expressions: Vec<Expression>,
    input_schema: Schema,
}

impl ProjectionProcessor {
    pub fn new(
        input_schema: Schema,
        expressions: Vec<Expression>,
        _checkpoint_data: Option<Vec<u8>>,
    ) -> Self {
        Self {
            input_schema,
            expressions,
        }
    }

    fn delete(&mut self, record: &Record) -> Result<Operation, PipelineError> {
        let mut results = vec![];

        for expr in &self.expressions {
            results.push(expr.evaluate(record, &self.input_schema)?);
        }

        let mut output_record = Record::new(results);
        output_record.set_lifetime(record.lifetime.to_owned());

        Ok(Operation::Delete { old: output_record })
    }

    fn insert(&mut self, record: &Record) -> Result<Operation, PipelineError> {
        let mut results = vec![];

        for expr in self.expressions.clone() {
            results.push(expr.evaluate(record, &self.input_schema)?);
        }

        let mut output_record = Record::new(results);
        output_record.set_lifetime(record.lifetime.to_owned());
        Ok(Operation::Insert { new: output_record })
    }

    fn update(&self, old: &Record, new: &Record) -> Result<Operation, PipelineError> {
        let mut old_results = vec![];
        let mut new_results = vec![];

        for expr in &self.expressions {
            old_results.push(expr.evaluate(old, &self.input_schema)?);
            new_results.push(expr.evaluate(new, &self.input_schema)?);
        }

        let mut old_output_record = Record::new(old_results);
        old_output_record.set_lifetime(old.lifetime.to_owned());
        let mut new_output_record = Record::new(new_results);
        new_output_record.set_lifetime(new.lifetime.to_owned());
        Ok(Operation::Update {
            old: old_output_record,
            new: new_output_record,
        })
    }
}

impl Processor for ProjectionProcessor {
    fn process(
        &mut self,
        _from_port: PortHandle,
        record_store: &ProcessorRecordStore,
        op: ProcessorOperation,
        fw: &mut dyn ProcessorChannelForwarder,
    ) -> Result<(), BoxedError> {
        let op = record_store.load_operation(&op)?;
        let output_op = match op {
            Operation::Delete { ref old } => self.delete(old)?,
            Operation::Insert { ref new } => self.insert(new)?,
            Operation::Update { ref old, ref new } => self.update(old, new)?,
        };
        let output_op = record_store.create_operation(&output_op)?;
        fw.send(output_op, DEFAULT_PORT_HANDLE);
        Ok(())
    }

    fn commit(&self, _epoch: &Epoch) -> Result<(), BoxedError> {
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
