use std::sync::Arc;

use crate::pipeline::errors::PipelineError;
use crate::pipeline::expression::execution::Expression;

use dozer_core::channels::ProcessorChannelForwarder;
use dozer_core::epoch::Epoch;
use dozer_core::executor_operation::ProcessorOperation;
use dozer_core::node::{PortHandle, Processor};
use dozer_core::processor_record::ProcessorRecord;
use dozer_core::DEFAULT_PORT_HANDLE;
use dozer_types::errors::internal::BoxedError;
use dozer_types::types::Schema;

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

    fn delete(&mut self, record: &ProcessorRecord) -> Result<ProcessorOperation, PipelineError> {
        let mut output_values = Vec::new();
        for expr in &self.expressions {
            output_values.push(expr.evaluate(record, &self.input_schema)?);
        }

        let mut output_record = ProcessorRecord::new();
        output_record.push(Arc::new(output_values));
        output_record.set_lifetime(record.get_lifetime());

        Ok(ProcessorOperation::Delete { old: output_record })
    }

    fn insert(&mut self, record: &ProcessorRecord) -> Result<ProcessorOperation, PipelineError> {
        let mut output_values = Vec::new();
        for expr in &self.expressions {
            output_values.push(expr.evaluate(record, &self.input_schema)?);
        }

        let mut output_record = ProcessorRecord::new();
        output_record.push(Arc::new(output_values));
        output_record.set_lifetime(record.get_lifetime());
        Ok(ProcessorOperation::Insert { new: output_record })
    }

    fn update(
        &self,
        old: &ProcessorRecord,
        new: &ProcessorRecord,
    ) -> Result<ProcessorOperation, PipelineError> {
        let mut old_output_values = Vec::new();
        for expr in &self.expressions {
            old_output_values.push(expr.evaluate(old, &self.input_schema)?);
        }

        let mut new_output_values = Vec::new();
        for expr in &self.expressions {
            new_output_values.push(expr.evaluate(new, &self.input_schema)?);
        }

        let mut old_output_record = ProcessorRecord::new();
        old_output_record.push(Arc::new(old_output_values));
        old_output_record.set_lifetime(old.get_lifetime());

        let mut new_output_record = ProcessorRecord::new();
        new_output_record.push(Arc::new(new_output_values));
        new_output_record.set_lifetime(new.get_lifetime());

        Ok(ProcessorOperation::Update {
            old: old_output_record,
            new: new_output_record,
        })
    }
}

impl Processor for ProjectionProcessor {
    fn process(
        &mut self,
        _from_port: PortHandle,
        op: ProcessorOperation,
        fw: &mut dyn ProcessorChannelForwarder,
    ) -> Result<(), BoxedError> {
        match op {
            ProcessorOperation::Delete { ref old } => {
                fw.send(self.delete(old)?, DEFAULT_PORT_HANDLE)
            }
            ProcessorOperation::Insert { ref new } => {
                fw.send(self.insert(new)?, DEFAULT_PORT_HANDLE)
            }
            ProcessorOperation::Update { ref old, ref new } => {
                fw.send(self.update(old, new)?, DEFAULT_PORT_HANDLE)
            }
        };
        Ok(())
    }

    fn commit(&self, _epoch: &Epoch) -> Result<(), BoxedError> {
        Ok(())
    }
}
