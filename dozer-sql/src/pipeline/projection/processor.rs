use crate::pipeline::errors::PipelineError;
use crate::pipeline::expression::execution::Expression;

use dozer_core::channels::ProcessorChannelForwarder;
use dozer_core::epoch::Epoch;
use dozer_core::executor_operation::ProcessorOperation;
use dozer_core::node::{PortHandle, Processor};
use dozer_core::processor_record::{ProcessorRecord, ProcessorRecordRef};
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

    fn delete(&mut self, record: &ProcessorRecordRef) -> Result<ProcessorOperation, PipelineError> {
        let mut output_record = ProcessorRecord::new();
        for expr in &self.expressions {
            output_record
                .extend_direct_field(expr.evaluate(record.get_record(), &self.input_schema)?);
        }

        output_record.set_lifetime(record.get_record().get_lifetime());

        Ok(ProcessorOperation::Delete {
            old: ProcessorRecordRef::new(output_record),
        })
    }

    fn insert(&mut self, record: &ProcessorRecordRef) -> Result<ProcessorOperation, PipelineError> {
        let mut output_record = ProcessorRecord::new();

        for expr in self.expressions.clone() {
            output_record
                .extend_direct_field(expr.evaluate(record.get_record(), &self.input_schema)?);
        }

        output_record.set_lifetime(record.get_record().get_lifetime());
        Ok(ProcessorOperation::Insert {
            new: ProcessorRecordRef::new(output_record),
        })
    }

    fn update(
        &self,
        old: &ProcessorRecordRef,
        new: &ProcessorRecordRef,
    ) -> Result<ProcessorOperation, PipelineError> {
        let mut old_output_record = ProcessorRecord::new();
        let mut new_output_record = ProcessorRecord::new();
        for expr in &self.expressions {
            old_output_record
                .extend_direct_field(expr.evaluate(old.get_record(), &self.input_schema)?);
            new_output_record
                .extend_direct_field(expr.evaluate(new.get_record(), &self.input_schema)?);
        }

        old_output_record.set_lifetime(old.get_record().get_lifetime());

        new_output_record.set_lifetime(new.get_record().get_lifetime());
        Ok(ProcessorOperation::Update {
            old: ProcessorRecordRef::new(old_output_record),
            new: ProcessorRecordRef::new(new_output_record),
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
