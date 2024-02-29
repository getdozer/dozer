use crate::errors::PipelineError;
use dozer_sql_expression::execution::Expression;

use dozer_core::channels::ProcessorChannelForwarder;
use dozer_core::epoch::Epoch;
use dozer_core::node::Processor;
use dozer_core::DEFAULT_PORT_HANDLE;
use dozer_types::errors::internal::BoxedError;
use dozer_types::types::{Operation, Record, Schema, TableOperation};

#[derive(Debug)]
pub struct ProjectionProcessor {
    expressions: Vec<Expression>,
    input_schema: Schema,
}

impl ProjectionProcessor {
    pub fn new(input_schema: Schema, expressions: Vec<Expression>) -> Result<Self, PipelineError> {
        Ok(Self {
            input_schema,
            expressions,
        })
    }

    fn delete(&mut self, record: &Record) -> Result<Operation, PipelineError> {
        let mut results = vec![];

        for expr in &mut self.expressions {
            results.push(expr.evaluate(record, &self.input_schema)?);
        }

        let mut output_record = Record::new(results);
        output_record.set_lifetime(record.lifetime.to_owned());

        Ok(Operation::Delete { old: output_record })
    }

    fn insert(&mut self, record: &Record) -> Result<Record, PipelineError> {
        let mut results = vec![];

        for expr in &mut self.expressions {
            results.push(expr.evaluate(record, &self.input_schema)?);
        }

        let mut output_record = Record::new(results);
        output_record.set_lifetime(record.lifetime.to_owned());
        Ok(output_record)
    }

    fn update(&mut self, old: &Record, new: &Record) -> Result<Operation, PipelineError> {
        let mut old_results = vec![];
        let mut new_results = vec![];

        for expr in &mut self.expressions {
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
        op: TableOperation,
        fw: &mut dyn ProcessorChannelForwarder,
    ) -> Result<(), BoxedError> {
        let output_op = match op.op {
            Operation::Delete { ref old } => self.delete(old)?,
            Operation::Insert { ref new } => Operation::Insert {
                new: self.insert(new)?,
            },
            Operation::Update { ref old, ref new } => self.update(old, new)?,
            Operation::BatchInsert { new } => {
                let records = new
                    .iter()
                    .map(|record| self.insert(record))
                    .collect::<Result<Vec<_>, _>>()?;
                Operation::BatchInsert { new: records }
            }
        };
        fw.send(TableOperation {
            id: op.id,
            op: output_op,
            port: DEFAULT_PORT_HANDLE,
        });
        Ok(())
    }

    fn commit(&self, _epoch: &Epoch) -> Result<(), BoxedError> {
        Ok(())
    }
}
