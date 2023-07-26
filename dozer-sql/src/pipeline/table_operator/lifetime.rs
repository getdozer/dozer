use dozer_core::processor_record::{ProcessorRecord, ProcessorRecordRef};
use dozer_types::types::{Field, Lifetime, Schema};

use crate::pipeline::{errors::TableOperatorError, expression::execution::Expression};

use super::operator::{TableOperator, TableOperatorType};

#[derive(Debug)]
pub struct LifetimeTableOperator {
    operator: Option<Box<TableOperatorType>>,
    expression: Expression,
    duration: std::time::Duration,
}

impl LifetimeTableOperator {
    pub fn new(
        operator: Option<Box<TableOperatorType>>,
        expression: Expression,
        duration: std::time::Duration,
    ) -> Self {
        Self {
            operator,
            expression,
            duration,
        }
    }
}

impl TableOperator for LifetimeTableOperator {
    fn get_name(&self) -> String {
        "TTL".to_owned()
    }

    fn execute(
        &self,
        record: &ProcessorRecordRef,
        schema: &Schema,
    ) -> Result<Vec<ProcessorRecordRef>, TableOperatorError> {
        let source_record = record.clone();
        let mut ttl_records = vec![];
        if let Some(operator) = &self.operator {
            let operator_records = operator.execute(&source_record, schema)?;

            let schema = operator.get_output_schema(schema)?;

            let reference = match self
                .expression
                .evaluate(source_record.get_record(), &schema)
                .map_err(|err| TableOperatorError::InternalError(Box::new(err)))?
            {
                Field::Timestamp(timestamp) => timestamp,
                other => return Err(TableOperatorError::InvalidTtlInputType(other)),
            };

            let lifetime = Some(Lifetime {
                reference,
                duration: self.duration,
            });
            for operator_record in operator_records {
                let mut cloned_record = ProcessorRecord::from_referenced_record(operator_record);
                cloned_record.set_lifetime(lifetime.clone());
                ttl_records.push(ProcessorRecordRef::new(cloned_record));
            }
        } else {
            let reference = match self
                .expression
                .evaluate(source_record.get_record(), schema)
                .map_err(|err| TableOperatorError::InternalError(Box::new(err)))?
            {
                Field::Timestamp(timestamp) => timestamp,
                other => return Err(TableOperatorError::InvalidTtlInputType(other)),
            };

            let lifetime = Some(Lifetime {
                reference,
                duration: self.duration,
            });

            let mut cloned_record = ProcessorRecord::from_referenced_record(source_record);
            cloned_record.set_lifetime(lifetime);
            ttl_records.push(ProcessorRecordRef::new(cloned_record));
        }

        Ok(ttl_records)
    }

    fn get_output_schema(&self, schema: &Schema) -> Result<Schema, TableOperatorError> {
        Ok(schema.clone())
    }
}
