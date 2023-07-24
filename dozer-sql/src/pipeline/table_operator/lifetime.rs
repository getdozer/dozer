use dozer_types::types::{
    ref_types::ProcessorRecordRef, DozerDuration, Lifetime, ProcessorRecord, Schema,
};

use crate::pipeline::{
    errors::TableOperatorError,
    expression::execution::{Expression, ExpressionExecutor},
};

use super::operator::{TableOperator, TableOperatorType};

#[derive(Debug)]
pub struct LifetimeTableOperator {
    operator: Option<Box<TableOperatorType>>,
    expression: Expression,
    duration: DozerDuration,
}

impl LifetimeTableOperator {
    pub fn new(
        operator: Option<Box<TableOperatorType>>,
        expression: Expression,
        duration: DozerDuration,
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
        let mut source_record = record.clone();
        let mut ttl_records = vec![];
        if let Some(operator) = &self.operator {
            let operator_records = operator.execute(&source_record, schema)?;

            let schema = operator.get_output_schema(schema)?;

            let lifetime = Some(Lifetime {
                reference: self
                    .expression
                    .evaluate(&source_record.get_record(), &schema)
                    .map_err(|err| TableOperatorError::InternalError(Box::new(err)))?,
                duration: self.duration,
            });
            for operator_record in operator_records {
                let mut cloned_record = ProcessorRecord::from_referenced_record(operator_record);
                cloned_record.set_lifetime(lifetime.clone());
                ttl_records.push(ProcessorRecordRef::new(cloned_record));
            }
        } else {
            let lifetime = Some(Lifetime {
                reference: self
                    .expression
                    .evaluate(&source_record.get_record(), schema)
                    .map_err(|err| TableOperatorError::InternalError(Box::new(err)))?,
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
