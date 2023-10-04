use dozer_recordstore::{ProcessorRecord, ProcessorRecordStore, StoreRecord};
use dozer_sql_expression::execution::Expression;
use dozer_types::types::{Field, Lifetime, Schema};

use crate::errors::TableOperatorError;

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
        record_store: &ProcessorRecordStore,
        record: &ProcessorRecord,
        schema: &Schema,
    ) -> Result<Vec<ProcessorRecord>, TableOperatorError> {
        let mut ttl_records = vec![];
        if let Some(operator) = &self.operator {
            let operator_records = operator.execute(record_store, record, schema)?;

            let schema = operator.get_output_schema(schema)?;

            let record = record_store.load_record(record)?;
            let reference = match self
                .expression
                .evaluate(&record, &schema)
                .map_err(|err| TableOperatorError::InternalError(Box::new(err)))?
            {
                Field::Timestamp(timestamp) => timestamp,
                other => return Err(TableOperatorError::InvalidTtlInputType(other)),
            };

            let lifetime = Some(Lifetime {
                reference,
                duration: self.duration,
            });
            for mut operator_record in operator_records {
                operator_record.set_lifetime(lifetime.clone());
                ttl_records.push(operator_record);
            }
        } else {
            let record_decoded = record_store.load_record(record)?;
            let reference = match self
                .expression
                .evaluate(&record_decoded, schema)
                .map_err(|err| TableOperatorError::InternalError(Box::new(err)))?
            {
                Field::Timestamp(timestamp) => timestamp,
                other => return Err(TableOperatorError::InvalidTtlInputType(other)),
            };

            let lifetime = Some(Lifetime {
                reference,
                duration: self.duration,
            });

            let mut cloned_record = record.clone();
            cloned_record.set_lifetime(lifetime);
            ttl_records.push(cloned_record);
        }

        Ok(ttl_records)
    }

    fn get_output_schema(&self, schema: &Schema) -> Result<Schema, TableOperatorError> {
        Ok(schema.clone())
    }
}
