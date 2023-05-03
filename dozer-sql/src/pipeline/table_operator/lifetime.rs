use dozer_types::types::{DozerDuration, Record, Schema};

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

    fn execute(&self, record: &Record, schema: &Schema) -> Result<Vec<Record>, TableOperatorError> {
        let mut source_record = record.clone();
        let mut ttl_records = vec![];
        if let Some(operator) = &self.operator {
            let operator_records = operator.execute(&source_record, schema)?;

            let schema = operator.get_output_schema(schema)?;

            let lt = (
                self.duration,
                self.expression
                    .evaluate(&source_record, &schema)
                    .map_err(|err| TableOperatorError::InternalError(Box::new(err)))?,
            );
            for operator_record in operator_records {
                source_record = operator_record;
                source_record.set_lifetime(Some(self.duration));
                ttl_records.push(source_record);
            }
        } else {
            source_record.set_lifetime(Some(self.duration));
            ttl_records.push(source_record);
        }

        Ok(ttl_records)
    }

    fn get_output_schema(&self, schema: &Schema) -> Result<Schema, TableOperatorError> {
        Ok(schema.clone())
    }
}
