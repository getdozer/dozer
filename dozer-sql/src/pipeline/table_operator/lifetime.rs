use dozer_types::types::{DozerDuration, Record, Schema};

use crate::pipeline::errors::TableOperatorError;

use super::operator::TableOperator;

#[derive(Debug)]
pub struct LifetimeTableOperator {
    duration: DozerDuration,
}

impl LifetimeTableOperator {
    pub fn new(duration: DozerDuration) -> Self {
        Self { duration }
    }
}

impl TableOperator for LifetimeTableOperator {
    fn execute(&self, record: &Record) -> Result<Vec<Record>, TableOperatorError> {
        let mut ttl_record = record.clone();
        ttl_record.set_lifetime(self.duration);
        Ok(vec![ttl_record])
    }

    fn get_output_schema(&self, schema: &Schema) -> Result<Schema, TableOperatorError> {
        Ok(schema.clone())
    }
}
