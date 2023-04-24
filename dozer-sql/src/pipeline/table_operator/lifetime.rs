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
    fn execute(&self, record: &mut Record) -> Result<Vec<Record>, TableOperatorError> {
        record.set_lifetime(self.duration);
        Ok(vec![record.clone()])
    }

    fn get_output_schema(&self, schema: &Schema) -> Result<Schema, TableOperatorError> {
        Ok(schema.clone())
    }
}
