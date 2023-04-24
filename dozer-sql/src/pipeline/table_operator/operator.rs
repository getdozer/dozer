use dozer_types::types::{Record, Schema};
use enum_dispatch::enum_dispatch;

use crate::pipeline::errors::TableOperatorError;

#[enum_dispatch]
pub trait TableOperator: Send + Sync {
    fn execute(&self, record: &mut Record) -> Result<Vec<Record>, TableOperatorError>;
    fn get_output_schema(&self, schema: &Schema) -> Result<Schema, TableOperatorError>;
}
