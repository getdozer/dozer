use crate::pipeline::table_operator::lifetime::LifetimeTableOperator;
use dozer_types::types::{Record, Schema};
use enum_dispatch::enum_dispatch;

use crate::pipeline::errors::TableOperatorError;

#[enum_dispatch]
pub trait TableOperator: Send + Sync {
    fn execute(&self, record: &Record) -> Result<Vec<Record>, TableOperatorError>;
    fn get_output_schema(&self, schema: &Schema) -> Result<Schema, TableOperatorError>;
}

#[enum_dispatch(TableOperator)]
#[derive(Debug)]
pub enum TableOperatorType {
    LifetimeTableOperator,
}
