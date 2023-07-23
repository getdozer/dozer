use crate::pipeline::table_operator::lifetime::LifetimeTableOperator;
use dozer_types::types::{ProcessorRecord, Schema};
use enum_dispatch::enum_dispatch;

use crate::pipeline::errors::TableOperatorError;

#[enum_dispatch]
pub trait TableOperator: Send + Sync {
    fn get_name(&self) -> String;
    fn execute(
        &self,
        record: &ProcessorRecord,
        schema: &Schema,
    ) -> Result<Vec<ProcessorRecord>, TableOperatorError>;
    fn get_output_schema(&self, schema: &Schema) -> Result<Schema, TableOperatorError>;
}

#[enum_dispatch(TableOperator)]
#[derive(Debug)]
pub enum TableOperatorType {
    LifetimeTableOperator,
}
