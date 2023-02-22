use std::collections::HashMap;

use crate::pipeline::builder::SchemaSQLContext;
use crate::pipeline::expression::builder::ExpressionBuilder;
use dozer_core::{
    errors::ExecutionError,
    node::{OutputPortDef, OutputPortType, PortHandle, Processor, ProcessorFactory},
    storage::lmdb_storage::LmdbExclusiveTransaction,
    DEFAULT_PORT_HANDLE,
};
use dozer_types::types::Schema;
use sqlparser::ast::Expr as SqlExpr;

use super::processor::SelectionProcessor;

#[derive(Debug)]
pub struct SelectionProcessorFactory {
    statement: SqlExpr,
}

impl SelectionProcessorFactory {
    /// Creates a new [`SelectionProcessorFactory`].
    pub fn new(statement: SqlExpr) -> Self {
        Self { statement }
    }
}

impl ProcessorFactory<SchemaSQLContext> for SelectionProcessorFactory {
    fn get_input_ports(&self) -> Vec<PortHandle> {
        vec![DEFAULT_PORT_HANDLE]
    }

    fn get_output_ports(&self) -> Vec<OutputPortDef> {
        vec![OutputPortDef::new(
            DEFAULT_PORT_HANDLE,
            OutputPortType::Stateless,
        )]
    }

    fn get_output_schema(
        &self,
        _output_port: &PortHandle,
        input_schemas: &HashMap<PortHandle, (Schema, SchemaSQLContext)>,
    ) -> Result<(Schema, SchemaSQLContext), ExecutionError> {
        let schema = input_schemas
            .get(&DEFAULT_PORT_HANDLE)
            .ok_or(ExecutionError::InvalidPortHandle(DEFAULT_PORT_HANDLE))?;
        Ok(schema.clone())
    }

    fn build(
        &self,
        input_schemas: HashMap<PortHandle, Schema>,
        _output_schemas: HashMap<PortHandle, Schema>,
        _txn: &mut LmdbExclusiveTransaction,
    ) -> Result<Box<dyn Processor>, ExecutionError> {
        let schema = input_schemas
            .get(&DEFAULT_PORT_HANDLE)
            .ok_or(ExecutionError::InvalidPortHandle(DEFAULT_PORT_HANDLE))?;

        match ExpressionBuilder::new(schema.fields.len()).build(false, &self.statement, schema) {
            Ok(expression) => Ok(Box::new(SelectionProcessor::new(
                schema.clone(),
                expression,
            ))),
            Err(e) => Err(ExecutionError::InternalStringError(e.to_string())),
        }
    }
}
