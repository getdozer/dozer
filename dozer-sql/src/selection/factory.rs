use std::{collections::HashMap, sync::Arc};

use crate::errors::PipelineError;
use dozer_core::{
    event::EventHub,
    node::{PortHandle, Processor, ProcessorFactory},
    DEFAULT_PORT_HANDLE,
};
use dozer_sql_expression::builder::ExpressionBuilder;
use dozer_sql_expression::sqlparser::ast::Expr as SqlExpr;
use dozer_types::{errors::internal::BoxedError, types::Schema};
use dozer_types::{models::udf_config::UdfConfig, tonic::async_trait};
use tokio::runtime::Runtime;

use super::processor::SelectionProcessor;

#[derive(Debug)]
pub struct SelectionProcessorFactory {
    statement: SqlExpr,
    id: String,
    udfs: Vec<UdfConfig>,
    runtime: Arc<Runtime>,
    internal_fields: usize,
}

impl SelectionProcessorFactory {
    /// Creates a new [`SelectionProcessorFactory`].
    pub fn new(
        id: String,
        statement: SqlExpr,
        udf_config: Vec<UdfConfig>,
        runtime: Arc<Runtime>,
    ) -> Self {
        Self {
            statement,
            id,
            udfs: udf_config,
            runtime,
            internal_fields: 0,
        }
    }

    /// Remove the trailing mark-join fields after evaluating WHERE.
    pub(crate) fn with_internal_fields(mut self, count: usize) -> Self {
        self.internal_fields = count;
        self
    }
}

#[async_trait]
impl ProcessorFactory for SelectionProcessorFactory {
    fn id(&self) -> String {
        self.id.clone()
    }
    fn type_name(&self) -> String {
        "Selection".to_string()
    }
    fn get_input_ports(&self) -> Vec<PortHandle> {
        vec![DEFAULT_PORT_HANDLE]
    }

    fn get_output_ports(&self) -> Vec<PortHandle> {
        vec![DEFAULT_PORT_HANDLE]
    }

    async fn get_output_schema(
        &self,
        _output_port: &PortHandle,
        input_schemas: &HashMap<PortHandle, Schema>,
    ) -> Result<Schema, BoxedError> {
        let schema = input_schemas
            .get(&DEFAULT_PORT_HANDLE)
            .ok_or(PipelineError::InvalidPortHandle(DEFAULT_PORT_HANDLE))?;
        let mut output = schema.clone();
        let width = output
            .fields
            .len()
            .checked_sub(self.internal_fields)
            .ok_or_else(|| PipelineError::InvalidQuery("Invalid internal WHERE fields".into()))?;
        output.fields.truncate(width);
        output.primary_index.retain(|index| *index < width);
        Ok(output)
    }

    async fn build(
        &self,
        input_schemas: HashMap<PortHandle, Schema>,
        _output_schemas: HashMap<PortHandle, Schema>,
        _event_hub: EventHub,
    ) -> Result<Box<dyn Processor>, BoxedError> {
        let schema = input_schemas
            .get(&DEFAULT_PORT_HANDLE)
            .ok_or(PipelineError::InvalidPortHandle(DEFAULT_PORT_HANDLE))?;

        match ExpressionBuilder::new(schema.fields.len(), self.runtime.clone())
            .build(false, &self.statement, schema, &self.udfs)
            .await
        {
            Ok(expression) => Ok(Box::new(
                SelectionProcessor::new(schema.clone(), expression)?
                    .with_internal_fields(self.internal_fields),
            )),
            Err(e) => Err(e.into()),
        }
    }
}
