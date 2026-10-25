use std::{collections::HashMap, sync::Arc};

use crate::errors::PipelineError;
use dozer_core::{
    event::EventHub,
    node::{PortHandle, Processor, ProcessorFactory},
    DEFAULT_PORT_HANDLE,
};
use dozer_sql_expression::builder::ExpressionBuilder;
use dozer_sql_expression::execution::Expression;
use dozer_sql_expression::sqlparser::ast::Expr as SqlExpr;
use dozer_sql_expression::udf_registry::UdfRegistry;
use dozer_types::tonic::async_trait;
use dozer_types::{errors::internal::BoxedError, types::Schema};
use tokio::runtime::Runtime;

use super::processor::SelectionProcessor;

#[derive(Debug)]
pub struct SelectionProcessorFactory {
    statement: SqlExpr,
    id: String,
    udfs: UdfRegistry,
    runtime: Arc<Runtime>,
}

impl SelectionProcessorFactory {
    /// Creates a new [`SelectionProcessorFactory`].
    pub fn new(
        id: String,
        statement: SqlExpr,
        udf_config: impl Into<UdfRegistry>,
        runtime: Arc<Runtime>,
    ) -> Self {
        Self {
            statement,
            id,
            udfs: udf_config.into(),
            runtime,
        }
    }

    async fn expression(&self, schema: &Schema) -> Result<Expression, BoxedError> {
        Ok(
            ExpressionBuilder::new(schema.fields.len(), self.runtime.clone())
                .with_udf_registry(self.udfs.clone())
                .build(false, &self.statement, schema, &self.udfs)
                .await?,
        )
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
        // Validate WHERE UDF signatures during schema propagation as well as build.
        self.expression(schema).await?;
        Ok(schema.clone())
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

        Ok(Box::new(SelectionProcessor::new(
            schema.clone(),
            self.expression(schema).await?,
        )?))
    }
}
