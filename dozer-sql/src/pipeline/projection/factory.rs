use std::collections::HashMap;

use dozer_core::dag::{
    dag::DEFAULT_PORT_HANDLE,
    errors::ExecutionError,
    node::{OutputPortDef, OutputPortDefOptions, PortHandle, Processor, ProcessorFactory},
};
use dozer_types::types::{FieldDefinition, Schema};
use sqlparser::ast::SelectItem;

use crate::pipeline::expression::{
    builder::{ExpressionBuilder, ExpressionType},
    execution::{Expression, ExpressionExecutor},
};

use super::processor::ProjectionProcessor;

pub struct ProjectionProcessorFactory {
    statement: Vec<SelectItem>,
}

impl ProjectionProcessorFactory {
    /// Creates a new [`PreAggregationProcessorFactory`].
    pub fn new(statement: Vec<SelectItem>) -> Self {
        Self { statement }
    }
}

impl ProcessorFactory for ProjectionProcessorFactory {
    fn get_input_ports(&self) -> Vec<PortHandle> {
        vec![DEFAULT_PORT_HANDLE]
    }

    fn get_output_ports(&self) -> Vec<OutputPortDef> {
        vec![OutputPortDef::new(
            DEFAULT_PORT_HANDLE,
            OutputPortDefOptions::default(),
        )]
    }

    fn build(
        &self,
        input_schemas: HashMap<PortHandle, Schema>,
        _output_schemas: HashMap<PortHandle, Schema>,
    ) -> Result<Box<dyn Processor>, ExecutionError> {
        let input_schema = input_schemas
            .get(&DEFAULT_PORT_HANDLE)
            .ok_or(ExecutionError::InvalidPortHandle(DEFAULT_PORT_HANDLE))?;

        let expressions = self
            .statement
            .iter()
            .map(|item| parse_sql_select_item(item, input_schema))
            .collect::<Result<Vec<(String, Expression)>, ExecutionError>>()?;
        Ok(Box::new(ProjectionProcessor::new(
            input_schema.clone(),
            expressions,
        )))
    }

    fn get_output_schema(
        &self,
        output_port: &PortHandle,
        input_schemas: &HashMap<PortHandle, Schema>,
    ) -> Result<Schema, ExecutionError> {
        let input_schema = input_schemas
            .get(&DEFAULT_PORT_HANDLE)
            .ok_or(ExecutionError::InvalidPortHandle(DEFAULT_PORT_HANDLE))?;

        let mut output_schema = input_schema.clone();

        let expressions = self
            .statement
            .iter()
            .map(|item| parse_sql_select_item(item, input_schema))
            .collect::<Result<Vec<(String, Expression)>, ExecutionError>>()?;

        for e in expressions.iter() {
            let field_name = e.0.clone();
            let field_type = e.1.get_type(input_schema);
            let field_nullable = true;

            if output_schema.get_field_index(field_name.as_str()).is_err() {
                output_schema.fields.push(FieldDefinition::new(
                    field_name,
                    field_type,
                    field_nullable,
                ));
            }
        }

        Ok(output_schema)
    }
}

fn parse_sql_select_item(
    sql: &SelectItem,
    schema: &Schema,
) -> Result<(String, Expression), ExecutionError> {
    let builder = ExpressionBuilder {};
    match sql {
        SelectItem::UnnamedExpr(sql_expr) => {
            match builder.parse_sql_expression(&ExpressionType::PreAggregation, sql_expr, schema) {
                Ok(expr) => Ok((sql_expr.to_string(), *expr.0)),
                Err(error) => Err(ExecutionError::InternalStringError(error.to_string())),
            }
        }
        SelectItem::ExprWithAlias { expr, alias } => Err(ExecutionError::InternalStringError(
            format!("{}:{}", expr, alias),
        )),
        SelectItem::Wildcard => Err(ExecutionError::InternalStringError("*".to_string())),
        SelectItem::QualifiedWildcard(ref object_name) => {
            Err(ExecutionError::InternalStringError(object_name.to_string()))
        }
    }
}
