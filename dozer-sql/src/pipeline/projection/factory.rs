use std::collections::HashMap;

use dozer_core::dag::{
    dag::DEFAULT_PORT_HANDLE,
    errors::ExecutionError,
    node::{OutputPortDef, OutputPortType, PortHandle, Processor, ProcessorFactory},
};
use dozer_types::types::{FieldDefinition, Schema};
use sqlparser::ast::{Expr, Ident, SelectItem};

use crate::pipeline::builder::SchemaSQLContext;
use crate::pipeline::{
    errors::PipelineError,
    expression::{
        builder::{BuilderExpressionType, ExpressionBuilder},
        execution::Expression,
        execution::ExpressionExecutor,
    },
};

use super::processor::ProjectionProcessor;

#[derive(Debug)]
pub struct ProjectionProcessorFactory {
    select: Vec<SelectItem>,
}

impl ProjectionProcessorFactory {
    /// Creates a new [`ProjectionProcessorFactory`].
    pub fn _new(select: Vec<SelectItem>) -> Self {
        Self { select }
    }
}

impl ProcessorFactory<SchemaSQLContext> for ProjectionProcessorFactory {
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
        let (input_schema, context) = input_schemas.get(&DEFAULT_PORT_HANDLE).unwrap();

        let mut select_expr: Vec<(String, Expression)> = vec![];
        for s in self.select.iter() {
            match s {
                SelectItem::Wildcard(_) => {
                    let fields: Vec<SelectItem> = input_schema
                        .fields
                        .iter()
                        .map(|col| {
                            SelectItem::UnnamedExpr(Expr::Identifier(Ident::new(
                                col.to_owned().name,
                            )))
                        })
                        .collect();
                    for f in fields {
                        let res = parse_sql_select_item(&f, input_schema);
                        if let Ok(..) = res {
                            select_expr.push(res.unwrap())
                        }
                    }
                }
                _ => {
                    let res = parse_sql_select_item(s, input_schema);
                    if let Ok(..) = res {
                        select_expr.push(res.unwrap())
                    }
                }
            }
        }

        let mut output_schema = input_schema.clone();
        let mut fields = vec![];
        for e in select_expr.iter() {
            let field_name = e.0.clone();
            let field_type =
                e.1.get_type(input_schema)
                    .map_err(|e| ExecutionError::InternalError(Box::new(e)))?;
            fields.push(FieldDefinition::new(
                field_name,
                field_type.return_type,
                field_type.nullable,
                field_type.source,
            ));
        }
        output_schema.fields = fields;

        Ok((output_schema, context.clone()))
    }

    fn build(
        &self,
        input_schemas: HashMap<PortHandle, Schema>,
        _output_schemas: HashMap<PortHandle, Schema>,
    ) -> Result<Box<dyn Processor>, ExecutionError> {
        let schema = match input_schemas.get(&DEFAULT_PORT_HANDLE) {
            Some(schema) => Ok(schema),
            None => Err(ExecutionError::InternalStringError(
                "Invalid Projection input port".to_string(),
            )),
        }?;

        match self
            .select
            .iter()
            .map(|item| parse_sql_select_item(item, schema))
            .collect::<Result<Vec<(String, Expression)>, PipelineError>>()
        {
            Ok(expressions) => Ok(Box::new(ProjectionProcessor::new(
                schema.clone(),
                expressions,
            ))),
            Err(error) => Err(ExecutionError::InternalStringError(error.to_string())),
        }
    }

    fn prepare(
        &self,
        _input_schemas: HashMap<PortHandle, (Schema, SchemaSQLContext)>,
        _output_schemas: HashMap<PortHandle, (Schema, SchemaSQLContext)>,
    ) -> Result<(), ExecutionError> {
        Ok(())
    }
}

pub(crate) fn parse_sql_select_item(
    sql: &SelectItem,
    schema: &Schema,
) -> Result<(String, Expression), PipelineError> {
    let builder = ExpressionBuilder {};
    match sql {
        SelectItem::UnnamedExpr(sql_expr) => {
            match builder.parse_sql_expression(
                &BuilderExpressionType::FullExpression,
                sql_expr,
                schema,
            ) {
                Ok(expr) => Ok((sql_expr.to_string(), *expr.0)),
                Err(error) => Err(error),
            }
        }
        SelectItem::ExprWithAlias { expr, alias } => {
            match builder.parse_sql_expression(&BuilderExpressionType::FullExpression, expr, schema)
            {
                Ok(expr) => Ok((alias.value.clone(), *expr.0)),
                Err(error) => Err(error),
            }
        }
        SelectItem::Wildcard(_) => Err(PipelineError::InvalidOperator("*".to_string())),
        SelectItem::QualifiedWildcard(ref object_name, ..) => {
            Err(PipelineError::InvalidOperator(object_name.to_string()))
        }
    }
}
