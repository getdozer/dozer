use std::collections::HashMap;

use dozer_core::{
    node::{OutputPortDef, OutputPortType, PortHandle, Processor, ProcessorFactory},
    processor_record::ProcessorRecordStore,
    DEFAULT_PORT_HANDLE,
};
use dozer_types::{
    errors::internal::BoxedError,
    types::{FieldDefinition, Schema},
};
use sqlparser::ast::{Expr, Ident, SelectItem};

use crate::pipeline::{
    errors::PipelineError,
    expression::{builder::ExpressionBuilder, execution::Expression},
};

use super::processor::ProjectionProcessor;

#[derive(Debug)]
pub struct ProjectionProcessorFactory {
    select: Vec<SelectItem>,
    id: String,
}

impl ProjectionProcessorFactory {
    /// Creates a new [`ProjectionProcessorFactory`].
    pub fn _new(id: String, select: Vec<SelectItem>) -> Self {
        Self { select, id }
    }
}

impl ProcessorFactory for ProjectionProcessorFactory {
    fn id(&self) -> String {
        self.id.clone()
    }
    fn type_name(&self) -> String {
        "Projection".to_string()
    }

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
        input_schemas: &HashMap<PortHandle, Schema>,
    ) -> Result<Schema, BoxedError> {
        let input_schema = input_schemas.get(&DEFAULT_PORT_HANDLE).unwrap();

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
                        if let Ok(res) = parse_sql_select_item(&f, input_schema) {
                            select_expr.push(res)
                        }
                    }
                }
                _ => {
                    if let Ok(res) = parse_sql_select_item(s, input_schema) {
                        select_expr.push(res)
                    }
                }
            }
        }

        let mut output_schema = input_schema.clone();
        let mut fields = vec![];
        for e in select_expr.iter() {
            let field_name = e.0.clone();
            let field_type = e.1.get_type(input_schema)?;
            fields.push(FieldDefinition::new(
                field_name,
                field_type.return_type,
                field_type.nullable,
                field_type.source,
            ));
        }
        output_schema.fields = fields;

        Ok(output_schema)
    }

    fn build(
        &self,
        input_schemas: HashMap<PortHandle, Schema>,
        _output_schemas: HashMap<PortHandle, Schema>,
        _record_store: &ProcessorRecordStore,
    ) -> Result<Box<dyn Processor>, BoxedError> {
        let schema = match input_schemas.get(&DEFAULT_PORT_HANDLE) {
            Some(schema) => Ok(schema),
            None => Err(PipelineError::InvalidPortHandle(DEFAULT_PORT_HANDLE)),
        }?;

        match self
            .select
            .iter()
            .map(|item| parse_sql_select_item(item, schema))
            .collect::<Result<Vec<(String, Expression)>, PipelineError>>()
        {
            Ok(expressions) => Ok(Box::new(ProjectionProcessor::new(
                schema.clone(),
                expressions.into_iter().map(|e| e.1).collect(),
            ))),
            Err(error) => Err(error.into()),
        }
    }
}

pub(crate) fn parse_sql_select_item(
    sql: &SelectItem,
    schema: &Schema,
) -> Result<(String, Expression), PipelineError> {
    match sql {
        SelectItem::UnnamedExpr(sql_expr) => {
            match ExpressionBuilder::new(0).parse_sql_expression(true, sql_expr, schema) {
                Ok(expr) => Ok((sql_expr.to_string(), expr)),
                Err(error) => Err(error),
            }
        }
        SelectItem::ExprWithAlias { expr, alias } => {
            match ExpressionBuilder::new(0).parse_sql_expression(true, expr, schema) {
                Ok(expr) => Ok((alias.value.clone(), expr)),
                Err(error) => Err(error),
            }
        }
        SelectItem::Wildcard(_) => Err(PipelineError::InvalidOperator("*".to_string())),
        SelectItem::QualifiedWildcard(ref object_name, ..) => {
            Err(PipelineError::InvalidOperator(object_name.to_string()))
        }
    }
}
