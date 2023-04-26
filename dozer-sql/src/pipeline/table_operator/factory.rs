use std::collections::HashMap;

use dozer_core::{
    errors::ExecutionError,
    node::{OutputPortDef, OutputPortType, PortHandle, Processor, ProcessorFactory},
    DEFAULT_PORT_HANDLE,
};
use dozer_types::types::{DozerDuration, Schema, TimeUnit};
use sqlparser::ast::{Expr, FunctionArg, FunctionArgExpr};

use crate::pipeline::{
    builder::SchemaSQLContext, errors::TableOperatorError, expression::builder::ExpressionBuilder,
    pipeline_builder::from_builder::TableOperatorDescriptor,
};

use super::{
    lifetime::LifetimeTableOperator,
    operator::{TableOperator, TableOperatorType},
    processor::TableOperatorProcessor,
};

const SOURCE_TABLE_ARGUMENT: usize = 0;

#[derive(Debug)]
pub struct TableOperatorProcessorFactory {
    table: TableOperatorDescriptor,
    name: String,
}

impl TableOperatorProcessorFactory {
    pub fn new(table: TableOperatorDescriptor) -> Self {
        Self {
            table: table.to_owned(),
            name: format!("TOP_{0}_{1}", table.name, uuid::Uuid::new_v4()),
        }
    }

    pub fn get_name(&self) -> String {
        self.name.clone()
    }

    pub(crate) fn get_source_name(&self) -> Result<String, TableOperatorError> {
        let source_arg = self.table.args.get(SOURCE_TABLE_ARGUMENT).ok_or(
            TableOperatorError::MissingSourceArgument(self.table.name.to_owned()),
        )?;

        let source_name = get_source_name(self.table.name.to_owned(), source_arg)?;

        Ok(source_name)
    }
}

impl ProcessorFactory<SchemaSQLContext> for TableOperatorProcessorFactory {
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
        let (input_schema, _) = input_schemas
            .get(&DEFAULT_PORT_HANDLE)
            .ok_or(ExecutionError::InternalError(
                "Invalid Table Operator".to_string().into(),
            ))?
            .clone();

        let output_schema = match operator_from_descriptor(&self.table, &input_schema)
            .map_err(|e| ExecutionError::TableProcessorError(Box::new(e)))?
        {
            Some(operator) => operator
                .get_output_schema(&input_schema)
                .map_err(|e| ExecutionError::WindowProcessorFactoryError(Box::new(e)))?,
            None => {
                return Err(ExecutionError::TableProcessorError(Box::new(
                    TableOperatorError::InternalError("Invalid Table Operator".into()),
                )))
            }
        };

        Ok((output_schema, SchemaSQLContext::default()))
    }

    fn build(
        &self,
        input_schemas: HashMap<PortHandle, dozer_types::types::Schema>,
        _output_schemas: HashMap<PortHandle, dozer_types::types::Schema>,
    ) -> Result<Box<dyn Processor>, ExecutionError> {
        let input_schema = input_schemas
            .get(&DEFAULT_PORT_HANDLE)
            .ok_or(ExecutionError::InternalError(
                "Invalid Window".to_string().into(),
            ))?
            .clone();

        match operator_from_descriptor(&self.table, &input_schema)
            .map_err(|e| ExecutionError::TableProcessorError(Box::new(e)))?
        {
            Some(operator) => Ok(Box::new(TableOperatorProcessor::new(operator))),
            None => Err(ExecutionError::TableProcessorError(Box::new(
                TableOperatorError::InternalError("Invalid Table Operator".into()),
            ))),
        }
    }
}

pub(crate) fn operator_from_descriptor(
    descriptor: &TableOperatorDescriptor,
    _schema: &Schema,
) -> Result<Option<TableOperatorType>, ExecutionError> {
    if &descriptor.name.to_uppercase() == "TTL" {
        let operator = LifetimeTableOperator::new(
            None,
            DozerDuration(
                std::time::Duration::from_nanos(0_u64),
                TimeUnit::Nanoseconds,
            ),
        );

        Ok(Some(operator.into()))
    } else {
        Err(ExecutionError::InternalError(
            descriptor.name.clone().into(),
        ))
    }
}

fn get_source_name(function_name: String, arg: &FunctionArg) -> Result<String, TableOperatorError> {
    match arg {
        FunctionArg::Named { name, arg: _ } => {
            let source_name = ExpressionBuilder::normalize_ident(name);
            Err(TableOperatorError::InvalidSourceArgument(
                source_name,
                function_name,
            ))
        }
        FunctionArg::Unnamed(arg_expr) => match arg_expr {
            FunctionArgExpr::Expr(expr) => match expr {
                Expr::Identifier(ident) => {
                    let source_name = ExpressionBuilder::normalize_ident(ident);
                    Ok(source_name)
                }
                Expr::CompoundIdentifier(ident) => {
                    let source_name = ExpressionBuilder::fullname_from_ident(ident);
                    Ok(source_name)
                }
                _ => Err(TableOperatorError::InvalidSourceArgument(
                    expr.to_string(),
                    function_name,
                )),
            },
            FunctionArgExpr::QualifiedWildcard(_) => Err(
                TableOperatorError::InvalidSourceArgument("*".to_string(), function_name),
            ),
            FunctionArgExpr::Wildcard => Err(TableOperatorError::InvalidSourceArgument(
                "*".to_string(),
                function_name,
            )),
        },
    }
}
