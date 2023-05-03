use std::{collections::HashMap, time::Duration};

use dozer_core::{
    errors::ExecutionError,
    node::{OutputPortDef, OutputPortType, PortHandle, Processor, ProcessorFactory},
    DEFAULT_PORT_HANDLE,
};
use dozer_types::types::{DozerDuration, Schema, TimeUnit};
use sqlparser::ast::{Expr, FunctionArg, FunctionArgExpr, Value};

use crate::pipeline::{
    builder::SchemaSQLContext,
    errors::TableOperatorError,
    expression::{builder::ExpressionBuilder, execution::Expression},
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
            Some(operator) => Ok(Box::new(TableOperatorProcessor::new(
                operator,
                input_schema,
            ))),
            None => Err(ExecutionError::TableProcessorError(Box::new(
                TableOperatorError::InternalError("Invalid Table Operator".into()),
            ))),
        }
    }
}

pub(crate) fn operator_from_descriptor(
    descriptor: &TableOperatorDescriptor,
    schema: &Schema,
) -> Result<Option<TableOperatorType>, ExecutionError> {
    if &descriptor.name.to_uppercase() == "TTL" {
        let operator = lifetime_from_descriptor(descriptor, schema)
            .map_err(|e| ExecutionError::TableProcessorError(Box::new(e)))?;

        Ok(Some(operator.into()))
    } else {
        Err(ExecutionError::InternalError(
            descriptor.name.clone().into(),
        ))
    }
}

fn lifetime_from_descriptor(
    descriptor: &TableOperatorDescriptor,
    schema: &Schema,
) -> Result<LifetimeTableOperator, TableOperatorError> {
    let expression_arg = descriptor
        .args
        .get(1)
        .ok_or(TableOperatorError::MissingArgument(
            descriptor.name.to_owned(),
        ))?;
    let duration_arg = descriptor
        .args
        .get(2)
        .ok_or(TableOperatorError::MissingArgument(
            descriptor.name.to_owned(),
        ))?;

    let expression = get_expression(descriptor.name.to_owned(), expression_arg, schema)?;
    let duration = get_interval(descriptor.name.to_owned(), duration_arg)?;

    let operator = LifetimeTableOperator::new(None, expression, duration);

    Ok(operator)
}

fn get_interval(
    function_name: String,
    interval_arg: &FunctionArg,
) -> Result<DozerDuration, TableOperatorError> {
    match interval_arg {
        FunctionArg::Named { name, arg: _ } => {
            let column_name = ExpressionBuilder::normalize_ident(name);
            Err(TableOperatorError::InvalidInterval(
                column_name,
                function_name,
            ))
        }
        FunctionArg::Unnamed(arg_expr) => match arg_expr {
            FunctionArgExpr::Expr(expr) => match expr {
                Expr::Value(Value::SingleQuotedString(s) | Value::DoubleQuotedString(s)) => {
                    let interval: DozerDuration =
                        parse_duration_string(function_name.to_owned(), s).map_err(|_| {
                            TableOperatorError::InvalidInterval(s.to_owned(), function_name)
                        })?;
                    Ok(interval)
                }
                _ => Err(TableOperatorError::InvalidInterval(
                    expr.to_string(),
                    function_name,
                )),
            },
            FunctionArgExpr::QualifiedWildcard(_) => Err(TableOperatorError::InvalidInterval(
                "*".to_string(),
                function_name,
            )),
            FunctionArgExpr::Wildcard => Err(TableOperatorError::InvalidInterval(
                "*".to_string(),
                function_name,
            )),
        },
    }
}

fn get_expression(
    function_name: String,
    interval_arg: &FunctionArg,
    schema: &Schema,
) -> Result<Expression, TableOperatorError> {
    match interval_arg {
        FunctionArg::Named { name, arg: _ } => {
            let column_name = ExpressionBuilder::normalize_ident(name);
            Err(TableOperatorError::InvalidReference(
                column_name,
                function_name,
            ))
        }
        FunctionArg::Unnamed(arg_expr) => match arg_expr {
            FunctionArgExpr::Expr(expr) => {
                let mut builder = ExpressionBuilder::new(schema.fields.len());
                let expression = builder.build(false, expr, schema).map_err(|_| {
                    TableOperatorError::InvalidReference(expr.to_string(), function_name)
                })?;

                Ok(expression)
            }
            FunctionArgExpr::QualifiedWildcard(_) => Err(TableOperatorError::InvalidReference(
                "*".to_string(),
                function_name,
            )),
            FunctionArgExpr::Wildcard => Err(TableOperatorError::InvalidReference(
                "*".to_string(),
                function_name,
            )),
        },
    }
}

fn parse_duration_string(
    function_name: String,
    duration_string: &str,
) -> Result<DozerDuration, TableOperatorError> {
    let duration_string = duration_string
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");

    let duration_tokens = duration_string.split(' ').collect::<Vec<_>>();
    if duration_tokens.len() != 2 {
        return Err(TableOperatorError::InvalidInterval(
            duration_string,
            function_name,
        ));
    }

    let duration_value = duration_tokens[0].parse::<u64>().map_err(|_| {
        TableOperatorError::InvalidInterval(duration_string.to_owned(), function_name.clone())
    })?;

    let duration_unit = duration_tokens[1].to_uppercase();

    match duration_unit.as_str() {
        "MILLISECOND" | "MILLISECONDS" => Ok(DozerDuration(
            Duration::from_millis(duration_value),
            TimeUnit::Milliseconds,
        )),
        "SECOND" | "SECONDS" => Ok(DozerDuration(
            Duration::from_secs(duration_value),
            TimeUnit::Seconds,
        )),
        "MINUTE" | "MINUTES" => Ok(DozerDuration(
            Duration::from_secs(duration_value * 60),
            TimeUnit::Seconds,
        )),
        "HOUR" | "HOURS" => Ok(DozerDuration(
            Duration::from_secs(duration_value * 60 * 60),
            TimeUnit::Seconds,
        )),
        "DAY" | "DAYS" => Ok(DozerDuration(
            Duration::from_secs(duration_value * 60 * 60 * 24),
            TimeUnit::Seconds,
        )),
        _ => Err(TableOperatorError::InvalidInterval(
            duration_string,
            function_name,
        )),
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
