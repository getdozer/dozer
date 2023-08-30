use std::{collections::HashMap, time::Duration};

use dozer_core::{
    node::{OutputPortDef, OutputPortType, PortHandle, Processor, ProcessorFactory},
    processor_record::ProcessorRecordStore,
    DEFAULT_PORT_HANDLE,
};
use dozer_types::{errors::internal::BoxedError, types::Schema};
use sqlparser::ast::{Expr, FunctionArg, FunctionArgExpr, Value};

use crate::pipeline::{
    errors::{PipelineError, TableOperatorError},
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
    id: String,
    table: TableOperatorDescriptor,
    name: String,
}

impl TableOperatorProcessorFactory {
    pub fn new(id: String, table: TableOperatorDescriptor) -> Self {
        Self {
            id: id.clone(),
            table,
            name: id,
        }
    }

    pub(crate) fn get_source_name(&self) -> Result<String, TableOperatorError> {
        let source_arg = self.table.args.get(SOURCE_TABLE_ARGUMENT).ok_or(
            TableOperatorError::MissingSourceArgument(self.table.name.to_owned()),
        )?;

        let source_name = get_source_name(self.table.name.to_owned(), source_arg)?;

        Ok(source_name)
    }
}

impl ProcessorFactory for TableOperatorProcessorFactory {
    fn id(&self) -> String {
        self.id.clone()
    }

    fn type_name(&self) -> String {
        self.name.clone()
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
        let input_schema = input_schemas
            .get(&DEFAULT_PORT_HANDLE)
            .ok_or(PipelineError::InvalidPortHandle(DEFAULT_PORT_HANDLE))?;

        let output_schema =
            match operator_from_descriptor(&self.table, input_schema)? {
                Some(operator) => operator
                    .get_output_schema(input_schema)
                    .map_err(PipelineError::TableOperatorError)?,
                None => {
                    return Err(PipelineError::TableOperatorError(
                        TableOperatorError::InternalError("Invalid Table Operator".into()),
                    )
                    .into())
                }
            };

        Ok(output_schema)
    }

    fn build(
        &self,
        input_schemas: HashMap<PortHandle, dozer_types::types::Schema>,
        _output_schemas: HashMap<PortHandle, dozer_types::types::Schema>,
        _record_store: &ProcessorRecordStore,
    ) -> Result<Box<dyn Processor>, BoxedError> {
        let input_schema = input_schemas
            .get(&DEFAULT_PORT_HANDLE)
            .ok_or(PipelineError::InternalError(
                "Invalid Window".to_string().into(),
            ))?
            .clone();

        match operator_from_descriptor(&self.table, &input_schema)? {
            Some(operator) => Ok(Box::new(TableOperatorProcessor::new(
                self.id.clone(),
                operator,
                input_schema,
            ))),
            None => Err(
                PipelineError::TableOperatorError(TableOperatorError::InternalError(
                    "Invalid Table Operator".into(),
                ))
                .into(),
            ),
        }
    }
}

pub(crate) fn operator_from_descriptor(
    descriptor: &TableOperatorDescriptor,
    schema: &Schema,
) -> Result<Option<TableOperatorType>, PipelineError> {
    if &descriptor.name.to_uppercase() == "TTL" {
        let operator = lifetime_from_descriptor(descriptor, schema)?;

        Ok(Some(operator.into()))
    } else {
        Err(PipelineError::InternalError(descriptor.name.clone().into()))
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
) -> Result<Duration, TableOperatorError> {
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
                    let interval =
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
) -> Result<Duration, TableOperatorError> {
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
        "MILLISECOND" | "MILLISECONDS" => Ok(Duration::from_millis(duration_value)),
        "SECOND" | "SECONDS" => Ok(Duration::from_secs(duration_value)),
        "MINUTE" | "MINUTES" => Ok(Duration::from_secs(duration_value * 60)),
        "HOUR" | "HOURS" => Ok(Duration::from_secs(duration_value * 60 * 60)),
        "DAY" | "DAYS" => Ok(Duration::from_secs(duration_value * 60 * 60 * 24)),
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
