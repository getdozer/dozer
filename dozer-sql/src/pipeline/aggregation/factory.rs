use std::collections::HashMap;

use dozer_core::dag::{
    dag::DEFAULT_PORT_HANDLE,
    errors::ExecutionError,
    node::{OutputPortDef, OutputPortDefOptions, PortHandle, Processor, ProcessorFactory},
};
use dozer_types::types::{FieldDefinition, Schema};
use sqlparser::ast::{Expr as SqlExpr, SelectItem};

use crate::pipeline::{
    errors::PipelineError,
    expression::{
        aggregate::AggregateFunctionType,
        builder::{ExpressionBuilder, ExpressionType},
        execution::{Expression, ExpressionExecutor},
    },
    projection::{factory::parse_sql_select_item, processor::ProjectionProcessor},
    projection::{factory::parse_sql_select_item, processor::ProjectionProcessor},
};

use super::{
    aggregator::Aggregator,
    processor::{AggregationProcessor, FieldRule},
};

pub struct AggregationProcessorFactory {
    select: Vec<SelectItem>,
    groupby: Vec<SqlExpr>,
}

impl AggregationProcessorFactory {
    /// Creates a new [`AggregationProcessorFactory`].
    pub fn new(select: Vec<SelectItem>, groupby: Vec<SqlExpr>) -> Self {
        Self { select, groupby }
    }
}

impl ProcessorFactory for AggregationProcessorFactory {
    fn get_input_ports(&self) -> Vec<PortHandle> {
        vec![DEFAULT_PORT_HANDLE]
    }

    fn get_output_ports(&self) -> Vec<OutputPortDef> {
        vec![OutputPortDef::new(
            DEFAULT_PORT_HANDLE,
            OutputPortDefOptions::default(),
        )]
    }

    fn get_output_schema(
        &self,
        _output_port: &PortHandle,
        input_schemas: &HashMap<PortHandle, Schema>,
    ) -> Result<Schema, ExecutionError> {
        let input_schema = input_schemas
            .get(&DEFAULT_PORT_HANDLE)
            .ok_or(ExecutionError::InvalidPortHandle(DEFAULT_PORT_HANDLE))?;
        let output_field_rules =
            get_aggregation_rules(&self.select, &self.groupby, input_schema).unwrap();
        if is_aggregation(&self.groupby, &output_field_rules) {
            return build_output_schema(input_schema, output_field_rules);
        }

        build_projection_schema(input_schema, &self.select)
        if is_aggregation(&self.groupby, &output_field_rules) {
            return build_output_schema(input_schema, output_field_rules);
        }

        build_projection_schema(input_schema, &self.select)
    }

    fn build(
        &self,
        input_schemas: HashMap<PortHandle, Schema>,
        _output_schemas: HashMap<PortHandle, Schema>,
    ) -> Result<Box<dyn Processor>, ExecutionError> {
        let input_schema = input_schemas
            .get(&DEFAULT_PORT_HANDLE)
            .ok_or(ExecutionError::InvalidPortHandle(DEFAULT_PORT_HANDLE))?;
        let output_field_rules =
            get_aggregation_rules(&self.select, &self.groupby, input_schema).unwrap();

        if is_aggregation(&self.groupby, &output_field_rules) {
            return Ok(Box::new(AggregationProcessor::new(
                output_field_rules,
                input_schema.clone(),
            )));
        }

        // Build a Projection
        match self
            .select
            .iter()
            .map(|item| parse_sql_select_item(item, input_schema))
            .collect::<Result<Vec<(String, Expression)>, PipelineError>>()
        {
            Ok(expressions) => Ok(Box::new(ProjectionProcessor::new(expressions))),
            Err(error) => Err(ExecutionError::InternalStringError(error.to_string())),
        }

        if is_aggregation(&self.groupby, &output_field_rules) {
            return Ok(Box::new(AggregationProcessor::new(
                output_field_rules,
                input_schema.clone(),
            )));
        }

        // Build a Projection
        match self
            .select
            .iter()
            .map(|item| parse_sql_select_item(item, input_schema))
            .collect::<Result<Vec<(String, Expression)>, PipelineError>>()
        {
            Ok(expressions) => Ok(Box::new(ProjectionProcessor::new(expressions))),
            Err(error) => Err(ExecutionError::InternalStringError(error.to_string())),
        }
    }
}

fn is_aggregation(groupby: &[SqlExpr], output_field_rules: &[FieldRule]) -> bool {
    if !groupby.is_empty() {
        return true;
    }

    output_field_rules
        .iter()
        .any(|rule| matches!(rule, FieldRule::Measure(_, _, _)))
}

fn is_aggregation(groupby: &[SqlExpr], output_field_rules: &[FieldRule]) -> bool {
    if !groupby.is_empty() {
        return true;
    }

    output_field_rules
        .iter()
        .any(|rule| matches!(rule, FieldRule::Measure(_, _, _)))
}

pub(crate) fn get_aggregation_rules(
    select: &[SelectItem],
    groupby: &[SqlExpr],
    groupby: &[SqlExpr],
    schema: &Schema,
) -> Result<Vec<FieldRule>, PipelineError> {
    let mut select_rules = select
    let mut select_rules = select
        .iter()
        .map(|item| parse_sql_aggregate_item(item, schema))
        .filter(|e| e.is_ok())
        .collect::<Result<Vec<FieldRule>, PipelineError>>()?;

    let mut groupby_rules = groupby
        .iter()
        .map(|expr| parse_sql_groupby_item(expr, schema))
        .collect::<Result<Vec<FieldRule>, PipelineError>>()?;

    select_rules.append(&mut groupby_rules);

    Ok(select_rules)
}

fn parse_sql_aggregate_item(
    item: &SelectItem,
    schema: &Schema,
) -> Result<FieldRule, PipelineError> {
    let builder = ExpressionBuilder {};

    match item {
        SelectItem::UnnamedExpr(sql_expr) => {
            let expression =
                builder.parse_sql_expression(&ExpressionType::Aggregation, sql_expr, schema)?;

            match get_aggregator(expression.0.clone(), schema) {
                Ok(aggregator) => Ok(FieldRule::Measure(
                    ExpressionBuilder {}
                        .parse_sql_expression(&ExpressionType::PreAggregation, sql_expr, schema)?
                        .0,
                    aggregator,
                    sql_expr.to_string(),
                )),
                Err(_) => Ok(FieldRule::Dimension(
                    expression.0,
                    true,
                    sql_expr.to_string(),
                )),
                Err(_) => Ok(FieldRule::Dimension(
                    expression.0,
                    true,
                    sql_expr.to_string(),
                )),
            }
        }
        SelectItem::ExprWithAlias { expr, alias } => Err(PipelineError::InvalidExpression(
            format!("Unsupported Expression {}:{}", expr, alias),
        )),
        SelectItem::Wildcard => Err(PipelineError::InvalidExpression(
            "Wildcard Operator is not supported".to_string(),
        )),
        SelectItem::QualifiedWildcard(ref _object_name) => Err(PipelineError::InvalidExpression(
            "Qualified Wildcard Operator is not supported".to_string(),
        )),
    }
}

fn parse_sql_groupby_item(
    sql_expression: &SqlExpr,
    schema: &Schema,
) -> Result<FieldRule, PipelineError> {
    Ok(FieldRule::Dimension(
        ExpressionBuilder {}.build(&ExpressionType::FullExpression, sql_expression, schema)?,
        false,
        sql_expression.to_string(),
    ))
}

fn parse_sql_groupby_item(
    sql_expression: &SqlExpr,
    schema: &Schema,
) -> Result<FieldRule, PipelineError> {
    Ok(FieldRule::Dimension(
        ExpressionBuilder {}.build(&ExpressionType::FullExpression, sql_expression, schema)?,
        false,
        sql_expression.to_string(),
    ))
}

fn get_aggregator(
    expression: Box<Expression>,
    schema: &Schema,
) -> Result<Aggregator, PipelineError> {
    match *expression {
        Expression::AggregateFunction { fun, args } => {
            let arg_type = args[0].get_type(schema);
            match (&fun, arg_type) {
                (AggregateFunctionType::Avg, _) => Ok(Aggregator::Avg),
                (AggregateFunctionType::Count, _) => Ok(Aggregator::Count),
                (AggregateFunctionType::Max, _) => Ok(Aggregator::Max),
                (AggregateFunctionType::Min, _) => Ok(Aggregator::Min),
                (AggregateFunctionType::Sum, _) => Ok(Aggregator::Sum),
                _ => Err(PipelineError::InvalidExpression(format!(
                    "Not implemented Aggregation function: {:?}",
                    fun
                ))),
            }
        }
        _ => Err(PipelineError::InvalidExpression(format!(
            "Not an Aggregation function: {:?}",
            expression
        ))),
    }
}

fn build_output_schema(
    input_schema: &Schema,
    output_field_rules: Vec<FieldRule>,
) -> Result<Schema, ExecutionError> {
    let mut output_schema = Schema::empty();

    for e in output_field_rules.iter().enumerate() {
        match e.1 {
            FieldRule::Measure(pre_aggr, aggr, name) => {
                output_schema.fields.push(FieldDefinition::new(
                    name.clone(),
                    aggr.get_return_type(
                        pre_aggr
                            .get_type(input_schema)
                            .map_err(|e| ExecutionError::InternalError(Box::new(e)))?,
                    ),
                    false,
                ));
            }

            FieldRule::Dimension(expression, is_value, name) => {
                if *is_value {
                    output_schema.fields.push(FieldDefinition::new(
                        name.clone(),
                        expression
                            .get_type(input_schema)
                            .map_err(|e| ExecutionError::InternalError(Box::new(e)))?,
                        true,
                    ));
                    output_schema.primary_index.push(e.0);
                }
            }
        }
    }
    Ok(output_schema)
}

fn build_projection_schema(
    input_schema: &Schema,
    select: &[SelectItem],
) -> Result<Schema, ExecutionError> {
    match select
        .iter()
        .map(|item| parse_sql_select_item(item, input_schema))
        .collect::<Result<Vec<(String, Expression)>, PipelineError>>()
    {
        Ok(expressions) => {
            let mut output_schema = Schema::empty();

            for e in expressions.iter() {
                let field_name = e.0.clone();
                let field_type =
                    e.1.get_type(input_schema)
                        .map_err(|e| ExecutionError::InternalError(Box::new(e)))?;
                let field_nullable = true;
                output_schema.fields.push(FieldDefinition::new(
                    field_name,
                    field_type,
                    field_nullable,
                ));
            }

            Ok(output_schema)
        }
        Err(error) => Err(ExecutionError::InternalStringError(error.to_string())),
    }
}
