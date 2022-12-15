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
        build_output_schema(input_schema, output_field_rules)
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
        Ok(Box::new(AggregationProcessor::new(
            output_field_rules,
            input_schema.clone(),
        )))
    }
}

pub(crate) fn get_aggregation_rules(
    select: &[SelectItem],
    _groupby: &[SqlExpr],
    schema: &Schema,
) -> Result<Vec<FieldRule>, PipelineError> {
    let select_rules = select
        .iter()
        .map(|item| parse_sql_aggregate_item(item, schema))
        .filter(|e| e.is_ok())
        .collect::<Result<Vec<FieldRule>, PipelineError>>()?;

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
                Err(_) => Ok(FieldRule::Dimension(expression.0, sql_expr.to_string())),
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
                    aggr.get_return_type(pre_aggr.get_type(input_schema)),
                    false,
                ));
            }

            FieldRule::Dimension(expression, name) => {
                //let src_fld = input_schema.get_field_index(idx.as_str())?;
                output_schema.fields.push(FieldDefinition::new(
                    name.clone(),
                    expression.get_type(input_schema),
                    true,
                ));
                output_schema.primary_index.push(e.0);
            }
        }
    }
    Ok(output_schema)
}
