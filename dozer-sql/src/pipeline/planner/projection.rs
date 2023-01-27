#![allow(dead_code)]

use crate::pipeline::errors::PipelineError;
use crate::pipeline::expression::builder_new::{
    AggregationMeasure, ExpressionBuilder, ExpressionContext,
};
use crate::pipeline::expression::execution::{Expression, ExpressionExecutor};
use dozer_types::types::{FieldDefinition, Schema, SourceDefinition};
use sqlparser::ast::{Expr as SqlExpr, Expr, Ident, SelectItem};
use std::vec;

#[derive(Clone, Copy)]
pub enum PrimaryKeyAction {
    Retain,
    Drop,
    Force,
}

pub struct ProjectionPlanner {
    input_schema: Schema,
    output_schema: Schema,
    // Vector of aggregations to be appended to the original record
    aggregation_output: Vec<AggregationMeasure>,
    projection_output: Vec<Box<Expression>>,
}

impl ProjectionPlanner {
    fn append_to_output_schema(
        &mut self,
        expr: &Expression,
        alias: Option<String>,
        pk_action: PrimaryKeyAction,
    ) -> Result<(), PipelineError> {
        let expr_type = expr.get_type(&self.input_schema)?;
        let primary_key = match pk_action {
            PrimaryKeyAction::Force => true,
            PrimaryKeyAction::Drop => false,
            PrimaryKeyAction::Retain => expr_type.is_primary_key,
        };

        self.output_schema.fields.push(FieldDefinition::new(
            alias.unwrap_or(expr.to_string(&self.input_schema)),
            expr_type.return_type,
            expr_type.nullable,
            expr_type.source,
        ));

        if primary_key {
            self.output_schema
                .primary_index
                .push(self.output_schema.fields.len() - 1);
        }
        Ok(())
    }

    pub fn add_select_item(
        &mut self,
        item: SelectItem,
        pk_action: PrimaryKeyAction,
    ) -> Result<(), PipelineError> {
        let expr_items: Vec<(Expr, Option<String>)> = match item {
            SelectItem::UnnamedExpr(expr) => vec![(expr, None)],
            SelectItem::ExprWithAlias { expr, alias } => vec![(expr, Some(alias.value))],
            SelectItem::QualifiedWildcard(_, _) => panic!("not supported yet"),
            SelectItem::Wildcard(_) => panic!("not supported yet"),
        };

        for (expr, alias) in expr_items {
            let mut context = ExpressionContext::new(
                &self.input_schema.fields.len() + &self.aggregation_output.len(),
            );
            let projection_expression =
                ExpressionBuilder::build(&mut context, true, &expr, &self.input_schema)?;

            self.aggregation_output.extend(context.aggrgeations);
            self.projection_output.push(projection_expression.clone());
            self.append_to_output_schema(&projection_expression, alias, pk_action)?;
        }

        Ok(())
    }
    pub fn new(input_schema: Schema) -> Self {
        Self {
            input_schema,
            output_schema: Schema::empty(),
            aggregation_output: Vec::new(),
            projection_output: Vec::new(),
        }
    }
}
