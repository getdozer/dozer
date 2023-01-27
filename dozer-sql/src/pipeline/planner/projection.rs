#![allow(dead_code)]

use crate::pipeline::errors::PipelineError;
use crate::pipeline::expression::builder_new::{ExpressionBuilder, ExpressionContext};
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
    pub post_aggregation_schema: Schema,
    pub post_projection_schema: Schema,
    // Vector of aggregations to be appended to the original record
    pub aggregation_output: Vec<Expression>,
    pub projection_output: Vec<Box<Expression>>,
}

impl ProjectionPlanner {
    fn append_to_output_schema(
        &mut self,
        expr: &Expression,
        alias: Option<String>,
        pk_action: PrimaryKeyAction,
        schema: &mut Schema,
    ) -> Result<(), PipelineError> {
        let expr_type = expr.get_type(&self.input_schema)?;
        let primary_key = match pk_action {
            PrimaryKeyAction::Force => true,
            PrimaryKeyAction::Drop => false,
            PrimaryKeyAction::Retain => expr_type.is_primary_key,
        };

        schema.fields.push(FieldDefinition::new(
            alias.unwrap_or(expr.to_string(&self.input_schema)),
            expr_type.return_type,
            expr_type.nullable,
            expr_type.source,
        ));

        if primary_key {
            schema.primary_index.push(schema.fields.len() - 1);
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

            for new_aggr in context.aggrgeations {
                self.aggregation_output.push(new_aggr);
                //  self.append_to_schema()
            }

            // if context.aggrgeations.len() > 0 {
            //     self.aggregation_output.extend(context.aggrgeations);
            // }
            //
            // self.projection_output.push(projection_expression.clone());
            // self.append_to_output_schema(&projection_expression, alias, pk_action)?;
        }

        Ok(())
    }
    pub fn new(input_schema: Schema) -> Self {
        Self {
            input_schema: input_schema.clone(),
            post_aggregation_schema: input_schema.to_owned(),
            post_projection_schema: Schema::empty(),
            aggregation_output: Vec::new(),
            projection_output: Vec::new(),
        }
    }
}
