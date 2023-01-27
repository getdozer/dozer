#![allow(dead_code)]

use crate::pipeline::errors::PipelineError;
use crate::pipeline::expression::builder_new::{ExpressionBuilder, ExpressionContext};
use crate::pipeline::expression::execution::{Expression, ExpressionExecutor};
use dozer_types::types::{FieldDefinition, Schema};
use sqlparser::ast::{Expr, Select, SelectItem};
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
    pub having: Option<Expression>,
    pub groupby: Vec<Expression>,
    pub projection_output: Vec<Expression>,
}

impl ProjectionPlanner {
    fn append_to_schema(
        expr: &Expression,
        alias: Option<String>,
        pk_action: PrimaryKeyAction,
        input_schema: &Schema,
        output_schema: &mut Schema,
    ) -> Result<(), PipelineError> {
        let expr_type = expr.get_type(input_schema)?;
        let primary_key = match pk_action {
            PrimaryKeyAction::Force => true,
            PrimaryKeyAction::Drop => false,
            PrimaryKeyAction::Retain => expr_type.is_primary_key,
        };

        output_schema.fields.push(FieldDefinition::new(
            alias.unwrap_or_else(|| expr.to_string(input_schema)),
            expr_type.return_type,
            expr_type.nullable,
            expr_type.source,
        ));

        if primary_key {
            output_schema
                .primary_index
                .push(output_schema.fields.len() - 1);
        }
        Ok(())
    }

    fn add_select_item(&mut self, item: SelectItem) -> Result<(), PipelineError> {
        //
        let expr_items: Vec<(Expr, Option<String>)> = match item {
            SelectItem::UnnamedExpr(expr) => vec![(expr, None)],
            SelectItem::ExprWithAlias { expr, alias } => vec![(expr, Some(alias.value))],
            SelectItem::QualifiedWildcard(_, _) => panic!("not supported yet"),
            SelectItem::Wildcard(_) => panic!("not supported yet"),
        };

        for (expr, alias) in expr_items {
            let mut context = ExpressionContext::new(
                self.input_schema.fields.len() + self.aggregation_output.len(),
            );
            let projection_expression =
                ExpressionBuilder::build(&mut context, true, &expr, &self.input_schema)?;

            for new_aggr in context.aggrgeations {
                Self::append_to_schema(
                    &new_aggr,
                    alias.clone(),
                    PrimaryKeyAction::Drop,
                    &self.input_schema,
                    &mut self.post_aggregation_schema,
                )?;
                self.aggregation_output.push(new_aggr);
            }

            self.projection_output.push(*projection_expression.clone());
            Self::append_to_schema(
                &projection_expression,
                alias,
                PrimaryKeyAction::Retain,
                &self.post_aggregation_schema,
                &mut self.post_projection_schema,
            )?;
        }

        Ok(())
    }

    fn add_having_item(&mut self, expr: Expr) -> Result<(), PipelineError> {
        //

        let mut context = ExpressionContext::from(
            self.input_schema.fields.len(),
            self.aggregation_output.clone(),
        );
        let having_expression =
            ExpressionBuilder::build(&mut context, true, &expr, &self.input_schema)?;

        let mut post_aggregation_schema = self.input_schema.clone();
        let mut aggregation_output = Vec::new();

        for new_aggr in context.aggrgeations {
            Self::append_to_schema(
                &new_aggr,
                None,
                PrimaryKeyAction::Drop,
                &self.input_schema,
                &mut post_aggregation_schema,
            )?;
            aggregation_output.push(new_aggr);
        }
        self.aggregation_output = aggregation_output;
        self.post_aggregation_schema = post_aggregation_schema;

        self.having = Some(*having_expression);

        Ok(())
    }

    fn add_groupby_items(&mut self, expr_items: Vec<Expr>) -> Result<(), PipelineError> {
        //

        for expr in expr_items {
            let mut context = ExpressionContext::new(
                self.input_schema.fields.len() + self.aggregation_output.len(),
            );
            let groupby_expression =
                ExpressionBuilder::build(&mut context, false, &expr, &self.input_schema)?;
            self.groupby.push(*groupby_expression.clone());
        }

        Ok(())
    }

    pub fn plan(&mut self, select: Select) -> Result<(), PipelineError> {
        for expr in select.projection {
            self.add_select_item(expr)?;
        }
        if !select.group_by.is_empty() {
            self.add_groupby_items(select.group_by)?;
        }

        if let Some(having) = select.having {
            self.add_having_item(having)?;
        }

        Ok(())
    }

    pub fn new(input_schema: Schema) -> Self {
        Self {
            input_schema: input_schema.clone(),
            post_aggregation_schema: input_schema,
            post_projection_schema: Schema::empty(),
            aggregation_output: Vec::new(),
            having: None,
            groupby: Vec::new(),
            projection_output: Vec::new(),
        }
    }
}
