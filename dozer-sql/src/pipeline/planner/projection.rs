#![allow(dead_code)]

use crate::pipeline::errors::PipelineError;
use crate::pipeline::expression::builder_new::{
    AggregationMeasure, ExpressionBuilder, ExpressionContext,
};
use crate::pipeline::expression::execution::{Expression, ExpressionExecutor};
use dozer_types::types::{FieldDefinition, Schema, SourceDefinition};
use sqlparser::ast::{Expr as SqlExpr, Ident, SelectItem};

pub enum PrimaryKeyAction {
    Retain,
    Drop,
    Force,
}

struct ProjectionElement {
    expr: Box<Expression>,
    projected: bool,
}

impl ProjectionElement {
    pub fn new(expr: Box<Expression>, projected: bool) -> Self {
        Self { expr, projected }
    }
}

struct ProjectionPlanner {
    input_schema: Schema,
    output_schema: Schema,
    // Vector of aggregations to be appended to the original record
    aggregation_output: Vec<AggregationMeasure>,
    projection_output: Vec<ProjectionElement>,
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

    fn add_select_item(
        &mut self,
        expr: SelectItem,
        output_name: Option<String>,
    ) -> Result<(), PipelineError> {
        match expr {
            SelectItem::UnnamedExpr(expr) => {}
            SelectItem::ExprWithAlias { expr, alias } => {}
            SelectItem::QualifiedWildcard(_, _) => panic!("not supported yet"),
            SelectItem::Wildcard(_) => panic!("not supported yet"),
        }

        let mut context =
            ExpressionContext::new(self.input_schema.fields.len() + self.aggregation_output.len());
        let projection_expression =
            ExpressionBuilder::build(&mut context, true, &expr, &self.input_schema)?;

        self.aggregation_output.extend(context.aggrgeations);
        //  self.projection_output.push(projection_expression);
        Ok(())
    }
}
