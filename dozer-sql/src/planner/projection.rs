#![allow(dead_code)]
use std::sync::Arc;

use crate::builder::string_from_sql_object_name;
use crate::errors::PipelineError;
use dozer_sql_expression::builder::ExpressionBuilder;
use dozer_sql_expression::execution::Expression;
use dozer_sql_expression::sqlparser::ast::{Expr, Ident, SelectItem};
use dozer_sql_expression::udf_registry::UdfRegistry;
use dozer_types::models::udf_config::UdfConfig;
use dozer_types::types::{FieldDefinition, Schema};
use tokio::runtime::Runtime;

#[derive(Clone, Copy)]
pub enum PrimaryKeyAction {
    Retain,
    Drop,
    Force,
}

pub struct CommonPlanner<'a> {
    input_schema: Schema,
    pub post_aggregation_schema: Schema,
    pub post_projection_schema: Schema,
    // Vector of aggregations to be appended to the original record
    pub aggregation_output: Vec<Expression>,
    pub having: Option<Expression>,
    pub groupby: Vec<Expression>,
    pub projection_output: Vec<Expression>,
    pub udfs: &'a [UdfConfig],
    pub runtime: Arc<Runtime>,
    udf_registry: Option<UdfRegistry>,
}

impl<'a> CommonPlanner<'_> {
    fn expression_builder(
        &self,
        offset: usize,
        aggregations: Vec<Expression>,
    ) -> ExpressionBuilder {
        let builder = ExpressionBuilder::from(offset, aggregations, self.runtime.clone());
        if let Some(registry) = &self.udf_registry {
            builder.with_udf_registry(registry.clone())
        } else {
            builder
        }
    }

    pub fn with_udf_registry(
        input_schema: Schema,
        udfs: &'a UdfRegistry,
        runtime: Arc<Runtime>,
    ) -> CommonPlanner<'a> {
        let mut planner = Self::new(input_schema, udfs.configs(), runtime);
        planner.udf_registry = Some(udfs.clone());
        planner
    }

    fn append_to_schema(
        expr: &Expression,
        alias: Option<String>,
        input_schema: &Schema,
        output_schema: &mut Schema,
    ) -> Result<(), PipelineError> {
        let expr_type = expr.get_type(input_schema)?;
        output_schema.fields.push(FieldDefinition::new(
            alias.unwrap_or_else(|| expr.to_string(input_schema)),
            expr_type.return_type,
            expr_type.nullable,
            expr_type.source,
        ));

        Ok(())
    }

    async fn add_select_item(&mut self, item: SelectItem) -> Result<(), PipelineError> {
        let expr_items: Vec<(Expr, Option<String>)> = match item {
            SelectItem::UnnamedExpr(expr) => vec![(expr, None)],
            SelectItem::ExprWithAlias { expr, alias } => vec![(expr, Some(alias.value))],
            SelectItem::QualifiedWildcard(alias, _) => self
                .input_schema
                .fields
                .iter()
                .filter(|c| c.check_from(alias.to_string()))
                .map(|col| {
                    (
                        Expr::CompoundIdentifier(vec![
                            Ident::new(string_from_sql_object_name(&alias)),
                            Ident::new(col.to_owned().name),
                        ]),
                        None,
                    )
                })
                .collect(),
            SelectItem::Wildcard(_) => self
                .input_schema
                .fields
                .iter()
                .map(|col| (Expr::Identifier(Ident::new(col.to_owned().name)), None))
                .collect(),
        };

        for (expr, alias) in expr_items {
            let mut builder = self.expression_builder(
                self.input_schema.fields.len() + self.aggregation_output.len(),
                Vec::new(),
            );
            let projection_expression = builder
                .build(true, &expr, &self.input_schema, self.udfs)
                .await?;

            for new_aggr in builder.aggregations {
                Self::append_to_schema(
                    &new_aggr,
                    alias.clone(),
                    &self.input_schema,
                    &mut self.post_aggregation_schema,
                )?;
                self.aggregation_output.push(new_aggr);
            }

            self.projection_output.push(projection_expression.clone());
            Self::append_to_schema(
                &projection_expression,
                alias,
                &self.post_aggregation_schema,
                &mut self.post_projection_schema,
            )?;
        }

        Ok(())
    }

    async fn add_join_item(&mut self, item: SelectItem) -> Result<(), PipelineError> {
        let expr_items: Vec<(Expr, Option<String>)> = match item {
            SelectItem::UnnamedExpr(expr) => vec![(expr, None)],
            SelectItem::ExprWithAlias { expr, alias } => vec![(expr, Some(alias.value))],
            SelectItem::QualifiedWildcard(_, _) => panic!("not supported yet"),
            SelectItem::Wildcard(_) => panic!("not supported yet"),
        };

        for (expr, alias) in expr_items {
            let mut builder = self.expression_builder(
                self.input_schema.fields.len() + self.aggregation_output.len(),
                Vec::new(),
            );
            let projection_expression = builder
                .build(true, &expr, &self.input_schema, self.udfs)
                .await?;

            for new_aggr in builder.aggregations {
                Self::append_to_schema(
                    &new_aggr,
                    alias.clone(),
                    &self.input_schema,
                    &mut self.post_aggregation_schema,
                )?;
                self.aggregation_output.push(new_aggr);
            }

            self.projection_output.push(projection_expression.clone());
            Self::append_to_schema(
                &projection_expression,
                alias,
                &self.post_aggregation_schema,
                &mut self.post_projection_schema,
            )?;
        }

        Ok(())
    }

    async fn add_having_item(&mut self, expr: Expr) -> Result<(), PipelineError> {
        let mut builder = self.expression_builder(
            self.input_schema.fields.len(),
            self.aggregation_output.clone(),
        );
        let having_expression = builder
            .build(true, &expr, &self.input_schema, self.udfs)
            .await?;

        let mut post_aggregation_schema = self.input_schema.clone();
        let mut aggregation_output = Vec::new();

        for new_aggr in builder.aggregations {
            Self::append_to_schema(
                &new_aggr,
                None,
                &self.input_schema,
                &mut post_aggregation_schema,
            )?;
            aggregation_output.push(new_aggr);
        }
        self.aggregation_output = aggregation_output;
        self.post_aggregation_schema = post_aggregation_schema;

        self.having = Some(having_expression);

        Ok(())
    }

    async fn add_groupby_items(&mut self, expr_items: Vec<Expr>) -> Result<(), PipelineError> {
        let mut indexes = vec![];
        let mut set_pk = true;
        for expr in expr_items {
            let mut builder = self.expression_builder(
                self.input_schema.fields.len() + self.aggregation_output.len(),
                Vec::new(),
            );
            let groupby_expression = builder
                .build(false, &expr, &self.input_schema, self.udfs)
                .await?;
            self.groupby.push(groupby_expression.clone());

            if let Some(e) = self
                .projection_output
                .iter()
                .enumerate()
                .find(|e| e.1 == &groupby_expression)
            {
                indexes.push(e.0);
            } else {
                set_pk = false
            }
        }

        if set_pk {
            indexes.sort();
            self.post_projection_schema.primary_index = indexes;
        }

        Ok(())
    }

    pub async fn plan(
        &mut self,
        projection: Vec<SelectItem>,
        group_by: Vec<Expr>,
        having: Option<Expr>,
    ) -> Result<(), PipelineError> {
        if let Some(registry) = &self.udf_registry {
            registry.prepare()?;
        }
        for expr in projection {
            self.add_select_item(expr).await?;
        }
        if !group_by.is_empty() {
            self.add_groupby_items(group_by).await?;
        }

        if let Some(having) = having {
            self.add_having_item(having).await?;
        }

        Ok(())
    }

    pub fn new(
        input_schema: Schema,
        udfs: &'a [UdfConfig],
        runtime: Arc<Runtime>,
    ) -> CommonPlanner<'a> {
        CommonPlanner {
            input_schema: input_schema.clone(),
            post_aggregation_schema: input_schema,
            post_projection_schema: Schema::default(),
            aggregation_output: Vec::new(),
            having: None,
            groupby: Vec::new(),
            projection_output: Vec::new(),
            udfs,
            runtime,
            udf_registry: None,
        }
    }
}
