use crate::pipeline::aggregation::factory::AggregationProcessorFactory;
use crate::pipeline::builder::PipelineError::InvalidQuery;
use crate::pipeline::errors::PipelineError;
use crate::pipeline::selection::factory::SelectionProcessorFactory;
use dozer_core::dag::app::AppPipeline;
use dozer_core::dag::app::PipelineEntryPoint;
use dozer_core::dag::appsource::AppSourceId;
use dozer_core::dag::node::PortHandle;
use dozer_core::dag::DEFAULT_PORT_HANDLE;
use sqlparser::ast::{Join, TableFactor, TableWithJoins};
use sqlparser::{
    ast::{Query, Select, SetExpr, Statement},
    dialect::AnsiDialect,
    parser::Parser,
};
use std::collections::HashMap;
use std::sync::Arc;
use crate::pipeline::expression::builder::{ExpressionBuilder, NameOrAlias};

use super::errors::UnsupportedSqlError;
use super::product::factory::FromProcessorFactory;

#[derive(Debug, Clone, Default)]
pub struct SchemaSQLContext {}

/// The struct contains some contexts during query to pipeline.
#[derive(Debug, Clone, Default)]
pub struct QueryContext {
    pub pipeline_map: HashMap<String, (String, PortHandle)>,
}

#[derive(Debug, Clone)]
pub struct IndexedTableWithJoins {
    pub relation: (NameOrAlias, TableFactor),
    pub joins: Vec<(NameOrAlias, Join)>,
}

pub fn statement_to_pipeline(
    sql: &str,
) -> Result<(AppPipeline<SchemaSQLContext>, (String, PortHandle)), PipelineError> {
    let dialect = AnsiDialect {};
    let mut ctx = QueryContext::default();

    let ast = Parser::parse_sql(&dialect, sql).unwrap();
    let query_name = NameOrAlias(format!("query_{}", uuid::Uuid::new_v4()), None);
    let statement = ast.get(0).expect("First statement is missing").to_owned();

    let mut pipeline = AppPipeline::new();
    if let Statement::Query(query) = statement {
        query_to_pipeline(&query_name, &query, &mut pipeline, &mut ctx, false)?;
    };
    let node = ctx
        .pipeline_map
        .get(&query_name.0)
        .expect("query should have been initialized")
        .to_owned();
    Ok((pipeline, node))
}

fn query_to_pipeline(
    processor_name: &NameOrAlias,
    query: &Query,
    pipeline: &mut AppPipeline<SchemaSQLContext>,
    query_ctx: &mut QueryContext,
    stateful: bool,
) -> Result<(), PipelineError> {
    // return error if there is unsupported syntax
    if !query.order_by.is_empty() {
        return Err(PipelineError::UnsupportedSqlError(
            UnsupportedSqlError::OrderByError,
        ));
    }

    if query.limit.is_some() || query.offset.is_some() {
        return Err(PipelineError::UnsupportedSqlError(
            UnsupportedSqlError::LimitOffsetError,
        ));
    }

    // Attach the first pipeline if there is with clause
    if let Some(with) = &query.with {
        if with.recursive {
            return Err(PipelineError::UnsupportedSqlError(
                UnsupportedSqlError::Recursive,
            ));
        }

        for table in &with.cte_tables {
            if table.from.is_some() {
                return Err(PipelineError::UnsupportedSqlError(
                    UnsupportedSqlError::CteFromError,
                ));
            }
            let table_name = table.alias.name.to_string();
            if query_ctx.pipeline_map.contains_key(&table_name) {
                return Err(InvalidQuery(format!(
                    "WITH query name {table_name:?} specified more than once"
                )));
            }
            query_to_pipeline(
                &NameOrAlias(table_name.clone(), Some(table_name)),
                &table.query,
                pipeline,
                query_ctx,
                true,
            )?;
        }
    };

    match *query.body.clone() {
        SetExpr::Select(select) => {
            select_to_pipeline(processor_name, *select, pipeline, query_ctx, stateful)?;
        }
        SetExpr::Query(query) => {
            let query_name = format!("subquery_{}", uuid::Uuid::new_v4());
            let mut ctx = QueryContext::default();
            query_to_pipeline(
                &NameOrAlias(query_name, None),
                &query,
                pipeline,
                &mut ctx,
                stateful,
            )?
        }
        _ => {
            return Err(PipelineError::UnsupportedSqlError(
                UnsupportedSqlError::SelectOnlyError,
            ))
        }
    };
    Ok(())
}

fn select_to_pipeline(
    processor_name: &NameOrAlias,
    select: Select,
    pipeline: &mut AppPipeline<SchemaSQLContext>,
    query_ctx: &mut QueryContext,
    stateful: bool,
) -> Result<(), PipelineError> {
    // FROM clause
    if select.from.len() != 1 {
        return Err(PipelineError::UnsupportedSqlError(
            UnsupportedSqlError::FromCommaSyntax,
        ));
    }

    let input_tables = get_input_tables(&select.from[0], pipeline, query_ctx)?;

    let product = FromProcessorFactory::new(input_tables.clone());

    let input_endpoints = get_entry_points(&input_tables, &mut query_ctx.pipeline_map)?;

    let gen_product_name = format!("product_{}", uuid::Uuid::new_v4());
    let gen_agg_name = format!("agg_{}", uuid::Uuid::new_v4());
    let gen_selection_name = format!("select_{}", uuid::Uuid::new_v4());
    pipeline.add_processor(Arc::new(product), &gen_product_name, input_endpoints);

    let input_names = get_input_names(&input_tables);
    for (port_index, table_name) in input_names.iter().enumerate() {
        if let Some((processor_name, processor_port)) = query_ctx.pipeline_map.get(&table_name.0) {
            pipeline.connect_nodes(
                processor_name,
                Some(*processor_port),
                &gen_product_name,
                Some(port_index as PortHandle),
            )?;
        }
    }

    let aggregation = AggregationProcessorFactory::new(select.clone(), stateful);

    pipeline.add_processor(Arc::new(aggregation), &gen_agg_name, vec![]);

    // Where clause
    if let Some(selection) = select.selection {
        let selection = SelectionProcessorFactory::new(selection);

        pipeline.add_processor(Arc::new(selection), &gen_selection_name, vec![]);

        pipeline.connect_nodes(
            &gen_product_name,
            Some(DEFAULT_PORT_HANDLE),
            &gen_selection_name,
            Some(DEFAULT_PORT_HANDLE),
        )?;

        pipeline.connect_nodes(
            &gen_selection_name,
            Some(DEFAULT_PORT_HANDLE),
            &gen_agg_name,
            Some(DEFAULT_PORT_HANDLE),
        )?;
    } else {
        pipeline.connect_nodes(
            &gen_product_name,
            Some(DEFAULT_PORT_HANDLE),
            &gen_agg_name,
            Some(DEFAULT_PORT_HANDLE),
        )?;
    }

    query_ctx.pipeline_map.insert(
        processor_name.0.clone(),
        (gen_agg_name, DEFAULT_PORT_HANDLE),
    );

    Ok(())
}

/// Returns a vector of input port handles and relative table name
///
/// # Errors
///
/// This function will return an error if it's not possible to get an input name.
pub fn get_input_tables(
    from: &TableWithJoins,
    pipeline: &mut AppPipeline<SchemaSQLContext>,
    query_ctx: &mut QueryContext,
) -> Result<IndexedTableWithJoins, PipelineError> {
    let name = get_from_source(&from.relation, pipeline, query_ctx)?;
    let mut joins = vec![];

    for join in from.joins.iter() {
        let input_name = get_from_source(&join.relation, pipeline, query_ctx)?;
        joins.push((input_name.clone(), join.clone()));
    }

    Ok(IndexedTableWithJoins {
        relation: (name, from.relation.clone()),
        joins,
    })
}

pub fn get_input_names(input_tables: &IndexedTableWithJoins) -> Vec<NameOrAlias> {
    let mut input_names = vec![];
    input_names.push(input_tables.relation.0.clone());

    for join in &input_tables.joins {
        input_names.push(join.0.clone());
    }
    input_names
}
pub fn get_entry_points(
    input_tables: &IndexedTableWithJoins,
    pipeline_map: &mut HashMap<String, (String, PortHandle)>,
) -> Result<Vec<PipelineEntryPoint>, PipelineError> {
    let mut endpoints = vec![];

    let input_names = get_input_names(input_tables);

    for (input_port, table) in input_names.iter().enumerate() {
        let name = table.0.clone();
        if !pipeline_map.contains_key(&name) {
            endpoints.push(PipelineEntryPoint::new(
                AppSourceId::new(name, None),
                input_port as PortHandle,
            ));
        }
    }

    Ok(endpoints)
}

pub fn get_from_source(
    relation: &TableFactor,
    pipeline: &mut AppPipeline<SchemaSQLContext>,
    query_ctx: &mut QueryContext,
) -> Result<NameOrAlias, PipelineError> {
    match relation {
        TableFactor::Table { name, alias, .. } => {
            let input_name = name
                .0
                .iter()
                .map(ExpressionBuilder::normalize_ident)
                .collect::<Vec<String>>()
                .join(".");
            let alias_name = alias
                .as_ref()
                .map(|a| ExpressionBuilder::fullname_from_ident(&[a.name.clone()]));

            Ok(NameOrAlias(input_name, alias_name))
        }
        TableFactor::Derived {
            lateral: _,
            subquery,
            alias,
        } => {
            let name = format!("derived_{}", uuid::Uuid::new_v4());
            let alias_name = alias
                .as_ref()
                .map(|alias_ident| ExpressionBuilder::fullname_from_ident(&[alias_ident.name.clone()]));

            let name_or = NameOrAlias(name, alias_name);
            query_to_pipeline(&name_or, subquery, pipeline, query_ctx, false)?;

            Ok(name_or)
        }
        _ => Err(PipelineError::UnsupportedSqlError(
            UnsupportedSqlError::JoinTable,
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::statement_to_pipeline;

    #[test]
    fn parse_sql_pipeline() {
        let statements: Vec<&str> = vec![
            r#"
                SELECT
                    a.name as "Genre",
                    SUM(amount) as "Gross Revenue(in $)"
                FROM
                (
                    SELECT
                        c.name,
                        f.title,
                        p.amount
                    FROM film f
                    LEFT JOIN film_category fc
                        ON fc.film_id = f.film_id
                    LEFT JOIN category c
                        ON fc.category_id = c.category_id
                    LEFT JOIN inventory i
                        ON i.film_id = f.film_id
                    LEFT JOIN rental r
                        ON r.inventory_id = i.inventory_id
                    LEFT JOIN payment p
                        ON p.rental_id = r.rental_id
                    WHERE p.amount IS NOT NULL
                ) a

                GROUP BY name;
            "#,
            r#"
                SELECT
                    c.name,
                    f.title,
                    p.amount
                FROM film f
                LEFT JOIN film_category fc
                    "#,
            r#"
                WITH tbl as (select id from a)
                select id from tbl
                    "#,
            r#"
                WITH tbl as (select id from  a),
                tbl2 as (select id from tbl)
                select id from tbl2
                    "#,
            r#"
                WITH cte_table1 as (select id_dt1 from (select id_t1 from table_1) as derived_table_1),
                cte_table2 as (select id_ct1 from cte_table1)
                select id_ct2 from cte_table2
            "#,
            r#"
                with tbl as (select id, ticker from stocks)
                select tbl.id from  stocks join tbl on tbl.id = stocks.id;
            "#,
        ];
        for sql in statements {
            let _pipeline = statement_to_pipeline(sql).unwrap();
        }
    }
}
