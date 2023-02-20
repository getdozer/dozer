use crate::pipeline::aggregation::factory::AggregationProcessorFactory;
use crate::pipeline::builder::PipelineError::InvalidQuery;
use crate::pipeline::errors::PipelineError;
use crate::pipeline::expression::builder::{ExpressionBuilder, NameOrAlias};
use crate::pipeline::selection::factory::SelectionProcessorFactory;
use dozer_core::app::AppPipeline;
use dozer_core::app::PipelineEntryPoint;
use dozer_core::appsource::AppSourceId;
use dozer_core::node::PortHandle;
use dozer_core::DEFAULT_PORT_HANDLE;
use sqlparser::ast::{Join, TableFactor, TableWithJoins};
use sqlparser::{
    ast::{Query, Select, SetExpr, Statement},
    dialect::AnsiDialect,
    parser::Parser,
};
use std::collections::HashMap;
use std::sync::Arc;

use super::errors::{SqlError, UnsupportedSqlError};
use super::product::factory::FromProcessorFactory;

#[derive(Debug, Clone, Default)]
pub struct SchemaSQLContext {}

#[derive(Debug, Clone)]
pub struct QueryTableInfo {
    // Name to connect in dag
    pub node: String,
    // Port to connect in dag
    pub port: PortHandle,
    // If this table is originally from a source or created in transforms
    pub is_derived: bool,
    // TODO add:indexes to the tables
}

pub struct TableInfo {
    pub name: NameOrAlias,
    pub override_name: Option<String>,
    pub is_derived: bool,
}
/// The struct contains some contexts during query to pipeline.
#[derive(Debug, Clone, Default)]
pub struct QueryContext {
    // Internal tables map, used to store the tables that are created by the queries
    pub pipeline_map: HashMap<(usize, String), QueryTableInfo>,

    // Output tables map that are marked with "INTO" used to store the tables, these can be exposed to sinks.
    pub output_tables_map: HashMap<String, QueryTableInfo>,

    // Used Sources
    pub used_sources: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct IndexedTableWithJoins {
    pub relation: (NameOrAlias, TableFactor),
    pub joins: Vec<(NameOrAlias, Join)>,
}
pub fn statement_to_pipeline(
    sql: &str,
    pipeline: &mut AppPipeline<SchemaSQLContext>,
    override_name: Option<String>,
) -> Result<QueryContext, PipelineError> {
    let dialect = AnsiDialect {};
    let mut ctx = QueryContext::default();

    let ast = Parser::parse_sql(&dialect, sql).unwrap();
    let query_name = NameOrAlias(format!("query_{}", uuid::Uuid::new_v4()), None);

    for (idx, statement) in ast.iter().enumerate() {
        match statement {
            Statement::Query(query) => {
                query_to_pipeline(
                    &TableInfo {
                        name: query_name.clone(),
                        is_derived: false,
                        override_name: override_name.clone(),
                    },
                    query,
                    pipeline,
                    &mut ctx,
                    false,
                    idx,
                )?;
            }
            s => {
                return Err(PipelineError::UnsupportedSqlError(
                    UnsupportedSqlError::GenericError(s.to_string()),
                ))
            }
        }
    }

    Ok(ctx)
}

fn query_to_pipeline(
    table_info: &TableInfo,
    query: &Query,
    pipeline: &mut AppPipeline<SchemaSQLContext>,
    query_ctx: &mut QueryContext,
    stateful: bool,
    pipeline_idx: usize,
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
            if query_ctx
                .pipeline_map
                .contains_key(&(pipeline_idx, table_name.clone()))
            {
                return Err(InvalidQuery(format!(
                    "WITH query name {table_name:?} specified more than once"
                )));
            }
            query_to_pipeline(
                &TableInfo {
                    name: NameOrAlias(table_name.clone(), Some(table_name)),
                    is_derived: true,
                    override_name: None,
                },
                &table.query,
                pipeline,
                query_ctx,
                true,
                pipeline_idx,
            )?;
        }
    };

    match *query.body.clone() {
        SetExpr::Select(select) => {
            select_to_pipeline(
                table_info,
                *select,
                pipeline,
                query_ctx,
                stateful,
                pipeline_idx,
            )?;
        }
        SetExpr::Query(query) => {
            let query_name = format!("subquery_{}", uuid::Uuid::new_v4());
            let mut ctx = QueryContext::default();
            query_to_pipeline(
                &TableInfo {
                    name: NameOrAlias(query_name, None),
                    is_derived: true,
                    override_name: None,
                },
                &query,
                pipeline,
                &mut ctx,
                stateful,
                pipeline_idx,
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
    table_info: &TableInfo,
    select: Select,
    pipeline: &mut AppPipeline<SchemaSQLContext>,
    query_ctx: &mut QueryContext,
    stateful: bool,
    pipeline_idx: usize,
) -> Result<(), PipelineError> {
    // FROM clause
    if select.from.len() != 1 {
        return Err(PipelineError::UnsupportedSqlError(
            UnsupportedSqlError::FromCommaSyntax,
        ));
    }

    let input_tables = get_input_tables(&select.from[0], pipeline, query_ctx, pipeline_idx)?;

    let product = FromProcessorFactory::new(input_tables.clone());

    let input_endpoints =
        get_entry_points(&input_tables, &mut query_ctx.pipeline_map, pipeline_idx)?;

    let gen_product_name = format!("product_{}", uuid::Uuid::new_v4());
    let gen_agg_name = format!("agg_{}", uuid::Uuid::new_v4());
    let gen_selection_name = format!("select_{}", uuid::Uuid::new_v4());
    pipeline.add_processor(Arc::new(product), &gen_product_name, input_endpoints);

    let input_names = get_input_names(&input_tables);
    for (port_index, table_name) in input_names.iter().enumerate() {
        if let Some(table_info) = query_ctx
            .pipeline_map
            .get(&(pipeline_idx, table_name.0.clone()))
        {
            pipeline.connect_nodes(
                &table_info.node,
                Some(table_info.port),
                &gen_product_name,
                Some(port_index as PortHandle),
                true,
            )?;
            // If not present in pipeline_map, insert into used_sources as this is coming from source
        } else {
            query_ctx.used_sources.push(table_name.0.clone());
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
            true,
        )?;

        pipeline.connect_nodes(
            &gen_selection_name,
            Some(DEFAULT_PORT_HANDLE),
            &gen_agg_name,
            Some(DEFAULT_PORT_HANDLE),
            true,
        )?;
    } else {
        pipeline.connect_nodes(
            &gen_product_name,
            Some(DEFAULT_PORT_HANDLE),
            &gen_agg_name,
            Some(DEFAULT_PORT_HANDLE),
            true,
        )?;
    }

    query_ctx.pipeline_map.insert(
        (pipeline_idx, table_info.name.0.to_string()),
        QueryTableInfo {
            node: gen_agg_name.clone(),
            port: DEFAULT_PORT_HANDLE,
            is_derived: table_info.is_derived,
        },
    );

    let output_table_name = if let Some(into) = select.into {
        Some(into.name.to_string())
    } else {
        table_info.override_name.clone()
    };
    if let Some(table_name) = output_table_name {
        query_ctx.output_tables_map.insert(
            table_name,
            QueryTableInfo {
                node: gen_agg_name,
                port: DEFAULT_PORT_HANDLE,
                is_derived: false,
            },
        );
    }

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
    pipeline_idx: usize,
) -> Result<IndexedTableWithJoins, PipelineError> {
    let name = get_from_source(&from.relation, pipeline, query_ctx, pipeline_idx)?;
    let mut joins = vec![];

    for join in from.joins.iter() {
        let input_name = get_from_source(&join.relation, pipeline, query_ctx, pipeline_idx)?;
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
    pipeline_map: &mut HashMap<(usize, String), QueryTableInfo>,
    pipeline_idx: usize,
) -> Result<Vec<PipelineEntryPoint>, PipelineError> {
    let mut endpoints = vec![];

    let input_names = get_input_names(input_tables);

    for (input_port, table) in input_names.iter().enumerate() {
        let name = table.0.clone();
        if !pipeline_map.contains_key(&(pipeline_idx, name.clone())) {
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
    pipeline_idx: usize,
) -> Result<NameOrAlias, PipelineError> {
    match relation {
        TableFactor::Table {
            name, alias, args, ..
        } => {
            let input_name = name
                .0
                .iter()
                .map(ExpressionBuilder::normalize_ident)
                .collect::<Vec<String>>()
                .join(".");

            let alias_name = alias
                .as_ref()
                .map(|a| ExpressionBuilder::fullname_from_ident(&[a.name.clone()]));

            // it's a Window function
            if let Some(args) = args {
                if !args.is_empty() && (input_name == "TUMBLE" || input_name == "HOP") {
                    let table_arg = args.get(0).unwrap();
                    match table_arg {
                        sqlparser::ast::FunctionArg::Named { name, arg } => {
                            todo!()
                        }
                        sqlparser::ast::FunctionArg::Unnamed(expr) => match expr {
                            sqlparser::ast::FunctionArgExpr::Expr(expr) => match expr {
                                sqlparser::ast::Expr::Identifier(_) => todo!(),
                                sqlparser::ast::Expr::CompoundIdentifier(_) => todo!(),
                                sqlparser::ast::Expr::JsonAccess {
                                    left,
                                    operator,
                                    right,
                                } => todo!(),
                                sqlparser::ast::Expr::CompositeAccess { expr, key } => todo!(),
                                sqlparser::ast::Expr::IsFalse(_) => todo!(),
                                sqlparser::ast::Expr::IsNotFalse(_) => todo!(),
                                sqlparser::ast::Expr::IsTrue(_) => todo!(),
                                sqlparser::ast::Expr::IsNotTrue(_) => todo!(),
                                sqlparser::ast::Expr::IsNull(_) => todo!(),
                                sqlparser::ast::Expr::IsNotNull(_) => todo!(),
                                sqlparser::ast::Expr::IsUnknown(_) => todo!(),
                                sqlparser::ast::Expr::IsNotUnknown(_) => todo!(),
                                sqlparser::ast::Expr::IsDistinctFrom(_, _) => todo!(),
                                sqlparser::ast::Expr::IsNotDistinctFrom(_, _) => todo!(),
                                sqlparser::ast::Expr::InList {
                                    expr,
                                    list,
                                    negated,
                                } => todo!(),
                                sqlparser::ast::Expr::InSubquery {
                                    expr,
                                    subquery,
                                    negated,
                                } => todo!(),
                                sqlparser::ast::Expr::InUnnest {
                                    expr,
                                    array_expr,
                                    negated,
                                } => todo!(),
                                sqlparser::ast::Expr::Between {
                                    expr,
                                    negated,
                                    low,
                                    high,
                                } => todo!(),
                                sqlparser::ast::Expr::BinaryOp { left, op, right } => todo!(),
                                sqlparser::ast::Expr::Like {
                                    negated,
                                    expr,
                                    pattern,
                                    escape_char,
                                } => todo!(),
                                sqlparser::ast::Expr::ILike {
                                    negated,
                                    expr,
                                    pattern,
                                    escape_char,
                                } => todo!(),
                                sqlparser::ast::Expr::SimilarTo {
                                    negated,
                                    expr,
                                    pattern,
                                    escape_char,
                                } => todo!(),
                                sqlparser::ast::Expr::AnyOp(_) => todo!(),
                                sqlparser::ast::Expr::AllOp(_) => todo!(),
                                sqlparser::ast::Expr::UnaryOp { op, expr } => todo!(),
                                sqlparser::ast::Expr::Cast { expr, data_type } => todo!(),
                                sqlparser::ast::Expr::TryCast { expr, data_type } => todo!(),
                                sqlparser::ast::Expr::SafeCast { expr, data_type } => todo!(),
                                sqlparser::ast::Expr::AtTimeZone {
                                    timestamp,
                                    time_zone,
                                } => todo!(),
                                sqlparser::ast::Expr::Extract { field, expr } => todo!(),
                                sqlparser::ast::Expr::Ceil { expr, field } => todo!(),
                                sqlparser::ast::Expr::Floor { expr, field } => todo!(),
                                sqlparser::ast::Expr::Position { expr, r#in } => todo!(),
                                sqlparser::ast::Expr::Substring {
                                    expr,
                                    substring_from,
                                    substring_for,
                                } => todo!(),
                                sqlparser::ast::Expr::Trim {
                                    expr,
                                    trim_where,
                                    trim_what,
                                } => todo!(),
                                sqlparser::ast::Expr::Overlay {
                                    expr,
                                    overlay_what,
                                    overlay_from,
                                    overlay_for,
                                } => todo!(),
                                sqlparser::ast::Expr::Collate { expr, collation } => todo!(),
                                sqlparser::ast::Expr::Nested(_) => todo!(),
                                sqlparser::ast::Expr::Value(_) => todo!(),
                                sqlparser::ast::Expr::TypedString { data_type, value } => todo!(),
                                sqlparser::ast::Expr::MapAccess { column, keys } => todo!(),
                                sqlparser::ast::Expr::Function(_) => todo!(),
                                sqlparser::ast::Expr::AggregateExpressionWithFilter {
                                    expr,
                                    filter,
                                } => todo!(),
                                sqlparser::ast::Expr::Case {
                                    operand,
                                    conditions,
                                    results,
                                    else_result,
                                } => todo!(),
                                sqlparser::ast::Expr::Exists { subquery, negated } => todo!(),
                                sqlparser::ast::Expr::Subquery(_) => todo!(),
                                sqlparser::ast::Expr::ArraySubquery(_) => todo!(),
                                sqlparser::ast::Expr::ListAgg(_) => todo!(),
                                sqlparser::ast::Expr::ArrayAgg(_) => todo!(),
                                sqlparser::ast::Expr::GroupingSets(_) => todo!(),
                                sqlparser::ast::Expr::Cube(_) => todo!(),
                                sqlparser::ast::Expr::Rollup(_) => todo!(),
                                sqlparser::ast::Expr::Tuple(_) => todo!(),
                                sqlparser::ast::Expr::ArrayIndex { obj, indexes } => todo!(),
                                sqlparser::ast::Expr::Array(_) => todo!(),
                                sqlparser::ast::Expr::Interval {
                                    value,
                                    leading_field,
                                    leading_precision,
                                    last_field,
                                    fractional_seconds_precision,
                                } => todo!(),
                                sqlparser::ast::Expr::MatchAgainst {
                                    columns,
                                    match_value,
                                    opt_search_modifier,
                                } => todo!(),
                            },
                            sqlparser::ast::FunctionArgExpr::QualifiedWildcard(_) => {
                                return Err(PipelineError::SqlError(SqlError::WindowError(
                                    input_name,
                                )));
                            }
                            sqlparser::ast::FunctionArgExpr::Wildcard => {
                                return Err(PipelineError::SqlError(SqlError::WindowError(
                                    input_name,
                                )));
                            }
                        },
                    }

                    let table_name = "".to_string();

                    return Ok(NameOrAlias(table_name, alias_name));
                }
            }

            Ok(NameOrAlias(input_name, alias_name))
        }
        TableFactor::Derived {
            lateral: _,
            subquery,
            alias,
        } => {
            let name = format!("derived_{}", uuid::Uuid::new_v4());
            let alias_name = alias.as_ref().map(|alias_ident| {
                ExpressionBuilder::fullname_from_ident(&[alias_ident.name.clone()])
            });

            let name_or = NameOrAlias(name, alias_name);
            query_to_pipeline(
                &TableInfo {
                    name: name_or.clone(),
                    is_derived: true,
                    override_name: None,
                },
                subquery,
                pipeline,
                query_ctx,
                false,
                pipeline_idx,
            )?;

            Ok(name_or)
        }
        _ => Err(PipelineError::UnsupportedSqlError(
            UnsupportedSqlError::JoinTable,
        )),
    }
}

#[cfg(test)]
mod tests {
    use dozer_core::app::AppPipeline;

    use super::statement_to_pipeline;

    #[test]
    fn parse_sql_pipeline() {
        let sql = r#"
                SELECT
                    a.name as "Genre",
                    SUM(amount) as "Gross Revenue(in $)"
                INTO gross_revenue_stats
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

                SELECT
                f.name, f.title, p.amount
                INTO film_amounts
                FROM film f
                LEFT JOIN film_category fc;

                WITH tbl as (select id from a)
                select id
                into cte_table
                from tbl;

                WITH tbl as (select id from  a),
                tbl2 as (select id from tbl)
                select id
                into nested_cte_table
                from tbl2;

                WITH cte_table1 as (select id_dt1 from (select id_t1 from table_1) as derived_table_1),
                cte_table2 as (select id_ct1 from cte_table1)
                select id_ct2
                into nested_derived_table
                from cte_table2;

                with tbl as (select id, ticker from stocks)
                select tbl.id
                into nested_stocks_table
                from  stocks join tbl on tbl.id = stocks.id;
            "#;

        let context = statement_to_pipeline(sql, &mut AppPipeline::new(), None).unwrap();

        // Should create as many output tables as into statements
        let mut output_keys = context.output_tables_map.keys().collect::<Vec<_>>();
        output_keys.sort();
        let mut expected_keys = vec![
            "gross_revenue_stats",
            "film_amounts",
            "cte_table",
            "nested_cte_table",
            "nested_derived_table",
            "nested_stocks_table",
        ];
        expected_keys.sort();
        assert_eq!(output_keys, expected_keys);
    }
}
