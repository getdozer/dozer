use crate::pipeline::aggregation::factory::AggregationProcessorFactory;
use crate::pipeline::new_builder::PipelineError::InvalidQuery;
use crate::pipeline::selection::factory::SelectionProcessorFactory;
use crate::pipeline::{errors::PipelineError, product::factory::ProductProcessorFactory};
use dozer_core::dag::app::AppPipeline;
use dozer_core::dag::app::PipelineEntryPoint;
use dozer_core::dag::appsource::AppSourceId;
use dozer_core::dag::dag::DEFAULT_PORT_HANDLE;
use dozer_core::dag::node::PortHandle;
use sqlparser::ast::{Join, TableFactor, TableWithJoins};
use sqlparser::{
    ast::{Query, Select, SetExpr, Statement},
    dialect::AnsiDialect,
    parser::Parser,
};
use std::collections::HashMap;
use std::sync::Arc;

use super::errors::UnsupportedSqlError;
use super::expression::builder::{fullname_from_ident, normalize_ident};

#[derive(Debug, Clone)]
pub struct IndexedTabelWithJoins {
    pub relation: (NameOrAlias, TableFactor),
    pub joins: Vec<(NameOrAlias, Join)>,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct NameOrAlias(pub String, pub Option<String>);

pub fn statement_to_pipeline(
    sql: &str,
) -> Result<(AppPipeline, (String, PortHandle)), PipelineError> {
    let dialect = AnsiDialect {};

    let ast = Parser::parse_sql(&dialect, sql).unwrap();
    let query_name = NameOrAlias(format!("query_{}", uuid::Uuid::new_v4().to_string()), None);
    let statement = ast.get(0).expect("First statement is missing").to_owned();

    let mut pipeline = AppPipeline::new();
    let mut pipeline_map = HashMap::new();
    if let Statement::Query(query) = statement {
        query_to_pipeline(&query_name, &query, &mut pipeline, &mut pipeline_map, false)?;
    };
    let node = pipeline_map
        .get(&query_name)
        .expect("query should have been initialized")
        .to_owned();
    Ok((pipeline, node))
}

fn query_to_pipeline(
    processor_name: &NameOrAlias,
    query: &Query,
    pipeline: &mut AppPipeline,
    pipeline_map: &mut HashMap<NameOrAlias, (String, PortHandle)>,
    stateful: bool,
) -> Result<(), PipelineError> {
    println!("query_to_pipeline: {:?}, {:?}", processor_name, stateful);

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
            query_to_pipeline(
                &NameOrAlias(table_name.clone(), Some(table_name)),
                &table.query,
                pipeline,
                pipeline_map,
                true,
            )?;
        }
    };

    match *query.body.clone() {
        SetExpr::Select(select) => {
            select_to_pipeline(processor_name, *select, pipeline, pipeline_map, stateful)?;
        }
        SetExpr::Query(query) => {
            let query_name = format!("subquery_{}", uuid::Uuid::new_v4().to_string());

            query_to_pipeline(
                &NameOrAlias(query_name, None),
                &query,
                pipeline,
                pipeline_map,
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
    pipeline: &mut AppPipeline,
    pipeline_map: &mut HashMap<NameOrAlias, (String, PortHandle)>,
    stateful: bool,
) -> Result<(), PipelineError> {
    println!("select_to_pipeline: {:?}, {:?}", processor_name, stateful);

    // FROM clause
    if select.from.len() != 1 {
        return Err(InvalidQuery(
            "FROM clause doesn't support \"Comma Syntax\"".to_string(),
        ));
    }

    let input_tables = get_input_tables(&select.from[0], pipeline, pipeline_map)?;

    let product = ProductProcessorFactory::new(input_tables.clone());

    let input_endpoints = get_entry_points(&input_tables, pipeline_map)?;

    let gen_product_name = format!("product_{}", uuid::Uuid::new_v4().to_string());
    let gen_agg_name = format!("agg_{}", uuid::Uuid::new_v4().to_string());
    let gen_selection_name = format!("select_{}", uuid::Uuid::new_v4().to_string());
    pipeline.add_processor(Arc::new(product), &gen_product_name, input_endpoints);

    let input_names = get_input_names(&input_tables);
    for (port_index, table_name) in input_names.iter().enumerate() {
        if let Some((processor_name, processor_port)) = pipeline_map.get(table_name) {
            pipeline.connect_nodes(
                processor_name,
                Some(*processor_port),
                &gen_product_name,
                Some(port_index as PortHandle),
            )?;
        }
    }

    let aggregation =
        AggregationProcessorFactory::new(select.projection.clone(), select.group_by, stateful);

    pipeline.add_processor(Arc::new(aggregation), &gen_agg_name, vec![]);

    // Where clause
    if let Some(selection) = select.selection {
        let selection = SelectionProcessorFactory::new(selection);
        // first_node_name = String::from("selection");

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

    pipeline_map.insert(processor_name.clone(), (gen_agg_name, DEFAULT_PORT_HANDLE));

    Ok(())
}

/// Returns a vector of input port handles and relative table name
///
/// # Errors
///
/// This function will return an error if it's not possible to get an input name.
pub fn get_input_tables(
    from: &TableWithJoins,
    pipeline: &mut AppPipeline,
    pipeline_map: &mut HashMap<NameOrAlias, (String, PortHandle)>,
) -> Result<IndexedTabelWithJoins, PipelineError> {
    let mut input_tables = vec![];

    let name = get_from_source(&from.relation, pipeline, pipeline_map)?;
    input_tables.insert(0, name.clone());
    let mut joins = vec![];

    for (index, join) in from.joins.iter().enumerate() {
        let input_name = get_from_source(&join.relation, pipeline, pipeline_map)?;
        joins.push((input_name.clone(), join.clone()));
        input_tables.insert(index + 1, input_name);
    }

    Ok(IndexedTabelWithJoins {
        relation: (name, from.relation.clone()),
        joins,
    })
}

pub fn get_input_names(input_tables: &IndexedTabelWithJoins) -> Vec<NameOrAlias> {
    let mut input_names = vec![];
    input_names.push(input_tables.relation.0.clone());

    for join in &input_tables.joins {
        input_names.push(join.0.clone());
    }
    input_names
}
pub fn get_entry_points(
    input_tables: &IndexedTabelWithJoins,
    pipeline_map: &mut HashMap<NameOrAlias, (String, PortHandle)>,
) -> Result<Vec<PipelineEntryPoint>, PipelineError> {
    let mut endpoints = vec![];

    let input_names = get_input_names(input_tables);

    for (input_port, table) in input_names.iter().enumerate() {
        if !pipeline_map.contains_key(table) {
            let name = table.0.clone();
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
    pipeline: &mut AppPipeline,
    pipeline_map: &mut HashMap<NameOrAlias, (String, PortHandle)>,
) -> Result<NameOrAlias, PipelineError> {
    match relation {
        TableFactor::Table { name, alias, .. } => {
            let input_name = name
                .0
                .iter()
                .map(normalize_ident)
                .collect::<Vec<String>>()
                .join(".");
            let alias_name = alias
                .as_ref()
                .map(|a| fullname_from_ident(&[a.name.clone()]));

            Ok(NameOrAlias(input_name, alias_name))
        }
        TableFactor::Derived {
            lateral: _,
            subquery,
            alias,
        } => {
            let name = format!("derived_{}", uuid::Uuid::new_v4().to_string());
            let alias_name = alias
                .as_ref()
                .map(|alias_ident| fullname_from_ident(&[alias_ident.name.clone()]));

            let nameOr = NameOrAlias(name, alias_name);
            query_to_pipeline(&nameOr, subquery, pipeline, pipeline_map, true)?;

            Ok(nameOr)
        }
        _ => {
            return Err(PipelineError::UnsupportedSqlError(
                UnsupportedSqlError::JoinTable,
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::statement_to_pipeline;

    #[test]
    fn sql_logic_test_1() {
        let statements: Vec<&str> = vec![
            // r#"
            // SELECT
            // a.name as "Genre",
            //     SUM(amount) as "Gross Revenue(in $)"
            // FROM
            // (
            //     SELECT
            //     c.name, f.title, p.amount
            // FROM film f
            // LEFT JOIN film_category fc
            // ON fc.film_id = f.film_id
            // LEFT JOIN category c
            // ON fc.category_id = c.category_id
            // LEFT JOIN inventory i
            // ON i.film_id = f.film_id
            // LEFT JOIN rental r
            // ON r.inventory_id = i.inventory_id
            // LEFT JOIN payment p
            // ON p.rental_id = r.rental_id
            // WHERE p.amount IS NOT NULL
            // ) a

            // GROUP BY name
            // ORDER BY sum(amount) desc
            // LIMIT 5;
            // "#,
            // r#"
            //     SELECT
            //     c.name, f.title, p.amount
            // FROM film f
            // LEFT JOIN film_category fc
            // "#,
            // r#"
            // WITH tbl as (select id from a)
            // select id from tbl
            // "#,
            // r#"
            // WITH tbl as (select id from  a),
            // tbl2 as (select id from tbl)
            // select id from tbl2
            // "#,
            // r#"
            // WITH cte_table1 as (select id_dt1 from (select id_t1 from table_1) as derived_table_1),
            // cte_table2 as (select id_ct1 from cte_table1)
            // select id_ct2 from cte_table2
            // "#,
            r#"
                with tbl as (select id, ticker from stocks)
                select tbl.id from  stocks join tbl on tbl.id = stocks.id;
            "#,
        ];
        for sql in statements {
            println!("Parsing {:?}", sql);
            let _pipeline = statement_to_pipeline(sql).unwrap();
            println!("Pipeline ready");
        }
    }
}
