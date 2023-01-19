use crate::pipeline::aggregation::factory::AggregationProcessorFactory;
use crate::pipeline::new_builder::PipelineError::InvalidQuery;
use crate::pipeline::product::factory::get_input_name;
use crate::pipeline::selection::factory::SelectionProcessorFactory;
use crate::pipeline::{errors::PipelineError, product::factory::ProductProcessorFactory};
use dozer_core::dag::app::AppPipeline;
use dozer_core::dag::app::PipelineEntryPoint;
use dozer_core::dag::appsource::AppSourceId;
use dozer_core::dag::dag::DEFAULT_PORT_HANDLE;
use dozer_core::dag::node::PortHandle;
use sqlparser::ast::TableWithJoins;
use sqlparser::{
    ast::{Query, Select, SetExpr, Statement},
    dialect::AnsiDialect,
    parser::Parser,
};
use std::collections::HashMap;
use std::sync::Arc;

use super::errors::UnsupportedSqlError;
pub fn statement_to_pipeline(
    sql: &str,
) -> Result<(AppPipeline, (String, PortHandle)), PipelineError> {
    let dialect = AnsiDialect {};

    let ast = Parser::parse_sql(&dialect, sql).unwrap();
    let query_name = format!("query_{}", uuid::Uuid::new_v4().to_string());
    let statement = ast.get(0).expect("First statement is missing").to_owned();

    let mut pipeline = AppPipeline::new();
    let mut source_map = HashMap::new();
    let mut pipeline_map = HashMap::new();
    if let Statement::Query(query) = statement {
        query_to_pipeline(
            query_name.clone(),
            &query,
            &mut pipeline,
            &mut source_map,
            &mut pipeline_map,
        )?;
    };
    let node = pipeline_map
        .get(&query_name)
        .expect("query should have been initialized")
        .to_owned();
    Ok((pipeline, node))
}

fn query_to_pipeline(
    processor_name: String,
    query: &Query,
    pipeline: &mut AppPipeline,
    source_map: &mut HashMap<String, PortHandle>,
    pipeline_map: &mut HashMap<String, (String, PortHandle)>,
) -> Result<(), PipelineError> {
    // println!("alias: {:?}", table.alias);

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

            query_to_pipeline(
                table.alias.name.to_string(),
                &table.query,
                pipeline,
                source_map,
                pipeline_map,
            )?;
        }
    };

    match *query.body.clone() {
        SetExpr::Select(select) => {
            select_to_pipeline(processor_name, *select, pipeline, pipeline_map)?;
        }
        SetExpr::Query(query) => {
            let query_name = format!("subquery_{}", uuid::Uuid::new_v4().to_string());
            query_to_pipeline(query_name, &query, pipeline, source_map, pipeline_map)?
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
    processor_name: String,
    select: Select,
    pipeline: &mut AppPipeline,
    pipeline_map: &mut HashMap<String, (String, PortHandle)>,
) -> Result<(), PipelineError> {
    // FROM clause
    if select.from.len() != 1 {
        return Err(InvalidQuery(
            "FROM clause doesn't support \"Comma Syntax\"".to_string(),
        ));
    }

    let product = ProductProcessorFactory::new(select.from[0].clone());

    let input_tables = get_input_tables(&select.from[0])?;
    let input_endpoints = get_entry_points(&input_tables, pipeline_map)?;

    let gen_product_name = format!("product_{}", uuid::Uuid::new_v4().to_string());
    let gen_agg_name = format!("agg_{}", uuid::Uuid::new_v4().to_string());
    let gen_selection_name = format!("select_{}", uuid::Uuid::new_v4().to_string());
    pipeline.add_processor(Arc::new(product), &gen_product_name, input_endpoints);

    for (port_index, table_name) in input_tables.iter().enumerate() {
        if let Some((processor_name, processor_port)) = pipeline_map.get(table_name) {
            pipeline.connect_nodes(
                processor_name,
                Some(*processor_port),
                &gen_product_name,
                Some(port_index as PortHandle),
            )?;
        }
    }

    let aggregation = AggregationProcessorFactory::new(select.projection.clone(), select.group_by);

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

    pipeline_map.insert(processor_name, (gen_agg_name, DEFAULT_PORT_HANDLE));

    Ok(())
}

/// Returns a vector of input port handles and relative table name
///
/// # Errors
///
/// This function will return an error if it's not possible to get an input name.
pub fn get_input_tables(from: &TableWithJoins) -> Result<Vec<String>, PipelineError> {
    let mut input_tables = vec![];

    input_tables.insert(0, get_input_name(&from.relation)?);

    for (index, join) in from.joins.iter().enumerate() {
        let input_name = get_input_name(&join.relation)?;
        input_tables.insert(index + 1, input_name);
    }

    Ok(input_tables)
}

pub fn get_entry_points(
    input_tables: &[String],
    pipeline_map: &mut HashMap<String, (String, PortHandle)>,
) -> Result<Vec<PipelineEntryPoint>, PipelineError> {
    let mut endpoints = vec![];

    for (input_port, table) in input_tables.iter().enumerate() {
        if !pipeline_map.contains_key(table) {
            endpoints.push(PipelineEntryPoint::new(
                AppSourceId::new(table.clone(), None),
                input_port as PortHandle,
            ));
        }
    }

    Ok(endpoints)
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
            r#"
                SELECT
                c.name, f.title, p.amount 
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
            // r#"
            // WITH tbl as (select id from (select ttid from a) as a),
            // tbl2 as (select id from tbl)
            // select id from tbl2
            // "#,
        ];
        for sql in statements {
            println!("Parsing {:?}", sql);
            let _pipeline = statement_to_pipeline(sql).unwrap();
        }
    }
}
