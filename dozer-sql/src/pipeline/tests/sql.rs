use crate::pipeline::aggregation::factory::AggregationProcessorFactory;
use crate::pipeline::builder::PipelineBuilder;
use crate::pipeline::product::factory::get_input_name;
use crate::pipeline::selection::factory::SelectionProcessorFactory;
use crate::pipeline::tests::sql::PipelineError::InvalidQuery;
use crate::pipeline::{errors::PipelineError, product::factory::ProductProcessorFactory};
use dozer_core::dag::node::PortHandle;
use dozer_core::dag::{
    app::AppPipeline,
    dag::{Endpoint, DEFAULT_PORT_HANDLE},
    node::NodeHandle,
};

use sqlparser::ast::TableWithJoins;
use sqlparser::{
    ast::{Query, Select, SetExpr, Statement},
    dialect::AnsiDialect,
    parser::Parser,
};
use std::collections::HashMap;
use std::sync::Arc;

const SQL_FIRST_JOIN: &str = r#"
  SELECT
  a.name as "Genre",
    SUM(amount) as "Gross Revenue(in $)"
  FROM
  (	
    SELECT
    c.name, f.title, p.amount 
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

  GROUP BY name
  ORDER BY sum(amount) desc
  LIMIT 5;
"#;

const SQL_2: &str = r#"
    SELECT
    c.name, f.title, p.amount 
  FROM film f
  LEFT JOIN film_category fc
"#;

const SQL_3: &str = r#"
WITH tbl as (select id from a)    
select id from tbl
"#;

const SQL_4: &str = r#"
WITH tbl as (select id from (select ttid from a) as a),
tbl2 as (select id from tbl)    
select id from tbl2
"#;
#[test]
fn sql_logic_test_1() {
    let dialect = AnsiDialect {};

    let ast = Parser::parse_sql(&dialect, SQL_4).unwrap();

    let statement = ast.get(0).expect("First statement is missing").to_owned();

    let pipeline = statement_to_pipeline(&statement).unwrap();
}

fn statement_to_pipeline(statement: &Statement) -> Result<AppPipeline, PipelineError> {
    let mut pipeline = AppPipeline::new();
    let mut source_map = HashMap::new();
    if let Statement::Query(query) = statement {
        query_to_pipeline(&query, &mut pipeline, &mut source_map)?;
    };
    Ok(pipeline)
}

fn query_to_pipeline(
    query: &Query,
    pipeline: &mut AppPipeline,
    source_map: &mut HashMap<String, PortHandle>,
) -> Result<(), PipelineError> {
    // Attach the first pipeline if there is with clause
    if let Some(with) = &query.with {
        if with.recursive {
            panic!("Recursive CTE is not supported");
        }

        for table in &with.cte_tables {
            if table.from.is_some() {
                panic!("table.from Not supported");
            }

            println!("alias: {:?}", table.alias);

            source_map.insert(table.alias.to_string(), DEFAULT_PORT_HANDLE);

            println!("query: {:?}", table.query);
            query_to_pipeline(&table.query, pipeline, source_map)?;
        }
    };

    match *query.body.clone() {
        SetExpr::Select(select) => {
            select_to_pipeline(*select, pipeline, source_map)?;
        }
        SetExpr::Query(query) => query_to_pipeline(&query, pipeline, source_map)?,
        _ => panic!("Only select queries are supported"),
    };
    Ok(())
}

fn select_to_pipeline(
    select: Select,
    pipeline: &mut AppPipeline,
    source_map: &mut HashMap<String, PortHandle>,
) -> Result<(), PipelineError> {
    // FROM clause
    if select.from.len() != 1 {
        return Err(InvalidQuery(
            "FROM clause doesn't support \"Comma Syntax\"".to_string(),
        ));
    }

    let product = ProductProcessorFactory::new(select.from[0].clone());

    let input_tables = get_input_tables(&select.from[0])?;
    let input_endpoints = PipelineBuilder {}.get_input_endpoints(&input_tables)?;

    let gen_product_name = uuid::Uuid::new_v4().to_string();
    let gen_agg_name = uuid::Uuid::new_v4().to_string();
    let gen_selection_name = uuid::Uuid::new_v4().to_string();
    pipeline.add_processor(Arc::new(product), &gen_product_name, input_endpoints);

    for (port_index, table_name) in input_tables.iter().enumerate() {
        if let Some(output) = source_map.get(table_name) {
            pipeline.connect_nodes(
                table_name,
                Some(*output),
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
