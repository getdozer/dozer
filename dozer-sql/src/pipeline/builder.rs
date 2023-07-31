use crate::pipeline::aggregation::factory::AggregationProcessorFactory;
use crate::pipeline::builder::PipelineError::InvalidQuery;
use crate::pipeline::errors::PipelineError;
use crate::pipeline::expression::builder::{ExpressionBuilder, NameOrAlias};
use crate::pipeline::selection::factory::SelectionProcessorFactory;
use dozer_core::app::AppPipeline;
use dozer_core::app::PipelineEntryPoint;
use dozer_core::node::PortHandle;
use dozer_core::DEFAULT_PORT_HANDLE;
use sqlparser::ast::{Join, SetOperator, SetQuantifier, TableFactor, TableWithJoins};

use sqlparser::{
    ast::{Query, Select, SetExpr, Statement},
    dialect::DozerDialect,
    parser::Parser,
};
use std::collections::HashMap;

use super::errors::UnsupportedSqlError;
use super::pipeline_builder::from_builder::insert_from_to_pipeline;

use super::product::set::set_factory::SetProcessorFactory;

#[derive(Debug, Clone, Default)]
pub struct SchemaSQLContext {}

#[derive(Debug, Clone)]
pub struct OutputNodeInfo {
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
    pub pipeline_map: HashMap<(usize, String), OutputNodeInfo>,

    // Output tables map that are marked with "INTO" used to store the tables, these can be exposed to sinks.
    pub output_tables_map: HashMap<String, OutputNodeInfo>,

    // Used Sources
    pub used_sources: Vec<String>,

    // Processors counter
    pub processor_counter: usize,
}

impl QueryContext {
    pub fn get_next_processor_id(&mut self) -> usize {
        self.processor_counter += 1;
        self.processor_counter
    }
}

#[derive(Debug, Clone)]
pub struct IndexedTableWithJoins {
    pub relation: (NameOrAlias, TableFactor),
    pub joins: Vec<(NameOrAlias, Join)>,
}
pub fn sql_to_pipeline(
    sql: &str,
    override_name: Option<String>,
) -> Result<(QueryContext, AppPipeline<SchemaSQLContext>), PipelineError> {
    let dialect = DozerDialect {};
    let mut ctx = QueryContext::default();

    let ast = Parser::parse_sql(&dialect, sql)
        .map_err(|err| PipelineError::InternalError(Box::new(err)))?;
    let query_name = NameOrAlias(format!("query_{}", ctx.get_next_processor_id()), None);

    let mut pipeline = AppPipeline::new();
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
                    &mut pipeline,
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

    Ok((ctx, pipeline))
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
            let query_name = format!("subquery_{}", query_ctx.get_next_processor_id());
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
        SetExpr::SetOperation {
            op,
            set_quantifier,
            left,
            right,
        } => match op {
            SetOperator::Union => {
                set_to_pipeline(
                    table_info,
                    left,
                    right,
                    set_quantifier,
                    pipeline,
                    query_ctx,
                    stateful,
                    pipeline_idx,
                )?;
            }
            _ => return Err(PipelineError::InvalidOperator(op.to_string())),
        },
        _ => {
            return Err(PipelineError::UnsupportedSqlError(
                UnsupportedSqlError::GenericError("Unsupported query body structure".to_string()),
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
) -> Result<String, PipelineError> {
    // FROM clause
    if select.from.len() != 1 {
        return Err(PipelineError::UnsupportedSqlError(
            UnsupportedSqlError::FromCommaSyntax,
        ));
    }

    // let input_tables = get_input_tables(&select.from[0], pipeline, query_ctx, pipeline_idx)?;
    //
    // let (input_nodes, output_node, mut used_sources) = add_from_to_pipeline(
    //     pipeline,
    //     &input_tables,
    //     &mut query_ctx.pipeline_map,
    //     pipeline_idx,
    // )?;

    let connection_info =
        insert_from_to_pipeline(&select.from[0], pipeline, pipeline_idx, query_ctx)?;

    let input_nodes = connection_info.input_nodes;
    let output_node = connection_info.output_node;

    let gen_agg_name = format!("agg_{}", query_ctx.get_next_processor_id());

    let gen_selection_name = format!("select_{}", query_ctx.get_next_processor_id());
    let (gen_product_name, product_output_port) = output_node;

    for (source_name, processor_name, processor_port) in input_nodes.iter() {
        if let Some(table_info) = query_ctx
            .pipeline_map
            .get(&(pipeline_idx, source_name.clone()))
        {
            pipeline.connect_nodes(
                &table_info.node,
                table_info.port,
                processor_name,
                *processor_port as PortHandle,
            )?;
            // If not present in pipeline_map, insert into used_sources as this is coming from source
        } else {
            query_ctx.used_sources.push(source_name.clone());
        }
    }

    let aggregation =
        AggregationProcessorFactory::new(gen_agg_name.clone(), select.clone(), stateful);

    pipeline.add_processor(Box::new(aggregation), &gen_agg_name, vec![])?;

    // Where clause
    if let Some(selection) = select.selection {
        let selection = SelectionProcessorFactory::new(gen_selection_name.to_owned(), selection);

        pipeline.add_processor(Box::new(selection), &gen_selection_name, vec![])?;

        pipeline.connect_nodes(
            &gen_product_name,
            product_output_port,
            &gen_selection_name,
            DEFAULT_PORT_HANDLE,
        )?;

        pipeline.connect_nodes(
            &gen_selection_name,
            DEFAULT_PORT_HANDLE,
            &gen_agg_name,
            DEFAULT_PORT_HANDLE,
        )?;
    } else {
        pipeline.connect_nodes(
            &gen_product_name,
            product_output_port,
            &gen_agg_name,
            DEFAULT_PORT_HANDLE,
        )?;
    }

    query_ctx.pipeline_map.insert(
        (pipeline_idx, table_info.name.0.to_string()),
        OutputNodeInfo {
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
            OutputNodeInfo {
                node: gen_agg_name.clone(),
                port: DEFAULT_PORT_HANDLE,
                is_derived: false,
            },
        );
    }

    Ok(gen_agg_name)
}

#[allow(clippy::too_many_arguments)]
fn set_to_pipeline(
    table_info: &TableInfo,
    left_select: Box<SetExpr>,
    right_select: Box<SetExpr>,
    set_quantifier: SetQuantifier,
    pipeline: &mut AppPipeline<SchemaSQLContext>,
    query_ctx: &mut QueryContext,
    stateful: bool,
    pipeline_idx: usize,
) -> Result<String, PipelineError> {
    let gen_left_set_name = format!("set_left_{}", query_ctx.get_next_processor_id());
    let left_table_info = TableInfo {
        name: NameOrAlias(gen_left_set_name.clone(), None),
        override_name: None,
        is_derived: false,
    };
    let gen_right_set_name = format!("set_right_{}", query_ctx.get_next_processor_id());
    let right_table_info = TableInfo {
        name: NameOrAlias(gen_right_set_name.clone(), None),
        override_name: None,
        is_derived: false,
    };

    let _left_pipeline_name = match *left_select {
        SetExpr::Select(select) => select_to_pipeline(
            &left_table_info,
            *select,
            pipeline,
            query_ctx,
            stateful,
            pipeline_idx,
        )?,
        SetExpr::SetOperation {
            op: _,
            set_quantifier,
            left,
            right,
        } => set_to_pipeline(
            &left_table_info,
            left,
            right,
            set_quantifier,
            pipeline,
            query_ctx,
            stateful,
            pipeline_idx,
        )?,
        _ => {
            return Err(PipelineError::InvalidQuery(
                "Invalid UNION left Query".to_string(),
            ))
        }
    };

    let _right_pipeline_name = match *right_select {
        SetExpr::Select(select) => select_to_pipeline(
            &right_table_info,
            *select,
            pipeline,
            query_ctx,
            stateful,
            pipeline_idx,
        )?,
        SetExpr::SetOperation {
            op: _,
            set_quantifier,
            left,
            right,
        } => set_to_pipeline(
            &right_table_info,
            left,
            right,
            set_quantifier,
            pipeline,
            query_ctx,
            stateful,
            pipeline_idx,
        )?,
        _ => {
            return Err(PipelineError::InvalidQuery(
                "Invalid UNION right Query".to_string(),
            ))
        }
    };

    let mut gen_set_name = format!("set_{}", query_ctx.get_next_processor_id());

    let left_pipeline_output_node = match query_ctx
        .pipeline_map
        .get(&(pipeline_idx, gen_left_set_name))
    {
        Some(pipeline) => pipeline,
        None => {
            return Err(PipelineError::InvalidQuery(
                "Invalid UNION left Query".to_string(),
            ))
        }
    };

    let right_pipeline_output_node = match query_ctx
        .pipeline_map
        .get(&(pipeline_idx, gen_right_set_name))
    {
        Some(pipeline) => pipeline,
        None => {
            return Err(PipelineError::InvalidQuery(
                "Invalid UNION Right Query".to_string(),
            ))
        }
    };

    if table_info.override_name.is_some() {
        gen_set_name = table_info.override_name.to_owned().unwrap();
    }

    let set_proc_fac = SetProcessorFactory::new(gen_set_name.clone(), set_quantifier);

    pipeline.add_processor(Box::new(set_proc_fac), &gen_set_name, vec![])?;

    pipeline.connect_nodes(
        &left_pipeline_output_node.node,
        left_pipeline_output_node.port,
        &gen_set_name,
        0 as PortHandle,
    )?;

    pipeline.connect_nodes(
        &right_pipeline_output_node.node,
        right_pipeline_output_node.port,
        &gen_set_name,
        1 as PortHandle,
    )?;

    for (_, table_name) in query_ctx.pipeline_map.keys() {
        query_ctx.output_tables_map.remove_entry(table_name);
    }

    query_ctx.pipeline_map.insert(
        (pipeline_idx, table_info.name.0.to_string()),
        OutputNodeInfo {
            node: gen_set_name.clone(),
            port: DEFAULT_PORT_HANDLE,
            is_derived: table_info.is_derived,
        },
    );

    Ok(gen_set_name)
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
    pipeline_map: &mut HashMap<(usize, String), OutputNodeInfo>,
    pipeline_idx: usize,
) -> Result<Vec<PipelineEntryPoint>, PipelineError> {
    let mut endpoints = vec![];

    let input_names = get_input_names(input_tables);

    for (input_port, table) in input_names.iter().enumerate() {
        let name = table.0.clone();
        if !pipeline_map.contains_key(&(pipeline_idx, name.clone())) {
            endpoints.push(PipelineEntryPoint::new(name, input_port as PortHandle));
        }
    }

    Ok(endpoints)
}

pub fn is_an_entry_point(
    name: &str,
    pipeline_map: &mut HashMap<(usize, String), OutputNodeInfo>,
    pipeline_idx: usize,
) -> bool {
    if !pipeline_map.contains_key(&(pipeline_idx, name.to_owned())) {
        return true;
    }
    false
}

pub fn get_from_source(
    relation: &TableFactor,
    pipeline: &mut AppPipeline<SchemaSQLContext>,
    query_ctx: &mut QueryContext,
    pipeline_idx: usize,
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
            let name = format!("derived_{}", query_ctx.get_next_processor_id());
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
    use super::sql_to_pipeline;

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

        let (context, _) = sql_to_pipeline(sql, None).unwrap();

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
