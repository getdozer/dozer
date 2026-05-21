use crate::aggregation::factory::AggregationProcessorFactory;
use crate::builder::PipelineError::InvalidQuery;
use crate::errors::PipelineError;
use crate::selection::factory::SelectionProcessorFactory;
use crate::selection::in_subquery::{
    InSubqueryProcessorFactory, LEFT_IN_SUBQUERY_PORT, RIGHT_IN_SUBQUERY_PORT,
};
use dozer_core::app::AppPipeline;
use dozer_core::node::PortHandle;
use dozer_core::DEFAULT_PORT_HANDLE;
use dozer_sql_expression::builder::{ExpressionBuilder, NameOrAlias};
use dozer_sql_expression::sqlparser::ast::{
    BinaryOperator, Expr as SqlExpr, SetOperator, SetQuantifier, TableFactor,
};
use dozer_types::models::udf_config::UdfConfig;

use dozer_sql_expression::sqlparser::{
    ast::{Query, Select, SetExpr, Statement},
    dialect::DozerDialect,
    parser::Parser,
};
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use tokio::runtime::Runtime;

use super::errors::UnsupportedSqlError;

use super::product::set::set_factory::SetProcessorFactory;

#[derive(Debug, Clone)]
pub struct OutputNodeInfo {
    // Name to connect in dag
    pub node: String,
    // Port to connect in dag
    pub port: PortHandle,
    // TODO add:indexes to the tables
}

/// The struct contains some contexts during query to pipeline.
#[derive(Debug, Clone)]
pub struct QueryContext {
    // Internal tables map, used to store the tables that are created by the queries
    pipeline_map: HashMap<(usize, String), OutputNodeInfo>,

    // Output tables map that are marked with "INTO" used to store the tables, these can be exposed to sinks.
    pub output_tables_map: HashMap<String, OutputNodeInfo>,

    // Used Sources
    pub used_sources: Vec<String>,

    // Internal tables map, used to store the tables that are created by the queries
    processors_list: HashSet<String>,

    // Processors counter
    processor_counter: usize,

    // Udf related configs
    udfs: Vec<UdfConfig>,

    // The tokio runtime
    runtime: Arc<Runtime>,
}

impl QueryContext {
    fn get_next_processor_id(&mut self) -> usize {
        self.processor_counter += 1;
        self.processor_counter
    }

    pub fn new(udfs: Vec<UdfConfig>, runtime: Arc<Runtime>) -> Self {
        QueryContext {
            pipeline_map: Default::default(),
            output_tables_map: Default::default(),
            used_sources: Default::default(),
            processors_list: Default::default(),
            processor_counter: Default::default(),
            udfs,
            runtime,
        }
    }
}

pub fn statement_to_pipeline(
    sql: &str,
    pipeline: &mut AppPipeline,
    override_name: Option<String>,
    udfs: Vec<UdfConfig>,
    runtime: Arc<Runtime>,
) -> Result<QueryContext, PipelineError> {
    let dialect = DozerDialect {};
    let mut ctx = QueryContext::new(udfs, runtime);
    let is_top_select = true;
    let ast = Parser::parse_sql(&dialect, sql)
        .map_err(|err| PipelineError::InternalError(Box::new(err)))?;
    let query_name = NameOrAlias(format!("query_{}", ctx.get_next_processor_id()), None);

    for (idx, statement) in ast.into_iter().enumerate() {
        match statement {
            Statement::Query(query) => {
                query_to_pipeline(
                    TableInfo {
                        name: query_name.clone(),
                        override_name: override_name.clone(),
                    },
                    *query,
                    pipeline,
                    &mut ctx,
                    idx,
                    is_top_select,
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

struct TableInfo {
    name: NameOrAlias,
    override_name: Option<String>,
}

struct InSubquerySelection {
    expr: Box<SqlExpr>,
    subquery: Box<Query>,
}

fn query_to_pipeline(
    table_info: TableInfo,
    query: Query,
    pipeline: &mut AppPipeline,
    query_ctx: &mut QueryContext,
    pipeline_idx: usize,
    is_top_select: bool,
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
    if let Some(with) = query.with {
        if with.recursive {
            return Err(PipelineError::UnsupportedSqlError(
                UnsupportedSqlError::Recursive,
            ));
        }

        for table in with.cte_tables {
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
                TableInfo {
                    name: NameOrAlias(table_name.clone(), Some(table_name)),
                    override_name: None,
                },
                *table.query,
                pipeline,
                query_ctx,
                pipeline_idx,
                false, //Inside a with clause, so not top select
            )?;
        }
    };

    match *query.body {
        SetExpr::Select(select) => {
            select_to_pipeline(
                table_info,
                *select,
                pipeline,
                query_ctx,
                pipeline_idx,
                is_top_select,
            )?;
        }
        SetExpr::Query(query) => {
            let query_name = format!("subquery_{}", query_ctx.get_next_processor_id());
            let mut ctx = QueryContext::new(query_ctx.udfs.clone(), query_ctx.runtime.clone());
            query_to_pipeline(
                TableInfo {
                    name: NameOrAlias(query_name, None),
                    override_name: None,
                },
                *query,
                pipeline,
                &mut ctx,
                pipeline_idx,
                false, //Inside a subquery, so not top select
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
                    pipeline_idx,
                    is_top_select,
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
    table_info: TableInfo,
    select: Select,
    pipeline: &mut AppPipeline,
    query_ctx: &mut QueryContext,
    pipeline_idx: usize,
    is_top_select: bool,
) -> Result<String, PipelineError> {
    let mut select = select;
    let (in_subquery_selection, residual_selection) =
        split_in_subquery_selection(select.selection.take())?;
    select.selection = residual_selection;

    // FROM clause
    let Some(from) = select.from.into_iter().next() else {
        return Err(PipelineError::UnsupportedSqlError(
            UnsupportedSqlError::FromCommaSyntax,
        ));
    };

    let connection_info = from::insert_from_to_pipeline(from, pipeline, pipeline_idx, query_ctx)?;

    let input_nodes = connection_info.input_nodes;
    let output_node = connection_info.output_node;

    let gen_agg_name = format!("agg--{}", query_ctx.get_next_processor_id());

    let gen_selection_name = format!("select--{}", query_ctx.get_next_processor_id());
    let (gen_product_name, product_output_port) = output_node;
    let mut upstream_node = gen_product_name;
    let mut upstream_port = product_output_port;

    for (source_name, processor_name, processor_port) in input_nodes {
        if let Some(table_info) = query_ctx
            .pipeline_map
            .get(&(pipeline_idx, source_name.clone()))
        {
            pipeline.connect_nodes(
                table_info.node.clone(),
                table_info.port,
                processor_name,
                processor_port,
            );
            // If not present in pipeline_map, insert into used_sources as this is coming from source
        } else {
            query_ctx.used_sources.push(source_name.clone());
        }
    }

    if let Some(in_subquery_selection) = in_subquery_selection {
        let in_subquery_source_name = format!("in_subquery_{}", query_ctx.get_next_processor_id());
        query_to_pipeline(
            TableInfo {
                name: NameOrAlias(in_subquery_source_name.clone(), None),
                override_name: None,
            },
            *in_subquery_selection.subquery,
            pipeline,
            query_ctx,
            pipeline_idx,
            false,
        )?;

        let in_subquery_output = query_ctx
            .pipeline_map
            .get(&(pipeline_idx, in_subquery_source_name.clone()))
            .ok_or_else(|| {
                PipelineError::InvalidQuery(format!(
                    "Invalid IN subquery {in_subquery_source_name}"
                ))
            })?
            .clone();
        let in_subquery_processor_name =
            format!("in_subquery--{}", query_ctx.get_next_processor_id());
        if !query_ctx
            .processors_list
            .insert(in_subquery_processor_name.clone())
        {
            return Err(PipelineError::ProcessorAlreadyExists(
                in_subquery_processor_name,
            ));
        }

        let in_subquery_processor = InSubqueryProcessorFactory::new(
            in_subquery_processor_name.clone(),
            *in_subquery_selection.expr,
            query_ctx.udfs.clone(),
            query_ctx.runtime.clone(),
        );
        pipeline.add_processor(
            Box::new(in_subquery_processor),
            in_subquery_processor_name.clone(),
        );
        pipeline.connect_nodes(
            upstream_node,
            upstream_port,
            in_subquery_processor_name.clone(),
            LEFT_IN_SUBQUERY_PORT,
        );
        pipeline.connect_nodes(
            in_subquery_output.node,
            in_subquery_output.port,
            in_subquery_processor_name.clone(),
            RIGHT_IN_SUBQUERY_PORT,
        );
        upstream_node = in_subquery_processor_name;
        upstream_port = DEFAULT_PORT_HANDLE;
    }

    let aggregation = AggregationProcessorFactory::new(
        gen_agg_name.clone(),
        select.projection,
        select.group_by,
        select.having,
        pipeline
            .flags()
            .enable_probabilistic_optimizations
            .in_aggregations
            .unwrap_or(false),
        query_ctx.udfs.clone(),
        query_ctx.runtime.clone(),
    );

    pipeline.add_processor(Box::new(aggregation), gen_agg_name.clone());

    // Where clause
    if let Some(selection) = select.selection {
        let selection = SelectionProcessorFactory::new(
            gen_selection_name.clone(),
            selection,
            query_ctx.udfs.clone(),
            query_ctx.runtime.clone(),
        );

        pipeline.add_processor(Box::new(selection), gen_selection_name.clone());

        pipeline.connect_nodes(
            upstream_node,
            upstream_port,
            gen_selection_name.clone(),
            DEFAULT_PORT_HANDLE,
        );

        pipeline.connect_nodes(
            gen_selection_name,
            DEFAULT_PORT_HANDLE,
            gen_agg_name.clone(),
            DEFAULT_PORT_HANDLE,
        );
    } else {
        pipeline.connect_nodes(
            upstream_node,
            upstream_port,
            gen_agg_name.clone(),
            DEFAULT_PORT_HANDLE,
        );
    }

    query_ctx.pipeline_map.insert(
        (pipeline_idx, table_info.name.0.to_string()),
        OutputNodeInfo {
            node: gen_agg_name.clone(),
            port: DEFAULT_PORT_HANDLE,
        },
    );

    let output_table_name = if let Some(into) = select.into {
        Some(into.name.to_string())
    } else {
        table_info.override_name.clone()
    };

    if is_top_select && output_table_name.is_none() {
        return Err(PipelineError::MissingIntoClause);
    }

    if let Some(table_name) = output_table_name {
        if query_ctx.output_tables_map.contains_key(&table_name) {
            return Err(PipelineError::DuplicateIntoClause(table_name));
        }

        query_ctx.output_tables_map.insert(
            table_name,
            OutputNodeInfo {
                node: gen_agg_name.clone(),
                port: DEFAULT_PORT_HANDLE,
            },
        );
    }

    Ok(gen_agg_name)
}

fn split_in_subquery_selection(
    selection: Option<SqlExpr>,
) -> Result<(Option<InSubquerySelection>, Option<SqlExpr>), PipelineError> {
    match selection {
        Some(selection) => extract_in_subquery_selection(selection),
        None => Ok((None, None)),
    }
}

fn extract_in_subquery_selection(
    selection: SqlExpr,
) -> Result<(Option<InSubquerySelection>, Option<SqlExpr>), PipelineError> {
    match selection {
        SqlExpr::InSubquery {
            expr,
            subquery,
            negated,
        } => {
            if negated {
                return Err(PipelineError::InvalidQuery(
                    "NOT IN subqueries are not supported".to_string(),
                ));
            }
            Ok((Some(InSubquerySelection { expr, subquery }), None))
        }
        SqlExpr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => {
            let (left_in_subquery, left_residual) = extract_in_subquery_selection(*left)?;
            let (right_in_subquery, right_residual) = extract_in_subquery_selection(*right)?;
            let in_subquery = match (left_in_subquery, right_in_subquery) {
                (Some(_), Some(_)) => {
                    return Err(PipelineError::InvalidQuery(
                        "Only one IN subquery is supported in a WHERE clause".to_string(),
                    ))
                }
                (Some(in_subquery), None) | (None, Some(in_subquery)) => Some(in_subquery),
                (None, None) => None,
            };

            Ok((
                in_subquery,
                combine_residual_and(left_residual, right_residual),
            ))
        }
        SqlExpr::Nested(expr) => {
            let (in_subquery, residual) = extract_in_subquery_selection(*expr)?;
            Ok((
                in_subquery,
                residual.map(|expr| SqlExpr::Nested(Box::new(expr))),
            ))
        }
        _ => Ok((None, Some(selection))),
    }
}

fn combine_residual_and(left: Option<SqlExpr>, right: Option<SqlExpr>) -> Option<SqlExpr> {
    match (left, right) {
        (Some(left), Some(right)) => Some(SqlExpr::BinaryOp {
            left: Box::new(left),
            op: BinaryOperator::And,
            right: Box::new(right),
        }),
        (Some(expr), None) | (None, Some(expr)) => Some(expr),
        (None, None) => None,
    }
}

#[allow(clippy::too_many_arguments)]
fn set_to_pipeline(
    table_info: TableInfo,
    left_select: Box<SetExpr>,
    right_select: Box<SetExpr>,
    set_quantifier: SetQuantifier,
    pipeline: &mut AppPipeline,
    query_ctx: &mut QueryContext,
    pipeline_idx: usize,
    is_top_select: bool,
) -> Result<String, PipelineError> {
    let gen_left_set_name = format!("set_left_{}", query_ctx.get_next_processor_id());
    let left_table_info = TableInfo {
        name: NameOrAlias(gen_left_set_name.clone(), None),
        override_name: None,
    };
    let gen_right_set_name = format!("set_right_{}", query_ctx.get_next_processor_id());
    let right_table_info = TableInfo {
        name: NameOrAlias(gen_right_set_name.clone(), None),
        override_name: None,
    };

    let _left_pipeline_name = match *left_select {
        SetExpr::Select(select) => select_to_pipeline(
            left_table_info,
            *select,
            pipeline,
            query_ctx,
            pipeline_idx,
            is_top_select,
        )?,
        SetExpr::SetOperation {
            op: _,
            set_quantifier,
            left,
            right,
        } => set_to_pipeline(
            left_table_info,
            left,
            right,
            set_quantifier,
            pipeline,
            query_ctx,
            pipeline_idx,
            is_top_select,
        )?,
        _ => {
            return Err(PipelineError::InvalidQuery(
                "Invalid UNION left Query".to_string(),
            ))
        }
    };

    let _right_pipeline_name = match *right_select {
        SetExpr::Select(select) => select_to_pipeline(
            right_table_info,
            *select,
            pipeline,
            query_ctx,
            pipeline_idx,
            is_top_select,
        )?,
        SetExpr::SetOperation {
            op: _,
            set_quantifier,
            left,
            right,
        } => set_to_pipeline(
            right_table_info,
            left,
            right,
            set_quantifier,
            pipeline,
            query_ctx,
            pipeline_idx,
            is_top_select,
        )?,
        _ => {
            return Err(PipelineError::InvalidQuery(
                "Invalid UNION right Query".to_string(),
            ))
        }
    };

    let mut gen_set_name = format!("set_{}", query_ctx.get_next_processor_id());

    let left_pipeline_output_node = query_ctx
        .pipeline_map
        .get(&(pipeline_idx, gen_left_set_name))
        .ok_or_else(|| PipelineError::InvalidQuery("Invalid UNION left Query".to_string()))?;

    let right_pipeline_output_node = query_ctx
        .pipeline_map
        .get(&(pipeline_idx, gen_right_set_name))
        .ok_or_else(|| PipelineError::InvalidQuery("Invalid UNION right Query".to_string()))?;

    if table_info.override_name.is_some() {
        gen_set_name = table_info.override_name.to_owned().unwrap();
    }

    let set_proc_fac = SetProcessorFactory::new(
        gen_set_name.clone(),
        set_quantifier,
        pipeline
            .flags()
            .enable_probabilistic_optimizations
            .in_sets
            .unwrap_or(false),
    );

    pipeline.add_processor(Box::new(set_proc_fac), gen_set_name.clone());

    pipeline.connect_nodes(
        left_pipeline_output_node.node.clone(),
        left_pipeline_output_node.port,
        gen_set_name.clone(),
        0,
    );

    pipeline.connect_nodes(
        right_pipeline_output_node.node.clone(),
        right_pipeline_output_node.port,
        gen_set_name.clone(),
        1,
    );

    for (_, table_name) in query_ctx.pipeline_map.keys() {
        query_ctx.output_tables_map.remove_entry(table_name);
    }

    query_ctx.pipeline_map.insert(
        (pipeline_idx, table_info.name.0.to_string()),
        OutputNodeInfo {
            node: gen_set_name.clone(),
            port: DEFAULT_PORT_HANDLE,
        },
    );

    Ok(gen_set_name)
}

fn get_from_source(
    relation: TableFactor,
    pipeline: &mut AppPipeline,
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
            let is_top_select = false; //inside FROM clause, so not top select
            let name_or = NameOrAlias(name, alias_name);
            query_to_pipeline(
                TableInfo {
                    name: name_or.clone(),
                    override_name: None,
                },
                *subquery,
                pipeline,
                query_ctx,
                pipeline_idx,
                is_top_select,
            )?;

            Ok(name_or)
        }
        _ => Err(PipelineError::UnsupportedSqlError(
            UnsupportedSqlError::JoinTable,
        )),
    }
}

#[derive(Clone, Debug)]
struct ConnectionInfo {
    input_nodes: Vec<(String, String, PortHandle)>,
    output_node: (String, PortHandle),
}

mod common;
mod from;
mod join;
mod table_operator;

pub use common::string_from_sql_object_name;
pub use table_operator::{TableOperatorArg, TableOperatorDescriptor};

#[cfg(test)]
mod tests;
