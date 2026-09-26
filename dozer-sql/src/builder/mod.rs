use crate::aggregation::factory::AggregationProcessorFactory;
use crate::builder::PipelineError::InvalidQuery;
use crate::errors::PipelineError;
use crate::projection::trim::TrimMembershipColumns;
use crate::selection::factory::SelectionProcessorFactory;
use crate::selection::replay::ReplayRequirement;
use dozer_core::app::AppPipeline;
use dozer_core::node::PortHandle;
use dozer_core::DEFAULT_PORT_HANDLE;
use dozer_sql_expression::builder::{ExpressionBuilder, NameOrAlias};
use dozer_sql_expression::sqlparser::ast::{SetOperator, SetQuantifier, TableFactor};
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
    replay_requirements: HashMap<(usize, String), Vec<ReplayRequirement>>,

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
            replay_requirements: Default::default(),
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

fn query_to_pipeline(
    table_info: TableInfo,
    query: Query,
    pipeline: &mut AppPipeline,
    query_ctx: &mut QueryContext,
    pipeline_idx: usize,
    is_top_select: bool,
) -> Result<(), PipelineError> {
    // Each query owns a lexical WITH scope. Keep graph-wide processor IDs and
    // shared replay requirements while exposing only this query's output.
    let restriction = membership::validate_query_scope(&query, is_top_select).err();
    let visible_tables = query_ctx.pipeline_map.clone();
    let visible_requirements = query_ctx.replay_requirements.clone();
    let output_key = (pipeline_idx, table_info.name.0.clone());
    let result = query_body_to_pipeline(
        table_info,
        query,
        pipeline,
        query_ctx,
        pipeline_idx,
        is_top_select,
    );
    let output = query_ctx.pipeline_map.get(&output_key).cloned();
    let requirements = query_ctx.replay_requirements.get(&output_key).cloned();
    if let (Some(error), Some(requirement)) = (
        restriction,
        requirements
            .as_ref()
            .and_then(|requirements| requirements.last()),
    ) {
        // An outer CTE may have been planned before its membership consumer.
        // Preserve unsupported syntax until that consumer requires replay.
        requirement.reject_when_required(error.to_string());
    }
    query_ctx.pipeline_map = visible_tables;
    query_ctx.replay_requirements = visible_requirements;
    result?;
    if let Some(output) = output {
        query_ctx.pipeline_map.insert(output_key.clone(), output);
    }
    if let Some(requirements) = requirements {
        query_ctx
            .replay_requirements
            .insert(output_key, requirements);
    }
    Ok(())
}

fn query_body_to_pipeline(
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

        let mut local_names = HashSet::new();
        for table in with.cte_tables {
            if table.from.is_some() {
                return Err(PipelineError::UnsupportedSqlError(
                    UnsupportedSqlError::CteFromError,
                ));
            }
            let table_name = ExpressionBuilder::normalize_ident(&table.alias.name);
            if !local_names.insert(table_name.clone()) {
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

    set_body_to_pipeline(
        table_info,
        *query.body,
        pipeline,
        query_ctx,
        pipeline_idx,
        is_top_select,
    )
}

fn set_body_to_pipeline(
    table_info: TableInfo,
    body: SetExpr,
    pipeline: &mut AppPipeline,
    query_ctx: &mut QueryContext,
    pipeline_idx: usize,
    is_top_select: bool,
) -> Result<(), PipelineError> {
    match body {
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
        SetExpr::Query(query) => query_to_pipeline(
            table_info,
            *query,
            pipeline,
            query_ctx,
            pipeline_idx,
            is_top_select,
        )?,
        SetExpr::SetOperation {
            op: SetOperator::Union,
            set_quantifier,
            left,
            right,
        } => {
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
        SetExpr::SetOperation { op, .. } => {
            return Err(PipelineError::InvalidOperator(op.to_string()));
        }
        _ => {
            return Err(PipelineError::UnsupportedSqlError(
                UnsupportedSqlError::GenericError("Unsupported query body structure".to_string()),
            ))
        }
    }
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

    let mut replay_requirements = Vec::new();
    for (source_name, processor_name, processor_port) in input_nodes {
        if let Some(requirements) = query_ctx
            .replay_requirements
            .get(&(pipeline_idx, source_name.clone()))
        {
            replay_requirements.extend(requirements.iter().cloned());
        }
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

    replay_requirements.push(aggregation.replay_requirement());
    pipeline.add_processor(Box::new(aggregation), gen_agg_name.clone());

    // Where clause
    if let Some(mut selection) = select.selection {
        let (input, membership_columns) = membership::plan(
            &mut selection,
            OutputNodeInfo {
                node: gen_product_name,
                port: product_output_port,
            },
            pipeline,
            query_ctx,
            pipeline_idx,
        )?;
        let selection = SelectionProcessorFactory::new(
            gen_selection_name.clone(),
            selection,
            query_ctx.udfs.clone(),
            query_ctx.runtime.clone(),
        );
        replay_requirements.push(selection.replay_requirement());
        if !membership_columns.is_empty() {
            for requirement in &replay_requirements {
                requirement.require();
            }
        }

        pipeline.add_processor(Box::new(selection), gen_selection_name.clone());

        pipeline.connect_nodes(
            input.node,
            input.port,
            gen_selection_name.clone(),
            DEFAULT_PORT_HANDLE,
        );

        let selection_output = if membership_columns.is_empty() {
            gen_selection_name
        } else {
            let name = format!(
                "membership_projection--{}",
                query_ctx.get_next_processor_id()
            );
            pipeline.add_processor(
                Box::new(TrimMembershipColumns {
                    id: name.clone(),
                    columns: membership_columns,
                }),
                name.clone(),
            );
            pipeline.connect_nodes(
                gen_selection_name,
                DEFAULT_PORT_HANDLE,
                name.clone(),
                DEFAULT_PORT_HANDLE,
            );
            name
        };
        pipeline.connect_nodes(
            selection_output,
            DEFAULT_PORT_HANDLE,
            gen_agg_name.clone(),
            DEFAULT_PORT_HANDLE,
        );
    } else {
        pipeline.connect_nodes(
            gen_product_name,
            product_output_port,
            gen_agg_name.clone(),
            DEFAULT_PORT_HANDLE,
        );
    }

    query_ctx.replay_requirements.insert(
        (pipeline_idx, table_info.name.0.clone()),
        replay_requirements,
    );
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

    // UNION operands are internal relations; only the complete set expression
    // owns the output name. Parenthesized operands retain their own WITH scope.
    set_body_to_pipeline(
        left_table_info,
        *left_select,
        pipeline,
        query_ctx,
        pipeline_idx,
        false,
    )?;
    set_body_to_pipeline(
        right_table_info,
        *right_select,
        pipeline,
        query_ctx,
        pipeline_idx,
        false,
    )?;

    let mut gen_set_name = format!("set_{}", query_ctx.get_next_processor_id());

    let mut replay_requirements = Vec::new();
    for name in [&gen_left_set_name, &gen_right_set_name] {
        if let Some(requirements) = query_ctx
            .replay_requirements
            .get(&(pipeline_idx, name.clone()))
        {
            replay_requirements.extend(requirements.iter().cloned());
        }
    }
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

    query_ctx.replay_requirements.insert(
        (pipeline_idx, table_info.name.0.clone()),
        replay_requirements,
    );
    query_ctx.pipeline_map.insert(
        (pipeline_idx, table_info.name.0.to_string()),
        OutputNodeInfo {
            node: gen_set_name.clone(),
            port: DEFAULT_PORT_HANDLE,
        },
    );

    if is_top_select {
        let name = table_info
            .override_name
            .ok_or(PipelineError::MissingIntoClause)?;
        if query_ctx.output_tables_map.contains_key(&name) {
            return Err(PipelineError::DuplicateIntoClause(name));
        }
        query_ctx.output_tables_map.insert(
            name,
            OutputNodeInfo {
                node: gen_set_name.clone(),
                port: DEFAULT_PORT_HANDLE,
            },
        );
    }
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
mod membership;
mod table_operator;

pub use common::string_from_sql_object_name;
pub use table_operator::{TableOperatorArg, TableOperatorDescriptor};

#[cfg(test)]
mod tests;
