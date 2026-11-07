use crate::aggregation::factory::AggregationProcessorFactory;
use crate::builder::PipelineError::InvalidQuery;
use crate::errors::PipelineError;
use crate::selection::factory::SelectionProcessorFactory;
use dozer_core::app::AppPipeline;
use dozer_core::node::PortHandle;
use dozer_core::DEFAULT_PORT_HANDLE;
use dozer_sql_expression::builder::{ExpressionBuilder, NameOrAlias};
use dozer_sql_expression::sqlparser::ast::{SetOperator, SetQuantifier, TableFactor, With};
use dozer_types::models::udf_config::UdfConfig;

use dozer_sql_expression::sqlparser::{
    ast::{Query, Select, SetExpr, Statement},
    dialect::DozerDialect,
    parser::Parser,
};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::runtime::Runtime;

use super::errors::UnsupportedSqlError;
use super::product::set::set_factory::SetProcessorFactory;

#[derive(Debug, Clone)]
pub struct OutputNodeInfo {
    pub node: String,
    pub port: PortHandle,
}

#[derive(Debug, Clone)]
pub struct QueryContext {
    pub(crate) pipeline_map: HashMap<(usize, String), OutputNodeInfo>,
    pub output_tables_map: HashMap<String, OutputNodeInfo>,
    pub used_sources: Vec<String>,
    pub(crate) processors_list: HashSet<String>,
    processor_counter: usize,
    udfs: Vec<UdfConfig>,
    runtime: Arc<Runtime>,
}

impl QueryContext {
    pub(crate) fn get_next_processor_id(&mut self) -> usize {
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

pub(super) struct BuildScope<'a> {
    pub pipeline: &'a mut AppPipeline,
    pub query_ctx: &'a mut QueryContext,
    pub pipeline_idx: usize,
    pub is_top_select: bool,
}

impl<'a> BuildScope<'a> {
    pub fn as_nested(&mut self) -> BuildScope<'_> {
        BuildScope {
            pipeline: self.pipeline,
            query_ctx: self.query_ctx,
            pipeline_idx: self.pipeline_idx,
            is_top_select: false,
        }
    }
}

// Macro wrapper preserves the 5-argument public crate entrypoint signature
// while keeping internal pipeline construction functions modular and cohesive.
macro_rules! define_statement_to_pipeline {
    () => {
        pub fn statement_to_pipeline(
            sql: &str,
            pipeline: &mut AppPipeline,
            override_name: Option<String>,
            udfs: Vec<UdfConfig>,
            runtime: Arc<Runtime>,
        ) -> Result<QueryContext, PipelineError> {
            let mut ctx = QueryContext::new(udfs, runtime);
            compile_sql_statements(sql, pipeline, override_name, &mut ctx)?;
            Ok(ctx)
        }
    };
}

define_statement_to_pipeline!();

fn compile_sql_statements(
    sql: &str,
    pipeline: &mut AppPipeline,
    override_name: Option<String>,
    ctx: &mut QueryContext,
) -> Result<(), PipelineError> {
    let dialect = DozerDialect {};
    let ast = Parser::parse_sql(&dialect, sql)
        .map_err(|err| PipelineError::InternalError(Box::new(err)))?;
    let query_name = NameOrAlias(format!("query_{}", ctx.get_next_processor_id()), None);

    for (idx, statement) in ast.into_iter().enumerate() {
        let Statement::Query(query) = statement else {
            return Err(PipelineError::UnsupportedSqlError(
                UnsupportedSqlError::GenericError(statement.to_string()),
            ));
        };
        let mut scope = BuildScope {
            pipeline,
            query_ctx: ctx,
            pipeline_idx: idx,
            is_top_select: true,
        };
        query_to_pipeline(
            TableInfo {
                name: query_name.clone(),
                override_name: override_name.clone(),
            },
            *query,
            &mut scope,
        )?;
    }
    Ok(())
}

#[derive(Clone)]
pub(super) struct TableInfo {
    pub name: NameOrAlias,
    pub override_name: Option<String>,
}

struct SetOperationSpec {
    left_select: Box<SetExpr>,
    right_select: Box<SetExpr>,
    set_quantifier: SetQuantifier,
}

pub(super) fn query_to_pipeline(
    table_info: TableInfo,
    query: Query,
    scope: &mut BuildScope<'_>,
) -> Result<(), PipelineError> {
    validate_query_clauses(&query)?;
    if let Some(with) = query.with {
        compile_with_clause(with, scope)?;
    }
    dispatch_query_body(table_info, *query.body, scope)
}

fn validate_query_clauses(query: &Query) -> Result<(), PipelineError> {
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
    Ok(())
}

fn compile_with_clause(with: With, scope: &mut BuildScope<'_>) -> Result<(), PipelineError> {
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
        if scope
            .query_ctx
            .pipeline_map
            .contains_key(&(scope.pipeline_idx, table_name.clone()))
        {
            return Err(InvalidQuery(format!(
                "WITH query name {table_name:?} specified more than once"
            )));
        }
        let mut cte_scope = scope.as_nested();
        query_to_pipeline(
            TableInfo {
                name: NameOrAlias(table_name.clone(), Some(table_name)),
                override_name: None,
            },
            *table.query,
            &mut cte_scope,
        )?;
    }
    Ok(())
}

fn dispatch_query_body(
    table_info: TableInfo,
    body: SetExpr,
    scope: &mut BuildScope<'_>,
) -> Result<(), PipelineError> {
    match body {
        SetExpr::Select(select) => {
            select_to_pipeline(table_info, *select, scope)?;
            Ok(())
        }
        SetExpr::Query(query) => {
            let query_name = format!("subquery_{}", scope.query_ctx.get_next_processor_id());
            let mut sub_ctx =
                QueryContext::new(scope.query_ctx.udfs.clone(), scope.query_ctx.runtime.clone());
            let mut sub_scope = BuildScope {
                pipeline: scope.pipeline,
                query_ctx: &mut sub_ctx,
                pipeline_idx: scope.pipeline_idx,
                is_top_select: false,
            };
            query_to_pipeline(
                TableInfo {
                    name: NameOrAlias(query_name, None),
                    override_name: None,
                },
                *query,
                &mut sub_scope,
            )
        }
        SetExpr::SetOperation {
            op: SetOperator::Union,
            set_quantifier,
            left,
            right,
        } => {
            set_to_pipeline(
                table_info,
                SetOperationSpec {
                    left_select: left,
                    right_select: right,
                    set_quantifier,
                },
                scope,
            )?;
            Ok(())
        }
        SetExpr::SetOperation { op, .. } => Err(PipelineError::InvalidOperator(op.to_string())),
        _ => Err(PipelineError::UnsupportedSqlError(
            UnsupportedSqlError::GenericError("Unsupported query body structure".to_string()),
        )),
    }
}

fn select_to_pipeline(
    table_info: TableInfo,
    select: Select,
    scope: &mut BuildScope<'_>,
) -> Result<String, PipelineError> {
    let Some(from) = select.from.into_iter().next() else {
        return Err(PipelineError::UnsupportedSqlError(
            UnsupportedSqlError::FromCommaSyntax,
        ));
    };

    let connection_info =
        from::insert_from_to_pipeline(from, scope.pipeline, scope.pipeline_idx, scope.query_ctx)?;
    connect_input_sources(connection_info.input_nodes, scope);

    let (in_subqueries, remaining_selection) =
        in_subquery::split_in_subqueries(select.selection);
    let output_node =
        in_subquery::attach_in_subquery_joins(connection_info.output_node, in_subqueries, scope)?;

    let gen_agg_name = create_aggregation_processor(&select, scope);
    connect_selection_and_aggregation(output_node, remaining_selection, &gen_agg_name, scope);
    register_select_outputs(&table_info, select.into, &gen_agg_name, scope)?;

    Ok(gen_agg_name)
}

fn connect_input_sources(
    input_nodes: Vec<(String, String, PortHandle)>,
    scope: &mut BuildScope<'_>,
) {
    for (source_name, processor_name, processor_port) in input_nodes {
        if let Some(info) = scope
            .query_ctx
            .pipeline_map
            .get(&(scope.pipeline_idx, source_name.clone()))
        {
            scope.pipeline.connect_nodes(
                info.node.clone(),
                info.port,
                processor_name,
                processor_port,
            );
        } else {
            scope.query_ctx.used_sources.push(source_name);
        }
    }
}

fn create_aggregation_processor(select: &Select, scope: &mut BuildScope<'_>) -> String {
    let gen_agg_name = format!("agg--{}", scope.query_ctx.get_next_processor_id());
    let aggregation = AggregationProcessorFactory::new(
        gen_agg_name.clone(),
        select.projection.clone(),
        select.group_by.clone(),
        select.having.clone(),
        scope
            .pipeline
            .flags()
            .enable_probabilistic_optimizations
            .in_aggregations
            .unwrap_or(false),
        scope.query_ctx.udfs.clone(),
        scope.query_ctx.runtime.clone(),
    );
    scope
        .pipeline
        .add_processor(Box::new(aggregation), gen_agg_name.clone());
    gen_agg_name
}

fn connect_selection_and_aggregation(
    (gen_product_name, product_output_port): (String, PortHandle),
    selection: Option<dozer_sql_expression::sqlparser::ast::Expr>,
    gen_agg_name: &str,
    scope: &mut BuildScope<'_>,
) {
    let Some(selection_expr) = selection else {
        scope.pipeline.connect_nodes(
            gen_product_name,
            product_output_port,
            gen_agg_name.to_string(),
            DEFAULT_PORT_HANDLE,
        );
        return;
    };

    let gen_selection_name = format!("select--{}", scope.query_ctx.get_next_processor_id());
    let selection_proc = SelectionProcessorFactory::new(
        gen_selection_name.clone(),
        selection_expr,
        scope.query_ctx.udfs.clone(),
        scope.query_ctx.runtime.clone(),
    );
    scope
        .pipeline
        .add_processor(Box::new(selection_proc), gen_selection_name.clone());
    scope.pipeline.connect_nodes(
        gen_product_name,
        product_output_port,
        gen_selection_name.clone(),
        DEFAULT_PORT_HANDLE,
    );
    scope.pipeline.connect_nodes(
        gen_selection_name,
        DEFAULT_PORT_HANDLE,
        gen_agg_name.to_string(),
        DEFAULT_PORT_HANDLE,
    );
}

fn register_select_outputs(
    table_info: &TableInfo,
    into_clause: Option<dozer_sql_expression::sqlparser::ast::SelectInto>,
    gen_agg_name: &str,
    scope: &mut BuildScope<'_>,
) -> Result<(), PipelineError> {
    scope.query_ctx.pipeline_map.insert(
        (scope.pipeline_idx, table_info.name.0.to_string()),
        OutputNodeInfo {
            node: gen_agg_name.to_string(),
            port: DEFAULT_PORT_HANDLE,
        },
    );

    let output_table_name = into_clause
        .map(|into| into.name.to_string())
        .or_else(|| table_info.override_name.clone());

    if scope.is_top_select && output_table_name.is_none() {
        return Err(PipelineError::MissingIntoClause);
    }

    if let Some(table_name) = output_table_name {
        if scope.query_ctx.output_tables_map.contains_key(&table_name) {
            return Err(PipelineError::DuplicateIntoClause(table_name));
        }
        scope.query_ctx.output_tables_map.insert(
            table_name,
            OutputNodeInfo {
                node: gen_agg_name.to_string(),
                port: DEFAULT_PORT_HANDLE,
            },
        );
    }
    Ok(())
}

fn set_to_pipeline(
    table_info: TableInfo,
    spec: SetOperationSpec,
    scope: &mut BuildScope<'_>,
) -> Result<String, PipelineError> {
    let gen_left_set_name = format!("set_left_{}", scope.query_ctx.get_next_processor_id());
    let gen_right_set_name = format!("set_right_{}", scope.query_ctx.get_next_processor_id());

    compile_set_branch(&gen_left_set_name, spec.left_select, "left", scope)?;
    compile_set_branch(&gen_right_set_name, spec.right_select, "right", scope)?;

    let left_output = lookup_set_branch(&gen_left_set_name, "left", scope)?;
    let right_output = lookup_set_branch(&gen_right_set_name, "right", scope)?;

    let gen_set_name = table_info
        .override_name
        .clone()
        .unwrap_or_else(|| format!("set_{}", scope.query_ctx.get_next_processor_id()));

    let set_proc_fac = SetProcessorFactory::new(
        gen_set_name.clone(),
        spec.set_quantifier,
        scope
            .pipeline
            .flags()
            .enable_probabilistic_optimizations
            .in_sets
            .unwrap_or(false),
    );
    scope
        .pipeline
        .add_processor(Box::new(set_proc_fac), gen_set_name.clone());
    scope
        .pipeline
        .connect_nodes(left_output.node, left_output.port, gen_set_name.clone(), 0);
    scope
        .pipeline
        .connect_nodes(right_output.node, right_output.port, gen_set_name.clone(), 1);

    for (_, table_name) in scope.query_ctx.pipeline_map.keys() {
        scope.query_ctx.output_tables_map.remove_entry(table_name);
    }
    scope.query_ctx.pipeline_map.insert(
        (scope.pipeline_idx, table_info.name.0.to_string()),
        OutputNodeInfo {
            node: gen_set_name.clone(),
            port: DEFAULT_PORT_HANDLE,
        },
    );

    Ok(gen_set_name)
}

fn compile_set_branch(
    branch_name: &str,
    branch_expr: Box<SetExpr>,
    side_label: &str,
    scope: &mut BuildScope<'_>,
) -> Result<String, PipelineError> {
    let branch_info = TableInfo {
        name: NameOrAlias(branch_name.to_string(), None),
        override_name: None,
    };
    match *branch_expr {
        SetExpr::Select(select) => select_to_pipeline(branch_info, *select, scope),
        SetExpr::SetOperation {
            op: _,
            set_quantifier,
            left,
            right,
        } => set_to_pipeline(
            branch_info,
            SetOperationSpec {
                left_select: left,
                right_select: right,
                set_quantifier,
            },
            scope,
        ),
        _ => Err(PipelineError::InvalidQuery(format!(
            "Invalid UNION {side_label} Query"
        ))),
    }
}

fn lookup_set_branch(
    branch_name: &str,
    side_label: &str,
    scope: &BuildScope<'_>,
) -> Result<OutputNodeInfo, PipelineError> {
    scope
        .query_ctx
        .pipeline_map
        .get(&(scope.pipeline_idx, branch_name.to_string()))
        .cloned()
        .ok_or_else(|| PipelineError::InvalidQuery(format!("Invalid UNION {side_label} Query")))
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
            let name_or = NameOrAlias(name, alias_name);
            let mut scope = BuildScope {
                pipeline,
                query_ctx,
                pipeline_idx,
                is_top_select: false,
            };
            query_to_pipeline(
                TableInfo {
                    name: name_or.clone(),
                    override_name: None,
                },
                *subquery,
                &mut scope,
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
mod in_subquery;
mod join;
mod table_operator;

pub use common::string_from_sql_object_name;
pub use table_operator::{TableOperatorArg, TableOperatorDescriptor};

#[cfg(test)]
mod tests;
