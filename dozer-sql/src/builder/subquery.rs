//! Lexical query scope and graph wiring for uncorrelated membership inputs.
use std::collections::HashSet;

use dozer_core::{app::AppPipeline, node::PortHandle};
use dozer_sql_expression::{
    builder::ExpressionBuilder,
    sqlparser::ast::{
        Distinct, Expr, JoinConstraint, JoinOperator, Query, SelectItem, SetExpr, SetOperator,
        TableFactor,
    },
};
use dozer_types::models::udf_config::UdfConfig;

use super::{
    query_to_pipeline,
    table_operator::{is_table_operator, TableOperatorArg, TableOperatorDescriptor},
    OutputNodeInfo, QueryContext, TableInfo,
};

pub(super) fn has_membership(query: &Query) -> bool {
    fn expression(expr: &Expr) -> bool {
        matches!(expr, Expr::InSubquery { .. })
            || children(&mut expr.clone())
                .into_iter()
                .any(|child| expression(child))
    }
    fn relation(table: &TableFactor) -> bool {
        match table {
            TableFactor::Derived { subquery, .. } => has_membership(subquery),
            _ => false,
        }
    }
    fn body(query: &SetExpr) -> bool {
        match query {
            SetExpr::Query(query) => has_membership(query),
            SetExpr::SetOperation { left, right, .. } => body(left) || body(right),
            SetExpr::Select(select) => {
                select
                    .selection
                    .iter()
                    .chain(select.group_by.iter())
                    .chain(select.having.iter())
                    .any(expression)
                    || select.projection.iter().any(|item| match item {
                        SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                            expression(expr)
                        }
                        _ => false,
                    })
                    || select.from.iter().any(|from| {
                        relation(&from.relation)
                            || from.joins.iter().any(|join| relation(&join.relation))
                    })
            }
            _ => false,
        }
    }
    query
        .with
        .as_ref()
        .is_some_and(|with| with.cte_tables.iter().any(|cte| has_membership(&cte.query)))
        || body(&query.body)
}

/// Resolve lexical dependencies before building CTE processors. Definitions
/// shadowed everywhere by nested WITH clauses must not leave orphan DAG nodes.
pub(super) fn required_ctes(query: &Query) -> Result<HashSet<String>, PipelineError> {
    Ok(query_dependencies(query)?.0)
}

fn query_dependencies(query: &Query) -> Result<(HashSet<String>, HashSet<String>), PipelineError> {
    fn expression(expr: &Expr, tables: &mut HashSet<String>) -> Result<(), PipelineError> {
        match expr {
            Expr::InSubquery { subquery, .. } | Expr::Exists { subquery, .. } => {
                tables.extend(query_dependencies(subquery)?.1);
            }
            Expr::Subquery(query) | Expr::ArraySubquery(query) => {
                tables.extend(query_dependencies(query)?.1);
            }
            _ => {}
        }
        for child in children(&mut expr.clone()) {
            expression(child, tables)?;
        }
        Ok(())
    }
    fn operator_source(operator: &TableOperatorDescriptor) -> Result<String, PipelineError> {
        // The existing operator grammar puts its input relation in argument 0.
        // Other arguments are scalar expressions, not relation dependencies.
        match operator.args.first() {
            Some(TableOperatorArg::Argument(arg)) => Ok(
                crate::table_operator::factory::get_source_name(&operator.name, arg)?,
            ),
            Some(TableOperatorArg::Descriptor(nested)) => operator_source(nested),
            None => Err(PipelineError::UnsupportedTableOperator(
                operator.name.clone(),
            )),
        }
    }
    fn relation(table: &TableFactor, tables: &mut HashSet<String>) -> Result<(), PipelineError> {
        match table {
            TableFactor::Table { name, .. } => {
                let source = match is_table_operator(table)? {
                    Some(operator) => operator_source(&operator)?,
                    None => name.to_string(),
                };
                tables.insert(source);
            }
            TableFactor::Derived { subquery, .. } => {
                tables.extend(query_dependencies(subquery)?.1);
            }
            _ => {}
        }
        Ok(())
    }
    fn body(query: &SetExpr, tables: &mut HashSet<String>) -> Result<(), PipelineError> {
        match query {
            SetExpr::Query(query) => tables.extend(query_dependencies(query)?.1),
            SetExpr::SetOperation { left, right, .. } => {
                body(left, tables)?;
                body(right, tables)?;
            }
            SetExpr::Select(select) => {
                for from in &select.from {
                    relation(&from.relation, tables)?;
                    for join in &from.joins {
                        relation(&join.relation, tables)?;
                        match &join.join_operator {
                            JoinOperator::Inner(JoinConstraint::On(expr))
                            | JoinOperator::LeftOuter(JoinConstraint::On(expr))
                            | JoinOperator::RightOuter(JoinConstraint::On(expr)) => {
                                expression(expr, tables)?;
                            }
                            _ => {}
                        }
                    }
                }
                for expr in select
                    .selection
                    .iter()
                    .chain(&select.group_by)
                    .chain(&select.having)
                {
                    expression(expr, tables)?;
                }
                for item in &select.projection {
                    if let SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } =
                        item
                    {
                        expression(expr, tables)?;
                    }
                }
            }
            _ => {}
        }
        Ok(())
    }
    let mut free = HashSet::new();
    let mut required = HashSet::new();
    body(&query.body, &mut free)?;
    if let Some(with) = &query.with {
        for cte in with.cte_tables.iter().rev() {
            let name = cte.alias.name.to_string();
            if free.remove(&name) {
                required.insert(name);
                free.extend(query_dependencies(&cte.query)?.1);
            }
        }
    }
    Ok((required, free))
}
use crate::{
    errors::PipelineError,
    selection::dynamic::{
        membership_port,
        plan::{children, validate_repeatable, Plan},
        DynamicSelectionFactory,
    },
};

pub(super) fn scoped_query(
    table: TableInfo,
    query: Query,
    pipeline: &mut AppPipeline,
    ctx: &mut QueryContext,
    index: usize,
    strict_qualifiers: bool,
) -> Result<OutputNodeInfo, PipelineError> {
    let name = table.name.0.clone();
    let mut scoped = ctx.clone();
    scoped.strict_qualifiers = strict_qualifiers;
    scoped.output_tables_map.clear();
    scoped.used_sources.clear();
    // A local WITH can shadow an inherited CTE. Duplicate local definitions are
    // still rejected by query_to_pipeline, and none leak back into the parent.
    if let Some(with) = &query.with {
        for cte in &with.cte_tables {
            scoped
                .pipeline_map
                .remove(&(index, cte.alias.name.to_string()));
        }
    }
    query_to_pipeline(table, query, pipeline, &mut scoped, index, false)?;
    let output = scoped
        .pipeline_map
        .get(&(index, name))
        .cloned()
        .ok_or_else(|| PipelineError::InvalidQuery("Subquery did not produce an output".into()))?;
    ctx.processor_counter = scoped.processor_counter;
    ctx.processors_list = scoped.processors_list;
    ctx.used_sources.extend(scoped.used_sources);
    Ok(output)
}

pub(super) fn selection(
    statement: &Expr,
    id: &str,
    pipeline: &mut AppPipeline,
    ctx: &mut QueryContext,
    index: usize,
) -> Result<Option<DynamicSelectionFactory>, PipelineError> {
    let mut queries = vec![];
    let plan = Plan::parse(statement.clone(), &mut |query| {
        let port = membership_port(queries.len())?;
        queries.push(query);
        Ok(port)
    })?;
    if queries.is_empty() {
        return Ok(None);
    }
    let input_count = queries.len() as PortHandle;
    for (offset, mut query) in queries.into_iter().enumerate() {
        validate_query(&mut query, &ctx.udfs)?;
        let name = format!("membership_{}", ctx.get_next_processor_id());
        let output = scoped_query(
            TableInfo {
                name: dozer_sql_expression::builder::NameOrAlias(name, None),
                override_name: None,
            },
            query,
            pipeline,
            ctx,
            index,
            true,
        )?;
        pipeline.connect_nodes(
            output.node,
            output.port,
            id.to_owned(),
            (offset + 1) as PortHandle,
        );
    }
    Ok(Some(DynamicSelectionFactory {
        id: id.into(),
        plan,
        input_count,
        udfs: ctx.udfs.clone(),
        runtime: ctx.runtime.clone(),
    }))
}

fn unsupported(message: impl Into<String>) -> PipelineError {
    PipelineError::InvalidQuery(message.into())
}

pub(super) fn validate_query(query: &mut Query, udfs: &[UdfConfig]) -> Result<(), PipelineError> {
    if query.fetch.is_some() || !query.locks.is_empty() {
        return Err(unsupported(
            "FETCH and locking clauses are not supported in IN subqueries",
        ));
    }
    if let Some(with) = &mut query.with {
        for cte in &mut with.cte_tables {
            if !cte.alias.columns.is_empty() {
                return Err(unsupported(
                    "CTE column alias lists are not supported in IN subqueries",
                ));
            }
            validate_query(&mut cte.query, udfs)?;
        }
    }
    validate_body(&mut query.body, udfs)
}

fn validate_body(body: &mut SetExpr, udfs: &[UdfConfig]) -> Result<(), PipelineError> {
    match body {
        SetExpr::Query(query) => validate_query(query, udfs),
        SetExpr::SetOperation {
            op: SetOperator::Union,
            left,
            right,
            ..
        } => {
            validate_body(left, udfs)?;
            validate_body(right, udfs)
        }
        SetExpr::Select(select) => {
            if select.from.len() != 1
                || select.top.is_some()
                || select.into.is_some()
                || !select.lateral_views.is_empty()
                || !select.cluster_by.is_empty()
                || !select.distribute_by.is_empty()
                || !select.sort_by.is_empty()
                || !select.named_window.is_empty()
                || select.qualify.is_some()
                || matches!(&select.distinct, Some(Distinct::On(_)))
            {
                return Err(unsupported("Unsupported SELECT clause in IN subquery"));
            }
            // DISTINCT is implemented after projection/aggregation. It must not
            // be discarded here: a containing aggregate can observe its counts.
            let mut local = HashSet::new();
            let from = &mut select.from[0];
            relation_scope(&mut from.relation, &mut local, udfs)?;
            for join in &mut from.joins {
                relation_scope(&mut join.relation, &mut local, udfs)?;
            }
            for join in &mut from.joins {
                match &mut join.join_operator {
                    JoinOperator::Inner(JoinConstraint::On(expr))
                    | JoinOperator::LeftOuter(JoinConstraint::On(expr))
                    | JoinOperator::RightOuter(JoinConstraint::On(expr)) => {
                        validate_expr(expr, &local, udfs)?
                    }
                    _ => {} // Existing join planner supplies the precise unsupported-join error.
                }
            }
            for item in &mut select.projection {
                match item {
                    SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                        validate_expr(expr, &local, udfs)?
                    }
                    SelectItem::QualifiedWildcard(name, _) => {
                        if !local.contains(&name.to_string()) {
                            return Err(unsupported(
                                "Correlated qualified wildcard in IN subquery",
                            ));
                        }
                    }
                    _ => {}
                }
            }
            for expr in select
                .selection
                .iter_mut()
                .chain(select.group_by.iter_mut())
                .chain(select.having.iter_mut())
            {
                validate_expr(expr, &local, udfs)?;
            }
            Ok(())
        }
        _ => Err(unsupported(
            "Only SELECT and UNION are supported in IN subqueries",
        )),
    }
}

fn relation_scope(
    relation: &mut TableFactor,
    local: &mut HashSet<String>,
    udfs: &[UdfConfig],
) -> Result<(), PipelineError> {
    match relation {
        TableFactor::Table {
            name,
            alias,
            args,
            with_hints,
        } => {
            if args.is_some() || !with_hints.is_empty() {
                return Err(unsupported(
                    "Table functions and hints are not supported in IN subqueries",
                ));
            }
            if let Some(alias) = alias {
                if !alias.columns.is_empty() {
                    return Err(unsupported(
                        "Table column alias lists are not supported in IN subqueries",
                    ));
                }
                local.insert(ExpressionBuilder::normalize_ident(&alias.name));
            } else {
                local.insert(
                    name.0
                        .iter()
                        .map(ExpressionBuilder::normalize_ident)
                        .collect::<Vec<_>>()
                        .join("."),
                );
                if let Some(last) = name.0.last() {
                    local.insert(ExpressionBuilder::normalize_ident(last));
                }
            }
        }
        TableFactor::Derived {
            lateral,
            subquery,
            alias,
        } => {
            if *lateral {
                return Err(unsupported(
                    "Correlated/lateral IN subqueries are not supported",
                ));
            }
            validate_query(subquery, udfs)?;
            if let Some(alias) = alias {
                if !alias.columns.is_empty() {
                    return Err(unsupported(
                        "Derived column alias lists are not supported in IN subqueries",
                    ));
                }
                local.insert(ExpressionBuilder::normalize_ident(&alias.name));
            }
        }
        _ => return Err(unsupported("Unsupported relation in IN subquery")),
    }
    Ok(())
}

fn validate_expr(
    expr: &mut Expr,
    local: &HashSet<String>,
    udfs: &[UdfConfig],
) -> Result<(), PipelineError> {
    validate_repeatable(expr, udfs)?;
    if let Expr::CompoundIdentifier(parts) = expr {
        let qualifier = parts[..parts.len() - 1]
            .iter()
            .map(ExpressionBuilder::normalize_ident)
            .collect::<Vec<_>>()
            .join(".");
        if !local.contains(&qualifier) {
            return Err(unsupported(format!(
                "Correlated or unknown qualifier {qualifier:?} in IN subquery"
            )));
        }
    }
    if let Expr::InSubquery { subquery, .. } = expr {
        validate_query(subquery, udfs)?;
    }
    if let Expr::Function(function) = expr {
        if function.over.is_some() || !function.order_by.is_empty() || function.distinct {
            return Err(unsupported(
                "Window, ordered and DISTINCT functions are not supported in IN subqueries",
            ));
        }
    }
    for child in children(expr) {
        validate_expr(child, local, udfs)?;
    }
    Ok(())
}
