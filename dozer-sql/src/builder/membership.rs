use dozer_core::{app::AppPipeline, DEFAULT_PORT_HANDLE};
use dozer_sql_expression::{
    builder::NameOrAlias,
    sqlparser::ast::{
        Expr, FunctionArg, FunctionArgExpr, Ident, Query, SelectItem, SetExpr, SetOperator,
        TableFactor,
    },
};

use crate::{
    errors::PipelineError,
    selection::membership::{MembershipProcessorFactory, OUTER_PORT, VALUES_PORT},
};

use super::{query_to_pipeline, OutputNodeInfo, QueryContext, TableInfo};

pub(super) fn plan(
    predicate: &mut Expr,
    input: OutputNodeInfo,
    pipeline: &mut AppPipeline,
    context: &mut QueryContext,
    pipeline_idx: usize,
) -> Result<(OutputNodeInfo, Vec<String>), PipelineError> {
    let mut planner = MembershipPlanner {
        pipeline,
        context,
        pipeline_idx,
        input,
        columns: Vec::new(),
    };
    planner.rewrite(predicate)?;
    Ok((planner.input, planner.columns))
}

struct MembershipPlanner<'a> {
    pipeline: &'a mut AppPipeline,
    context: &'a mut QueryContext,
    pipeline_idx: usize,
    input: OutputNodeInfo,
    columns: Vec<String>,
}

impl MembershipPlanner<'_> {
    fn rewrite(&mut self, expression: &mut Expr) -> Result<(), PipelineError> {
        match expression {
            Expr::InSubquery {
                expr,
                subquery,
                negated,
            } => {
                self.rewrite(expr)?;
                validate_query(subquery)?;
                let id = self.context.get_next_processor_id();
                let table = format!("__membership_input_{id}");
                let column = format!("__membership_result_{id}");
                let node = format!("membership--{id}");
                // Preserve visible CTEs, but do not expose this query's generated
                // table names to a subsequent sibling subquery.
                let visible_tables = self.context.pipeline_map.clone();
                let visible_requirements = self.context.replay_requirements.clone();
                let result = query_to_pipeline(
                    TableInfo {
                        name: NameOrAlias(table.clone(), None),
                        override_name: None,
                    },
                    *subquery.clone(),
                    self.pipeline,
                    self.context,
                    self.pipeline_idx,
                    false,
                );
                if let Some(requirements) = self
                    .context
                    .replay_requirements
                    .get(&(self.pipeline_idx, table.clone()))
                {
                    for requirement in requirements {
                        requirement.require();
                    }
                }
                let values = self
                    .context
                    .pipeline_map
                    .get(&(self.pipeline_idx, table))
                    .cloned();
                self.context.pipeline_map = visible_tables;
                self.context.replay_requirements = visible_requirements;
                result?;
                let values = values.ok_or_else(|| {
                    PipelineError::InvalidQuery("IN subquery has no output relation".into())
                })?;
                self.pipeline.add_processor(
                    Box::new(MembershipProcessorFactory {
                        id: node.clone(),
                        operand: *expr.clone(),
                        negated: *negated,
                        column: column.clone(),
                        udfs: self.context.udfs.clone(),
                        runtime: self.context.runtime.clone(),
                    }),
                    node.clone(),
                );
                self.pipeline.connect_nodes(
                    self.input.node.clone(),
                    self.input.port,
                    node.clone(),
                    OUTER_PORT,
                );
                self.pipeline
                    .connect_nodes(values.node, values.port, node.clone(), VALUES_PORT);
                self.input = OutputNodeInfo {
                    node,
                    port: DEFAULT_PORT_HANDLE,
                };
                self.columns.push(column.clone());
                *expression = Expr::Identifier(Ident::new(column));
            }
            Expr::BinaryOp { left, right, .. } => {
                self.rewrite(left)?;
                self.rewrite(right)?;
            }
            Expr::Nested(expr)
            | Expr::UnaryOp { expr, .. }
            | Expr::IsNull(expr)
            | Expr::IsNotNull(expr)
            | Expr::Cast { expr, .. }
            | Expr::Extract { expr, .. } => self.rewrite(expr)?,
            Expr::InList { expr, list, .. } => {
                self.rewrite(expr)?;
                for item in list {
                    self.rewrite(item)?;
                }
            }
            Expr::Like { expr, pattern, .. } => {
                self.rewrite(expr)?;
                self.rewrite(pattern)?;
            }
            Expr::Function(function) => {
                for argument in &mut function.args {
                    let (FunctionArg::Named { arg, .. } | FunctionArg::Unnamed(arg)) = argument;
                    if let FunctionArgExpr::Expr(expr) = arg {
                        self.rewrite(expr)?;
                    }
                }
            }
            Expr::Case {
                operand,
                conditions,
                results,
                else_result,
            } => {
                if let Some(operand) = operand {
                    self.rewrite(operand)?;
                }
                for item in conditions.iter_mut().chain(results.iter_mut()) {
                    self.rewrite(item)?;
                }
                if let Some(otherwise) = else_result {
                    self.rewrite(otherwise)?;
                }
            }
            // Unsupported expression forms are still rejected by the normal
            // ExpressionBuilder rather than being silently discarded here.
            _ => {}
        }
        Ok(())
    }
}

fn validate_query(query: &Query) -> Result<(), PipelineError> {
    validate_query_scope(query, false)
}

pub(super) fn validate_query_scope(query: &Query, allow_into: bool) -> Result<(), PipelineError> {
    if query.fetch.is_some() || !query.locks.is_empty() {
        return Err(PipelineError::InvalidQuery(
            "IN subqueries do not support FETCH or locking clauses".into(),
        ));
    }
    if let Some(with) = &query.with {
        for cte in &with.cte_tables {
            if !cte.alias.columns.is_empty() {
                return Err(PipelineError::InvalidQuery(
                    "IN subqueries do not support CTE column aliases".into(),
                ));
            }
            validate_query(&cte.query)?;
        }
    }
    validate_body(&query.body, allow_into)
}

fn validate_body(body: &SetExpr, allow_into: bool) -> Result<(), PipelineError> {
    let select = match body {
        SetExpr::Select(select) => select,
        SetExpr::Query(query) => return validate_query_scope(query, allow_into),
        SetExpr::SetOperation {
            op: SetOperator::Union,
            left,
            right,
            ..
        } => {
            validate_body(left, false)?;
            return validate_body(right, false);
        }
        _ => {
            return Err(PipelineError::InvalidQuery(
                "IN subqueries require a SELECT or UNION body".into(),
            ))
        }
    };
    if (!allow_into && select.into.is_some()) || select.distinct.is_some() {
        return Err(PipelineError::InvalidQuery(
            "IN subqueries do not support INTO or SELECT DISTINCT".into(),
        ));
    }
    if select.top.is_some()
        || select.qualify.is_some()
        || !select.lateral_views.is_empty()
        || !select.cluster_by.is_empty()
        || !select.distribute_by.is_empty()
        || !select.sort_by.is_empty()
        || !select.named_window.is_empty()
    {
        return Err(PipelineError::InvalidQuery(
            "IN subquery contains unsupported SELECT modifiers".into(),
        ));
    }
    if select.from.len() != 1 {
        return Err(PipelineError::InvalidQuery(
            "IN subqueries require one FROM relation, optionally with JOINs".into(),
        ));
    }
    validate_relation(&select.from[0].relation)?;
    for join in &select.from[0].joins {
        validate_relation(&join.relation)?;
    }
    for projection in &select.projection {
        match projection {
            SelectItem::UnnamedExpr(expression)
            | SelectItem::ExprWithAlias {
                expr: expression, ..
            } => {
                validate_expression(expression)?;
            }
            SelectItem::Wildcard(options) | SelectItem::QualifiedWildcard(_, options)
                if options.opt_exclude.is_some()
                    || options.opt_except.is_some()
                    || options.opt_rename.is_some()
                    || options.opt_replace.is_some() =>
            {
                return Err(PipelineError::InvalidQuery(
                    "IN subqueries do not support wildcard modifiers".into(),
                ));
            }
            _ => {}
        }
    }
    for expression in select
        .selection
        .iter()
        .chain(select.group_by.iter())
        .chain(select.having.iter())
    {
        validate_expression(expression)?;
    }
    Ok(())
}

fn validate_expression(expression: &Expr) -> Result<(), PipelineError> {
    match expression {
        Expr::InSubquery { expr, subquery, .. } => {
            validate_expression(expr)?;
            validate_query(subquery)?;
        }
        Expr::BinaryOp { left, right, .. } => {
            validate_expression(left)?;
            validate_expression(right)?;
        }
        Expr::Nested(expr)
        | Expr::UnaryOp { expr, .. }
        | Expr::IsNull(expr)
        | Expr::IsNotNull(expr)
        | Expr::Cast { expr, .. }
        | Expr::Extract { expr, .. } => {
            validate_expression(expr)?;
        }
        Expr::Interval(interval) => validate_expression(&interval.value)?,
        Expr::InList { expr, list, .. } => {
            validate_expression(expr)?;
            for item in list {
                validate_expression(item)?;
            }
        }
        Expr::Like { expr, pattern, .. } => {
            validate_expression(expr)?;
            validate_expression(pattern)?;
        }
        Expr::Function(function) => {
            if function.distinct || function.over.is_some() || !function.order_by.is_empty() {
                return Err(PipelineError::InvalidQuery(
                    "IN subqueries do not support DISTINCT, OVER or ORDER BY function modifiers"
                        .into(),
                ));
            }
            for argument in &function.args {
                let (FunctionArg::Named { arg, .. } | FunctionArg::Unnamed(arg)) = argument;
                if let FunctionArgExpr::Expr(expression) = arg {
                    validate_expression(expression)?;
                }
            }
        }
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            for expression in operand.iter().chain(else_result.iter()) {
                validate_expression(expression)?;
            }
            for expression in conditions.iter().chain(results.iter()) {
                validate_expression(expression)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn validate_relation(relation: &TableFactor) -> Result<(), PipelineError> {
    match relation {
        TableFactor::Table {
            args,
            with_hints,
            alias,
            ..
        } => {
            if args.is_some()
                || !with_hints.is_empty()
                || alias
                    .as_ref()
                    .is_some_and(|alias| !alias.columns.is_empty())
            {
                return Err(PipelineError::InvalidQuery(
                    "IN subqueries do not support table arguments, hints or column aliases".into(),
                ));
            }
            Ok(())
        }
        TableFactor::Derived {
            lateral,
            subquery,
            alias,
        } => {
            if *lateral
                || alias
                    .as_ref()
                    .is_some_and(|alias| !alias.columns.is_empty())
            {
                return Err(PipelineError::InvalidQuery(
                    "IN subqueries do not support lateral relations or column aliases".into(),
                ));
            }
            validate_query(subquery)
        }
        _ => Err(PipelineError::InvalidQuery(
            "IN subquery contains an unsupported input relation".into(),
        )),
    }
}
