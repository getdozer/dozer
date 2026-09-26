use dozer_core::{app::AppPipeline, DEFAULT_PORT_HANDLE};
use dozer_sql_expression::{
    builder::NameOrAlias,
    sqlparser::ast::{
        Expr, FunctionArg, FunctionArgExpr, Ident, Query, SelectItem, SetExpr, TableFactor,
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
    if query.fetch.is_some() || !query.locks.is_empty() {
        return Err(PipelineError::InvalidQuery(
            "IN subqueries do not support FETCH or locking clauses".into(),
        ));
    }
    let SetExpr::Select(select) = query.body.as_ref() else {
        return Err(PipelineError::InvalidQuery(
            "IN subqueries currently require a SELECT body; set operations are unsupported".into(),
        ));
    };
    if query.with.is_some()
        || select.into.is_some()
        || select.distinct.is_some()
        || !select.group_by.is_empty()
        || select.having.is_some()
    {
        return Err(PipelineError::InvalidQuery(
            "IN subqueries do not yet support WITH, INTO, DISTINCT, GROUP BY or HAVING".into(),
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
    if select.from.len() != 1
        || !select.from[0].joins.is_empty()
        || !matches!(select.from[0].relation, TableFactor::Table { .. })
    {
        return Err(PipelineError::InvalidQuery(
            "IN subqueries currently require one named input relation".into(),
        ));
    }
    if let TableFactor::Table {
        args,
        with_hints,
        alias,
        ..
    } = &select.from[0].relation
    {
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
    }
    for projection in &select.projection {
        match projection {
            SelectItem::UnnamedExpr(Expr::Identifier(_) | Expr::CompoundIdentifier(_))
            | SelectItem::ExprWithAlias { expr: Expr::Identifier(_) | Expr::CompoundIdentifier(_), .. }
            => {}
            SelectItem::Wildcard(options) | SelectItem::QualifiedWildcard(_, options)
                if options.opt_exclude.is_none() && options.opt_except.is_none()
                    && options.opt_rename.is_none() && options.opt_replace.is_none() => {}
            _ => return Err(PipelineError::InvalidQuery(
                "IN subqueries currently require a column projection; computed and aggregate projections are unsupported".into(),
            )),
        }
    }
    Ok(())
}
