use dozer_core::{node::PortHandle, DEFAULT_PORT_HANDLE};
use dozer_sql_expression::{
    builder::NameOrAlias,
    sqlparser::ast::{BinaryOperator, Expr as SqlExpr, Query},
};

use crate::{
    errors::PipelineError,
    product::join::factory::{JoinProcessorFactory, LEFT_JOIN_PORT, RIGHT_JOIN_PORT},
};

use super::{query_to_pipeline, BuildScope, TableInfo};

#[derive(Debug, Clone)]
pub(super) struct InSubqueryClause {
    pub expr: SqlExpr,
    pub subquery: Query,
    pub negated: bool,
}

// Separate `expr [NOT] IN (SELECT ...)` predicates from scalar `WHERE` conditions
// so streaming subquery membership is evaluated through JoinProcessorFactory nodes.
pub(super) fn split_in_subqueries(
    selection: Option<SqlExpr>,
) -> (Vec<InSubqueryClause>, Option<SqlExpr>) {
    let Some(expr) = selection else {
        return (vec![], None);
    };
    let mut clauses = Vec::new();
    let remaining = extract_from_expr(expr, &mut clauses);
    (clauses, remaining)
}

fn extract_from_expr(expr: SqlExpr, clauses: &mut Vec<InSubqueryClause>) -> Option<SqlExpr> {
    match expr {
        SqlExpr::InSubquery {
            expr,
            subquery,
            negated,
        } => {
            clauses.push(InSubqueryClause {
                expr: *expr,
                subquery: *subquery,
                negated,
            });
            None
        }
        SqlExpr::Nested(inner) => extract_from_expr(*inner, clauses).map(Box::new).map(SqlExpr::Nested),
        SqlExpr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => combine_and_branches(
            extract_from_expr(*left, clauses),
            extract_from_expr(*right, clauses),
        ),
        other => Some(other),
    }
}

fn combine_and_branches(left: Option<SqlExpr>, right: Option<SqlExpr>) -> Option<SqlExpr> {
    match (left, right) {
        (Some(l), Some(r)) => Some(SqlExpr::BinaryOp {
            left: Box::new(l),
            op: BinaryOperator::And,
            right: Box::new(r),
        }),
        (Some(l), None) => Some(l),
        (None, Some(r)) => Some(r),
        (None, None) => None,
    }
}

// Materialize each extracted `IN (SELECT ...)` subquery into the pipeline DAG and
// wire its output port into a LeftSemi / LeftAnti JoinProcessorFactory node.
pub(super) fn attach_in_subquery_joins(
    mut current_output: (String, PortHandle),
    clauses: Vec<InSubqueryClause>,
    scope: &mut BuildScope<'_>,
) -> Result<(String, PortHandle), PipelineError> {
    for clause in clauses {
        current_output = attach_single_subquery_join(current_output, clause, scope)?;
    }
    Ok(current_output)
}

fn attach_single_subquery_join(
    current_output: (String, PortHandle),
    clause: InSubqueryClause,
    scope: &mut BuildScope<'_>,
) -> Result<(String, PortHandle), PipelineError> {
    let subquery_name = format!("in_subquery_{}", scope.query_ctx.get_next_processor_id());
    let subquery_table = TableInfo {
        name: NameOrAlias(subquery_name.clone(), Some(subquery_name.clone())),
        override_name: None,
    };
    let mut sub_scope = scope.as_nested();
    query_to_pipeline(subquery_table, clause.subquery, &mut sub_scope)?;

    let subquery_output = scope
        .query_ctx
        .pipeline_map
        .get(&(scope.pipeline_idx, subquery_name))
        .cloned()
        .ok_or_else(|| PipelineError::InvalidQuery("Invalid IN subquery pipeline".to_string()))?;

    let join_name = format!("in_join_{}", scope.query_ctx.get_next_processor_id());
    if !scope.query_ctx.processors_list.insert(join_name.clone()) {
        return Err(PipelineError::ProcessorAlreadyExists(join_name));
    }
    let probabilistic = scope
        .pipeline
        .flags()
        .enable_probabilistic_optimizations
        .in_joins
        .unwrap_or(false);
    let factory = JoinProcessorFactory::new_in_subquery(
        join_name.clone(),
        clause.expr,
        clause.negated,
        probabilistic,
    );
    scope.pipeline.add_processor(Box::new(factory), join_name.clone());
    scope.pipeline.connect_nodes(
        current_output.0,
        current_output.1,
        join_name.clone(),
        LEFT_JOIN_PORT,
    );
    scope.pipeline.connect_nodes(
        subquery_output.node,
        subquery_output.port,
        join_name.clone(),
        RIGHT_JOIN_PORT,
    );

    Ok((join_name, DEFAULT_PORT_HANDLE))
}

#[cfg(test)]
mod tests {
    use crate::builder::statement_to_pipeline;
    use dozer_core::app::AppPipeline;
    use std::sync::Arc;
    use tokio::runtime::Builder;

    #[test]
    fn test_in_and_not_in_subquery_pipeline() {
        let sql = r#"
            SELECT u.id, u.name
            INTO matched_users
            FROM users u
            WHERE u.id IN (SELECT o.user_id FROM orders o WHERE o.amount > 100)
              AND u.active = 1;

            SELECT id
            INTO unmatched_users
            FROM users
            WHERE id NOT IN (SELECT user_id FROM banned_users);
        "#;
        let runtime = Arc::new(Builder::new_current_thread().enable_all().build().unwrap());
        let context = statement_to_pipeline(
            sql,
            &mut AppPipeline::new_with_default_flags(),
            None,
            vec![],
            runtime,
        )
        .unwrap();

        assert!(context.output_tables_map.contains_key("matched_users"));
        assert!(context.output_tables_map.contains_key("unmatched_users"));
        assert!(context.used_sources.contains(&"users".to_string()));
        assert!(context.used_sources.contains(&"orders".to_string()));
        assert!(context.used_sources.contains(&"banned_users".to_string()));
    }
}
