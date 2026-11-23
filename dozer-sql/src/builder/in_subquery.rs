use std::ops::ControlFlow;

use dozer_core::{app::AppPipeline, node::PortHandle, DEFAULT_PORT_HANDLE};
use dozer_sql_expression::{
    builder::NameOrAlias,
    sqlparser::ast::{Expr, Ident, Value, VisitMut, VisitorMut},
};

use crate::{errors::PipelineError, product::join::mark::MarkJoinProcessorFactory};

use super::{query_to_pipeline, QueryContext, TableInfo};

pub(super) fn insert_mark_joins(
    predicate: &mut Expr,
    input: (String, PortHandle),
    pipeline: &mut AppPipeline,
    context: &mut QueryContext,
    pipeline_idx: usize,
) -> Result<((String, PortHandle), usize), PipelineError> {
    let mut visitor = InSubqueryVisitor {
        input,
        pipeline,
        context,
        pipeline_idx,
        count: 0,
    };
    if let ControlFlow::Break(error) = predicate.visit(&mut visitor) {
        return Err(error);
    }
    Ok((visitor.input, visitor.count))
}

struct InSubqueryVisitor<'a> {
    input: (String, PortHandle),
    pipeline: &'a mut AppPipeline,
    context: &'a mut QueryContext,
    pipeline_idx: usize,
    count: usize,
}

impl InSubqueryVisitor<'_> {
    fn insert(&mut self, expression: &mut Expr) -> Result<(), PipelineError> {
        if !matches!(expression, Expr::InSubquery { .. }) {
            return Ok(());
        }
        let Expr::InSubquery {
            mut expr,
            subquery,
            negated,
        } = std::mem::replace(expression, Expr::Value(Value::Null))
        else {
            unreachable!()
        };
        // Visit the left operand in the current scope; build the inner SELECT
        // independently so its own IN expressions never bind to outer fields.
        if let ControlFlow::Break(error) = expr.visit(self) {
            return Err(error);
        }
        let id = self.context.get_next_processor_id();
        let query_name = format!("__dozer_in_query_{id}");
        let mark_name = format!("__dozer_in_mark_{id}");
        let join_name = format!("mark_join--{id}");
        // Inner WITH names are lexical: they can shadow, but cannot overwrite,
        // outer CTE names used by later subqueries in the same predicate.
        let outer_tables = self.context.pipeline_map.clone();
        let result = query_to_pipeline(
            TableInfo {
                name: NameOrAlias(query_name.clone(), None),
                override_name: None,
            },
            *subquery,
            self.pipeline,
            self.context,
            self.pipeline_idx,
            false,
        );
        let output = self
            .context
            .pipeline_map
            .get(&(self.pipeline_idx, query_name))
            .cloned();
        self.context.pipeline_map = outer_tables;
        result?;
        let output = output
            .ok_or_else(|| PipelineError::InvalidQuery("IN subquery has no output".into()))?;
        self.pipeline.add_processor(
            Box::new(MarkJoinProcessorFactory::new(
                join_name.clone(),
                *expr,
                mark_name.clone(),
                negated,
                self.context.udfs.clone(),
                self.context.runtime.clone(),
            )),
            join_name.clone(),
        );
        self.pipeline
            .connect_nodes(self.input.0.clone(), self.input.1, join_name.clone(), 0);
        self.pipeline
            .connect_nodes(output.node, output.port, join_name.clone(), 1);
        self.input = (join_name, DEFAULT_PORT_HANDLE);
        self.count += 1;
        *expression = Expr::Identifier(Ident::new(mark_name));
        Ok(())
    }
}

impl VisitorMut for InSubqueryVisitor<'_> {
    type Break = PipelineError;

    fn pre_visit_expr(&mut self, expression: &mut Expr) -> ControlFlow<Self::Break> {
        match self.insert(expression) {
            Ok(()) => ControlFlow::Continue(()),
            Err(error) => ControlFlow::Break(error),
        }
    }
}
