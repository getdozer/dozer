use dozer_sql_expression::{builder::ExpressionBuilder, sqlparser::ast::ObjectName};

use super::QueryContext;

pub fn is_an_entry_point(name: &str, query_context: &QueryContext, pipeline_idx: usize) -> bool {
    if query_context
        .pipeline_map
        .contains_key(&(pipeline_idx, name.to_owned()))
    {
        return false;
    }
    if query_context.processors_list.contains(&name.to_owned()) {
        return false;
    }
    true
}

pub fn is_a_pipeline_output(
    name: &str,
    query_context: &mut QueryContext,
    pipeline_idx: usize,
) -> bool {
    if query_context
        .pipeline_map
        .contains_key(&(pipeline_idx, name.to_owned()))
    {
        return true;
    }
    false
}

pub fn string_from_sql_object_name(name: &ObjectName) -> String {
    name.0
        .iter()
        .map(ExpressionBuilder::normalize_ident)
        .collect::<Vec<String>>()
        .join(".")
}
