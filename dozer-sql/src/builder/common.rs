use dozer_sql_expression::{
    builder::{ExpressionBuilder, NameOrAlias},
    sqlparser::ast::{ObjectName, TableFactor},
};

use crate::errors::{PipelineError, ProductError};

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

pub fn get_name_or_alias(relation: &TableFactor) -> Result<NameOrAlias, PipelineError> {
    match relation {
        TableFactor::Table { name, alias, .. } => {
            let table_name = string_from_sql_object_name(name);
            if let Some(table_alias) = alias {
                let alias = table_alias.name.value.clone();
                return Ok(NameOrAlias(table_name, Some(alias)));
            }
            Ok(NameOrAlias(table_name, None))
        }
        TableFactor::Derived { alias, .. } => {
            if let Some(table_alias) = alias {
                let alias = table_alias.name.value.clone();
                return Ok(NameOrAlias("dozer_derived".to_string(), Some(alias)));
            }
            Ok(NameOrAlias("dozer_derived".to_string(), None))
        }
        TableFactor::TableFunction { .. } => Err(PipelineError::ProductError(
            ProductError::UnsupportedTableFunction,
        )),
        TableFactor::UNNEST { .. } => {
            Err(PipelineError::ProductError(ProductError::UnsupportedUnnest))
        }
        TableFactor::NestedJoin { alias, .. } => {
            if let Some(table_alias) = alias {
                let alias = table_alias.name.value.clone();
                return Ok(NameOrAlias("dozer_nested".to_string(), Some(alias)));
            }
            Ok(NameOrAlias("dozer_nested".to_string(), None))
        }
        TableFactor::Pivot { .. } => {
            Err(PipelineError::ProductError(ProductError::UnsupportedPivot))
        }
    }
}

/// A CTE is a local relation, even when its projected fields retain the source
/// metadata of an underlying table (or have no table source after a UNION).
pub(super) fn get_scoped_name_or_alias(
    relation: &TableFactor,
    context: &QueryContext,
    pipeline_idx: usize,
) -> Result<NameOrAlias, PipelineError> {
    let mut name = get_name_or_alias(relation)?;
    if context.strict_qualifiers
        && name.1.is_none()
        && context
            .pipeline_map
            .contains_key(&(pipeline_idx, name.0.clone()))
    {
        name.1 = Some(name.0.clone());
    }
    Ok(name)
}

pub fn string_from_sql_object_name(name: &ObjectName) -> String {
    name.0
        .iter()
        .map(ExpressionBuilder::normalize_ident)
        .collect::<Vec<String>>()
        .join(".")
}
