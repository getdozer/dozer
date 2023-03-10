use dozer_core::node::PortHandle;
use sqlparser::ast::{FunctionArg, ObjectName, TableFactor, TableWithJoins};

use crate::pipeline::{
    builder::QueryContext, errors::PipelineError, window::builder::string_from_sql_object_name,
};

struct ConnectionInfo {
    pub input_nodes: Vec<(String, String, PortHandle)>,
    pub output_node: (String, PortHandle),
    pub used_sources: Vec<String>,
}

#[derive(Debug)]
pub struct TableOperator {
    pub name: ObjectName,
    pub args: Vec<FunctionArg>,
}

fn insert_from_to_pipeline(
    from: &TableWithJoins,
    pipeline_idx: usize,
    query_context: &mut QueryContext,
) -> Result<ConnectionInfo, PipelineError> {
    if from.joins.is_empty() {
        insert_table_processor_to_pipeline(from.relation, pipeline_idx, query_context)
    } else {
        insert_join_to_pipeline(from, pipeline_idx, query_context)
    }
}

fn insert_join_to_pipeline(
    from: &TableWithJoins,
    pipeline_idx: usize,
    query_context: &mut QueryContext,
) -> Result<ConnectionInfo, PipelineError> {
    todo!()
}

fn insert_table_processor_to_pipeline(
    relation: TableFactor,
    pipeline_idx: usize,
    query_context: &mut QueryContext,
) -> Result<ConnectionInfo, PipelineError> {
    if let Some(table_operator) = table_is_an_operator(&relation)? {
        insert_function_processor_to_pipeline(table_operator, pipeline_idx, query_context)
    } else {
        insert_table_to_pipeline(relation, pipeline_idx, query_context)
    }
}

fn insert_function_processor_to_pipeline(
    operator: TableOperator,
    pipeline_idx: usize,
    query_context: &mut QueryContext,
) -> Result<ConnectionInfo, PipelineError> {
    let function_name = string_from_sql_object_name(operator.name);

    if function_name.to_uppercase() == "TUMBLE" {
        let column_index = get_window_column_index(args, schema)?;
        let interval_arg = args
            .get(2)
            .ok_or(WindowError::WindowMissingIntervalArgument)?;
        let interval = get_window_interval(interval_arg)?;

        Ok(Some(WindowType::Tumble {
            column_index,
            interval,
        }))
    } else if function_name.to_uppercase() == "HOP" {
        let column_index = get_window_column_index(args, schema)?;
        let hop_arg = args
            .get(2)
            .ok_or(WindowError::WindowMissingHopSizeArgument)?;
        let hop_size = get_window_hop(hop_arg)?;
        let interval_arg = args
            .get(3)
            .ok_or(WindowError::WindowMissingIntervalArgument)?;
        let interval = get_window_interval(interval_arg)?;

        return Ok(Some(WindowType::Hop {
            column_index,
            hop_size,
            interval,
        }));
    } else {
        return Err(PipelineError::UnsupportedTableOperator(function_name));
    }
}

pub fn table_is_an_operator(
    relation: &TableFactor,
) -> Result<Option<TableOperator>, PipelineError> {
    match relation {
        TableFactor::Table { name, args, .. } => {
            if args.is_some() {
                Ok(Some(TableOperator {
                    name: name.clone(),
                    args: args.clone().unwrap(),
                }))
            } else {
                Ok(None)
            }
        }
        TableFactor::Derived { .. } => Ok(None),
        TableFactor::TableFunction { .. } => Err(PipelineError::UnsupportedTableFunction),
        TableFactor::UNNEST { .. } => Err(PipelineError::UnsupportedUnnest),
        TableFactor::NestedJoin { .. } => Err(PipelineError::UnsupportedNestedJoin),
    }
}
