use dozer_core::{
    app::{AppPipeline, PipelineEntryPoint},
    DEFAULT_PORT_HANDLE,
};
use dozer_sql_expression::sqlparser::ast::{
    Expr, FunctionArg, FunctionArgExpr, ObjectName, TableFactor,
};

use crate::{
    errors::PipelineError,
    table_operator::factory::{get_source_name, TableOperatorProcessorFactory},
    window::factory::WindowProcessorFactory,
};

use super::{
    common::{is_a_pipeline_output, is_an_entry_point, string_from_sql_object_name},
    ConnectionInfo, QueryContext,
};

#[derive(Clone, Debug)]
pub struct TableOperatorDescriptor {
    pub name: String,
    pub args: Vec<TableOperatorArg>,
}

#[derive(Clone, Debug)]
pub enum TableOperatorArg {
    Argument(FunctionArg),
    Descriptor(TableOperatorDescriptor),
}

pub fn is_table_operator(
    relation: &TableFactor,
) -> Result<Option<TableOperatorDescriptor>, PipelineError> {
    match relation {
        TableFactor::Table { name, args, .. } => {
            if args.is_none() {
                return Ok(None);
            }
            let operator = get_table_operator_descriptor(name, args)?;

            Ok(operator)
        }
        TableFactor::Derived { .. } => Ok(None),
        TableFactor::TableFunction { .. } => Err(PipelineError::UnsupportedTableFunction),
        TableFactor::UNNEST { .. } => Err(PipelineError::UnsupportedUnnest),
        TableFactor::NestedJoin { .. } => Err(PipelineError::UnsupportedNestedJoin),
        TableFactor::Pivot { .. } => Err(PipelineError::UnsupportedPivot),
    }
}

fn get_table_operator_descriptor(
    name: &ObjectName,
    args: &Option<Vec<FunctionArg>>,
) -> Result<Option<TableOperatorDescriptor>, PipelineError> {
    let mut operator_args = vec![];

    if let Some(args) = args {
        for arg in args {
            let operator_arg = get_table_operator_arg(arg)?;
            operator_args.push(operator_arg);
        }
    }

    Ok(Some(TableOperatorDescriptor {
        name: string_from_sql_object_name(name),
        args: operator_args,
    }))
}

fn get_table_operator_arg(arg: &FunctionArg) -> Result<TableOperatorArg, PipelineError> {
    match arg {
        FunctionArg::Named { name, arg: _ } => {
            Err(PipelineError::UnsupportedTableOperator(name.to_string()))
        }
        FunctionArg::Unnamed(arg_expr) => match arg_expr {
            FunctionArgExpr::Expr(Expr::Function(function)) => {
                let operator_descriptor = get_table_operator_descriptor(
                    &function.clone().name,
                    &Some(function.clone().args),
                )?;
                if let Some(descriptor) = operator_descriptor {
                    Ok(TableOperatorArg::Descriptor(descriptor))
                } else {
                    Err(PipelineError::UnsupportedTableOperator(
                        string_from_sql_object_name(&function.name),
                    ))
                }
            }
            _ => Ok(TableOperatorArg::Argument(arg.clone())),
        },
    }
}

pub fn insert_table_operator_processor_to_pipeline(
    operator: &TableOperatorDescriptor,
    pipeline: &mut AppPipeline,
    pipeline_idx: usize,
    query_context: &mut QueryContext,
) -> Result<ConnectionInfo, PipelineError> {
    // the sources names that are used in this pipeline

    let mut input_nodes = vec![];

    if operator.name.to_uppercase() == "TTL" {
        let mut entry_points = vec![];

        let processor_name = generate_name("TOP", operator, query_context);
        if !query_context.processors_list.insert(processor_name.clone()) {
            return Err(PipelineError::ProcessorAlreadyExists(processor_name));
        }
        let processor = TableOperatorProcessorFactory::new(
            processor_name.clone(),
            operator.clone(),
            query_context.udfs.to_owned(),
            query_context.runtime.clone(),
        );

        if let Some(table) = operator.args.first() {
            let source_name = match table {
                TableOperatorArg::Argument(argument) => get_source_name(&operator.name, argument)?,
                TableOperatorArg::Descriptor(descriptor) => {
                    let connection_info = insert_table_operator_processor_to_pipeline(
                        descriptor,
                        pipeline,
                        pipeline_idx,
                        query_context,
                    )?;
                    connection_info.output_node.0
                }
            };

            if is_an_entry_point(&source_name.clone(), query_context, pipeline_idx) {
                let entry_point = PipelineEntryPoint::new(source_name.clone(), DEFAULT_PORT_HANDLE);

                entry_points.push(entry_point);
                query_context.used_sources.push(source_name.clone());
            } else if is_a_pipeline_output(&source_name, query_context, pipeline_idx) {
                input_nodes.push((
                    source_name.clone(),
                    processor_name.clone(),
                    DEFAULT_PORT_HANDLE,
                ));
            }

            pipeline.add_processor(Box::new(processor), &processor_name.clone(), entry_points);

            if !is_an_entry_point(&source_name.clone(), query_context, pipeline_idx)
                && !is_a_pipeline_output(&source_name.clone(), query_context, pipeline_idx)
            {
                pipeline.connect_nodes(
                    &source_name,
                    DEFAULT_PORT_HANDLE,
                    &processor_name.clone(),
                    DEFAULT_PORT_HANDLE,
                );
            }

            Ok(ConnectionInfo {
                input_nodes,
                output_node: (processor_name, DEFAULT_PORT_HANDLE),
            })
        } else {
            Err(PipelineError::UnsupportedTableOperator(
                operator.name.clone(),
            ))
        }
    } else if operator.name.to_uppercase() == "TUMBLE" || operator.name.to_uppercase() == "HOP" {
        let mut entry_points = vec![];

        let processor_name = generate_name("WIN", operator, query_context);
        if !query_context.processors_list.insert(processor_name.clone()) {
            return Err(PipelineError::ProcessorAlreadyExists(processor_name));
        }
        let processor = WindowProcessorFactory::new(processor_name.clone(), operator.clone());

        if let Some(table) = operator.args.first() {
            let source_name = match table {
                TableOperatorArg::Argument(argument) => get_source_name(&operator.name, argument)?,
                TableOperatorArg::Descriptor(descriptor) => {
                    let connection_info = insert_table_operator_processor_to_pipeline(
                        descriptor,
                        pipeline,
                        pipeline_idx,
                        query_context,
                    )?;
                    connection_info.output_node.0
                }
            };

            if is_an_entry_point(&source_name, query_context, pipeline_idx) {
                let entry_point = PipelineEntryPoint::new(source_name.clone(), DEFAULT_PORT_HANDLE);

                entry_points.push(entry_point);
                query_context.used_sources.push(source_name.clone());
            } else if is_a_pipeline_output(&source_name.clone(), query_context, pipeline_idx) {
                input_nodes.push((
                    source_name.clone(),
                    processor_name.to_owned(),
                    DEFAULT_PORT_HANDLE,
                ));
            }

            pipeline.add_processor(Box::new(processor), &processor_name, entry_points);

            if !is_an_entry_point(&source_name.clone(), query_context, pipeline_idx)
                && !is_a_pipeline_output(&source_name.clone(), query_context, pipeline_idx)
            {
                pipeline.connect_nodes(
                    &source_name,
                    DEFAULT_PORT_HANDLE,
                    &processor_name,
                    DEFAULT_PORT_HANDLE,
                );
            }

            Ok(ConnectionInfo {
                input_nodes,
                output_node: (processor_name, DEFAULT_PORT_HANDLE),
            })
        } else {
            Err(PipelineError::UnsupportedTableOperator(
                operator.name.clone(),
            ))
        }
    } else {
        Err(PipelineError::UnsupportedTableOperator(
            operator.name.clone(),
        ))
    }
}

fn generate_name(
    prefix: &str,
    operator: &TableOperatorDescriptor,
    query_context: &mut QueryContext,
) -> String {
    let processor_name = format!(
        "{0}_{1}_{2}",
        prefix,
        operator.name,
        query_context.get_next_processor_id()
    );
    processor_name
}
