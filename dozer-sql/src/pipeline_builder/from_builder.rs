use dozer_core::{
    app::{AppPipeline, PipelineEntryPoint},
    node::PortHandle,
    DEFAULT_PORT_HANDLE,
};
use dozer_sql_expression::{
    builder::ExpressionBuilder,
    sqlparser::ast::{Expr, FunctionArg, FunctionArgExpr, ObjectName, TableFactor, TableWithJoins},
};

use crate::{
    builder::{get_from_source, QueryContext},
    errors::PipelineError,
    product::table::factory::TableProcessorFactory,
    table_operator::factory::{get_source_name, TableOperatorProcessorFactory},
    window::factory::WindowProcessorFactory,
};

use super::join_builder::insert_join_to_pipeline;

#[derive(Clone, Debug)]
pub struct ConnectionInfo {
    pub processor_name: String,
    pub input_nodes: Vec<(String, String, PortHandle)>,
    pub output_node: (String, PortHandle),
}

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

pub fn insert_from_to_pipeline(
    from: &TableWithJoins,
    pipeline: &mut AppPipeline,
    pipeline_idx: usize,
    query_context: &mut QueryContext,
) -> Result<ConnectionInfo, PipelineError> {
    if from.joins.is_empty() {
        insert_table_to_pipeline(&from.relation, pipeline, pipeline_idx, query_context)
    } else {
        insert_join_to_pipeline(from, pipeline, pipeline_idx, query_context)
    }
}

fn insert_table_to_pipeline(
    relation: &TableFactor,
    pipeline: &mut AppPipeline,
    pipeline_idx: usize,
    query_context: &mut QueryContext,
) -> Result<ConnectionInfo, PipelineError> {
    if let Some(operator) = is_table_operator(relation)? {
        let product_processor_name =
            insert_from_processor_to_pipeline(query_context, relation, pipeline);

        let connection_info = insert_table_operator_processor_to_pipeline(
            &operator,
            pipeline,
            pipeline_idx,
            query_context,
        )?;

        pipeline.connect_nodes(
            &connection_info.output_node.0,
            connection_info.output_node.1,
            &product_processor_name.clone(),
            DEFAULT_PORT_HANDLE,
        );

        Ok(ConnectionInfo {
            processor_name: product_processor_name.clone(),
            input_nodes: connection_info.input_nodes,
            output_node: (product_processor_name, DEFAULT_PORT_HANDLE),
        })
    } else {
        insert_table_processor_to_pipeline(relation, pipeline, pipeline_idx, query_context)
    }
}

fn insert_table_processor_to_pipeline(
    relation: &TableFactor,
    pipeline: &mut AppPipeline,
    pipeline_idx: usize,
    query_context: &mut QueryContext,
) -> Result<ConnectionInfo, PipelineError> {
    // let relation_name_or_alias = get_name_or_alias(relation)?;
    let relation_name_or_alias = get_from_source(relation, pipeline, query_context, pipeline_idx)?;

    let processor_name = format!(
        "from:{}--{}",
        relation_name_or_alias.0,
        query_context.get_next_processor_id()
    );
    if !query_context.processors_list.insert(processor_name.clone()) {
        return Err(PipelineError::ProcessorAlreadyExists(processor_name));
    }
    let product_processor_factory =
        TableProcessorFactory::new(processor_name.clone(), relation.to_owned());

    let product_input_name = relation_name_or_alias.0;

    let mut input_nodes = vec![];
    let mut product_entry_points = vec![];

    // is a node that is an entry point to the pipeline
    if is_an_entry_point(&product_input_name, query_context, pipeline_idx) {
        let entry_point = PipelineEntryPoint::new(product_input_name.clone(), DEFAULT_PORT_HANDLE);

        product_entry_points.push(entry_point);
        query_context.used_sources.push(product_input_name);
    }
    // is a node that is connected to another pipeline
    else {
        input_nodes.push((
            product_input_name,
            processor_name.clone(),
            DEFAULT_PORT_HANDLE,
        ));
    }

    pipeline.add_processor(
        Box::new(product_processor_factory),
        &processor_name,
        product_entry_points,
    );

    Ok(ConnectionInfo {
        processor_name: processor_name.clone(),
        input_nodes,
        output_node: (processor_name, DEFAULT_PORT_HANDLE),
    })
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
                    connection_info.processor_name
                }
            };

            if is_an_entry_point(&source_name.clone(), query_context, pipeline_idx) {
                let entry_point =
                    PipelineEntryPoint::new(source_name.clone(), DEFAULT_PORT_HANDLE as PortHandle);

                entry_points.push(entry_point);
                query_context.used_sources.push(source_name.clone());
            } else if is_a_pipeline_output(&source_name, query_context, pipeline_idx) {
                input_nodes.push((
                    source_name.clone(),
                    processor_name.clone(),
                    DEFAULT_PORT_HANDLE as PortHandle,
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
                processor_name: processor_name.clone(),
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
                    connection_info.processor_name
                }
            };

            if is_an_entry_point(&source_name, query_context, pipeline_idx) {
                let entry_point =
                    PipelineEntryPoint::new(source_name.clone(), DEFAULT_PORT_HANDLE as PortHandle);

                entry_points.push(entry_point);
                query_context.used_sources.push(source_name.clone());
            } else if is_a_pipeline_output(&source_name.clone(), query_context, pipeline_idx) {
                input_nodes.push((
                    source_name.clone(),
                    processor_name.to_owned(),
                    DEFAULT_PORT_HANDLE as PortHandle,
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
                processor_name: processor_name.clone(),
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

pub fn generate_name(
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

fn insert_from_processor_to_pipeline(
    query_context: &mut QueryContext,
    relation: &TableFactor,
    pipeline: &mut AppPipeline,
) -> String {
    let product_processor_name = format!("from--{}", query_context.get_next_processor_id());
    let product_processor =
        TableProcessorFactory::new(product_processor_name.clone(), relation.clone());

    pipeline.add_processor(Box::new(product_processor), &product_processor_name, vec![]);
    product_processor_name
}

pub fn get_table_operator_arg(arg: &FunctionArg) -> Result<TableOperatorArg, PipelineError> {
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

pub fn get_table_operator_descriptor(
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
