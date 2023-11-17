use dozer_core::{
    app::{AppPipeline, PipelineEntryPoint},
    node::PortHandle,
    DEFAULT_PORT_HANDLE,
};
use dozer_sql_expression::sqlparser::ast::TableWithJoins;

use crate::{
    builder::{get_from_source, QueryContext},
    errors::PipelineError,
    product::{
        join::factory::{JoinProcessorFactory, LEFT_JOIN_PORT, RIGHT_JOIN_PORT},
        table::factory::get_name_or_alias,
    },
    table_operator::factory::{get_source_name, TableOperatorProcessorFactory},
    window::factory::WindowProcessorFactory,
};

use super::from_builder::{
    generate_name, insert_table_operator_processor_to_pipeline, is_a_pipeline_output,
    is_an_entry_point, is_table_operator, ConnectionInfo, TableOperatorArg,
    TableOperatorDescriptor,
};

#[derive(Clone, Debug)]
enum JoinSource {
    Table(String),
    Operator(ConnectionInfo),
    Join(ConnectionInfo),
}

pub(crate) fn insert_join_to_pipeline(
    from: &TableWithJoins,
    pipeline: &mut AppPipeline,
    pipeline_idx: usize,
    query_context: &mut QueryContext,
) -> Result<ConnectionInfo, PipelineError> {
    let mut input_nodes = vec![];

    let left_table = &from.relation;
    let mut left_name_or_alias = Some(get_name_or_alias(left_table)?);
    let mut left_join_source =
        insert_join_source_to_pipeline(left_table.clone(), pipeline, pipeline_idx, query_context)?;

    for join in &from.joins {
        let right_table = &join.relation;
        let right_name_or_alias = Some(get_name_or_alias(right_table)?);
        let right_join_source = insert_join_source_to_pipeline(
            right_table.clone(),
            pipeline,
            pipeline_idx,
            query_context,
        )?;

        let join_processor_name = format!("join_{}", query_context.get_next_processor_id());
        if !query_context
            .processors_list
            .insert(join_processor_name.clone())
        {
            return Err(PipelineError::ProcessorAlreadyExists(join_processor_name));
        }
        let join_processor_factory = JoinProcessorFactory::new(
            join_processor_name.clone(),
            left_name_or_alias.clone(),
            right_name_or_alias,
            join.join_operator.clone(),
            pipeline
                .flags()
                .enable_probabilistic_optimizations
                .in_joins
                .unwrap_or(false),
        );

        let mut pipeline_entry_points = vec![];
        if let JoinSource::Table(ref source_table) = left_join_source {
            if is_an_entry_point(source_table, query_context, pipeline_idx) {
                let entry_point = PipelineEntryPoint::new(source_table.clone(), LEFT_JOIN_PORT);

                pipeline_entry_points.push(entry_point);
                query_context.used_sources.push(source_table.to_string());
            } else {
                input_nodes.push((
                    source_table.to_string(),
                    join_processor_name.clone(),
                    LEFT_JOIN_PORT,
                ));
            }
        }

        if let JoinSource::Table(ref source_table) = right_join_source.clone() {
            if is_an_entry_point(source_table, query_context, pipeline_idx) {
                let entry_point = PipelineEntryPoint::new(source_table.clone(), RIGHT_JOIN_PORT);

                pipeline_entry_points.push(entry_point);
                query_context.used_sources.push(source_table.to_string());
            } else {
                input_nodes.push((
                    source_table.to_string(),
                    join_processor_name.clone(),
                    RIGHT_JOIN_PORT,
                ));
            }
        }

        pipeline.add_processor(
            Box::new(join_processor_factory),
            &join_processor_name,
            pipeline_entry_points,
        );

        match left_join_source {
            JoinSource::Table(_) => {}
            JoinSource::Operator(ref connection_info) => pipeline.connect_nodes(
                &connection_info.output_node.0,
                connection_info.output_node.1,
                &join_processor_name,
                LEFT_JOIN_PORT,
            ),
            JoinSource::Join(ref connection_info) => pipeline.connect_nodes(
                &connection_info.output_node.0,
                connection_info.output_node.1,
                &join_processor_name,
                LEFT_JOIN_PORT,
            ),
        }

        match right_join_source {
            JoinSource::Table(_) => {}
            JoinSource::Operator(connection_info) => pipeline.connect_nodes(
                &connection_info.output_node.0,
                connection_info.output_node.1,
                &join_processor_name,
                RIGHT_JOIN_PORT,
            ),
            JoinSource::Join(connection_info) => pipeline.connect_nodes(
                &connection_info.output_node.0,
                connection_info.output_node.1,
                &join_processor_name,
                RIGHT_JOIN_PORT,
            ),
        }

        // TODO: refactor join source name and aliasing logic
        left_name_or_alias = None;
        left_join_source = JoinSource::Join(ConnectionInfo {
            processor_name: join_processor_name.clone(),
            input_nodes: input_nodes.clone(),
            output_node: (join_processor_name, DEFAULT_PORT_HANDLE),
        });
    }

    match left_join_source {
        JoinSource::Table(_) => Err(PipelineError::InvalidJoin(
            "No JOIN operator found".to_string(),
        )),
        JoinSource::Operator(_) => Err(PipelineError::InvalidJoin(
            "No JOIN operator found".to_string(),
        )),
        JoinSource::Join(connection_info) => Ok(connection_info),
    }
}

// TODO: refactor this
fn insert_join_source_to_pipeline(
    source: dozer_sql_expression::sqlparser::ast::TableFactor,
    pipeline: &mut AppPipeline,
    pipeline_idx: usize,
    query_context: &mut QueryContext,
) -> Result<JoinSource, PipelineError> {
    let join_source = if let Some(table_operator) = is_table_operator(&source)? {
        let connection_info = insert_table_operator_to_pipeline(
            &table_operator,
            pipeline,
            pipeline_idx,
            query_context,
        )?;
        JoinSource::Operator(connection_info)
    } else if is_nested_join(&source) {
        return Err(PipelineError::InvalidJoin(
            "Nested JOINs are not supported".to_string(),
        ));
    } else {
        let name_or_alias = get_from_source(&source, pipeline, query_context, pipeline_idx)?;
        JoinSource::Table(name_or_alias.0)
    };
    Ok(join_source)
}

fn insert_table_operator_to_pipeline(
    operator: &TableOperatorDescriptor,
    pipeline: &mut AppPipeline,
    pipeline_idx: usize,
    query_context: &mut QueryContext,
) -> Result<ConnectionInfo, PipelineError> {
    let mut entry_points = vec![];
    let mut input_nodes = vec![];

    if operator.name.to_uppercase() == "TTL" {
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

        if let Some(table) = operator.args.get(0) {
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
        // for now, we only support window operators
        let processor_name = format!("window_{}", query_context.get_next_processor_id());
        if !query_context.processors_list.insert(processor_name.clone()) {
            return Err(PipelineError::ProcessorAlreadyExists(processor_name));
        }
        let processor = WindowProcessorFactory::new(processor_name.clone(), operator.clone());
        if let Some(table) = operator.args.get(0) {
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

            pipeline.add_processor(Box::new(processor), &processor_name.clone(), entry_points);

            if !is_an_entry_point(&source_name, query_context, pipeline_idx)
                && !is_a_pipeline_output(&source_name, query_context, pipeline_idx)
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
    } else {
        Err(PipelineError::UnsupportedTableOperator(
            operator.name.clone(),
        ))
    }
}

fn is_nested_join(left_table: &dozer_sql_expression::sqlparser::ast::TableFactor) -> bool {
    matches!(
        left_table,
        dozer_sql_expression::sqlparser::ast::TableFactor::NestedJoin { .. }
    )
}
