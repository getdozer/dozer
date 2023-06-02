use std::sync::Arc;

use dozer_core::{
    app::{AppPipeline, PipelineEntryPoint},
    appsource::AppSourceId,
    DEFAULT_PORT_HANDLE,
};
use sqlparser::ast::TableWithJoins;

use crate::pipeline::{
    builder::{get_from_source, QueryContext, SchemaSQLContext},
    errors::PipelineError,
    product::{
        join::factory::{JoinProcessorFactory, LEFT_JOIN_PORT, RIGHT_JOIN_PORT},
        table::factory::get_name_or_alias,
    },
    table_operator::factory::TableOperatorProcessorFactory,
    window::factory::WindowProcessorFactory,
};

use super::from_builder::{
    is_an_entry_point, is_table_operator, ConnectionInfo, TableOperatorDescriptor,
};

#[derive(Clone, Debug)]
enum JoinSource {
    Table(String),
    Operator(ConnectionInfo),
    Join(ConnectionInfo),
}

pub(crate) fn insert_join_to_pipeline(
    from: &TableWithJoins,
    pipeline: &mut AppPipeline<SchemaSQLContext>,
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

        let join_processor_factory = JoinProcessorFactory::new(
            left_name_or_alias.clone(),
            right_name_or_alias,
            join.join_operator.clone(),
        );
        let join_processor_name = format!("join_{}", uuid::Uuid::new_v4());

        let mut pipeline_entry_points = vec![];
        if let JoinSource::Table(ref source_table) = left_join_source {
            if is_an_entry_point(source_table, &mut query_context.pipeline_map, pipeline_idx) {
                let entry_point = PipelineEntryPoint::new(
                    AppSourceId::new(source_table.clone(), None),
                    LEFT_JOIN_PORT,
                );

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
            if is_an_entry_point(source_table, &mut query_context.pipeline_map, pipeline_idx) {
                let entry_point = PipelineEntryPoint::new(
                    AppSourceId::new(source_table.clone(), None),
                    RIGHT_JOIN_PORT,
                );

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
            Arc::new(join_processor_factory),
            &join_processor_name,
            pipeline_entry_points,
        );

        match left_join_source {
            JoinSource::Table(_) => {}
            JoinSource::Operator(ref connection_info) => pipeline.connect_nodes(
                &connection_info.output_node.0,
                Some(connection_info.output_node.1),
                &join_processor_name,
                Some(LEFT_JOIN_PORT),
                true,
            ),
            JoinSource::Join(ref connection_info) => pipeline.connect_nodes(
                &connection_info.output_node.0,
                Some(connection_info.output_node.1),
                &join_processor_name,
                Some(LEFT_JOIN_PORT),
                true,
            ),
        }

        match right_join_source {
            JoinSource::Table(_) => {}
            JoinSource::Operator(connection_info) => pipeline.connect_nodes(
                &connection_info.output_node.0,
                Some(connection_info.output_node.1),
                &join_processor_name,
                Some(RIGHT_JOIN_PORT),
                true,
            ),
            JoinSource::Join(connection_info) => pipeline.connect_nodes(
                &connection_info.output_node.0,
                Some(connection_info.output_node.1),
                &join_processor_name,
                Some(RIGHT_JOIN_PORT),
                true,
            ),
        }

        // TODO: refactor join source name and aliasing logic
        left_name_or_alias = None;
        left_join_source = JoinSource::Join(ConnectionInfo {
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
    source: sqlparser::ast::TableFactor,
    pipeline: &mut AppPipeline<SchemaSQLContext>,
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
    table_operator: &TableOperatorDescriptor,
    pipeline: &mut AppPipeline<SchemaSQLContext>,
    pipeline_idx: usize,
    query_context: &mut QueryContext,
) -> Result<ConnectionInfo, PipelineError> {
    let mut input_nodes = vec![];

    if table_operator.name.to_uppercase() == "TTL" {
        let processor = TableOperatorProcessorFactory::new(table_operator.clone());
        let processor_name = processor.get_name();
        let source_name = processor
            .get_source_name()
            .map_err(PipelineError::TableOperatorError)?;

        let mut entry_points = vec![];

        if is_an_entry_point(&source_name, &mut query_context.pipeline_map, pipeline_idx) {
            let entry_point = PipelineEntryPoint::new(
                AppSourceId::new(source_name.clone(), None),
                DEFAULT_PORT_HANDLE,
            );

            entry_points.push(entry_point);
            query_context.used_sources.push(source_name);
        } else {
            input_nodes.push((source_name, processor_name.clone(), DEFAULT_PORT_HANDLE));
        }

        pipeline.add_processor(Arc::new(processor), &processor_name, entry_points);

        Ok(ConnectionInfo {
            input_nodes,
            output_node: (processor_name, DEFAULT_PORT_HANDLE),
        })
    } else if table_operator.name.to_uppercase() == "TUMBLE"
        || table_operator.name.to_uppercase() == "HOP"
    {
        // for now, we only support window operators
        let window_processor_factory = WindowProcessorFactory::new(table_operator.clone());
        let window_processor_name = format!("window_{}", uuid::Uuid::new_v4());
        let window_source_name = window_processor_factory.get_source_name()?;
        let mut window_entry_points = vec![];

        if is_an_entry_point(
            &window_source_name,
            &mut query_context.pipeline_map,
            pipeline_idx,
        ) {
            let entry_point = PipelineEntryPoint::new(
                AppSourceId::new(window_source_name.clone(), None),
                DEFAULT_PORT_HANDLE,
            );

            window_entry_points.push(entry_point);
            query_context.used_sources.push(window_source_name);
        } else {
            input_nodes.push((
                window_source_name,
                window_processor_name.clone(),
                DEFAULT_PORT_HANDLE,
            ));
        }

        pipeline.add_processor(
            Arc::new(window_processor_factory),
            &window_processor_name,
            window_entry_points,
        );

        Ok(ConnectionInfo {
            input_nodes,
            output_node: (window_processor_name, DEFAULT_PORT_HANDLE),
        })
    } else {
        Err(PipelineError::UnsupportedTableOperator(
            table_operator.name.clone(),
        ))
    }
}

fn is_nested_join(left_table: &sqlparser::ast::TableFactor) -> bool {
    matches!(left_table, sqlparser::ast::TableFactor::NestedJoin { .. })
}
