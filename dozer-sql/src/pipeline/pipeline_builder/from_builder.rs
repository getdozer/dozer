use std::{collections::HashMap, sync::Arc};

use dozer_core::{
    app::{AppPipeline, PipelineEntryPoint},
    appsource::AppSourceId,
    node::PortHandle,
    DEFAULT_PORT_HANDLE,
};
use sqlparser::ast::{FunctionArg, ObjectName, TableFactor, TableWithJoins};

use crate::pipeline::{
    builder::{get_from_source, OutputNodeInfo, QueryContext, SchemaSQLContext},
    errors::PipelineError,
    product::table::factory::TableProcessorFactory,
    window::{builder::string_from_sql_object_name, factory::WindowProcessorFactory},
};

use super::join_builder::insert_join_to_pipeline;

#[derive(Clone, Debug)]
pub struct ConnectionInfo {
    pub input_nodes: Vec<(String, String, PortHandle)>,
    pub output_node: (String, PortHandle),
}

#[derive(Clone, Debug)]
pub struct TableOperator {
    pub name: ObjectName,
    pub args: Vec<FunctionArg>,
}

pub fn insert_from_to_pipeline(
    from: &TableWithJoins,
    pipeline: &mut AppPipeline<SchemaSQLContext>,
    pipeline_idx: usize,
    query_context: &mut QueryContext,
) -> Result<ConnectionInfo, PipelineError> {
    if from.joins.is_empty() {
        insert_table_processor_to_pipeline(&from.relation, pipeline, pipeline_idx, query_context)
    } else {
        insert_join_to_pipeline(from, pipeline, pipeline_idx, query_context)
    }
}

fn insert_table_processor_to_pipeline(
    relation: &TableFactor,
    pipeline: &mut AppPipeline<SchemaSQLContext>,
    pipeline_idx: usize,
    query_context: &mut QueryContext,
) -> Result<ConnectionInfo, PipelineError> {
    if let Some(operator) = is_table_operator(relation)? {
        insert_function_processor_to_pipeline(
            relation,
            &operator,
            pipeline,
            pipeline_idx,
            query_context,
        )
    } else {
        insert_table_to_pipeline(relation, pipeline, pipeline_idx, query_context)
    }
}

fn insert_table_to_pipeline(
    relation: &TableFactor,
    pipeline: &mut AppPipeline<SchemaSQLContext>,
    pipeline_idx: usize,
    query_context: &mut QueryContext,
) -> Result<ConnectionInfo, PipelineError> {
    // let relation_name_or_alias = get_name_or_alias(relation)?;
    let relation_name_or_alias = get_from_source(relation, pipeline, query_context, pipeline_idx)?;

    let product_processor_factory = TableProcessorFactory::new(relation.to_owned());
    let product_processor_name = format!("from_{}", uuid::Uuid::new_v4());
    let product_input_name = relation_name_or_alias.0;

    let mut input_nodes = vec![];
    let mut product_entry_points = vec![];

    // is a node that is an entry point to the pipeline
    if is_an_entry_point(
        &product_input_name,
        &mut query_context.pipeline_map,
        pipeline_idx,
    ) {
        let entry_point = PipelineEntryPoint::new(
            AppSourceId::new(product_input_name.clone(), None),
            DEFAULT_PORT_HANDLE,
        );

        product_entry_points.push(entry_point);
        query_context.used_sources.push(product_input_name);
    }
    // is a node that is connected to another pipeline
    else {
        input_nodes.push((
            product_input_name,
            product_processor_name.clone(),
            DEFAULT_PORT_HANDLE,
        ));
    }

    pipeline.add_processor(
        Arc::new(product_processor_factory),
        &product_processor_name,
        product_entry_points,
    );

    Ok(ConnectionInfo {
        input_nodes,
        output_node: (product_processor_name, DEFAULT_PORT_HANDLE),
    })
}

fn insert_function_processor_to_pipeline(
    relation: &TableFactor,
    operator: &TableOperator,
    pipeline: &mut AppPipeline<SchemaSQLContext>,
    pipeline_idx: usize,
    query_context: &mut QueryContext,
) -> Result<ConnectionInfo, PipelineError> {
    // the sources names that are used in this pipeline
    let mut input_nodes = vec![];

    let product_processor = TableProcessorFactory::new(relation.clone());
    let product_processor_name = format!("product_{}", uuid::Uuid::new_v4());

    pipeline.add_processor(Arc::new(product_processor), &product_processor_name, vec![]);

    let function_name = string_from_sql_object_name(&operator.name.clone());

    if function_name.to_uppercase() == "TUMBLE" || function_name.to_uppercase() == "HOP" {
        let window_processor = WindowProcessorFactory::new(operator.clone());
        let window_processor_name = format!("window_{}", uuid::Uuid::new_v4());
        let window_source_name = window_processor.get_source_name()?;
        let mut window_entry_points = vec![];

        if is_an_entry_point(
            &window_source_name,
            &mut query_context.pipeline_map,
            pipeline_idx,
        ) {
            let entry_point = PipelineEntryPoint::new(
                AppSourceId::new(window_source_name.clone(), None),
                DEFAULT_PORT_HANDLE as PortHandle,
            );

            window_entry_points.push(entry_point);
            query_context.used_sources.push(window_source_name);
        } else {
            input_nodes.push((
                window_source_name,
                window_processor_name.clone(),
                DEFAULT_PORT_HANDLE as PortHandle,
            ));
        }

        pipeline.add_processor(
            Arc::new(window_processor),
            &window_processor_name,
            window_entry_points,
        );

        pipeline.connect_nodes(
            &window_processor_name,
            Some(DEFAULT_PORT_HANDLE),
            &product_processor_name,
            Some(DEFAULT_PORT_HANDLE),
            true,
        )?;

        Ok(ConnectionInfo {
            input_nodes,
            output_node: (product_processor_name, DEFAULT_PORT_HANDLE),
        })
    } else {
        Err(PipelineError::UnsupportedTableOperator(function_name))
    }
}

pub fn is_table_operator(relation: &TableFactor) -> Result<Option<TableOperator>, PipelineError> {
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

pub fn is_an_entry_point(
    name: &str,
    pipeline_map: &mut HashMap<(usize, String), OutputNodeInfo>,
    pipeline_idx: usize,
) -> bool {
    if !pipeline_map.contains_key(&(pipeline_idx, name.to_owned())) {
        return true;
    }
    false
}
