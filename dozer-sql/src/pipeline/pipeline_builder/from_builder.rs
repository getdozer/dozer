use std::collections::HashMap;

use dozer_core::{
    app::{AppPipeline, PipelineEntryPoint},
    node::PortHandle,
    DEFAULT_PORT_HANDLE,
};
use dozer_types::models::udf_config::UdfConfig;
use sqlparser::ast::{FunctionArg, ObjectName, TableFactor, TableWithJoins};

use crate::pipeline::{
    builder::{get_from_source, OutputNodeInfo, QueryContext, SchemaSQLContext},
    errors::PipelineError,
    expression::builder::ExpressionBuilder,
    product::table::factory::TableProcessorFactory,
    table_operator::factory::TableOperatorProcessorFactory,
    window::factory::WindowProcessorFactory,
};

use super::join_builder::insert_join_to_pipeline;

#[derive(Clone, Debug)]
pub struct ConnectionInfo {
    pub input_nodes: Vec<(String, String, PortHandle)>,
    pub output_node: (String, PortHandle),
}

#[derive(Clone, Debug)]
pub struct TableOperatorDescriptor {
    pub name: String,
    pub args: Vec<FunctionArg>,
}

pub fn insert_from_to_pipeline(
    from: &TableWithJoins,
    pipeline: &mut AppPipeline<SchemaSQLContext>,
    pipeline_idx: usize,
    query_context: &mut QueryContext,
    udfs: &Vec<UdfConfig>,
) -> Result<ConnectionInfo, PipelineError> {
    if from.joins.is_empty() {
        insert_table_to_pipeline(&from.relation, pipeline, pipeline_idx, query_context, udfs)
    } else {
        insert_join_to_pipeline(from, pipeline, pipeline_idx, query_context, udfs)
    }
}

fn insert_table_to_pipeline(
    relation: &TableFactor,
    pipeline: &mut AppPipeline<SchemaSQLContext>,
    pipeline_idx: usize,
    query_context: &mut QueryContext,
    udfs: &Vec<UdfConfig>,
) -> Result<ConnectionInfo, PipelineError> {
    if let Some(operator) = is_table_operator(relation)? {
        insert_table_operator_processor_to_pipeline(
            relation,
            &operator,
            pipeline,
            pipeline_idx,
            query_context,
            udfs,
        )
    } else {
        insert_table_processor_to_pipeline(relation, pipeline, pipeline_idx, query_context, udfs)
    }
}

fn insert_table_processor_to_pipeline(
    relation: &TableFactor,
    pipeline: &mut AppPipeline<SchemaSQLContext>,
    pipeline_idx: usize,
    query_context: &mut QueryContext,
    udfs: &Vec<UdfConfig>,
) -> Result<ConnectionInfo, PipelineError> {
    // let relation_name_or_alias = get_name_or_alias(relation)?;
    let relation_name_or_alias =
        get_from_source(relation, pipeline, query_context, pipeline_idx, udfs)?;

    let product_processor_name = format!(
        "from:{}--{}",
        relation_name_or_alias.0,
        query_context.get_next_processor_id()
    );
    let product_processor_factory =
        TableProcessorFactory::new(product_processor_name.clone(), relation.to_owned());

    let product_input_name = relation_name_or_alias.0;

    let mut input_nodes = vec![];
    let mut product_entry_points = vec![];

    // is a node that is an entry point to the pipeline
    if is_an_entry_point(
        &product_input_name,
        &mut query_context.pipeline_map,
        pipeline_idx,
    ) {
        let entry_point = PipelineEntryPoint::new(product_input_name.clone(), DEFAULT_PORT_HANDLE);

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
        Box::new(product_processor_factory),
        &product_processor_name,
        product_entry_points,
    );

    Ok(ConnectionInfo {
        input_nodes,
        output_node: (product_processor_name, DEFAULT_PORT_HANDLE),
    })
}

fn insert_table_operator_processor_to_pipeline(
    relation: &TableFactor,
    operator: &TableOperatorDescriptor,
    pipeline: &mut AppPipeline<SchemaSQLContext>,
    pipeline_idx: usize,
    query_context: &mut QueryContext,
    udfs: &[UdfConfig],
) -> Result<ConnectionInfo, PipelineError> {
    // the sources names that are used in this pipeline
    let mut input_nodes = vec![];

    let product_processor_name = format!("join--{}", query_context.get_next_processor_id());
    let product_processor =
        TableProcessorFactory::new(product_processor_name.clone(), relation.clone());

    pipeline.add_processor(Box::new(product_processor), &product_processor_name, vec![]);

    if operator.name.to_uppercase() == "TTL" {
        let processor_name = format!(
            "TOP_{0}_{1}",
            operator.name,
            query_context.get_next_processor_id()
        );
        let processor = TableOperatorProcessorFactory::new(
            processor_name.clone(),
            operator.clone(),
            udfs.to_owned(),
        );

        let source_name = processor
            .get_source_name()
            .map_err(PipelineError::TableOperatorError)?;

        let mut entry_points = vec![];

        if is_an_entry_point(&source_name, &mut query_context.pipeline_map, pipeline_idx) {
            let entry_point =
                PipelineEntryPoint::new(source_name.clone(), DEFAULT_PORT_HANDLE as PortHandle);

            entry_points.push(entry_point);
            query_context.used_sources.push(source_name);
        } else {
            input_nodes.push((
                source_name,
                processor_name.to_owned(),
                DEFAULT_PORT_HANDLE as PortHandle,
            ));
        }

        pipeline.add_processor(Box::new(processor), &processor_name, entry_points);

        pipeline.connect_nodes(
            &processor_name,
            DEFAULT_PORT_HANDLE,
            &product_processor_name,
            DEFAULT_PORT_HANDLE,
        );

        Ok(ConnectionInfo {
            input_nodes,
            output_node: (product_processor_name, DEFAULT_PORT_HANDLE),
        })
    } else if operator.name.to_uppercase() == "TUMBLE" || operator.name.to_uppercase() == "HOP" {
        let window_processor_name = format!("window--{}", query_context.get_next_processor_id());
        let window_processor =
            WindowProcessorFactory::new(window_processor_name.clone(), operator.clone());

        let window_source_name = window_processor.get_source_name()?;
        let mut window_entry_points = vec![];

        if is_an_entry_point(
            &window_source_name,
            &mut query_context.pipeline_map,
            pipeline_idx,
        ) {
            let entry_point = PipelineEntryPoint::new(
                window_source_name.clone(),
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
            Box::new(window_processor),
            &window_processor_name,
            window_entry_points,
        );

        pipeline.connect_nodes(
            &window_processor_name,
            DEFAULT_PORT_HANDLE,
            &product_processor_name,
            DEFAULT_PORT_HANDLE,
        );

        Ok(ConnectionInfo {
            input_nodes,
            output_node: (product_processor_name, DEFAULT_PORT_HANDLE),
        })
    } else {
        Err(PipelineError::UnsupportedTableOperator(
            operator.name.clone(),
        ))
    }
}

pub fn is_table_operator(
    relation: &TableFactor,
) -> Result<Option<TableOperatorDescriptor>, PipelineError> {
    match relation {
        TableFactor::Table { name, args, .. } => {
            if args.is_some() {
                Ok(Some(TableOperatorDescriptor {
                    name: string_from_sql_object_name(name),
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
        TableFactor::Pivot { .. } => Err(PipelineError::UnsupportedPivot),
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

pub fn string_from_sql_object_name(name: &ObjectName) -> String {
    name.0
        .iter()
        .map(ExpressionBuilder::normalize_ident)
        .collect::<Vec<String>>()
        .join(".")
}
