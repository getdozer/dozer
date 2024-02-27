use dozer_core::{
    app::{AppPipeline, PipelineEntryPoint},
    DEFAULT_PORT_HANDLE,
};
use dozer_sql_expression::sqlparser::ast::{TableFactor, TableWithJoins};

use crate::{
    builder::{get_from_source, QueryContext},
    errors::PipelineError,
    product::table::factory::TableProcessorFactory,
};

use super::{
    common::{get_name_or_alias, is_an_entry_point},
    join::insert_join_to_pipeline,
    table_operator::{insert_table_operator_processor_to_pipeline, is_table_operator},
    ConnectionInfo,
};

pub fn insert_from_to_pipeline(
    from: TableWithJoins,
    pipeline: &mut AppPipeline,
    pipeline_idx: usize,
    query_context: &mut QueryContext,
) -> Result<ConnectionInfo, PipelineError> {
    if from.joins.is_empty() {
        insert_table_to_pipeline(from.relation, pipeline, pipeline_idx, query_context)
    } else {
        insert_join_to_pipeline(from, pipeline, pipeline_idx, query_context)
    }
}

fn insert_table_to_pipeline(
    relation: TableFactor,
    pipeline: &mut AppPipeline,
    pipeline_idx: usize,
    query_context: &mut QueryContext,
) -> Result<ConnectionInfo, PipelineError> {
    if let Some(operator) = is_table_operator(&relation)? {
        let product_processor_name =
            insert_from_processor_to_pipeline(query_context, relation, pipeline)?;

        let connection_info = insert_table_operator_processor_to_pipeline(
            operator,
            pipeline,
            pipeline_idx,
            query_context,
        )?;

        pipeline.connect_nodes(
            connection_info.output_node.0,
            connection_info.output_node.1,
            product_processor_name.clone(),
            DEFAULT_PORT_HANDLE,
        );

        Ok(ConnectionInfo {
            input_nodes: connection_info.input_nodes,
            output_node: (product_processor_name, DEFAULT_PORT_HANDLE),
        })
    } else {
        insert_table_processor_to_pipeline(relation, pipeline, pipeline_idx, query_context)
    }
}

fn insert_table_processor_to_pipeline(
    relation: TableFactor,
    pipeline: &mut AppPipeline,
    pipeline_idx: usize,
    query_context: &mut QueryContext,
) -> Result<ConnectionInfo, PipelineError> {
    let relation_name_or_alias = get_name_or_alias(&relation)?;
    let product_input_name = get_from_source(relation, pipeline, query_context, pipeline_idx)?.0;

    let processor_name = format!(
        "from:{}--{}",
        product_input_name,
        query_context.get_next_processor_id()
    );
    if !query_context.processors_list.insert(processor_name.clone()) {
        return Err(PipelineError::ProcessorAlreadyExists(processor_name));
    }
    let product_processor_factory =
        TableProcessorFactory::new(processor_name.clone(), relation_name_or_alias.clone());
    pipeline.add_processor(Box::new(product_processor_factory), processor_name.clone());

    // is a node that is an entry point to the pipeline
    let input_nodes = if is_an_entry_point(&product_input_name, query_context, pipeline_idx) {
        let entry_point = PipelineEntryPoint::new(product_input_name.clone(), DEFAULT_PORT_HANDLE);
        pipeline.add_entry_point(processor_name.clone(), entry_point);
        query_context.used_sources.push(product_input_name);
        vec![]
    }
    // is a node that is connected to another pipeline
    else {
        vec![(
            product_input_name,
            processor_name.clone(),
            DEFAULT_PORT_HANDLE,
        )]
    };

    Ok(ConnectionInfo {
        input_nodes,
        output_node: (processor_name, DEFAULT_PORT_HANDLE),
    })
}

fn insert_from_processor_to_pipeline(
    query_context: &mut QueryContext,
    relation: TableFactor,
    pipeline: &mut AppPipeline,
) -> Result<String, PipelineError> {
    let product_processor_name = format!("from--{}", query_context.get_next_processor_id());
    let product_processor = TableProcessorFactory::new(
        product_processor_name.clone(),
        get_name_or_alias(&relation)?,
    );

    pipeline.add_processor(Box::new(product_processor), product_processor_name.clone());
    Ok(product_processor_name)
}
