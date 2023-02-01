use std::sync::Arc;

use dozer_core::dag::app::{AppPipeline, PipelineEntryPoint};
use dozer_core::dag::appsource::AppSourceId;
use dozer_core::dag::dag::DEFAULT_PORT_HANDLE;
use dozer_sql::pipeline::builder::SchemaSQLContext;
use dozer_types::models::api_endpoint::ApiEndpoint;

use crate::simple::basic_processor_factory::BasicProcessorFactory;

pub fn source_to_pipeline(
    api_endpoint: &ApiEndpoint,
) -> (AppPipeline<SchemaSQLContext>, (String, u16)) {
    let source_name = api_endpoint.source.as_ref().unwrap().name.clone();

    let p = BasicProcessorFactory::new();

    let processor_name = "direct_sink".to_string();
    let mut pipeline = AppPipeline::new();
    pipeline.add_processor(
        Arc::new(p),
        &processor_name,
        vec![PipelineEntryPoint::new(
            AppSourceId {
                id: source_name,
                connection: None,
            },
            DEFAULT_PORT_HANDLE,
        )],
    );

    (pipeline, (processor_name, DEFAULT_PORT_HANDLE))
}
