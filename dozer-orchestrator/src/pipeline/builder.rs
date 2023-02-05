use std::sync::Arc;

use dozer_core::dag::app::{AppPipeline, PipelineEntryPoint};
use dozer_core::dag::appsource::AppSourceId;
use dozer_core::dag::executor::DagExecutor;
use dozer_core::dag::DEFAULT_PORT_HANDLE;
use dozer_sql::pipeline::builder::SchemaSQLContext;
use dozer_types::models::api_endpoint::ApiEndpoint;

use dozer_api::grpc::internal_grpc::PipelineResponse;
use dozer_core::dag::app::App;
use dozer_sql::pipeline::builder::statement_to_pipeline;
use dozer_types::indicatif::MultiProgress;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;

use dozer_api::CacheEndpoint;
use dozer_types::models::source::Source;

use crate::pipeline::{CacheSinkFactory, CacheSinkSettings};
use dozer_ingestion::ingestion::{IngestionIterator, Ingestor};

use crate::errors::OrchestrationError;
use dozer_types::crossbeam;
use dozer_types::log::{error, info};
use dozer_types::parking_lot::RwLock;
use OrchestrationError::ExecutionError;

use super::basic_processor_factory::BasicProcessorFactory;
use super::source_builder::SourceBuilder;
use super::validate::validate_grouped_connections;

pub struct PipelineBuilder {
    sources: Vec<Source>,
    cache_endpoints: Vec<CacheEndpoint>,
    pipeline_dir: PathBuf,
    ingestor: Arc<RwLock<Ingestor>>,
    iterator: Arc<RwLock<IngestionIterator>>,
    running: Arc<AtomicBool>,
    progress: MultiProgress,
}
impl PipelineBuilder {
    pub fn new(
        sources: Vec<Source>,
        cache_endpoints: Vec<CacheEndpoint>,
        ingestor: Arc<RwLock<Ingestor>>,
        iterator: Arc<RwLock<IngestionIterator>>,
        running: Arc<AtomicBool>,
        pipeline_dir: PathBuf,
    ) -> Self {
        Self {
            sources,
            cache_endpoints,
            pipeline_dir,
            ingestor,
            iterator,
            running,
            progress: MultiProgress::new(),
        }
    }

    // This function is used by both migrate and actual execution
    pub fn build(
        &self,
        notifier: Option<crossbeam::channel::Sender<PipelineResponse>>,
        api_dir: PathBuf,
        settings: CacheSinkSettings,
    ) -> Result<dozer_core::dag::Dag<SchemaSQLContext>, OrchestrationError> {
        let grouped_connections = SourceBuilder::group_connections(self.sources.clone());

        validate_grouped_connections(&grouped_connections)?;

        let mut pipelines: Vec<AppPipeline<SchemaSQLContext>> = vec![];
        let mut used_sources = vec![];
        for cache_endpoint in self.cache_endpoints.iter().cloned() {
            let api_endpoint = cache_endpoint.endpoint.clone();
            let _api_endpoint_name = api_endpoint.name.clone();
            let cache = cache_endpoint.cache;

            // let mut pipeline = PipelineBuilder {}
            //     .build_pipeline(&api_endpoint.sql)
            //     .map_err(OrchestrationError::PipelineError)?;

            let (mut pipeline, (query_name, query_port)) = api_endpoint.sql.as_ref().map_or_else(
                || Ok(source_to_pipeline(&api_endpoint)),
                |sql| statement_to_pipeline(sql).map_err(OrchestrationError::PipelineError),
            )?;

            pipeline.add_sink(
                Arc::new(CacheSinkFactory::new(
                    vec![DEFAULT_PORT_HANDLE],
                    cache,
                    api_endpoint,
                    notifier.clone(),
                    api_dir.clone(),
                    self.progress.clone(),
                    settings.to_owned(),
                )),
                cache_endpoint.endpoint.name.as_str(),
            );

            pipeline
                .connect_nodes(
                    &query_name,
                    Some(query_port),
                    cache_endpoint.endpoint.name.as_str(),
                    Some(DEFAULT_PORT_HANDLE),
                )
                .map_err(ExecutionError)?;

            for name in pipeline.get_entry_points_sources_names() {
                used_sources.push(name);
            }

            pipelines.push(pipeline);
        }

        let asm = SourceBuilder::build_source_manager(
            used_sources,
            grouped_connections,
            self.ingestor.clone(),
            self.iterator.clone(),
            self.running.clone(),
        )?;
        let mut app = App::new(asm);

        Vec::into_iter(pipelines).for_each(|p| {
            app.add_pipeline(p);
        });

        let dag = app.get_dag().map_err(ExecutionError)?;

        DagExecutor::validate(&dag, &self.pipeline_dir)
            .map(|_| {
                info!("[pipeline] Validation completed");
            })
            .map_err(|e| {
                error!("[pipeline] Validation error: {}", e);
                OrchestrationError::PipelineValidationError
            })?;

        Ok(dag)
    }
}

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
