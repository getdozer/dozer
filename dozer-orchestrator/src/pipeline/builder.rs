use std::collections::HashMap;
use std::sync::Arc;

use dozer_core::dag::app::AppPipeline;
use dozer_core::dag::executor::DagExecutor;
use dozer_core::dag::DEFAULT_PORT_HANDLE;
use dozer_sql::pipeline::builder::{QueryTableInfo, SchemaSQLContext};

use dozer_api::grpc::internal_grpc::PipelineResponse;
use dozer_core::dag::app::App;
use dozer_sql::pipeline::builder::statement_to_pipeline;
use dozer_types::indicatif::MultiProgress;
use dozer_types::models::app_config::Config;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;

use dozer_api::CacheEndpoint;

use crate::pipeline::{CacheSinkFactory, CacheSinkSettings};
use dozer_ingestion::ingestion::{IngestionIterator, Ingestor};

use super::source_builder::SourceBuilder;
use super::validate::validate_grouped_connections;
use crate::errors::OrchestrationError;
use dozer_types::crossbeam;
use dozer_types::log::{error, info};
use dozer_types::parking_lot::RwLock;
use OrchestrationError::ExecutionError;

pub enum OutputTableInfo {
    Transformed(QueryTableInfo),
    Original(OriginalTableInfo),
}

pub struct OriginalTableInfo {
    pub table_name: String,
    pub connection_name: String,
}

pub struct PipelineBuilder {
    config: Config,
    cache_endpoints: Vec<CacheEndpoint>,
    pipeline_dir: PathBuf,
    ingestor: Arc<RwLock<Ingestor>>,
    iterator: Arc<RwLock<IngestionIterator>>,
    running: Arc<AtomicBool>,
    progress: MultiProgress,
}
impl PipelineBuilder {
    pub fn new(
        config: Config,
        cache_endpoints: Vec<CacheEndpoint>,
        ingestor: Arc<RwLock<Ingestor>>,
        iterator: Arc<RwLock<IngestionIterator>>,
        running: Arc<AtomicBool>,
        pipeline_dir: PathBuf,
    ) -> Self {
        Self {
            config,
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
        let sources = self.config.sources.clone();

        let grouped_connections = SourceBuilder::group_connections(sources.clone());

        validate_grouped_connections(&grouped_connections)?;

        let mut pipelines: Vec<AppPipeline<SchemaSQLContext>> = vec![];
        let mut used_sources = vec![];

        let mut pipeline = AppPipeline::new();

        let mut available_output_tables: HashMap<String, OutputTableInfo> = HashMap::new();

        // Add all source tables to available output tables
        for (connection_name, sources) in grouped_connections.clone() {
            for source in sources {
                available_output_tables.insert(
                    source.name.clone(),
                    OutputTableInfo::Original(OriginalTableInfo {
                        connection_name: connection_name.clone(),
                        table_name: source.name.clone(),
                    }),
                );
            }
        }

        if let Some(sql) = self.config.transforms.clone() {
            let query_context = statement_to_pipeline(&sql, &mut pipeline)
                .map_err(OrchestrationError::PipelineError)?;

            for (name, table_info) in query_context.output_tables_map {
                if available_output_tables.contains_key(name.as_str()) {
                    return Err(OrchestrationError::DuplicateTable(name));
                }
                available_output_tables
                    .insert(name.clone(), OutputTableInfo::Transformed(table_info));
            }

            for name in query_context.used_sources {
                // Add all source tables to input tables
                used_sources.push(name.clone());
            }
        }
        // Add Used Souces if direct from source
        for cache_endpoint in self.cache_endpoints.iter().cloned() {
            let api_endpoint = cache_endpoint.endpoint.clone();

            let table_name = api_endpoint.table_name.clone();

            let table_info = available_output_tables
                .get(&table_name)
                .ok_or_else(|| OrchestrationError::EndpointTableNotFound(table_name.clone()))?;

            if let OutputTableInfo::Original(table_info) = table_info {
                used_sources.push(table_info.table_name.clone());
            }
        }

        let source_builder = SourceBuilder::new(used_sources, grouped_connections);

        let conn_ports = source_builder.get_ports();

        let pipeline_ref = &mut pipeline;

        for cache_endpoint in self.cache_endpoints.iter().cloned() {
            let api_endpoint = cache_endpoint.endpoint.clone();

            let cache = cache_endpoint.cache;

            let table_name = api_endpoint.table_name.clone();

            let table_info = available_output_tables
                .get(&table_name)
                .ok_or_else(|| OrchestrationError::EndpointTableNotFound(table_name.clone()))?;

            let snk_factory = Arc::new(CacheSinkFactory::new(
                vec![DEFAULT_PORT_HANDLE],
                cache,
                api_endpoint,
                notifier.clone(),
                api_dir.clone(),
                self.progress.clone(),
                settings.to_owned(),
            ));

            match table_info {
                OutputTableInfo::Transformed(table_info) => {
                    pipeline_ref.add_sink(snk_factory, cache_endpoint.endpoint.name.as_str());

                    pipeline_ref
                        .connect_nodes(
                            &table_info.node,
                            Some(table_info.port),
                            cache_endpoint.endpoint.name.as_str(),
                            Some(DEFAULT_PORT_HANDLE),
                            true,
                        )
                        .map_err(ExecutionError)?;
                }
                OutputTableInfo::Original(table_info) => {
                    pipeline_ref.add_sink(snk_factory, cache_endpoint.endpoint.name.as_str());

                    let conn_port = conn_ports
                        .get(&(
                            table_info.connection_name.clone(),
                            table_info.table_name.clone(),
                        ))
                        .expect("port should be present based on source mapping");

                    pipeline_ref
                        .connect_nodes(
                            &table_info.connection_name,
                            Some(*conn_port),
                            cache_endpoint.endpoint.name.as_str(),
                            Some(DEFAULT_PORT_HANDLE),
                            false,
                        )
                        .map_err(ExecutionError)?;
                }
            }
        }

        pipelines.push(pipeline);

        let asm = source_builder.build_source_manager(
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
