use std::collections::HashMap;
use std::sync::Arc;

use dozer_cache::cache::{CacheManagerOptions, LmdbCacheManager};
use dozer_core::app::AppPipeline;
use dozer_core::executor::DagExecutor;
use dozer_core::DEFAULT_PORT_HANDLE;
use dozer_sql::pipeline::builder::{OutputNodeInfo, SchemaSQLContext};

use dozer_api::grpc::internal_grpc::PipelineResponse;
use dozer_core::app::App;
use dozer_sql::pipeline::builder::statement_to_pipeline;
use dozer_types::indicatif::MultiProgress;
use dozer_types::models::api_endpoint::ApiEndpoint;
use dozer_types::models::app_config::Config;
use std::path::PathBuf;

use crate::pipeline::{CacheSinkFactory, CacheSinkSettings};

use super::source_builder::SourceBuilder;
use super::validate::validate_grouped_connections;
use crate::errors::OrchestrationError;
use dozer_types::crossbeam;
use dozer_types::log::{error, info};
use OrchestrationError::ExecutionError;

pub enum OutputTableInfo {
    Transformed(OutputNodeInfo),
    Original(OriginalTableInfo),
}

pub struct OriginalTableInfo {
    pub table_name: String,
    pub connection_name: String,
}

pub struct PipelineBuilder {
    config: Config,
    api_endpoints: Vec<ApiEndpoint>,
    pipeline_dir: PathBuf,
    progress: MultiProgress,
}
impl PipelineBuilder {
    pub fn new(config: Config, api_endpoints: Vec<ApiEndpoint>, pipeline_dir: PathBuf) -> Self {
        Self {
            config,
            api_endpoints,
            pipeline_dir,
            progress: MultiProgress::new(),
        }
    }

    // This function is used by both migrate and actual execution
    pub fn build(
        &self,
        notifier: Option<crossbeam::channel::Sender<PipelineResponse>>,
        cache_manager_options: CacheManagerOptions,
        settings: CacheSinkSettings,
    ) -> Result<dozer_core::Dag<SchemaSQLContext>, OrchestrationError> {
        let sources = self.config.sources.clone();

        let grouped_connections = SourceBuilder::group_connections(sources);

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

        if let Some(sql) = self.config.sql.clone() {
            let query_context = statement_to_pipeline(&sql, &mut pipeline, None)
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
        for api_endpoint in &self.api_endpoints {
            let table_name = &api_endpoint.table_name;

            let table_info = available_output_tables
                .get(table_name)
                .ok_or_else(|| OrchestrationError::EndpointTableNotFound(table_name.clone()))?;

            if let OutputTableInfo::Original(table_info) = table_info {
                used_sources.push(table_info.table_name.clone());
            }
        }

        let source_builder = SourceBuilder::new(used_sources, grouped_connections);

        let conn_ports = source_builder.get_ports();

        let cache_manager = Arc::new(
            LmdbCacheManager::new(cache_manager_options)
                .map_err(OrchestrationError::CacheInitFailed)?,
        );
        for api_endpoint in &self.api_endpoints {
            let table_name = &api_endpoint.table_name;

            let table_info = available_output_tables
                .get(table_name)
                .ok_or_else(|| OrchestrationError::EndpointTableNotFound(table_name.clone()))?;

            let snk_factory = Arc::new(CacheSinkFactory::new(
                cache_manager.clone(),
                api_endpoint.clone(),
                notifier.clone(),
                self.progress.clone(),
                settings.clone(),
            )?);

            match table_info {
                OutputTableInfo::Transformed(table_info) => {
                    pipeline.add_sink(snk_factory, api_endpoint.name.as_str());

                    pipeline
                        .connect_nodes(
                            &table_info.node,
                            Some(table_info.port),
                            api_endpoint.name.as_str(),
                            Some(DEFAULT_PORT_HANDLE),
                            true,
                        )
                        .map_err(ExecutionError)?;
                }
                OutputTableInfo::Original(table_info) => {
                    pipeline.add_sink(snk_factory, api_endpoint.name.as_str());

                    let conn_port = conn_ports
                        .get(&(
                            table_info.connection_name.clone(),
                            table_info.table_name.clone(),
                        ))
                        .expect("port should be present based on source mapping");

                    pipeline
                        .connect_nodes(
                            &table_info.connection_name,
                            Some(*conn_port),
                            api_endpoint.name.as_str(),
                            Some(DEFAULT_PORT_HANDLE),
                            false,
                        )
                        .map_err(ExecutionError)?;
                }
            }
        }

        pipelines.push(pipeline);

        let asm = source_builder.build_source_manager()?;
        let mut app = App::new(asm);

        Vec::into_iter(pipelines).for_each(|p| {
            app.add_pipeline(p);
        });

        let dag = app.get_dag().map_err(ExecutionError)?;

        DagExecutor::validate(&dag, self.pipeline_dir.clone())
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
