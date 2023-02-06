use dozer_api::grpc::internal_grpc::PipelineResponse;
use dozer_core::dag::app::{App, AppPipeline};
use dozer_sql::pipeline::builder::{statement_to_pipeline, SchemaSQLContext};
use dozer_types::indicatif::MultiProgress;
use dozer_types::types::{Operation, SchemaWithChangesType};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use dozer_api::CacheEndpoint;
use dozer_types::models::source::Source;

use crate::pipeline::{CacheSinkFactory, CacheSinkSettings, StreamingSinkFactory};
use dozer_core::dag::executor::{DagExecutor, ExecutorOptions};
use dozer_core::dag::DEFAULT_PORT_HANDLE;
use dozer_ingestion::connectors::{get_connector, get_connector_info_table, TableInfo};

use dozer_ingestion::ingestion::{IngestionIterator, Ingestor};

use dozer_types::crossbeam;
use dozer_types::log::{error, info};

use dozer_types::models::connection::Connection;
use dozer_types::parking_lot::RwLock;
use OrchestrationError::ExecutionError;

use crate::console_helper::get_colored_text;
use crate::errors::OrchestrationError;
use crate::pipeline::source_builder::SourceBuilder;
use crate::simple::direct_cache_pipeline::source_to_pipeline;
use crate::{validate, validate_schema};

pub struct Executor {
    sources: Vec<Source>,
    cache_endpoints: Vec<CacheEndpoint>,
    pipeline_dir: PathBuf,
    ingestor: Arc<RwLock<Ingestor>>,
    iterator: Arc<RwLock<IngestionIterator>>,
    running: Arc<AtomicBool>,
    progress: MultiProgress,
}
impl Executor {
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

    pub fn get_connection_groups(&self) -> HashMap<String, Vec<Source>> {
        SourceBuilder::group_connections(self.sources.clone())
    }

    pub fn validate_grouped_connections(
        grouped_connections: &HashMap<String, Vec<Source>>,
    ) -> Result<(), OrchestrationError> {
        for sources_group in grouped_connections.values() {
            let first_source = sources_group.get(0).unwrap();

            if let Some(connection) = &first_source.connection {
                let tables: Vec<TableInfo> = sources_group
                    .iter()
                    .map(|source| TableInfo {
                        name: source.name.clone(),
                        table_name: source.table_name.clone(),
                        id: 0,
                        columns: Some(source.columns.clone()),
                    })
                    .collect();

                if let Some(info_table) = get_connector_info_table(connection) {
                    info!("[{}] Connection parameters", connection.name);
                    info_table.printstd();
                }

                validate(connection.clone(), Some(tables.clone()))
                    .map_err(|e| {
                        error!(
                            "[{}] {} Connection validation error: {}",
                            connection.name,
                            get_colored_text("X", "31"),
                            e
                        );
                        OrchestrationError::SourceValidationError
                    })
                    .map(|_| {
                        info!(
                            "[{}] {} Connection validation completed",
                            connection.name,
                            get_colored_text("✓", "32")
                        );
                    })?;

                validate_schema(connection.clone(), &tables).map_or_else(
                    |e| {
                        error!(
                            "[{}] {} Schema validation error: {}",
                            connection.name,
                            get_colored_text("X", "31"),
                            e
                        );
                        Err(OrchestrationError::SourceValidationError)
                    },
                    |r| {
                        let mut all_valid = true;
                        for (table_name, validation_result) in r.into_iter() {
                            let is_valid =
                                validation_result.iter().all(|(_, result)| result.is_ok());

                            if is_valid {
                                info!(
                                    "[{}][{}] {} Schema validation completed",
                                    connection.name,
                                    table_name,
                                    get_colored_text("✓", "32")
                                );
                            } else {
                                all_valid = false;
                                for (_, error) in validation_result {
                                    if let Err(e) = error {
                                        error!(
                                            "[{}][{}] {} Schema validation error: {}",
                                            connection.name,
                                            table_name,
                                            get_colored_text("X", "31"),
                                            e
                                        );
                                    }
                                }
                            }
                        }

                        if !all_valid {
                            return Err(OrchestrationError::SourceValidationError);
                        }

                        Ok(())
                    },
                )?;
            }
        }

        Ok(())
    }

    // This function is used to run a query using a temporary pipeline
    pub fn query(
        &self,
        sql: String,
        sender: crossbeam::channel::Sender<Operation>,
    ) -> Result<dozer_core::dag::Dag<SchemaSQLContext>, OrchestrationError> {
        let grouped_connections = self.get_connection_groups();

        let (mut pipeline, (query_name, query_port)) =
            statement_to_pipeline(&sql).map_err(OrchestrationError::PipelineError)?;
        pipeline.add_sink(
            Arc::new(StreamingSinkFactory::new(sender)),
            "streaming_sink",
        );
        pipeline
            .connect_nodes(
                &query_name,
                Some(query_port),
                "streaming_sink",
                Some(DEFAULT_PORT_HANDLE),
            )
            .map_err(OrchestrationError::ExecutionError)?;

        let used_sources: Vec<String> = pipeline.get_entry_points_sources_names();

        let asm = SourceBuilder::build_source_manager(
            used_sources,
            grouped_connections,
            self.ingestor.clone(),
            self.iterator.clone(),
            self.running.clone(),
        )?;
        let mut app = App::new(asm);
        app.add_pipeline(pipeline);

        let dag = app.get_dag().map_err(OrchestrationError::ExecutionError)?;
        let path = &self.pipeline_dir;
        let mut exec = DagExecutor::new(
            &dag,
            path.as_path(),
            ExecutorOptions::default(),
            self.running.clone(),
        )?;

        exec.start()?;
        Ok(dag)
    }

    // This function is used by both migrate and actual execution
    pub fn build_pipeline(
        &self,
        notifier: Option<crossbeam::channel::Sender<PipelineResponse>>,
        api_dir: PathBuf,
        settings: CacheSinkSettings,
    ) -> Result<dozer_core::dag::Dag<SchemaSQLContext>, OrchestrationError> {
        let grouped_connections = self.get_connection_groups();

        Self::validate_grouped_connections(&grouped_connections)?;

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

    pub fn get_tables(
        connections: &Vec<Connection>,
    ) -> Result<HashMap<String, Vec<SchemaWithChangesType>>, OrchestrationError> {
        let mut schema_map = HashMap::new();
        for connection in connections {
            validate(connection.to_owned(), None)?;

            let connector = get_connector(connection.to_owned())?;
            let schema_tuples = connector.get_schemas(None)?;
            schema_map.insert(connection.name.to_owned(), schema_tuples);
        }

        Ok(schema_map)
    }

    pub fn run(
        &self,
        notifier: Option<crossbeam::channel::Sender<PipelineResponse>>,
        settings: CacheSinkSettings,
    ) -> Result<(), OrchestrationError> {
        let running_wait = self.running.clone();

        let parent_dag = self.build_pipeline(notifier, PathBuf::default(), settings)?;
        let path = &self.pipeline_dir;

        if !path.exists() {
            return Err(OrchestrationError::PipelineDirectoryNotFound(
                path.to_string_lossy().to_string(),
            ));
        }

        let mut exec = DagExecutor::new(
            &parent_dag,
            path.as_path(),
            ExecutorOptions::default(),
            running_wait,
        )?;

        exec.start()?;
        exec.join().map_err(ExecutionError)
    }
}
