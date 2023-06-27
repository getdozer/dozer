use std::collections::HashMap;
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;

use dozer_core::app::App;
use dozer_core::app::AppPipeline;
use dozer_core::executor::DagExecutor;
use dozer_core::DEFAULT_PORT_HANDLE;
use dozer_ingestion::connectors::{get_connector, get_connector_info_table};
use dozer_sql::pipeline::builder::statement_to_pipeline;
use dozer_sql::pipeline::builder::{OutputNodeInfo, QueryContext, SchemaSQLContext};
use dozer_types::indicatif::MultiProgress;
use dozer_types::log::debug;
use dozer_types::models::api_endpoint::ApiEndpoint;
use dozer_types::models::connection::Connection;
use dozer_types::models::source::Source;
use std::hash::Hash;
use tokio::runtime::Runtime;

use crate::pipeline::{LogSinkFactory, LogSinkSettings};

use super::source_builder::SourceBuilder;
use crate::errors::OrchestrationError;
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

pub struct CalculatedSources {
    pub original_sources: Vec<String>,
    pub transformed_sources: Vec<String>,
    pub query_context: Option<QueryContext>,
}
pub struct PipelineBuilder<'a> {
    connections: &'a [Connection],
    sources: &'a [Source],
    sql: Option<&'a str>,
    /// `ApiEndpoint` and its log path.
    endpoint_and_log_paths: Vec<(ApiEndpoint, PathBuf)>,
    progress: MultiProgress,
}
impl<'a> PipelineBuilder<'a> {
    pub fn new(
        connections: &'a [Connection],
        sources: &'a [Source],
        sql: Option<&'a str>,
        endpoint_and_log_paths: Vec<(ApiEndpoint, PathBuf)>,
        progress: MultiProgress,
    ) -> Self {
        Self {
            connections,
            sources,
            sql,
            endpoint_and_log_paths,
            progress,
        }
    }

    // Based on used_sources, map it to the connection name and create sources
    // For not breaking current functionality, current format is to be still supported.
    pub async fn get_grouped_tables(
        &self,
        original_sources: &[String],
    ) -> Result<HashMap<Connection, Vec<Source>>, OrchestrationError> {
        let mut grouped_connections: HashMap<Connection, Vec<Source>> = HashMap::new();

        let mut connector_map = HashMap::new();
        for connection in self.connections {
            let connector = get_connector(connection.clone())?;

            if let Ok(info_table) = get_connector_info_table(connection) {
                info!("[{}] Connection parameters\n{info_table}", connection.name);
            }

            let connector_tables = connector.list_tables().await?;

            // override source name if specified
            let connector_tables: Vec<Source> = connector_tables
                .iter()
                .map(|table| {
                    match self.sources.iter().find(|s| {
                        // TODO: @dario - Replace this line with the actual schema parsed from SQL
                        s.connection.as_ref().unwrap().name == connection.name
                            && s.table_name == table.name
                    }) {
                        Some(source) => source.clone(),
                        None => Source {
                            name: table.name.clone(),
                            table_name: table.name.clone(),
                            schema: table.schema.clone(),
                            connection: Some(connection.clone()),
                            ..Default::default()
                        },
                    }
                })
                .collect();

            connector_map.insert(connection.clone(), connector_tables);
        }

        for table_name in original_sources {
            let mut table_found = false;
            for (connection, tables) in connector_map.iter() {
                if let Some(source) = tables
                    .iter()
                    .find(|table| table.name == table_name.as_str())
                {
                    table_found = true;
                    grouped_connections
                        .entry(connection.clone())
                        .or_default()
                        .push(source.clone());
                }
            }

            if !table_found {
                error!("Table {} not found in any of the connections", table_name);
                return Err(OrchestrationError::SourceValidationError);
            }
        }

        Ok(grouped_connections)
    }

    // This function is used to figure out the sources that are used in the pipeline
    // based on the SQL and API Endpoints
    pub fn calculate_sources(&self) -> Result<CalculatedSources, OrchestrationError> {
        let mut original_sources = vec![];

        let mut query_ctx = None;
        let mut pipeline = AppPipeline::new();

        let mut transformed_sources = vec![];

        if let Some(sql) = &self.sql {
            let query_context = statement_to_pipeline(sql, &mut pipeline, None)
                .map_err(OrchestrationError::PipelineError)?;

            query_ctx = Some(query_context.clone());

            for (name, _) in query_context.output_tables_map {
                if transformed_sources.contains(&name) {
                    return Err(OrchestrationError::DuplicateTable(name));
                }
                transformed_sources.push(name.clone());
            }

            for name in query_context.used_sources {
                // Add all source tables to input tables
                original_sources.push(name);
            }
        }

        // Add Used Souces if direct from source
        for (api_endpoint, _) in &self.endpoint_and_log_paths {
            let table_name = &api_endpoint.table_name;

            // Don't add if the table is a result of SQL
            if !transformed_sources.contains(table_name) {
                original_sources.push(table_name.clone());
            }
        }
        dedup(&mut original_sources);
        dedup(&mut transformed_sources);

        Ok(CalculatedSources {
            original_sources,
            transformed_sources,
            query_context: query_ctx,
        })
    }

    // This function is used by both migrate and actual execution
    pub fn build(
        self,
        runtime: Arc<Runtime>,
        settings: LogSinkSettings,
    ) -> Result<dozer_core::Dag<SchemaSQLContext>, OrchestrationError> {
        let calculated_sources = self.calculate_sources()?;

        debug!("Used Sources: {:?}", calculated_sources.original_sources);
        let grouped_connections =
            runtime.block_on(self.get_grouped_tables(&calculated_sources.original_sources))?;

        let mut pipelines: Vec<AppPipeline<SchemaSQLContext>> = vec![];

        let mut pipeline = AppPipeline::new();

        let mut available_output_tables: HashMap<String, OutputTableInfo> = HashMap::new();

        // Add all source tables to available output tables
        for (connection, sources) in &grouped_connections {
            for source in sources {
                available_output_tables.insert(
                    source.name.clone(),
                    OutputTableInfo::Original(OriginalTableInfo {
                        connection_name: connection.name.to_string(),
                        table_name: source.name.clone(),
                    }),
                );
            }
        }

        if let Some(sql) = &self.sql {
            let query_context = statement_to_pipeline(sql, &mut pipeline, None)
                .map_err(OrchestrationError::PipelineError)?;

            for (name, table_info) in query_context.output_tables_map {
                if available_output_tables.contains_key(name.as_str()) {
                    return Err(OrchestrationError::DuplicateTable(name));
                }
                available_output_tables
                    .insert(name.clone(), OutputTableInfo::Transformed(table_info));
            }
        }

        let source_builder = SourceBuilder::new(grouped_connections, Some(&self.progress));

        let conn_ports = source_builder.get_ports();

        for (api_endpoint, log_path) in self.endpoint_and_log_paths {
            let table_name = &api_endpoint.table_name;

            let table_info = available_output_tables
                .get(table_name)
                .ok_or_else(|| OrchestrationError::EndpointTableNotFound(table_name.clone()))?;

            let snk_factory = Arc::new(LogSinkFactory::new(
                log_path,
                settings.clone(),
                api_endpoint.name.clone(),
                self.progress.clone(),
            ));

            match table_info {
                OutputTableInfo::Transformed(table_info) => {
                    pipeline.add_sink(snk_factory, api_endpoint.name.as_str());

                    pipeline.connect_nodes(
                        &table_info.node,
                        Some(table_info.port),
                        api_endpoint.name.as_str(),
                        Some(DEFAULT_PORT_HANDLE),
                        true,
                    );
                }
                OutputTableInfo::Original(table_info) => {
                    pipeline.add_sink(snk_factory, api_endpoint.name.as_str());

                    let conn_port = conn_ports
                        .get(&(
                            table_info.connection_name.as_str(),
                            table_info.table_name.as_str(),
                        ))
                        .expect("port should be present based on source mapping");

                    pipeline.connect_nodes(
                        &table_info.connection_name,
                        Some(*conn_port),
                        api_endpoint.name.as_str(),
                        Some(DEFAULT_PORT_HANDLE),
                        false,
                    );
                }
            }
        }

        pipelines.push(pipeline);

        let asm = source_builder.build_source_manager(runtime)?;
        let mut app = App::new(asm);

        Vec::into_iter(pipelines).for_each(|p| {
            app.add_pipeline(p);
        });

        let dag = app.get_dag().map_err(ExecutionError)?;

        debug!("{}", dag);

        DagExecutor::validate(dag.clone())
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

fn dedup<T: Eq + Hash + Clone>(v: &mut Vec<T>) {
    let mut uniques = HashSet::new();
    v.retain(|e| uniques.insert(e.clone()));
}
