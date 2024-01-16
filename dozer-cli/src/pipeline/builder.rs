use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

use dozer_api::shutdown::ShutdownReceiver;
use dozer_cache::dozer_log::replication::Log;
use dozer_core::app::App;
use dozer_core::app::AppPipeline;
use dozer_core::app::PipelineEntryPoint;
use dozer_core::node::SinkFactory;
use dozer_core::DEFAULT_PORT_HANDLE;
use dozer_ingestion::{get_connector, get_connector_info_table};
use dozer_sinks::bigquery::BigQuerySinkFactory;
use dozer_sql::builder::statement_to_pipeline;
use dozer_sql::builder::{OutputNodeInfo, QueryContext};
use dozer_tracing::LabelsAndProgress;
use dozer_types::log::debug;
use dozer_types::models::connection::Connection;
use dozer_types::models::endpoint::{AerospikeSinkConfig, BigQuery};
use dozer_types::models::flags::Flags;
use dozer_types::models::source::Source;
use dozer_types::models::udf_config::UdfConfig;
use std::hash::Hash;
use tokio::runtime::Runtime;
use tokio::sync::Mutex;

use crate::pipeline::dummy_sink::DummySinkFactory;
use crate::pipeline::LogSinkFactory;
use dozer_sink_aerospike::AerospikeSinkFactory;

use super::connector_source::ConnectorSourceFactoryError;
use super::source_builder::SourceBuilder;
use crate::errors::OrchestrationError;
use dozer_types::log::info;
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

#[derive(Debug)]
pub struct EndpointLog {
    pub table_name: String,
    pub kind: EndpointLogKind,
}

#[derive(Debug)]
pub enum EndpointLogKind {
    Api { log: Arc<Mutex<Log>> },
    Dummy,
    Aerospike { config: AerospikeSinkConfig },
    BigQuery(BigQuery),
}

pub struct PipelineBuilder<'a> {
    connections: &'a [Connection],
    sources: &'a [Source],
    sql: Option<&'a str>,
    endpoint_logs: Vec<EndpointLog>,
    labels: LabelsAndProgress,
    flags: Flags,
    udfs: &'a [UdfConfig],
}

impl<'a> PipelineBuilder<'a> {
    pub fn new(
        connections: &'a [Connection],
        sources: &'a [Source],
        sql: Option<&'a str>,
        endpoint_logs: Vec<EndpointLog>,
        labels: LabelsAndProgress,
        flags: Flags,
        udfs: &'a [UdfConfig],
    ) -> Self {
        Self {
            connections,
            sources,
            sql,
            endpoint_logs,
            labels,
            flags,
            udfs,
        }
    }

    // Based on used_sources, map it to the connection name and create sources
    // For not breaking current functionality, current format is to be still supported.
    pub async fn get_grouped_tables(
        &self,
        runtime: &Arc<Runtime>,
        original_sources: &[String],
    ) -> Result<HashMap<Connection, Vec<Source>>, OrchestrationError> {
        let mut grouped_connections: HashMap<Connection, Vec<Source>> = HashMap::new();

        let mut connector_map = HashMap::new();
        for connection in self.connections {
            let connector = get_connector(runtime.clone(), connection.clone())
                .map_err(|e| ConnectorSourceFactoryError::Connector(e.into()))?;

            if let Some(info_table) = get_connector_info_table(connection) {
                info!("[{}] Connection parameters\n{info_table}", connection.name);
            }

            let connector_tables = connector
                .list_tables()
                .await
                .map_err(ConnectorSourceFactoryError::Connector)?;

            // override source name if specified
            let connector_tables: Vec<Source> = connector_tables
                .iter()
                .map(|table| {
                    match self.sources.iter().find(|s| {
                        // TODO: @dario - Replace this line with the actual schema parsed from SQL
                        s.connection == connection.name && s.table_name == table.name
                    }) {
                        Some(source) => source.clone(),
                        None => Source {
                            name: table.name.clone(),
                            table_name: table.name.clone(),
                            schema: table.schema.clone(),
                            connection: connection.name.clone(),
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
                return Err(OrchestrationError::SourceValidationError(
                    table_name.to_string(),
                ));
            }
        }

        Ok(grouped_connections)
    }

    // This function is used to figure out the sources that are used in the pipeline
    // based on the SQL and API Endpoints
    pub fn calculate_sources(
        &self,
        runtime: Arc<Runtime>,
    ) -> Result<CalculatedSources, OrchestrationError> {
        let mut original_sources = vec![];

        let mut query_ctx = None;
        let mut pipeline = AppPipeline::new((&self.flags).into());

        let mut transformed_sources = vec![];

        if let Some(sql) = &self.sql {
            let query_context =
                statement_to_pipeline(sql, &mut pipeline, None, self.udfs.to_vec(), runtime)
                    .map_err(OrchestrationError::PipelineError)?;

            query_ctx = Some(query_context.clone());

            transformed_sources = query_context.output_tables_map.keys().cloned().collect();

            for name in query_context.used_sources {
                // Add all source tables to input tables
                original_sources.push(name);
            }
        }

        // Add Used Souces if direct from source
        for endpoint_log in &self.endpoint_logs {
            let table_name = &endpoint_log.table_name;

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

    // This function is used by both building and actual execution
    pub async fn build(
        self,
        runtime: &Arc<Runtime>,
        shutdown: ShutdownReceiver,
    ) -> Result<dozer_core::Dag, OrchestrationError> {
        let calculated_sources = self.calculate_sources(runtime.clone())?;

        debug!("Used Sources: {:?}", calculated_sources.original_sources);
        let grouped_connections = self
            .get_grouped_tables(runtime, &calculated_sources.original_sources)
            .await?;

        let mut pipelines: Vec<AppPipeline> = vec![];

        let mut pipeline = AppPipeline::new(self.flags.into());

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
            let query_context = statement_to_pipeline(
                sql,
                &mut pipeline,
                None,
                self.udfs.to_vec(),
                runtime.clone(),
            )
            .map_err(OrchestrationError::PipelineError)?;

            for (name, table_info) in query_context.output_tables_map {
                available_output_tables
                    .insert(name.clone(), OutputTableInfo::Transformed(table_info));
            }
        }

        // Check if all output tables are used.
        for (table_name, table_info) in &available_output_tables {
            if matches!(table_info, OutputTableInfo::Transformed(_))
                && !self
                    .endpoint_logs
                    .iter()
                    .any(|endpoint_log| &endpoint_log.table_name == table_name)
            {
                return Err(OrchestrationError::OutputTableNotUsed(table_name.clone()));
            }
        }

        dbg!(&self.endpoint_logs);
        for endpoint_log in self.endpoint_logs {
            let table_info = available_output_tables
                .get(&endpoint_log.table_name)
                .ok_or_else(|| {
                    OrchestrationError::EndpointTableNotFound(endpoint_log.table_name.clone())
                })?;

            let snk_factory: Box<dyn SinkFactory> = match endpoint_log.kind {
                EndpointLogKind::Api { log, .. } => Box::new(LogSinkFactory::new(
                    runtime.clone(),
                    log,
                    endpoint_log.table_name.clone(),
                    self.labels.clone(),
                )),
                EndpointLogKind::Dummy => Box::new(DummySinkFactory),
                EndpointLogKind::Aerospike { config } => {
                    Box::new(AerospikeSinkFactory::new(config))
                }
                EndpointLogKind::BigQuery(config) => Box::new(BigQuerySinkFactory::new(config)),
            };

            match table_info {
                OutputTableInfo::Transformed(table_info) => {
                    pipeline.add_sink(snk_factory, &endpoint_log.table_name, None);

                    pipeline.connect_nodes(
                        &table_info.node,
                        table_info.port,
                        &endpoint_log.table_name,
                        DEFAULT_PORT_HANDLE,
                    );
                }
                OutputTableInfo::Original(table_info) => {
                    pipeline.add_sink(
                        snk_factory,
                        &endpoint_log.table_name,
                        Some(PipelineEntryPoint::new(
                            table_info.table_name.clone(),
                            DEFAULT_PORT_HANDLE,
                        )),
                    );
                }
            }
        }

        pipelines.push(pipeline);

        let source_builder = SourceBuilder::new(grouped_connections, self.labels);
        let asm = source_builder
            .build_source_manager(runtime, shutdown)
            .await?;
        let mut app = App::new(asm);

        Vec::into_iter(pipelines).for_each(|p| {
            app.add_pipeline(p);
        });

        let dag = app.into_dag().map_err(ExecutionError)?;

        Ok(dag)
    }
}

fn dedup<T: Eq + Hash + Clone>(v: &mut Vec<T>) {
    let mut uniques = HashSet::new();
    v.retain(|e| uniques.insert(e.clone()));
}
