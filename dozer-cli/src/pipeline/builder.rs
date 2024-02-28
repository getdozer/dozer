use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

use dozer_core::app::App;
use dozer_core::app::AppPipeline;
use dozer_core::app::PipelineEntryPoint;
use dozer_core::node::SinkFactory;
use dozer_core::shutdown::ShutdownReceiver;
use dozer_core::DEFAULT_PORT_HANDLE;
use dozer_sql::builder::statement_to_pipeline;
use dozer_sql::builder::{OutputNodeInfo, QueryContext};
use dozer_tracing::LabelsAndProgress;
use dozer_types::log::debug;
use dozer_types::models::connection::Connection;
use dozer_types::models::connection::ConnectionConfig;
use dozer_types::models::flags::Flags;
use dozer_types::models::sink::Sink;
use dozer_types::models::sink::SinkConfig;
use dozer_types::models::source::Source;
use dozer_types::models::udf_config::UdfConfig;
use dozer_types::types::PortHandle;
use std::hash::Hash;
use tokio::runtime::Runtime;

use crate::pipeline::dummy_sink::DummySinkFactory;
use dozer_sink_aerospike::AerospikeSinkFactory;
use dozer_sink_clickhouse::ClickhouseSinkFactory;
use dozer_sink_oracle::OracleSinkFactory;

use super::source_builder::SourceBuilder;
use crate::errors::OrchestrationError;

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
    sinks: &'a [Sink],
    labels: LabelsAndProgress,
    flags: Flags,
    udfs: &'a [UdfConfig],
}

impl<'a> PipelineBuilder<'a> {
    pub fn new(
        connections: &'a [Connection],
        sources: &'a [Source],
        sql: Option<&'a str>,
        sinks: &'a [Sink],
        labels: LabelsAndProgress,
        flags: Flags,
        udfs: &'a [UdfConfig],
    ) -> Self {
        Self {
            connections,
            sources,
            sql,
            sinks,
            labels,
            flags,
            udfs,
        }
    }

    // Based on used_sources, map it to the connection name and create sources
    // For not breaking current functionality, current format is to be still supported.
    pub async fn get_grouped_tables(
        &self,
        _runtime: &Arc<Runtime>,
        original_sources: &[String],
    ) -> Result<HashMap<Connection, Vec<Source>>, OrchestrationError> {
        let mut grouped_connections: HashMap<Connection, Vec<Source>> = HashMap::new();

        let mut connector_map: HashMap<Connection, Vec<Source>> = HashMap::new();
        for source in self.sources {
            let connection = self
                .connections
                .iter()
                .find(|conn| conn.name == source.connection)
                .ok_or_else(|| OrchestrationError::ConnectionNotFound(source.connection.clone()))?;
            connector_map
                .entry(connection.clone())
                .or_default()
                .push(source.clone());
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
        for sink in self.sinks {
            for table_name in table_names(sink) {
                // Don't add if the table is a result of SQL
                if !transformed_sources.contains(table_name) {
                    original_sources.push(table_name.to_string());
                }
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
                && !is_table_used(table_name, self.sinks)
            {
                return Err(OrchestrationError::OutputTableNotUsed(table_name.clone()));
            }
        }

        let get_table_info = |table_name: &String| {
            available_output_tables
                .get(table_name)
                .ok_or_else(|| OrchestrationError::SinkTableNotFound(table_name.clone()))
        };

        for sink in self.sinks {
            let id = &sink.name;
            match &sink.config {
                SinkConfig::Dummy(config) => add_sink_to_pipeline(
                    &mut pipeline,
                    Box::new(DummySinkFactory),
                    id,
                    vec![(get_table_info(&config.table_name)?, DEFAULT_PORT_HANDLE)],
                ),
                SinkConfig::Aerospike(config) => {
                    let connection = self
                        .connections
                        .iter()
                        .find_map(|conn| match conn {
                            Connection {
                                config: ConnectionConfig::Aerospike(conn_config),
                                name,
                            } if name == &config.connection => Some(conn_config),
                            _ => None,
                        })
                        .ok_or_else(|| {
                            OrchestrationError::ConnectionNotFound(config.connection.clone())
                        })?;
                    let sink_factory = Box::new(AerospikeSinkFactory::new(
                        connection.clone(),
                        config.clone(),
                    ));
                    let table_infos = config
                        .tables
                        .iter()
                        .enumerate()
                        .map(|(port, table)| {
                            let table_info = get_table_info(&table.source_table_name)?;
                            Ok((table_info, port as PortHandle))
                        })
                        .collect::<Result<Vec<_>, OrchestrationError>>()?;
                    add_sink_to_pipeline(&mut pipeline, sink_factory, id, table_infos);
                }
                SinkConfig::Clickhouse(config) => {
                    let sink =
                        Box::new(ClickhouseSinkFactory::new(config.clone(), runtime.clone()));
                    let table_info = get_table_info(&config.source_table_name)?;
                    add_sink_to_pipeline(
                        &mut pipeline,
                        sink,
                        id,
                        vec![(table_info, DEFAULT_PORT_HANDLE)],
                    );
                }
                SinkConfig::Oracle(config) => {
                    let connection = self
                        .connections
                        .iter()
                        .find_map(|conn| match conn {
                            Connection {
                                config: ConnectionConfig::Oracle(conn_config),
                                name,
                            } if name == &config.connection => Some(conn_config),
                            _ => None,
                        })
                        .ok_or_else(|| {
                            OrchestrationError::ConnectionNotFound(config.connection.clone())
                        })?;
                    let sink = Box::new(OracleSinkFactory {
                        config: connection.clone(),
                        table: config.table_name.clone(),
                    });
                    let table_info = get_table_info(&config.table_name)?;
                    add_sink_to_pipeline(
                        &mut pipeline,
                        sink,
                        id,
                        vec![(table_info, DEFAULT_PORT_HANDLE)],
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

fn table_names(sink: &Sink) -> Vec<&String> {
    match &sink.config {
        SinkConfig::Dummy(sink) => vec![&sink.table_name],
        SinkConfig::Aerospike(sink) => sink
            .tables
            .iter()
            .map(|table| &table.source_table_name)
            .collect(),
        SinkConfig::Clickhouse(sink) => vec![&sink.source_table_name],
        SinkConfig::Oracle(sink) => vec![&sink.table_name],
    }
}

fn is_table_used(table_name: &str, sinks: &[Sink]) -> bool {
    sinks.iter().any(|sink| {
        table_names(sink)
            .iter()
            .any(|sink_table_name| sink_table_name == &table_name)
    })
}

fn add_sink_to_pipeline(
    pipeline: &mut AppPipeline,
    sink: Box<dyn SinkFactory>,
    id: &str,
    table_infos: Vec<(&OutputTableInfo, PortHandle)>,
) {
    pipeline.add_sink(sink, id.to_string());

    for (table_info, port) in table_infos {
        match table_info {
            OutputTableInfo::Original(table_info) => {
                pipeline.add_entry_point(
                    id.to_string(),
                    PipelineEntryPoint::new(table_info.table_name.clone(), port),
                );
            }
            OutputTableInfo::Transformed(table_info) => {
                pipeline.connect_nodes(
                    table_info.node.clone(),
                    table_info.port,
                    id.to_string(),
                    port,
                );
            }
        }
    }
}
