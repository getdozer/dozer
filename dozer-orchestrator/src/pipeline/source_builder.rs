use crate::pipeline::connector_source::ConnectorSourceFactory;
use crate::OrchestrationError;
use dozer_core::appsource::{AppSource, AppSourceManager};
use dozer_ingestion::connectors::TableInfo;
use dozer_sql::pipeline::builder::SchemaSQLContext;
use dozer_types::indicatif::MultiProgress;
use dozer_types::models::connection::Connection;
use dozer_types::models::source::Source;
use std::collections::HashMap;
use std::sync::Arc;
use dozer_api::grpc::internal::internal_pipeline_server::PipelineEventSenders;
use tokio::runtime::Runtime;

pub struct SourceBuilder<'a> {
    grouped_connections: HashMap<Connection, Vec<Source>>,
    notifier: Option<PipelineEventSenders>
}

const SOURCE_PORTS_RANGE_START: u16 = 1000;

impl<'a> SourceBuilder<'a> {
    pub fn new(
        grouped_connections: HashMap<Connection, Vec<Source>>,
        notifier: Option<PipelineEventSenders>,
    ) -> Self {
        Self {
            grouped_connections,
            notifier
        }
    }

    pub fn get_ports(&self) -> HashMap<(&str, &str), u16> {
        let mut port: u16 = SOURCE_PORTS_RANGE_START;

        let mut ports = HashMap::new();
        for (conn, sources_group) in &self.grouped_connections {
            for source in sources_group {
                ports.insert((conn.name.as_str(), source.name.as_str()), port);
                port += 1;
            }
        }
        ports
    }

    pub fn build_source_manager(
        &self,
        runtime: Arc<Runtime>,
    ) -> Result<AppSourceManager<SchemaSQLContext>, OrchestrationError> {
        let mut asm = AppSourceManager::new();

        let mut port: u16 = SOURCE_PORTS_RANGE_START;

        for (connection, sources_group) in &self.grouped_connections {
            let mut ports = HashMap::new();
            let mut table_and_ports = vec![];
            for source in sources_group {
                ports.insert(source.name.clone(), port);

                table_and_ports.push((
                    TableInfo {
                        schema: source.schema.clone(),
                        name: source.table_name.clone(),
                        column_names: source.columns.clone(),
                    },
                    port,
                ));

                port += 1;
            }

            let source_factory = runtime.block_on(ConnectorSourceFactory::new(
                table_and_ports,
                connection.clone(),
                runtime.clone(),
                self.notifier.clone()
            ))?;

            asm.add(AppSource::new(
                connection.name.to_string(),
                Arc::new(source_factory),
                ports,
            ))?;
        }

        Ok(asm)
    }
}
