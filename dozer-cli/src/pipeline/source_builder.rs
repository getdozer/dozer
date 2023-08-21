use crate::pipeline::connector_source::ConnectorSourceFactory;
use crate::shutdown::ShutdownReceiver;
use crate::OrchestrationError;
use dozer_core::appsource::{AppSourceManager, AppSourceMappings};
use dozer_ingestion::connectors::TableInfo;
use dozer_sql::pipeline::builder::SchemaSQLContext;

use dozer_types::indicatif::MultiProgress;
use dozer_types::models::connection::Connection;
use dozer_types::models::source::Source;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::runtime::Runtime;

pub struct SourceBuilder<'a> {
    grouped_connections: HashMap<Connection, Vec<Source>>,
    progress: Option<&'a MultiProgress>,
}

const SOURCE_PORTS_RANGE_START: u16 = 1000;

impl<'a> SourceBuilder<'a> {
    pub fn new(
        grouped_connections: HashMap<Connection, Vec<Source>>,
        progress: Option<&'a MultiProgress>,
    ) -> Self {
        Self {
            grouped_connections,
            progress,
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

    pub async fn build_source_manager(
        &self,
        runtime: &Arc<Runtime>,
        shutdown: ShutdownReceiver,
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

            let source_factory = ConnectorSourceFactory::new(
                table_and_ports,
                connection.clone(),
                runtime.clone(),
                self.progress.cloned(),
                shutdown.clone(),
            )
            .await?;

            asm.add(
                Box::new(source_factory),
                AppSourceMappings::new(connection.name.to_string(), ports),
            )?;
        }

        Ok(asm)
    }
}
