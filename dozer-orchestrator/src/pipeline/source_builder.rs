use crate::pipeline::connector_source::ConnectorSourceFactory;
use crate::OrchestrationError;
use dozer_core::appsource::{AppSource, AppSourceManager};
use dozer_ingestion::connectors::{ColumnInfo, TableInfo};
use dozer_sql::pipeline::builder::SchemaSQLContext;
use dozer_types::indicatif::MultiProgress;
use dozer_types::models::source::Source;
use std::collections::HashMap;
use std::sync::Arc;

pub struct SourceBuilder<'a> {
    used_sources: &'a [String],
    grouped_connections: HashMap<&'a str, Vec<&'a Source>>,
    progress: Option<&'a MultiProgress>,
}

const SOURCE_PORTS_RANGE_START: u16 = 1000;

impl<'a> SourceBuilder<'a> {
    pub fn new(
        used_sources: &'a [String],
        grouped_connections: HashMap<&'a str, Vec<&'a Source>>,
        progress: Option<&'a MultiProgress>,
    ) -> Self {
        Self {
            used_sources,
            grouped_connections,
            progress,
        }
    }

    pub fn get_ports(&self) -> HashMap<(&str, &str), u16> {
        let mut port: u16 = SOURCE_PORTS_RANGE_START;

        let mut ports = HashMap::new();
        for (conn, sources_group) in &self.grouped_connections {
            for source in sources_group {
                if self.used_sources.contains(&source.name) {
                    ports.insert((*conn, source.name.as_str()), port);
                    port += 1;
                }
            }
        }
        ports
    }

    pub fn build_source_manager(
        &self,
    ) -> Result<AppSourceManager<SchemaSQLContext>, OrchestrationError> {
        let mut asm = AppSourceManager::new();

        let mut port: u16 = SOURCE_PORTS_RANGE_START;

        for (conn, sources_group) in self.grouped_connections.clone() {
            let first_source = sources_group.get(0).unwrap();

            if let Some(connection) = &first_source.connection {
                let mut ports = HashMap::new();
                let mut table_and_ports = vec![];
                for source in &sources_group {
                    if self.used_sources.contains(&source.name) {
                        ports.insert(source.name.clone(), port);

                        table_and_ports.push((
                            TableInfo {
                                table_name: source.table_name.clone(),
                                columns: Some(
                                    source
                                        .columns
                                        .iter()
                                        .map(|c| ColumnInfo {
                                            name: c.clone(),
                                            data_type: None,
                                        })
                                        .collect(),
                                ),
                            },
                            port,
                        ));

                        port += 1;
                    }
                }

                let source_factory = ConnectorSourceFactory::new(
                    table_and_ports,
                    connection.clone(),
                    self.progress.cloned(),
                )?;

                asm.add(AppSource::new(
                    conn.to_string(),
                    Arc::new(source_factory),
                    ports,
                ))?;
            }
        }

        Ok(asm)
    }

    pub fn group_connections(sources: &[Source]) -> HashMap<&str, Vec<&Source>> {
        sources
            .iter()
            .fold(HashMap::<&str, Vec<&Source>>::new(), |mut acc, a| {
                if let Some(conn) = a.connection.as_ref() {
                    acc.entry(&conn.name).or_default().push(a);
                }

                acc
            })
    }
}

#[cfg(test)]
mod tests {
    use crate::pipeline::source_builder::SourceBuilder;
    use dozer_types::ingestion_types::{GrpcConfig, GrpcConfigSchemas};
    use dozer_types::models::app_config::{
        default_app_buffer_size, default_app_max_map_size, default_cache_max_map_size,
        default_commit_size, default_commit_timeout, Config,
    };

    use dozer_core::appsource::{AppSourceId, AppSourceMappings};
    use dozer_sql::pipeline::builder::SchemaSQLContext;
    use dozer_types::models::connection::{Connection, ConnectionConfig};
    use dozer_types::models::source::Source;

    fn get_default_config() -> Config {
        let schema_str = include_str!("./schemas.json");
        let grpc_conn = Connection {
            config: Some(ConnectionConfig::Grpc(GrpcConfig {
                schemas: Some(GrpcConfigSchemas::Inline(schema_str.to_string())),
                ..Default::default()
            })),
            name: "pg_conn".to_string(),
        };

        Config {
            app_name: "multi".to_string(),
            api: Default::default(),
            flags: Default::default(),
            connections: vec![grpc_conn.clone()],
            sources: vec![
                Source {
                    name: "users".to_string(),
                    table_name: "users".to_string(),
                    columns: vec!["id".to_string(), "name".to_string()],
                    connection: Some(grpc_conn.clone()),
                    refresh_config: None,
                },
                Source {
                    name: "customers".to_string(),
                    table_name: "customers".to_string(),
                    columns: vec!["id".to_string(), "name".to_string()],
                    connection: Some(grpc_conn),
                    refresh_config: None,
                },
            ],
            endpoints: vec![],
            sql: None,
            home_dir: "test".to_string(),
            cache_max_map_size: Some(default_cache_max_map_size()),
            app_max_map_size: Some(default_app_max_map_size()),
            app_buffer_size: Some(default_app_buffer_size()),
            commit_size: Some(default_commit_size()),
            commit_timeout: Some(default_commit_timeout()),
        }
    }

    #[test]
    fn load_multi_sources() {
        let config = get_default_config();

        let tables = config
            .sources
            .iter()
            .map(|s| s.table_name.clone())
            .collect::<Vec<_>>();

        let source_builder = SourceBuilder::new(
            &tables,
            SourceBuilder::group_connections(&config.sources),
            None,
        );
        let asm = source_builder.build_source_manager().unwrap();

        let conn_name_1 = config.connections.get(0).unwrap().name.clone();
        let pg_source_mapping: Vec<AppSourceMappings<SchemaSQLContext>> = asm
            .get(vec![
                AppSourceId::new(
                    config.sources.get(0).unwrap().table_name.clone(),
                    Some(conn_name_1.clone()),
                ),
                AppSourceId::new(
                    config.sources.get(1).unwrap().table_name.clone(),
                    Some(conn_name_1),
                ),
            ])
            .unwrap();

        assert_eq!(2, pg_source_mapping.get(0).unwrap().mappings.len());
    }
}
