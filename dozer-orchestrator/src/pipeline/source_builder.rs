use crate::pipeline::connector_source::ConnectorSourceFactory;
use crate::OrchestrationError;
use dozer_core::appsource::{AppSource, AppSourceManager};
use dozer_ingestion::connectors::{ColumnInfo, TableInfo};
use dozer_sql::pipeline::builder::SchemaSQLContext;
use dozer_types::models::source::Source;
use std::collections::HashMap;
use std::sync::Arc;

pub struct SourceBuilder {
    used_sources: Vec<String>,
    grouped_connections: HashMap<String, Vec<Source>>,
}

const SOURCE_PORTS_RANGE_START: u16 = 1000;

impl SourceBuilder {
    pub fn new(
        used_sources: Vec<String>,
        grouped_connections: HashMap<String, Vec<Source>>,
    ) -> Self {
        Self {
            used_sources,
            grouped_connections,
        }
    }

    pub fn get_ports(&self) -> HashMap<(String, String), u16> {
        let mut port: u16 = SOURCE_PORTS_RANGE_START;

        let mut ports = HashMap::new();
        for (conn, sources_group) in self.grouped_connections.clone() {
            for source in &sources_group {
                if self.used_sources.contains(&source.name) {
                    ports.insert((conn.clone(), source.name.clone()), port);
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
                let mut tables = vec![];
                for source in &sources_group {
                    if self.used_sources.contains(&source.name) {
                        ports.insert(source.name.clone(), port);

                        tables.push(TableInfo {
                            name: source.name.clone(),
                            table_name: source.table_name.clone(),
                            id: port as u32,
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
                        });

                        port += 1;
                    }
                }

                let source_factory =
                    ConnectorSourceFactory::new(ports.clone(), tables, connection.clone());

                asm.add(AppSource::new(
                    conn.clone(),
                    Arc::new(source_factory),
                    ports,
                ))?;
            }
        }

        Ok(asm)
    }

    pub fn group_connections(sources: Vec<Source>) -> HashMap<String, Vec<Source>> {
        sources
            .into_iter()
            .fold(HashMap::<String, Vec<Source>>::new(), |mut acc, a| {
                if let Some(conn) = a.connection.clone() {
                    acc.entry(conn.name).or_default().push(a);
                }

                acc
            })
    }
}

#[cfg(test)]
mod tests {
    use crate::pipeline::source_builder::SourceBuilder;
    use dozer_types::models::app_config::Config;

    use dozer_core::appsource::{AppSourceId, AppSourceMappings};
    use dozer_sql::pipeline::builder::SchemaSQLContext;
    use dozer_types::models::connection::{
        Authentication, Connection, DBType, EventsAuthentication,
    };
    use dozer_types::models::source::Source;

    fn get_default_config() -> Config {
        let events1_conn = Connection {
            authentication: Some(Authentication::Events(EventsAuthentication {})),
            db_type: DBType::Postgres.into(),
            name: "pg_conn".to_string(),
        };

        let events2_conn = Connection {
            authentication: Some(Authentication::Events(EventsAuthentication {})),
            db_type: DBType::Snowflake.into(),
            name: "snow".to_string(),
        };

        Config {
            app_name: "multi".to_string(),
            api: Default::default(),
            flags: Default::default(),
            connections: vec![events1_conn.clone(), events2_conn.clone()],
            sources: vec![
                Source {
                    id: None,
                    name: "customers".to_string(),
                    table_name: "customers".to_string(),
                    columns: vec!["id".to_string()],
                    connection: Some(events1_conn.clone()),
                    refresh_config: None,
                    app_id: None,
                },
                Source {
                    id: None,
                    name: "addresses".to_string(),
                    table_name: "addresses".to_string(),
                    columns: vec!["id".to_string()],
                    connection: Some(events1_conn),
                    refresh_config: None,
                    app_id: None,
                },
                Source {
                    id: None,
                    name: "prices".to_string(),
                    table_name: "prices".to_string(),
                    columns: vec!["id".to_string()],
                    connection: Some(events2_conn.clone()),
                    refresh_config: None,
                    app_id: None,
                },
                Source {
                    id: None,
                    name: "prices_history".to_string(),
                    table_name: "prices_history".to_string(),
                    columns: vec!["id".to_string()],
                    connection: Some(events2_conn),
                    refresh_config: None,
                    app_id: None,
                },
            ],
            endpoints: vec![],
            sql: None,
            home_dir: "test".to_string(),
        }
    }

    #[test]
    fn load_multi_sources() {
        let config = get_default_config();

        let tables = config
            .sources
            .iter()
            .map(|s| s.table_name.clone())
            .collect();

        let source_builder = SourceBuilder::new(
            tables,
            SourceBuilder::group_connections(config.sources.clone()),
        );
        let asm = source_builder.build_source_manager().unwrap();

        let conn_name_1 = config.connections.get(0).unwrap().name.clone();
        let conn_name_2 = config.connections.get(1).unwrap().name.clone();
        let pg_source_mapping: Vec<AppSourceMappings<SchemaSQLContext>> = asm
            .get(vec![
                AppSourceId::new(
                    config.sources.get(0).unwrap().table_name.clone(),
                    Some(conn_name_1),
                ),
                AppSourceId::new(
                    config.sources.get(2).unwrap().table_name.clone(),
                    Some(conn_name_2),
                ),
            ])
            .unwrap();

        assert_eq!(1, pg_source_mapping.get(0).unwrap().mappings.len());
    }

    #[test]
    fn load_only_used_sources() {
        let config = get_default_config();

        let only_used_table_name = vec![config.sources.get(0).unwrap().table_name.clone()];
        let conn_name = config.connections.get(0).unwrap().name.clone();
        let source_builder = SourceBuilder::new(
            only_used_table_name,
            SourceBuilder::group_connections(config.sources.clone()),
        );
        let asm = source_builder.build_source_manager().unwrap();

        let pg_source_mapping: Vec<AppSourceMappings<SchemaSQLContext>> = asm
            .get(vec![AppSourceId::new(
                config.sources.get(0).unwrap().table_name.clone(),
                Some(conn_name.clone()),
            )])
            .unwrap();

        assert_eq!(1, pg_source_mapping.get(0).unwrap().mappings.len());

        assert!(asm
            .get(vec![AppSourceId::new(
                config.sources.get(0).unwrap().table_name.clone(),
                Some(conn_name),
            ),])
            .is_ok())
    }
}
