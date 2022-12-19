use crate::pipeline::connector_source::NewConnectorSourceFactory;
use dozer_core::dag::appsource::{AppSource, AppSourceManager};
use dozer_ingestion::connectors::{get_connector_outputs, TableInfo};
use dozer_ingestion::ingestion::{IngestionIterator, Ingestor};
use dozer_types::models::source::Source;
use dozer_types::parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;

pub struct SourceBuilder {}

const SOURCE_PORTS_RANGE_START: u16 = 1000;

impl SourceBuilder {
    pub fn build_source_manager(
        sources: Vec<Source>,
        ingestor: Arc<RwLock<Ingestor>>,
        iterator: Arc<RwLock<IngestionIterator>>,
    ) -> AppSourceManager {
        let mut asm = AppSourceManager::new();

        let grouped_connections =
            sources
                .into_iter()
                .fold(HashMap::<String, Vec<Source>>::new(), |mut acc, a| {
                    acc.entry(a.connection.name.clone())
                        .or_default()
                        .push(a.clone());
                    acc
                });

        let mut port: u16 = SOURCE_PORTS_RANGE_START;

        grouped_connections
            .iter()
            .for_each(|(conn, sources_group)| {
                let first_source = sources_group.get(0).unwrap();
                let grouped_connector_sources =
                    get_connector_outputs(first_source.connection.clone(), sources_group.clone());

                for same_connection_sources in grouped_connector_sources {
                    let mut ports = HashMap::new();
                    let mut tables = vec![];
                    for source in same_connection_sources {
                        ports.insert(source.table_name.clone(), port);

                        tables.push(TableInfo {
                            name: source.table_name,
                            id: port as u32,
                            columns: source.columns,
                        });

                        port += 1;
                    }

                    let source_factory = NewConnectorSourceFactory {
                        ingestor: Arc::clone(&ingestor),
                        iterator: Arc::clone(&iterator),
                        ports: ports.clone(),
                        tables,
                        connection: first_source.connection.clone(),
                    };
                    asm.add(AppSource::new(
                        conn.clone(),
                        Arc::new(source_factory),
                        ports,
                    ));
                }
            });

        asm
    }
}

#[cfg(test)]
mod tests {
    use crate::pipeline::source_builder::SourceBuilder;
    use dozer_ingestion::ingestion::{IngestionConfig, Ingestor};
    use dozer_types::models::app_config::Config;
    use dozer_types::serde_yaml;
    use std::collections::HashMap;
    use std::sync::Arc;

    use dozer_core::dag::appsource::{AppSourceId, AppSourceMappings};
    use dozer_core::dag::forwarder::LocalChannelForwarder;
    use dozer_types::ingestion_types::IngestionOperation;
    use dozer_types::models::connection::{Authentication, Connection, DBType};
    use dozer_types::models::source::{RefreshConfig, Source};
    use include_dir::{include_dir, Dir};

    #[cfg(not(doc))]
    static TESTS_CONFIG_DIR: Dir<'_> = include_dir!("config/tests/local");
    #[cfg(doc)]
    static TESTS_CONFIG_DIR: Dir<'_> = include_dir!("../config/tests/local");

    pub fn load_config(file_name: &str) -> &str {
        TESTS_CONFIG_DIR
            .get_file(file_name)
            .unwrap()
            .contents_utf8()
            .unwrap()
    }

    #[test]
    fn load_multi_sources() {
        let config = serde_yaml::from_str::<Config>(load_config("test.multi.yaml")).unwrap();

        let pg_conn = Connection {
            db_type: DBType::Postgres,
            authentication: Authentication::PostgresAuthentication {
                user: "".to_string(),
                password: "".to_string(),
                host: "".to_string(),
                port: 0,
                database: "".to_string(),
            },
            name: "pg_conn".to_string(),
            id: None,
        };
        let snow_conn = Connection {
            db_type: DBType::Snowflake,
            authentication: Authentication::SnowflakeAuthentication {
                server: "".to_string(),
                user: "".to_string(),
                password: "".to_string(),
                port: "1111".to_string(),
                database: "".to_string(),
                schema: "".to_string(),
                warehouse: "".to_string(),
                driver: None,
            },
            name: "snow".to_string(),
            id: None,
        };

        let config = Config {
            app_name: "multi".to_string(),
            api: Default::default(),
            connections: vec![pg_conn.clone(), snow_conn.clone()],
            sources: vec![
                Source {
                    id: None,
                    name: "customers".to_string(),
                    table_name: "customers".to_string(),
                    columns: None,
                    connection: pg_conn.clone(),
                    history_type: None,
                    refresh_config: RefreshConfig::RealTime,
                },
                Source {
                    id: None,
                    name: "addresses".to_string(),
                    table_name: "addresses".to_string(),
                    columns: None,
                    connection: pg_conn.clone(),
                    history_type: None,
                    refresh_config: RefreshConfig::RealTime,
                },
                Source {
                    id: None,
                    name: "prices".to_string(),
                    table_name: "prices".to_string(),
                    columns: None,
                    connection: snow_conn.clone(),
                    history_type: None,
                    refresh_config: RefreshConfig::RealTime,
                },
                Source {
                    id: None,
                    name: "prices_history".to_string(),
                    table_name: "prices_history".to_string(),
                    columns: None,
                    connection: snow_conn.clone(),
                    history_type: None,
                    refresh_config: RefreshConfig::RealTime,
                },
            ],
            endpoints: vec![],
        };

        let (ingestor, iterator) = Ingestor::initialize_channel(IngestionConfig::default());

        let iterator_ref = Arc::clone(&iterator);
        let asm =
            SourceBuilder::build_source_manager(config.sources.clone(), ingestor, iterator_ref);

        let pg_source_mapping: Vec<AppSourceMappings> = asm
            .get(vec![
                AppSourceId::new(
                    config.sources.get(0).unwrap().table_name.clone(),
                    Some(pg_conn.name.clone()),
                ),
                AppSourceId::new(
                    config.sources.get(1).unwrap().table_name.clone(),
                    Some(pg_conn.name.clone()),
                ),
            ])
            .unwrap();

        assert_eq!(2, pg_source_mapping.get(0).unwrap().mappings.len());

        let snowflake_source_1_mapping: Vec<AppSourceMappings> = asm
            .get(vec![AppSourceId::new(
                config.sources.get(2).unwrap().table_name.clone(),
                Some(snow_conn.name.clone()),
            )])
            .unwrap();

        assert_eq!(1, snowflake_source_1_mapping.get(0).unwrap().mappings.len());

        let snowflake_source_2_mapping: Vec<AppSourceMappings> = asm
            .get(vec![AppSourceId::new(
                config.sources.get(3).unwrap().table_name.clone(),
                Some(snow_conn.name.clone()),
            )])
            .unwrap();

        assert_eq!(1, snowflake_source_2_mapping.get(0).unwrap().mappings.len());
    }
}
