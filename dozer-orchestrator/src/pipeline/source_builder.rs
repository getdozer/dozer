use crate::pipeline::connector_source::ConnectorSourceFactory;
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
        grouped_connections: HashMap<String, Vec<Source>>,
        ingestor: Arc<RwLock<Ingestor>>,
        iterator: Arc<RwLock<IngestionIterator>>,
    ) -> AppSourceManager {
        let mut asm = AppSourceManager::new();

        let mut port: u16 = SOURCE_PORTS_RANGE_START;

        for (conn, sources_group) in grouped_connections {
            let first_source = sources_group.get(0).unwrap();

            if let Some(connection) = &first_source.connection {
                let grouped_connector_sources =
                    get_connector_outputs(connection.clone(), sources_group.clone());

                for same_connection_sources in grouped_connector_sources {
                    let mut ports = HashMap::new();
                    let mut tables = vec![];
                    for source in same_connection_sources {
                        ports.insert(source.table_name.clone(), port);

                        tables.push(TableInfo {
                            name: source.table_name,
                            id: port as u32,
                            columns: Some(source.columns),
                        });

                        port += 1;
                    }

                    let source_factory = ConnectorSourceFactory::new(
                        Arc::clone(&ingestor),
                        Arc::clone(&iterator),
                        ports.clone(),
                        tables,
                        connection.clone(),
                    );

                    asm.add(AppSource::new(
                        conn.clone(),
                        Arc::new(source_factory),
                        ports,
                    ));
                }
            }
        }

        asm
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
    use dozer_ingestion::ingestion::{IngestionConfig, Ingestor};
    use dozer_types::models::app_config::Config;
    use std::sync::Arc;

    use dozer_core::dag::appsource::{AppSourceId, AppSourceMappings};
    use dozer_types::models::connection::{
        Authentication, Connection, DBType, EventsAuthentication,
    };
    use dozer_types::models::source::Source;

    #[test]
    fn load_multi_sources() {
        let events1_conn = Connection {
            authentication: Some(Authentication::Events(EventsAuthentication {})),
            id: None,
            app_id: None,
            db_type: DBType::Postgres.into(),
            name: "pg_conn".to_string(),
        };

        let events2_conn = Connection {
            authentication: Some(Authentication::Events(EventsAuthentication {})),
            id: None,
            app_id: None,
            db_type: DBType::Snowflake.into(),
            name: "snow".to_string(),
        };

        let config = Config {
            id: None,
            app_name: "multi".to_string(),
            api: Default::default(),
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
                    connection: Some(events1_conn.clone()),
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
                    connection: Some(events2_conn.clone()),
                    refresh_config: None,
                    app_id: None,
                },
            ],
            endpoints: vec![],
            home_dir: "test".to_string(),
        };

        let (ingestor, iterator) = Ingestor::initialize_channel(IngestionConfig::default());

        let iterator_ref = Arc::clone(&iterator);

        let asm = SourceBuilder::build_source_manager(
            SourceBuilder::group_connections(config.sources.clone()),
            ingestor,
            iterator_ref,
        );

        let pg_source_mapping: Vec<AppSourceMappings> = asm
            .get(vec![
                AppSourceId::new(
                    config.sources.get(0).unwrap().table_name.clone(),
                    Some(events1_conn.name.clone()),
                ),
                AppSourceId::new(
                    config.sources.get(1).unwrap().table_name.clone(),
                    Some(events1_conn.name),
                ),
            ])
            .unwrap();

        assert_eq!(2, pg_source_mapping.get(0).unwrap().mappings.len());
    }
}
