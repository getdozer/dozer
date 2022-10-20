use crate::connectors::connector::Connector;
use crate::connectors::postgres::connector::{PostgresConfig, PostgresConnector};
use crate::connectors::seq_no_resolver::SeqNoResolver;
use crate::connectors::storage::RocksStorage;
use dozer_types::models::connection::Authentication::PostgresAuthentication;
use dozer_types::models::connection::Connection;
use log::debug;
use std::sync::Arc;

pub mod connectors;

pub fn get_connector(connection: Connection) -> Box<dyn Connector> {
    match connection.authentication {
        PostgresAuthentication {
            user,
            password: _,
            host,
            port,
            database,
        } => {
            let postgres_config = PostgresConfig {
                name: connection.name,
                tables: None,
                conn_str: format!(
                    "host={} port={} user={} dbname={}",
                    host, port, user, database
                ),
            };
            debug!("Connecting to postgres database - {}", database);
            Box::new(PostgresConnector::new(
                connection.id.unwrap().parse().unwrap(),
                postgres_config,
            ))
        }
    }
}

pub fn get_seq_resolver(storage_client: Arc<RocksStorage>) -> SeqNoResolver {
    let mut seq_resolver = SeqNoResolver::new(Arc::clone(&storage_client));
    seq_resolver.init();
    seq_resolver
}
