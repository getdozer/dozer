use dozer_ingestion::connectors::{
    connector::Connector,
    postgres::connector::{PostgresConfig, PostgresConnector},
};
use dozer_types::models::connection::{Authentication, Connection};
use dozer_types::types::Schema;

pub struct ConnectionService {
    connector: Box<dyn Connector>,
}

impl ConnectionService {
    pub fn get_connector(connection: Connection) -> Box<dyn Connector> {
        match connection.authentication.clone() {
            Authentication::PostgresAuthentication {
                user,
                password,
                host,
                port,
                database,
            } => {
                let conn_str = format!(
                    "host={} port={} user={} dbname={} password={}",
                    host, port, user, database, password,
                );
                let config = PostgresConfig {
                    name: connection.name,
                    tables: None,
                    conn_str,
                };
                Box::new(PostgresConnector::new(1, config))
            }
        }
    }
    pub fn get_all_schema(&self) -> anyhow::Result<Vec<(String, Schema)>> {
        self.connector
            .get_all_schema()
            .map_err(anyhow::Error::from)
    }

    pub fn new(connection: Connection) -> Self {
        let connector: Box<dyn Connector> = Self::get_connector(connection);
        Self { connector }
    }

    pub fn test_connection(&self) -> anyhow::Result<()> {
        self.connector
            .test_connection()
            .map_err(anyhow::Error::from)
    }
}
