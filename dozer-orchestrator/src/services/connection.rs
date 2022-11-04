use dozer_ingestion::{
    connectors::{
        postgres::connector::{PostgresConfig, PostgresConnector},
        Connector,
    },
    errors::ConnectorError,
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
    pub fn get_all_schema(&self) -> Result<Vec<(String, Schema)>, ConnectorError> {
        self.connector.get_schemas()
    }

    pub fn new(connection: Connection) -> Self {
        let connector: Box<dyn Connector> = Self::get_connector(connection);
        Self { connector }
    }

    pub fn test_connection(&self) -> Result<(), ConnectorError> {
        self.connector.test_connection()
    }
}
