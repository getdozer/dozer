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
    pub fn get_connector(
        connection: Connection,
        tables: Option<Vec<(String, u32)>>,
    ) -> Box<dyn Connector> {
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
                    name: connection.name.clone(),
                    tables: tables,
                    conn_str: conn_str,
                };
                Box::new(PostgresConnector::new(config))
            }
        }
    }
    pub fn get_all_schema(&self) -> anyhow::Result<Vec<(String, Schema)>> {
        return self.connector.get_all_schema();
    }

    pub fn new(connection: Connection) -> Self {
        let connector: Box<dyn Connector> = Self::get_connector(connection, None);
        Self {
            connector: connector,
        }
    }

    pub fn test_connection(&self) -> anyhow::Result<()> {
        return self.connector.test_connection();
    }
}
