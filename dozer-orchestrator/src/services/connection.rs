use super::super::models::connection::{Authentication, Connection};
use dozer_ingestion::connectors::{
    connector::Connector,
    postgres::connector::{PostgresConfig, PostgresConnector},
};
use dozer_types::types::TableInfo;

pub struct ConnectionService {
    connector: Box<dyn Connector>,
}

impl ConnectionService {
    pub fn get_schema(&self) -> anyhow::Result<Vec<TableInfo>> {
        return self.connector.get_schema();
    }

    pub fn new(input: Connection) -> Self {
        let connector: Box<dyn Connector> = match input.authentication.clone() {
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
                    name: input.name.clone(),
                    tables: None,
                    conn_str: conn_str,
                };
                Box::new(PostgresConnector::new(config))
            }
        };
        Self {
            connector: connector,
        }
    }

    pub fn test_connection(&self) -> anyhow::Result<()> {
        return self.connector.test_connection();
    }
}
