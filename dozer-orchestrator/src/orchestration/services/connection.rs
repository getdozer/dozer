use dozer_ingestion::connectors::{
    connector::Connector,
    postgres::connector::{PostgresConfig, PostgresConnector},
};
use dozer_shared::types::TableInfo;
use std::error::Error;

use crate::orchestration::models::connection::{Authentication, Connection};

pub struct ConnectionService {
    connector: Box<dyn Connector>,
}
impl ConnectionService {}
impl ConnectionService {

    pub fn get_schema(&self) -> Result<Vec<TableInfo>, Box<dyn Error>> {
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

    pub fn test_connection(&self) -> Result<(), Box<dyn Error>> {
        return self.connector.test_connection();
    }
}
