use crate::db::{
    connection_db_svc::ConnectionDbSvc, connection_db_trait::ConnectionDbTrait,
    models::connection::Connection
};
use dozer_ingestion::connectors::{
    connector::Connector,
    postgres::connector::{PostgresConfig, PostgresConnector},
};
use dozer_shared::types::TableInfo;
use serde_json::Value;
use std::error::Error;

use super::connections_svc_trait::ConnectionSvcTrait;
pub struct ConnectionSvc {
    connection_db_svc: ConnectionDbSvc,
}
impl ConnectionSvc {
    pub fn new(connection_db_svc: ConnectionDbSvc) -> Self {
        Self {
            connection_db_svc: connection_db_svc,
        }
    }
}
impl ConnectionSvcTrait<PostgresConfig, Connection> for ConnectionSvc {
    fn get_connector(&self, config: PostgresConfig) -> Box<dyn Connector<PostgresConfig>> {
        Box::new(PostgresConnector::new(config))
    }

    fn get_db_svc(&self) -> Box<dyn ConnectionDbTrait<Connection>> {
        Box::new(self.connection_db_svc.clone())
    }

    fn convert_connection_to_config(&self, input: Connection) -> PostgresConfig {
        let postgres_auth: Value = serde_json::from_str(&input.auth).unwrap();
        let conn_str = format!(
            "host={} port={} user={} dbname={} password={}",
            postgres_auth["host"],
            postgres_auth["port"],
            postgres_auth["user"],
            postgres_auth["database"],
            postgres_auth["password"],
        );
        let postgres_config = PostgresConfig {
            name: postgres_auth["name"].to_string(),
            conn_str: conn_str.clone(),
            tables: None,
        };
        postgres_config
    }

    fn get_schema(&self, connection_id: String) -> Result<Vec<TableInfo>, Box<dyn Error>> {
        let connection_by_id = self.get_db_svc().get_connection_by_id(connection_id);
        match connection_by_id {
            Ok(connection) => {
                let config = self.convert_connection_to_config(connection);
                let connector = self.get_connector(config);
                let table_info = connector.get_schema();
                Ok(table_info)
            }
            Err(err) => Err(err),
        }
    }

    fn get_all_connections(&self) -> Result<Vec<Connection>, Box<dyn Error>> {
        self.get_db_svc().get_connections()
    }

    fn test_connection(&self, input: Connection) -> Result<(), Box<dyn Error>> {
        let config = self.convert_connection_to_config(input);
        let result = self.get_connector(config).test_connection();
        result
    }

    fn get_connection_by_id(&self, connection_id: String) -> Result<Connection, Box<dyn Error>> {
        self.get_db_svc().get_connection_by_id(connection_id)
    }

    fn create_connection(&self, input: Connection) -> Result<String, Box<dyn Error>> {
        self.get_db_svc().save_connection(input)
    }
}
