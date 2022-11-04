use dozer_ingestion::{
    connectors::{get_connector, Connector},
    errors::ConnectorError,
};
use dozer_types::models::connection::Connection;
use dozer_types::types::Schema;

pub struct ConnectionService {
    connector: Box<dyn Connector>,
}

impl ConnectionService {
    pub fn get_connector(connection: Connection) -> Box<dyn Connector> {
        get_connector(connection)
    }
    pub fn get_all_schema(&self) -> Result<Vec<(String, Schema)>, ConnectorError> {
        self.connector.get_schemas()
    }
    pub fn get_schema(&self, table_name: String) -> Result<Schema, ConnectorError> {
        self.connector.get_schema(table_name)
    }

    pub fn new(connection: Connection) -> Self {
        let connector: Box<dyn Connector> = Self::get_connector(connection);
        Self { connector }
    }

    pub fn test_connection(&self) -> Result<(), ConnectorError> {
        self.connector.test_connection()
    }
}
