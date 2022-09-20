use crate::db::{
    connection_db_trait::ConnectionDbTrait
};
use dozer_ingestion::connectors::connector::Connector;
use dozer_shared::types::TableInfo;
use std::error::Error;

pub trait ConnectionSvcTrait<Config, Connection> {
    fn get_connector(&self, config: Config) -> Box<dyn Connector<Config>>; // Connector;
    fn get_db_svc(&self) -> Box<dyn ConnectionDbTrait<Connection>>;
    fn convert_connection_to_config(&self, input: Connection) -> Config;

    fn create_connection(&self, input: Connection) -> Result<String, Box<dyn Error>>;
    fn get_schema(&self, connection_id: String) -> Result<Vec<TableInfo>, Box<dyn Error>>;
    fn get_connection_by_id(&self, connection_id: String) -> Result<Connection,Box<dyn Error>>;
    fn get_all_connections(&self) -> Result<Vec<Connection>, Box<dyn Error>>;
    fn test_connection(&self, input: Connection) -> Result<(), Box<dyn Error>>;
}
