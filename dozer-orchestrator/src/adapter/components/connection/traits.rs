use dozer_ingestion::connectors::connector::Connector;
use dozer_shared::types::TableInfo;
use std::error::Error;
use crate::adapter::db::db_persistent_trait::DbPersistentTrait;

pub trait ConnectionSvcTrait<Config, Connection> {
    fn get_connector(&self, config: Config) -> Box<dyn Connector>;
    fn get_db_svc(&self) -> Box<dyn DbPersistentTrait<Connection>>;
    fn convert_connection_to_config(&self, input: Connection) -> Config;
    
    // default implementation
    fn create_connection(&self, input: Connection) -> Result<String, Box<dyn Error>> {
        self.get_db_svc().save(input)
    }

    fn get_schema(&self, connection_id: String) -> Result<Vec<TableInfo>, Box<dyn Error>> {
        let connection_by_id = self.get_db_svc().get_by_id(connection_id);
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

    fn get_connection_by_id(&self, connection_id: String) -> Result<Connection, Box<dyn Error>> {
        self.get_db_svc().get_by_id(connection_id)
    }

    fn get_all_connections(&self) -> Result<Vec<Connection>, Box<dyn Error>> {
        self.get_db_svc().get_multiple()
    }

    fn test_connection(&self, input: Connection) -> Result<(), Box<dyn Error>> {
        let config = self.convert_connection_to_config(input);
        let result = self.get_connector(config).test_connection();
        result
    }
}
