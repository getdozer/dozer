use super::models as DBModels;

use super::models::connection::Connection;
use super::services::{
    connection::ConnectionDbService, endpoint::EndpointDbService, source::SourceDbService,
    db_persistent::DbPersistent
};

use std::error::Error;
struct DbPersistentService {
    source_db_service: SourceDbService,
    connection_db_service: ConnectionDbService,
    endpoint_db_servive: EndpointDbService,
}

impl DbPersistentService {
    fn new(database_url: String) -> Self {
        let source_db_service = SourceDbService::new(database_url.clone());
        let connection_db_service = ConnectionDbService::new(database_url.clone());
        let endpoint_db_servive = EndpointDbService::new(database_url.clone());
        Self {
            source_db_service,
            connection_db_service,
            endpoint_db_servive,
        }
    }
    fn save_connection(&self, input: Connection) -> Result<String, Box<dyn Error>> {
        let connection = DBModels::connection::Connection::try_from(input).map_err(|e| e)?;
        return self.connection_db_service.save(connection);
    }
}
