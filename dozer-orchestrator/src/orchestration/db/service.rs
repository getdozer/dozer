use crate::orchestration::models::connection::Connection;

use super::models as DBModels;
use super::services::{
    connection::ConnectionDbService, db_persistent::DbPersistent, endpoint::EndpointDbService,
    source::SourceDbService,
};

use std::error::Error;
pub struct DbPersistentService {
    source_db_service: SourceDbService,
    connection_db_service: ConnectionDbService,
    endpoint_db_servive: EndpointDbService,
}

impl DbPersistentService {
    pub fn new(database_url: String) -> Self {
        let source_db_service = SourceDbService::new(database_url.clone());
        let connection_db_service = ConnectionDbService::new(database_url.clone());
        let endpoint_db_servive = EndpointDbService::new(database_url.clone());
        Self {
            source_db_service,
            connection_db_service,
            endpoint_db_servive,
        }
    }
    pub fn save_connection(&self, input: Connection) -> Result<String, Box<dyn Error>> {
        let connection = DBModels::connection::Connection::try_from(input).map_err(|e| e)?;
        return self.connection_db_service.save(connection);
    }

    pub fn read_connection(&self, id: String) -> Result<Connection, Box<dyn Error>> {
        self.connection_db_service
            .get_by_id(id)
            .map(|con| Connection::try_from(con))?
    }
}
