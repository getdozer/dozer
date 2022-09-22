use super::{super::models as DBModels, helper::DbPool, db_persistent::DbPersistent};

#[derive(Clone)]
pub struct EndpointDbService {
    db_connection: DbPool,
}
impl EndpointDbService {
  pub fn new(database_url: String) -> Self {
      let db_connection = super::helper::establish_connection(database_url);
      Self { db_connection }
  }
}

impl DbPersistent<DBModels::endpoint::Endpoint> for EndpointDbService {
    fn get_by_id(&self, id: String) -> Result<DBModels::endpoint::Endpoint, Box<dyn std::error::Error>> {
        todo!()
    }

    fn get_multiple(&self) -> Result<Vec<DBModels::endpoint::Endpoint>, Box<dyn std::error::Error>> {
        todo!()
    }

    fn save(&self, input: DBModels::endpoint::Endpoint) -> Result<String, Box<dyn std::error::Error>> {
        todo!()
    }

    fn delete(&self, id: String) -> Result<(), Box<dyn std::error::Error>> {
        todo!()
    }
}

