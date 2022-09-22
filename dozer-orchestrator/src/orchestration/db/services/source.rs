use super::{super::models as DBModels, helper::{establish_connection, DbPool}, db_persistent::DbPersistent};

#[derive(Clone)]
pub struct SourceDbService {
    db_connection: DbPool,
}
impl SourceDbService {
  pub fn new(database_url: String) -> Self {
      let db_connection = establish_connection(database_url);
      Self { db_connection }
  }
}

impl DbPersistent<DBModels::resource::Resource> for SourceDbService {
    fn get_by_id(&self, id: String) -> Result<DBModels::resource::Resource, Box<dyn std::error::Error>> {
        todo!()
    }

    fn get_multiple(&self) -> Result<Vec<DBModels::resource::Resource>, Box<dyn std::error::Error>> {
        todo!()
    }

    fn save(&self, input: DBModels::resource::Resource) -> Result<String, Box<dyn std::error::Error>> {
        todo!()
    }

    fn delete(&self, id: String) -> Result<(), Box<dyn std::error::Error>> {
        todo!()
    }
}

