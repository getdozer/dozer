use std::error::Error;
pub trait DbPersistentTrait<C> {
    fn get_connection_by_id(&self, connection_id: String) -> Result<C, Box<dyn Error>>;
    fn get_connections(&self) -> Result<Vec<C>, Box<dyn Error>>;
    fn save_connection(&self, input: C) -> Result<String, Box<dyn Error>>;
}
