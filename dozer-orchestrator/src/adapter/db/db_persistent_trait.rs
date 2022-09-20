use std::error::Error;
pub trait DbPersistentTrait<C> {
    fn get_by_id(&self, connection_id: String) -> Result<C, Box<dyn Error>>;
    fn get_multiple(&self) -> Result<Vec<C>, Box<dyn Error>>;
    fn save(&self, input: C) -> Result<String, Box<dyn Error>>;
}
