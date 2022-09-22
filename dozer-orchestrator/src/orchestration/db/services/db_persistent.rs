use std::error::Error;
pub trait DbPersistent<C> {
    fn get_by_id(&self, id: String) -> Result<C, Box<dyn Error>>;
    fn get_multiple(&self) -> Result<Vec<C>, Box<dyn Error>>;
    fn save(&self, input: C) -> Result<String, Box<dyn Error>>;
    fn delete(&self, id: String) -> Result<(), Box<dyn Error>>;
}
