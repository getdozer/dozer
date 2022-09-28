use super::pool::DbPool;
use serde::{Serialize, Deserialize};
use std::error::Error;

pub trait Persistable<'a, T: Serialize + Deserialize<'a>> {
    fn save(&mut self, pool: DbPool) -> Result<&mut T, Box<dyn Error>>;
    fn get_by_id(pool: DbPool, input_id: String) -> Result<T, Box<dyn Error>>;
    fn get_multiple(pool: DbPool) -> Result<Vec<T>, Box<dyn Error>>;
    fn upsert(&mut self,pool: DbPool) -> Result<&mut T, Box<dyn Error>>;
}
