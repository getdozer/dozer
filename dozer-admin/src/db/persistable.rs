use super::pool::DbPool;
use crate::server::dozer_admin_grpc::Pagination;
use serde::{Deserialize, Serialize};
use std::error::Error;

pub trait Persistable<'a, T: Serialize + Deserialize<'a>> {
    fn save(&mut self, pool: DbPool) -> Result<&mut T, Box<dyn Error>>;
    fn by_id(pool: DbPool, input_id: String, app_id: String) -> Result<T, Box<dyn Error>>;
    fn list(
        pool: DbPool,
        app_id: String,
        limit: Option<u32>,
        offset: Option<u32>,
    ) -> Result<(Vec<T>, Pagination), Box<dyn Error>>;
    fn upsert(&mut self, pool: DbPool) -> Result<&mut T, Box<dyn Error>>;
}
