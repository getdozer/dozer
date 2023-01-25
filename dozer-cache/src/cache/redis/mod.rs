use crate::errors::CacheError;

use super::Cache;
mod helper;
pub struct RedisCache {
    pub client: redis::Client,
    pub connection: redis::Connection,
}

impl RedisCache {
    pub fn new() -> Result<Self, CacheError> {
        let (client, connection) =
            helper::init().map_err(|e| CacheError::InternalError(Box::new(e)))?;
        Ok(Self { client, connection })
    }
}
