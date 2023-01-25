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

impl Cache for RedisCache {
    fn insert_schema(
        &self,
        name: &str,
        schema: &dozer_types::types::Schema,
        secondary_indexes: &[dozer_types::types::IndexDefinition],
    ) -> Result<(), crate::errors::CacheError> {
        todo!()
    }

    fn get_schema(
        &self,
        schema_identifier: &dozer_types::types::SchemaIdentifier,
    ) -> Result<dozer_types::types::Schema, crate::errors::CacheError> {
        todo!()
    }

    fn get_schema_and_indexes_by_name(
        &self,
        name: &str,
    ) -> Result<
        (
            dozer_types::types::Schema,
            Vec<dozer_types::types::IndexDefinition>,
        ),
        crate::errors::CacheError,
    > {
        todo!()
    }

    fn insert(&self, record: &dozer_types::types::Record) -> Result<(), crate::errors::CacheError> {
        todo!()
    }

    fn delete(&self, key: &[u8]) -> Result<(), crate::errors::CacheError> {
        todo!()
    }

    fn update(
        &self,
        key: &[u8],
        record: &dozer_types::types::Record,
    ) -> Result<(), crate::errors::CacheError> {
        todo!()
    }

    fn get(&self, key: &[u8]) -> Result<dozer_types::types::Record, crate::errors::CacheError> {
        todo!()
    }

    fn count(
        &self,
        schema_name: &str,
        query: &super::expression::QueryExpression,
    ) -> Result<usize, crate::errors::CacheError> {
        todo!()
    }

    fn query(
        &self,
        schema_name: &str,
        query: &super::expression::QueryExpression,
    ) -> Result<Vec<dozer_types::types::Record>, crate::errors::CacheError> {
        todo!()
    }
}
