use std::sync::Arc;

use crate::cache::{expression::QueryExpression, Cache, LmdbCache};

use super::cache::expression::FilterExpression;
use crate::errors::CacheError;
use dozer_types::{
    serde,
    types::{IndexDefinition, Record, Schema},
};
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
#[serde(crate = "self::serde")]

/// This filter gets dynamically added to the query.
pub struct AccessFilter {
    /// FilterExpression to evaluate access
    pub filter: Option<FilterExpression>,

    /// Fields to be restricted
    pub fields: Vec<String>,
}

/// CacheReader dynamically attaches permissions on top of queries
pub struct CacheReader {
    pub cache: Arc<LmdbCache>,
    pub access: AccessFilter,
}

impl CacheReader {
    // TODO: Implement check_access
    pub fn check_access(&self, _rec: &Record) -> Result<(), CacheError> {
        Ok(())
    }

    pub fn get_schema_and_indexes_by_name(
        &self,
        name: &str,
    ) -> Result<(Schema, Vec<IndexDefinition>), CacheError> {
        self.cache.get_schema_and_indexes_by_name(name)
    }

    pub fn get(&self, key: &[u8]) -> Result<Record, CacheError> {
        let record = self.cache.get(key)?;
        match self.check_access(&record) {
            Ok(_) => Ok(record.to_owned()),
            Err(e) => Err(e),
        }
    }

    pub fn query(
        &self,
        schema_name: &str,
        query: &mut QueryExpression,
    ) -> Result<Vec<Record>, CacheError> {
        // Apply filter if specified in access
        if let Some(access_filter) = self.access.filter.to_owned() {
            let filter = query
                .filter
                .as_ref()
                .map_or(access_filter.to_owned(), |query_filter| {
                    FilterExpression::And(vec![access_filter.to_owned(), query_filter.to_owned()])
                });

            query.filter = Some(filter);
            self.cache.query(schema_name, query)
        } else {
            self.cache.query(schema_name, query)
        }
    }
}
