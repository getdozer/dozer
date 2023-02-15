use std::sync::Arc;

use crate::cache::{expression::QueryExpression, RecordWithId, RoCache};

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

#[derive(Debug, Clone)]
/// CacheReader dynamically attaches permissions on top of queries
pub struct CacheReader {
    cache: Arc<dyn RoCache>,
}

impl CacheReader {
    pub fn new(cache: Arc<dyn RoCache>) -> Self {
        Self { cache }
    }

    // TODO: Implement check_access
    fn check_access(&self, _rec: &Record, _access_filter: &AccessFilter) -> Result<(), CacheError> {
        Ok(())
    }

    pub fn get_schema_and_indexes_by_name(
        &self,
        name: &str,
    ) -> Result<(Schema, Vec<IndexDefinition>), CacheError> {
        self.cache.get_schema_and_indexes_by_name(name)
    }

    pub fn get(
        &self,
        key: &[u8],
        access_filter: &AccessFilter,
    ) -> Result<RecordWithId, CacheError> {
        let record = self.cache.get(key)?;
        match self.check_access(&record.record, access_filter) {
            Ok(_) => Ok(record),
            Err(e) => Err(e),
        }
    }

    pub fn query(
        &self,
        schema_name: &str,
        query: &mut QueryExpression,
        access_filter: AccessFilter,
    ) -> Result<Vec<RecordWithId>, CacheError> {
        self.apply_access_filter(query, access_filter);
        self.cache.query(schema_name, query)
    }

    pub fn count(
        &self,
        schema_name: &str,
        query: &mut QueryExpression,
        access_filter: AccessFilter,
    ) -> Result<usize, CacheError> {
        self.apply_access_filter(query, access_filter);
        self.cache.count(schema_name, query)
    }

    // Apply filter if specified in access
    fn apply_access_filter(&self, query: &mut QueryExpression, access_filter: AccessFilter) {
        if let Some(access_filter) = access_filter.filter {
            let filter = match query.filter.take() {
                Some(query_filter) => FilterExpression::And(vec![access_filter, query_filter]),
                None => access_filter,
            };

            query.filter = Some(filter);
        }
    }
}
