use crate::cache::{expression::QueryExpression, CacheRecord, CommitState, RoCache};

use super::cache::expression::FilterExpression;
use crate::errors::CacheError;
use dozer_types::{
    serde,
    types::{Record, SchemaWithIndex},
};
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
#[serde(crate = "self::serde")]

/// This filter gets dynamically added to the query.
pub struct AccessFilter {
    /// FilterExpression to evaluate access
    pub filter: Option<FilterExpression>,

    /// Fields to be restricted
    #[serde(default)]
    pub fields: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(crate = "dozer_types::serde")]
pub enum Phase {
    Snapshotting,
    Streaming,
}

#[derive(Debug)]
/// CacheReader dynamically attaches permissions on top of queries
pub struct CacheReader {
    cache: Box<dyn RoCache>,
}

impl CacheReader {
    pub fn new(cache: Box<dyn RoCache>) -> Self {
        Self { cache }
    }

    // TODO: Implement check_access
    fn check_access(&self, _rec: &Record, _access_filter: &AccessFilter) -> Result<(), CacheError> {
        Ok(())
    }

    pub fn cache_name(&self) -> &str {
        self.cache.name()
    }

    pub fn get_commit_state(&self) -> Result<Option<CommitState>, CacheError> {
        self.cache.get_commit_state()
    }

    pub fn get_schema(&self) -> &SchemaWithIndex {
        self.cache.get_schema()
    }

    pub fn get(&self, key: &[u8], access_filter: &AccessFilter) -> Result<CacheRecord, CacheError> {
        let record = self.cache.get(key)?;
        match self.check_access(&record.record, access_filter) {
            Ok(_) => Ok(record),
            Err(e) => Err(e),
        }
    }

    pub fn query(
        &self,
        query: &mut QueryExpression,
        access_filter: AccessFilter,
    ) -> Result<Vec<CacheRecord>, CacheError> {
        self.apply_access_filter(query, access_filter);
        self.cache.query(query)
    }

    pub fn count(
        &self,
        query: &mut QueryExpression,
        access_filter: AccessFilter,
    ) -> Result<usize, CacheError> {
        self.apply_access_filter(query, access_filter);
        self.cache.count(query)
    }

    pub fn get_phase(&self) -> Result<Phase, CacheError> {
        if self.cache.is_snapshotting_done()? {
            Ok(Phase::Streaming)
        } else {
            Ok(Phase::Snapshotting)
        }
    }

    // Apply filter if specified in access
    fn apply_access_filter(&self, query: &mut QueryExpression, access_filter: AccessFilter) {
        // TODO: Use `fields` in `access_filter`.
        if let Some(access_filter) = access_filter.filter {
            let filter = match query.filter.take() {
                Some(query_filter) => FilterExpression::And(vec![access_filter, query_filter]),
                None => access_filter,
            };

            query.filter = Some(filter);
        }
    }
}
