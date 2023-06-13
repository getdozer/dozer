use crate::auth::Access;
use crate::errors::{ApiError, AuthError};
use dozer_cache::cache::expression::QueryExpression;
use dozer_cache::cache::CacheRecord;
use dozer_cache::{AccessFilter, CacheReader};

pub const API_LATENCY_HISTOGRAM_NAME: &str = "api_latency";

pub fn get_record(
    cache_reader: &CacheReader,
    key: &[u8],
    endpoint: &str,
    access: Option<Access>,
) -> Result<CacheRecord, ApiError> {
    let access_filter = get_access_filter(access, endpoint)?;
    let record = cache_reader
        .get(key, &access_filter)
        .map_err(ApiError::NotFound)?;
    Ok(record)
}

pub fn get_records_count(
    cache_reader: &CacheReader,
    exp: &mut QueryExpression,
    endpoint: &str,
    access: Option<Access>,
) -> Result<usize, ApiError> {
    let access_filter = get_access_filter(access, endpoint)?;
    cache_reader
        .count(exp, access_filter)
        .map_err(ApiError::CountFailed)
}

/// Get multiple records
pub fn get_records(
    cache_reader: &CacheReader,
    exp: &mut QueryExpression,
    endpoint: &str,
    access: Option<Access>,
) -> Result<Vec<CacheRecord>, ApiError> {
    let access_filter = get_access_filter(access, endpoint)?;
    cache_reader
        .query(exp, access_filter)
        .map_err(ApiError::QueryFailed)
}

fn get_access_filter(access: Option<Access>, endpoint: &str) -> Result<AccessFilter, ApiError> {
    match access {
        None | Some(Access::All) => Ok(AccessFilter {
            filter: None,
            fields: vec![],
        }),
        Some(Access::Custom(mut access_filters)) => {
            if let Some(access_filter) = access_filters.remove(endpoint) {
                Ok(access_filter)
            } else {
                Err(ApiError::ApiAuthError(AuthError::InvalidToken))
            }
        }
    }
}
