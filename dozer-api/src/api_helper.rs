use std::slice;

use crate::auth::Access;
use crate::errors::{ApiError, AuthError};
use dozer_cache::cache::expression::QueryExpression;
use dozer_cache::cache::CacheRecord;
use dozer_cache::{AccessFilter, CacheReader};
use dozer_types::models::api_security::ApiSecurity;
use dozer_types::types::Field;

pub const API_LATENCY_HISTOGRAM_NAME: &str = "api_latency";
pub const API_REQUEST_COUNTER_NAME: &str = "api_requests";
pub fn get_record(
    cache_reader: &CacheReader,
    key: &Field,
    endpoint: &str,
    access: Option<Access>,
) -> Result<CacheRecord, ApiError> {
    let access_filter = get_access_filter(access, endpoint)?;
    let record = cache_reader
        .get(slice::from_ref(key), &access_filter)
        .map_err(ApiError::NotFound)?;
    Ok(record)
}

pub fn get_api_security(current_api_security: Option<ApiSecurity>) -> Option<ApiSecurity> {
    let dozer_master_secret = std::env::var("DOZER_MASTER_SECRET").ok();
    dozer_master_secret
        .map(ApiSecurity::Jwt)
        .or(current_api_security)
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
                Err(ApiError::ApiAuthError(AuthError::Unauthorized))
            }
        }
    }
}
