use crate::auth::Access;
use crate::errors::{ApiError, AuthError};
use dozer_cache::cache::expression::QueryExpression;
use dozer_cache::cache::RecordWithId;
use dozer_cache::{AccessFilter, CacheReader};

pub fn get_record(
    cache_reader: &CacheReader,
    key: &[u8],
    access: Option<Access>,
) -> Result<RecordWithId, ApiError> {
    let access_filter = get_access_filter(access)?;
    let record = cache_reader
        .get(key, &access_filter)
        .map_err(ApiError::NotFound)?;
    Ok(record)
}

pub fn get_records_count(
    cache_reader: &CacheReader,
    exp: &mut QueryExpression,
    access: Option<Access>,
) -> Result<usize, ApiError> {
    let access_filter = get_access_filter(access)?;
    cache_reader
        .count(exp, access_filter)
        .map_err(ApiError::CountFailed)
}

/// Get multiple records
pub fn get_records(
    cache_reader: &CacheReader,
    exp: &mut QueryExpression,
    access: Option<Access>,
) -> Result<Vec<RecordWithId>, ApiError> {
    let access_filter = get_access_filter(access)?;
    cache_reader
        .query(exp, access_filter)
        .map_err(ApiError::QueryFailed)
}

fn get_access_filter(access: Option<Access>) -> Result<AccessFilter, ApiError> {
    match access {
        None | Some(Access::All) => Ok(AccessFilter {
            filter: None,
            fields: vec![],
        }),
        Some(Access::Custom(mut access_filters)) => {
            if let Some(access_filter) = access_filters.remove("get_records") {
                Ok(access_filter)
            } else {
                Err(ApiError::ApiAuthError(AuthError::InvalidToken))
            }
        }
    }
}
