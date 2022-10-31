use actix_web::web::ReqData;
use actix_web::{web, HttpResponse};
use dozer_cache::cache::{expression::QueryExpression, LmdbCache};

use crate::api_helper::ApiHelper;
use crate::api_server::PipelineDetails;
use crate::auth::Access;
use crate::errors::ApiError;
use dozer_types::errors::cache::CacheError;
use dozer_types::serde_json;
use dozer_types::serde_json::Value;
use std::sync::Arc;

/// Generated function to return openapi.yaml documentation.
pub async fn generate_oapi(
    access: Option<ReqData<Access>>,
    pipeline_details: ReqData<PipelineDetails>,
    cache: web::Data<Arc<LmdbCache>>,
) -> Result<HttpResponse, ApiError> {
    let cache = cache.as_ref().to_owned();
    let helper = ApiHelper::new(
        pipeline_details.into_inner(),
        cache,
        access.map(|a| a.into_inner()),
    )?;

    helper
        .generate_oapi3()
        .map(|result| HttpResponse::Ok().json(result))
}

// Generated Get function to return a single record in JSON format
pub async fn get(
    access: Option<ReqData<Access>>,
    pipeline_details: ReqData<PipelineDetails>,
    path: web::Path<String>,
    cache: web::Data<Arc<LmdbCache>>,
) -> Result<HttpResponse, ApiError> {
    let cache = cache.as_ref().to_owned();
    let helper = ApiHelper::new(
        pipeline_details.into_inner(),
        cache,
        access.map(|a| a.into_inner()),
    )?;
    let key = path.into_inner();
    helper
        .get_record(key)
        .map(|map| HttpResponse::Ok().json(map))
        .map_err(ApiError::NotFound)
}

// Generated list function for multiple records with a default query expression
pub async fn list(
    access: Option<ReqData<Access>>,
    pipeline_details: ReqData<PipelineDetails>,
    cache: web::Data<Arc<LmdbCache>>,
) -> Result<HttpResponse, ApiError> {
    let cache = cache.as_ref().to_owned();
    let helper = ApiHelper::new(
        pipeline_details.into_inner(),
        cache,
        access.map(|a| a.into_inner()),
    )?;
    let exp = QueryExpression::new(None, vec![], 50, 0);
    helper
        .get_records(exp)
        .map(|maps| HttpResponse::Ok().json(maps))
        .map_err(|e| ApiError::InternalError(Box::new(e)))
}

// Generated query function for multiple records
pub async fn query(
    access: Option<ReqData<Access>>,
    pipeline_details: ReqData<PipelineDetails>,
    query_info: web::Json<Value>,
    cache: web::Data<Arc<LmdbCache>>,
) -> Result<HttpResponse, ApiError> {
    let query_expression = serde_json::from_value::<QueryExpression>(query_info.0)
        .map_err(ApiError::map_deserialization_error)?;
    let cache = cache.as_ref().to_owned();
    let helper = ApiHelper::new(
        pipeline_details.into_inner(),
        cache,
        access.map(|a| a.into_inner()),
    )?;
    helper
        .get_records(query_expression)
        .map(|maps| HttpResponse::Ok().json(maps))
        .map_err(|e| match e {
            CacheError::QueryValidationError(e) => ApiError::InvalidQuery(e),
            CacheError::TypeError(e) => ApiError::TypeError(e),
            CacheError::InternalError(e) => ApiError::InternalError(e),
            e => ApiError::InternalError(Box::new(e)),
        })
}
