use actix_web::web::ReqData;
use actix_web::{web, HttpResponse};
use dozer_cache::cache::{expression::QueryExpression, LmdbCache};

use dozer_types::errors::cache::CacheError;
use dozer_types::serde_json;
use dozer_types::serde_json::Value;
use std::sync::Arc;

use crate::api_helper::ApiHelper;
use crate::api_server::PipelineDetails;
use crate::errors::ApiError;

/// Generated function to return openapi.yaml documentation.
pub async fn generate_oapi(
    pipeline_details: Option<ReqData<PipelineDetails>>,
    cache: web::Data<Arc<LmdbCache>>,
) -> Result<HttpResponse, ApiError> {
    let helper = ApiHelper::new(pipeline_details, cache)?;

    helper
        .generate_oapi3()
        .map(|result| HttpResponse::Ok().json(result))
}

// Generated Get function to return a single record in JSON format
pub async fn get(
    pipeline_details: Option<ReqData<PipelineDetails>>,
    path: web::Path<String>,
    cache: web::Data<Arc<LmdbCache>>,
) -> Result<HttpResponse, ApiError> {
    let helper = ApiHelper::new(pipeline_details, cache)?;
    let key = path.into_inner();
    helper
        .get_record(key)
        .map(|map| {
            let str = serde_json::to_string(&map).unwrap();
            HttpResponse::Ok().body(str)
        })
        .map_err(ApiError::NotFound)
}

// Generated list function for multiple records with a default query expression
pub async fn list(
    pipeline_details: Option<ReqData<PipelineDetails>>,
    cache: web::Data<Arc<LmdbCache>>,
) -> Result<HttpResponse, ApiError> {
    let helper = ApiHelper::new(pipeline_details, cache)?;
    let exp = QueryExpression::new(None, vec![], 50, 0);
    helper
        .get_records(exp)
        .map(|maps| HttpResponse::Ok().json(maps))
        .map_err(|e| ApiError::InternalError(Box::new(e)))
}

// Generated query function for multiple records
pub async fn query(
    pipeline_details: Option<ReqData<PipelineDetails>>,
    query_info: web::Json<Value>,
    cache: web::Data<Arc<LmdbCache>>,
) -> Result<HttpResponse, ApiError> {
    let query_expression = serde_json::from_value::<QueryExpression>(query_info.0)
        .map_err(|e| ApiError::map_deserialization_error(e))?;
    let helper = ApiHelper::new(pipeline_details, cache)?;
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
