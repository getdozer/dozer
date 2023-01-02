use actix_web::web::ReqData;
use actix_web::{web, HttpResponse};
use dozer_cache::cache::expression::QueryExpression;
use dozer_types::log::info;

use super::super::api_helper::ApiHelper;
use crate::{auth::Access, errors::ApiError, PipelineDetails};
use crate::grpc::health_grpc::health_check_response::ServingStatus;
use dozer_cache::errors::CacheError;
use dozer_types::serde_json;
use dozer_types::serde_json::{json, Value};

/// Generated function to return openapi.yaml documentation.
pub async fn generate_oapi(
    access: Option<ReqData<Access>>,
    pipeline_details: ReqData<PipelineDetails>,
) -> Result<HttpResponse, ApiError> {
    let helper = ApiHelper::new(
        pipeline_details.into_inner(),
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
) -> Result<HttpResponse, ApiError> {
    let helper = ApiHelper::new(
        pipeline_details.into_inner(),
        access.map(|a| a.into_inner()),
    )?;
    let key = path.as_str();
    helper
        .get_record(key)
        .map(|map| HttpResponse::Ok().json(map))
        .map_err(ApiError::NotFound)
}

// Generated list function for multiple records with a default query expression
pub async fn list(
    access: Option<ReqData<Access>>,
    pipeline_details: ReqData<PipelineDetails>,
) -> Result<HttpResponse, ApiError> {
    let helper = ApiHelper::new(
        pipeline_details.into_inner(),
        access.map(|a| a.into_inner()),
    )?;
    let exp = QueryExpression::new(None, vec![], 50, 0);
    match helper
        .get_records_map(exp)
        .map(|maps| HttpResponse::Ok().json(maps))
    {
        Ok(res) => Ok(res),
        Err(e) => match e {
            CacheError::QueryError(_) => {
                let res: Vec<String> = vec![];
                info!("No records found.");
                Ok(HttpResponse::Ok().json(res))
            }
            _ => Err(ApiError::InternalError(Box::new(e))),
        },
    }
}

// Generated get function for health check
pub async fn health_route() -> Result<HttpResponse, ApiError> {
    let status = ServingStatus::Serving;
    let resp = json!({ "status": status.as_str_name() }).to_string();
    Ok(HttpResponse::Ok().body(resp))
}

// Generated query function for multiple records
pub async fn query(
    access: Option<ReqData<Access>>,
    pipeline_details: ReqData<PipelineDetails>,
    query_info: web::Json<Value>,
) -> Result<HttpResponse, ApiError> {
    let query_expression = serde_json::from_value::<QueryExpression>(query_info.0)
        .map_err(ApiError::map_deserialization_error)?;
    let helper = ApiHelper::new(
        pipeline_details.into_inner(),
        access.map(|a| a.into_inner()),
    )?;
    helper
        .get_records_map(query_expression)
        .map(|maps| HttpResponse::Ok().json(maps))
        .map_err(|e| match e {
            CacheError::QueryValidationError(e) => ApiError::InvalidQuery(e),
            CacheError::TypeError(e) => ApiError::TypeError(e),
            CacheError::InternalError(e) => ApiError::InternalError(e),
            e => ApiError::InternalError(Box::new(e)),
        })
}
