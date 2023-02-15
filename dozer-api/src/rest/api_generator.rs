use actix_web::web::ReqData;
use actix_web::{web, HttpResponse};
use dozer_cache::cache::expression::{default_limit_for_query, QueryExpression};
use dozer_cache::CacheReader;
use dozer_types::log::info;
use dozer_types::models::api_endpoint::ApiEndpoint;
use openapiv3::OpenAPI;

use super::super::api_helper::ApiHelper;
use crate::generator::oapi::generator::OpenApiGenerator;
use crate::grpc::health_grpc::health_check_response::ServingStatus;
use crate::RoCacheEndpoint;
use crate::{auth::Access, errors::ApiError};
use dozer_cache::errors::CacheError;
use dozer_types::serde_json;
use dozer_types::serde_json::{json, Value};

fn generate_oapi3(reader: &CacheReader, endpoint: ApiEndpoint) -> Result<OpenAPI, ApiError> {
    let (schema, secondary_indexes) = reader
        .get_schema_and_indexes_by_name(&endpoint.name)
        .map_err(ApiError::SchemaNotFound)?;

    let oapi_generator = OpenApiGenerator::new(
        schema,
        secondary_indexes,
        endpoint,
        vec![format!("http://localhost:{}", "8080")],
    );

    oapi_generator
        .generate_oas3()
        .map_err(ApiError::ApiGenerationError)
}

/// Generated function to return openapi.yaml documentation.
pub async fn generate_oapi(
    cache_endpoint: ReqData<RoCacheEndpoint>,
) -> Result<HttpResponse, ApiError> {
    generate_oapi3(
        &cache_endpoint.cache_reader,
        cache_endpoint.endpoint.clone(),
    )
    .map(|result| HttpResponse::Ok().json(result))
}

// Generated Get function to return a single record in JSON format
pub async fn get(
    access: Option<ReqData<Access>>,
    cache_endpoint: ReqData<RoCacheEndpoint>,
    path: web::Path<String>,
) -> Result<HttpResponse, ApiError> {
    let helper = ApiHelper::new(
        &cache_endpoint.cache_reader,
        &cache_endpoint.endpoint.name,
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
    cache_endpoint: ReqData<RoCacheEndpoint>,
) -> Result<HttpResponse, ApiError> {
    let helper = ApiHelper::new(
        &cache_endpoint.cache_reader,
        &cache_endpoint.endpoint.name,
        access.map(|a| a.into_inner()),
    )?;
    let exp = QueryExpression::new(None, vec![], Some(50), 0);
    match helper
        .get_records_map(exp)
        .map(|maps| HttpResponse::Ok().json(maps))
    {
        Ok(res) => Ok(res),
        Err(e) => match e {
            CacheError::Query(_) => {
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

pub async fn count(
    access: Option<ReqData<Access>>,
    cache_endpoint: ReqData<RoCacheEndpoint>,
    query_info: Option<web::Json<Value>>,
) -> Result<HttpResponse, ApiError> {
    let query_expression = match query_info {
        Some(query_info) => serde_json::from_value::<QueryExpression>(query_info.0)
            .map_err(ApiError::map_deserialization_error)?,
        None => QueryExpression::with_no_limit(),
    };

    let helper = ApiHelper::new(
        &cache_endpoint.cache_reader,
        &cache_endpoint.endpoint.name,
        access.map(|a| a.into_inner()),
    )?;
    helper
        .get_records_count(query_expression)
        .map(|count| HttpResponse::Ok().json(count))
        .map_err(|e| match e {
            CacheError::Type(e) => ApiError::TypeError(e),
            CacheError::Internal(e) => ApiError::InternalError(e),
            e => ApiError::InternalError(Box::new(e)),
        })
}

// Generated query function for multiple records
pub async fn query(
    access: Option<ReqData<Access>>,
    cache_endpoint: ReqData<RoCacheEndpoint>,
    query_info: Option<web::Json<Value>>,
) -> Result<HttpResponse, ApiError> {
    let mut query_expression = match query_info {
        Some(query_info) => serde_json::from_value::<QueryExpression>(query_info.0)
            .map_err(ApiError::map_deserialization_error)?,
        None => QueryExpression::with_default_limit(),
    };
    if query_expression.limit.is_none() {
        query_expression.limit = Some(default_limit_for_query());
    }
    let helper = ApiHelper::new(
        &cache_endpoint.cache_reader,
        &cache_endpoint.endpoint.name,
        access.map(|a| a.into_inner()),
    )?;
    helper
        .get_records_map(query_expression)
        .map(|maps| HttpResponse::Ok().json(maps))
        .map_err(|e| match e {
            CacheError::Type(e) => ApiError::TypeError(e),
            CacheError::Internal(e) => ApiError::InternalError(e),
            e => ApiError::InternalError(Box::new(e)),
        })
}
