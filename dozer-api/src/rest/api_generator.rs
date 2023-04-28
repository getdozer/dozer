use std::sync::Arc;

use actix_web::web::ReqData;
use actix_web::{web, HttpResponse};
use dozer_cache::cache::expression::{default_limit_for_query, QueryExpression, Skip};
use dozer_cache::cache::CacheRecord;
use dozer_cache::CacheReader;
use dozer_types::errors::types::TypeError;
use dozer_types::indexmap::IndexMap;
use dozer_types::log::warn;
use dozer_types::models::api_endpoint::ApiEndpoint;
use dozer_types::types::{Field, Schema};
use openapiv3::OpenAPI;

use crate::api_helper::{get_record, get_records, get_records_count};
use crate::generator::oapi::generator::OpenApiGenerator;
use crate::CacheEndpoint;
use crate::{auth::Access, errors::ApiError};
use dozer_types::grpc_types::health::health_check_response::ServingStatus;
use dozer_types::json_types::field_to_json_value;
use dozer_types::serde_json;
use dozer_types::serde_json::{json, Value};

fn generate_oapi3(reader: &CacheReader, endpoint: ApiEndpoint) -> Result<OpenAPI, ApiError> {
    let (schema, secondary_indexes) = reader.get_schema();

    let oapi_generator = OpenApiGenerator::new(
        schema,
        secondary_indexes,
        endpoint,
        vec![format!("http://localhost:{}", "8080")],
    );

    Ok(oapi_generator.generate_oas3())
}

/// Generated function to return openapi.yaml documentation.
pub async fn generate_oapi(
    cache_endpoint: ReqData<Arc<CacheEndpoint>>,
) -> Result<HttpResponse, ApiError> {
    generate_oapi3(
        &cache_endpoint.cache_reader(),
        cache_endpoint.endpoint.clone(),
    )
    .map(|result| HttpResponse::Ok().json(result))
}

// Generated Get function to return a single record in JSON format
pub async fn get(
    access: Option<ReqData<Access>>,
    cache_endpoint: ReqData<Arc<CacheEndpoint>>,
    path: web::Path<String>,
) -> Result<HttpResponse, ApiError> {
    let cache_reader = &cache_endpoint.cache_reader();
    let schema = &cache_reader.get_schema().0;

    let key = path.as_str();
    let key = if schema.primary_index.is_empty() {
        return Err(ApiError::NoPrimaryKey);
    } else if schema.primary_index.len() == 1 {
        let field = &schema.fields[schema.primary_index[0]];
        Field::from_str(key, field.typ, field.nullable)?
    } else {
        return Err(ApiError::MultiIndexFetch(key.to_string()));
    };

    // This implementation must be consistent with `dozer_cache::cache::index::get_primary_key`
    let key = key.encode();
    let record = get_record(
        &cache_endpoint.cache_reader(),
        &key,
        &cache_endpoint.endpoint.name,
        access.map(|a| a.into_inner()),
    )?;

    Ok(record_to_map(record, schema).map(|map| HttpResponse::Ok().json(map))?)
}

// Generated list function for multiple records with a default query expression
pub async fn list(
    access: Option<ReqData<Access>>,
    cache_endpoint: ReqData<Arc<CacheEndpoint>>,
) -> Result<HttpResponse, ApiError> {
    let mut exp = QueryExpression::new(None, vec![], Some(50), Skip::Skip(0));
    match get_records_map(access, cache_endpoint, &mut exp) {
        Ok(maps) => Ok(HttpResponse::Ok().json(maps)),
        Err(e) => match e {
            ApiError::QueryFailed(_) => {
                let res: Vec<String> = vec![];
                warn!("No records found.");
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
    cache_endpoint: ReqData<Arc<CacheEndpoint>>,
    query_info: Option<web::Json<QueryExpression>>,
) -> Result<HttpResponse, ApiError> {
    let mut query_expression = match query_info {
        Some(query_info) => query_info.0,
        None => QueryExpression::with_no_limit(),
    };

    get_records_count(
        &cache_endpoint.cache_reader(),
        &mut query_expression,
        &cache_endpoint.endpoint.name,
        access.map(|a| a.into_inner()),
    )
    .map(|count| HttpResponse::Ok().json(count))
}

// Generated query function for multiple records
pub async fn query(
    access: Option<ReqData<Access>>,
    cache_endpoint: ReqData<Arc<CacheEndpoint>>,
    query_info: Option<web::Json<QueryExpression>>,
) -> Result<HttpResponse, ApiError> {
    let mut query_expression = match query_info {
        Some(query_info) => query_info.0,
        None => QueryExpression::with_default_limit(),
    };
    if query_expression.limit.is_none() {
        query_expression.limit = Some(default_limit_for_query());
    }

    get_records_map(access, cache_endpoint, &mut query_expression)
        .map(|maps| HttpResponse::Ok().json(maps))
}

/// Get multiple records
fn get_records_map(
    access: Option<ReqData<Access>>,
    cache_endpoint: ReqData<Arc<CacheEndpoint>>,
    exp: &mut QueryExpression,
) -> Result<Vec<IndexMap<String, Value>>, ApiError> {
    let mut maps = vec![];
    let cache_reader = &cache_endpoint.cache_reader();
    let records = get_records(
        cache_reader,
        exp,
        &cache_endpoint.endpoint.name,
        access.map(|a| a.into_inner()),
    )?;
    let schema = &cache_reader.get_schema().0;
    for record in records.into_iter() {
        let map = record_to_map(record, schema)?;
        maps.push(map);
    }
    Ok(maps)
}

/// Used in REST APIs for converting to JSON
fn record_to_map(
    record: CacheRecord,
    schema: &Schema,
) -> Result<IndexMap<String, Value>, TypeError> {
    let mut map = IndexMap::new();

    for (field_def, field) in schema.fields.iter().zip(record.record.values) {
        let val = field_to_json_value(field);
        map.insert(field_def.name.clone(), val);
    }

    map.insert("__dozer_record_id".to_string(), Value::from(record.id));
    map.insert(
        "__dozer_record_version".to_string(),
        Value::from(record.version),
    );

    Ok(map)
}
