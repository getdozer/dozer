use actix_web::{http::header::ContentType, web, HttpResponse, Responder};
use dozer_cache::cache::{
    expression::{FilterExpression, QueryExpression},
    query_helper::value_to_expression,
    Cache, LmdbCache,
};
use dozer_types::models::api_endpoint::ApiEndpoint;
use serde_json::Value;
use std::sync::Arc;

use crate::{
    common_query::{get_record, get_records},
    generator::{oapi::generator::OpenApiGenerator, protoc::generator::ProtoGenerator},
    rest_error::RestError,
};

/// Generated function to return openapi.yaml documentation.
pub async fn generate_oapi(
    app_data: web::Data<(Arc<LmdbCache>, Vec<ApiEndpoint>)>,
) -> Result<HttpResponse, RestError> {
    let cache = app_data.0.to_owned();
    let endpoints = app_data.1.to_owned();
    let schema_by_name =
        cache
            .get_schema_by_name(&endpoints[0].name)
            .map_err(|e| RestError::Validation {
                message: Some(e.to_string()),
                details: None,
            })?;
    let schema_name = endpoints[0].clone().name;
    let oapi_generator = OpenApiGenerator::new(
        schema_by_name,
        schema_name,
        endpoints[0].clone(),
        vec![format!("http://localhost:{}", "8080")],
    );

    match oapi_generator.generate_oas3() {
        Ok(result) => Ok(HttpResponse::Ok().json(result)),
        Err(e) => {
            let error = RestError::Unknown {
                message: Some(e.to_string()),
                details: None,
            };
            Ok(HttpResponse::UnprocessableEntity().json(error))
        }
    }
}

/// Generated function to return proto documentation.
pub async fn generate_proto(
    app_data: web::Data<(Arc<LmdbCache>, Vec<ApiEndpoint>)>,
) -> Result<HttpResponse, RestError> {
    let cache = app_data.0.to_owned();
    let endpoints = app_data.1.to_owned();
    let schema_by_name =
        cache
            .get_schema_by_name(&endpoints[0].name)
            .map_err(|e| RestError::Validation {
                message: Some(e.to_string()),
                details: None,
            })?;
    let schema_name = endpoints[0].clone().name;
    let proto_generator = ProtoGenerator::new(schema_by_name, schema_name, endpoints[0].clone())
        .map_err(|e| RestError::Validation {
            message: Some(e.to_string()),
            details: None,
        })?;

    match proto_generator.generate_proto() {
        Ok(result) => Ok(HttpResponse::Ok().body(result.0)),
        Err(e) => {
            let error = RestError::Unknown {
                message: Some(e.to_string()),
                details: None,
            };
            Ok(HttpResponse::UnprocessableEntity().json(error))
        }
    }
}

// Generated Get function to return a single record in JSON format
pub async fn get(
    path: web::Path<String>,
    app_data: web::Data<(Arc<LmdbCache>, Vec<ApiEndpoint>)>,
) -> impl Responder {
    let cache = app_data.0.to_owned();
    let key_json: Value = serde_json::from_str(&path.into_inner()).unwrap();
    match get_record(cache, key_json) {
        Ok(map) => {
            let str = serde_json::to_string(&map).unwrap();
            HttpResponse::Ok().body(str)
        }
        Err(e) => HttpResponse::NotFound().body(e.to_string()),
    }
}

// Generated list function for multiple records with a default query expression
pub async fn list(app_data: web::Data<(Arc<LmdbCache>, Vec<ApiEndpoint>)>) -> impl Responder {
    let cache = app_data.0.to_owned();
    let exp = QueryExpression::new(None, vec![], 50, 0);
    let records = get_records(cache, exp);

    match records {
        Ok(maps) => HttpResponse::Ok().json(maps),
        Err(_) => HttpResponse::Ok()
            .content_type(ContentType::json())
            .body("[]"),
    }
}

// Generated query function for multiple records
pub async fn query(
    filter_info: web::Json<Value>,
    app_data: web::Data<(Arc<LmdbCache>, Vec<ApiEndpoint>)>,
) -> Result<HttpResponse, RestError> {
    let cache = app_data.0.to_owned();
    let filter_expression = value_to_expression(filter_info.0)
        .map_err(|e| RestError::Validation {
            message: Some(e.to_string()),
            details: None,
        })
        .map(|vec| -> Option<FilterExpression> {
            if vec.len() == 1 {
                Some(vec[0].to_owned())
            } else {
                None
            }
        })?;
    let exp = QueryExpression::new(filter_expression, vec![], 50, 0);
    let records = get_records(cache, exp);

    match records {
        Ok(maps) => {
            let str = serde_json::to_string(&maps).unwrap();
            Ok(HttpResponse::Ok().body(str))
        }
        Err(_) => Ok(HttpResponse::Ok()
            .content_type(ContentType::json())
            .body("[]")),
    }
}
