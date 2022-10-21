use actix_web::{http::header::ContentType, web, HttpResponse, Responder};
use anyhow::Context;
use dozer_cache::cache::{expression::QueryExpression, index, Cache, LmdbCache};
use dozer_types::json_value_to_field;
use dozer_types::serde_json;
use dozer_types::serde_json::Value;
use dozer_types::types::{Field, FieldType};
use dozer_types::{models::api_endpoint::ApiEndpoint, record_to_json};
use std::{collections::HashMap, sync::Arc};

use crate::{generator::oapi::generator::OpenApiGenerator, rest_error::RestError};

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

// Generated Get function to return a single record in JSON format
pub async fn get(
    path: web::Path<String>,
    app_data: web::Data<(Arc<LmdbCache>, Vec<ApiEndpoint>)>,
) -> impl Responder {
    let cache = app_data.0.to_owned();
    let key = path.into_inner();
    match get_record(cache, key) {
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
        Err(e) => HttpResponse::UnprocessableEntity()
            .content_type(ContentType::json())
            .body(e.to_string()),
    }
}

// Generated query function for multiple records
pub async fn query(
    query_info: web::Json<Value>,
    app_data: web::Data<(Arc<LmdbCache>, Vec<ApiEndpoint>)>,
) -> Result<HttpResponse, RestError> {
    let cache = app_data.0.to_owned();
    let query_expression =
        serde_json::from_value::<QueryExpression>(query_info.0).map_err(|e| {
            RestError::Validation {
                message: Some(e.to_string()),
                details: None,
            }
        })?;
    let records = get_records(cache, query_expression);

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

/// Get a single record
fn get_record(cache: Arc<LmdbCache>, key: String) -> anyhow::Result<HashMap<String, String>> {
    let schema = cache.get_schema_by_name("films")?;

    let field_types: Vec<FieldType> = schema
        .primary_index
        .iter()
        .map(|idx| schema.fields[*idx].typ.clone())
        .collect();
    let key = json_value_to_field(&key, &field_types[0]);

    let key: Field = match key {
        Ok(key) => key,
        Err(e) => {
            panic!("error : {:?}", e);
        }
    };
    let key = index::get_primary_key(&[0], &[key]);
    let rec = cache.get(&key).context("record not found")?;

    record_to_json(&rec, &schema)
}

/// Get multiple records
fn get_records(
    cache: Arc<LmdbCache>,
    exp: QueryExpression,
) -> anyhow::Result<Vec<HashMap<String, String>>> {
    let records = cache.query("films", &exp)?;
    let schema = cache.get_schema(
        &records[0]
            .schema_id
            .to_owned()
            .context("schema_id not found")?,
    )?;
    let mut maps = vec![];
    for rec in records.iter() {
        let map = record_to_json(rec, &schema)?;
        maps.push(map);
    }
    Ok(maps)
}
