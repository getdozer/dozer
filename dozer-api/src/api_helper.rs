use actix_web::web::ReqData;
use actix_web::{http::header::ContentType, web, HttpResponse, Responder};
use anyhow::Context;
use dozer_cache::cache::{expression::QueryExpression, index, Cache, LmdbCache};
use dozer_types::json_value_to_field;
use dozer_types::record_to_json;
use dozer_types::serde_json;
use dozer_types::serde_json::Value;
use dozer_types::types::{Field, FieldType};
use std::ops::Deref;
use std::{collections::HashMap, sync::Arc};

use crate::api_server::PipelineDetails;
use crate::{generator::oapi::generator::OpenApiGenerator, rest_error::RestError};

/// Generated function to return openapi.yaml documentation.
pub async fn generate_oapi(
    pipeline_details: Option<ReqData<PipelineDetails>>,
    cache: web::Data<Arc<LmdbCache>>,
) -> Result<HttpResponse, RestError> {
    if let Some(details) = pipeline_details {
        let schema_name = details.schema_name.clone();
        let schema = cache
            .get_schema_by_name(&schema_name)
            .map_err(|e| RestError::Validation {
                message: Some(e.to_string()),
                details: None,
            })?;

        let oapi_generator = OpenApiGenerator::new(
            schema,
            schema_name,
            details.endpoint.clone(),
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
    } else {
        Ok(HttpResponse::InternalServerError().body("pipeline_details not initialized"))
    }
}

// Generated Get function to return a single record in JSON format
pub async fn get(
    pipeline_details: Option<ReqData<PipelineDetails>>,
    path: web::Path<String>,
    cache: web::Data<Arc<LmdbCache>>,
) -> impl Responder {
    let key = path.into_inner();
    let unwrap_cache = cache.into_inner();
    let cache = unwrap_cache.deref();
    if let Some(details) = pipeline_details {
        match get_record(&details.schema_name, cache.to_owned(), key) {
            Ok(map) => {
                let str = serde_json::to_string(&map).unwrap();
                HttpResponse::Ok().body(str)
            }
            Err(e) => HttpResponse::NotFound().body(e.to_string()),
        }
    } else {
        HttpResponse::InternalServerError().body("pipeline_details not initialized")
    }
}

// Generated list function for multiple records with a default query expression
pub async fn list(
    pipeline_details: Option<ReqData<PipelineDetails>>,
    cache: web::Data<Arc<LmdbCache>>,
) -> impl Responder {
    if let Some(details) = pipeline_details {
        let unwrap_cache = cache.into_inner();
        let cache = unwrap_cache.deref();
        let exp = QueryExpression::new(None, vec![], 50, 0);
        let records = get_records(&details.schema_name, cache.to_owned(), exp);

        match records {
            Ok(maps) => HttpResponse::Ok().json(maps),
            Err(e) => HttpResponse::UnprocessableEntity()
                .content_type(ContentType::json())
                .body(e.to_string()),
        }
    } else {
        HttpResponse::InternalServerError().body("pipeline_details not initialized")
    }
}

// Generated query function for multiple records
pub async fn query(
    pipeline_details: Option<ReqData<PipelineDetails>>,
    query_info: web::Json<Value>,
    cache: web::Data<Arc<LmdbCache>>,
) -> Result<HttpResponse, RestError> {
    let unwrap_cache = cache.into_inner();
    let cache = unwrap_cache.deref();
    if let Some(details) = pipeline_details {
        let query_expression =
            serde_json::from_value::<QueryExpression>(query_info.0).map_err(|e| {
                RestError::Validation {
                    message: Some(e.to_string()),
                    details: None,
                }
            })?;
        let records = get_records(&details.schema_name, cache.to_owned(), query_expression);

        match records {
            Ok(maps) => {
                let str = serde_json::to_string(&maps).unwrap();
                Ok(HttpResponse::Ok().body(str))
            }
            Err(_) => Ok(HttpResponse::Ok()
                .content_type(ContentType::json())
                .body("[]")),
        }
    } else {
        Ok(HttpResponse::InternalServerError().body("pipeline_details not initialized"))
    }
}

/// Get a single record
pub fn get_record(
    schema_name: &str,
    cache: Arc<LmdbCache>,
    key: String,
) -> anyhow::Result<HashMap<String, String>> {
    let schema = cache.get_schema_by_name(schema_name)?;

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
pub fn get_records(
    schema_name: &str,
    cache: Arc<LmdbCache>,
    exp: QueryExpression,
) -> anyhow::Result<Vec<HashMap<String, String>>> {
    let records = cache.query(schema_name, &exp)?;
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
