use actix_web::{rt, web, App, HttpResponse, HttpServer, Responder};
use anyhow::Context;
use dozer_cache::cache::{
    expression::{FilterExpression, QueryExpression},
    index,
    lmdb::cache::LmdbCache,
    query_helper::value_to_expression,
    Cache,
};
use dozer_types::{json_value_to_field, models::api_endpoint::ApiEndpoint, record_to_json};

use serde_json::Value;
use std::{collections::HashMap, sync::Arc};

use crate::{generator::oapi::generator::OpenApiGenerator, rest_error::RestError};

fn get_record(cache: Arc<LmdbCache>, key: Value) -> anyhow::Result<HashMap<String, Value>> {
    let key = match json_value_to_field(key) {
        Ok(key) => key,
        Err(e) => {
            panic!("error : {:?}", e);
        }
    };
    let key = index::get_primary_key(&[0], &[key]);
    let rec = cache.get(&key).context("record not found")?;
    let schema = cache.get_schema(&rec.schema_id.to_owned().context("schema_id not found")?)?;
    record_to_json(&rec, &schema)
}

fn get_records(
    cache: Arc<LmdbCache>,
    exp: QueryExpression,
) -> anyhow::Result<Vec<HashMap<String, Value>>> {
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

async fn generate_oapi(
    app_data: web::Data<(Arc<LmdbCache>, Vec<ApiEndpoint>)>,
) -> Result<HttpResponse, RestError> {
    let cache = app_data.0.to_owned();
    let endpoints = app_data.1.to_owned();
    let schema_by_name = cache
        .get_schema_by_name(&endpoints[0].name)
        .map_err(|e| RestError::Validation {
            message: Some(e.to_string()),
            details: None,
        })?;
    let oapi_generator = OpenApiGenerator::new(
        schema_by_name,
        "film".to_owned(),
        endpoints[0].clone(),
        vec![format!("http://localhost:{}", "8080")],
    )
    .unwrap();
    oapi_generator
        .generate_oas3("../dozer-api/test_generate.yml".to_owned())
        .unwrap();
    Ok(HttpResponse::Ok().body(""))
}

async fn get(
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

async fn list(
    app_data: web::Data<(Arc<LmdbCache>, Vec<ApiEndpoint>)>,
) -> impl Responder  {
    let cache = app_data.0.to_owned();
    let exp = QueryExpression::new(None, vec![], 50, 0);
    let records = get_records(cache, exp);
    match records {
        Ok(map) => {
            let str = serde_json::to_string(&map).unwrap();
            HttpResponse::Ok().body(str)
        }
        Err(_) => HttpResponse::Ok().body("[]"),
    }
}

async fn query(
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
        Err(_) => Ok(HttpResponse::Ok().body("[]")),
    }
}

#[derive(Clone)]
pub struct ApiServer {
    shutdown_timeout: u64,
    port: u16,
}

// #[async_trait]
impl ApiServer {
    pub fn default() -> Self {
        Self {
            shutdown_timeout: 0,
            port: 8080,
        }
    }
    pub fn new(shutdown_timeout: u64, port: u16) -> Self {
        Self {
            shutdown_timeout,
            port,
        }
    }
    pub fn run(&self, endpoints: Vec<ApiEndpoint>, cache: Arc<LmdbCache>) -> std::io::Result<()> {
        let endpoints = endpoints;
        rt::System::new().block_on(async move {
            HttpServer::new(move || {
                let app = App::new();
                let app = app.app_data(web::Data::new((cache.clone(), endpoints.clone())));
                endpoints.iter().fold(app, |app, endpoint| {
                    let list_route = &endpoint.path.clone();
                    let get_route = format!("{}/{}", list_route, "{id}");
                    let query_route = format!("{}/query", list_route);
                    app.route(list_route, web::post().to(list))
                        .route(&get_route, web::get().to(get))
                        .route(&query_route, web::post().to(query))
                        .route("oapi", web::post().to(generate_oapi))
                })
            })
            .bind(("0.0.0.0", self.port.to_owned()))?
            .shutdown_timeout(self.shutdown_timeout.to_owned())
            .run()
            .await
        })
    }
}
