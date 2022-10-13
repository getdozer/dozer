use actix_web::{rt, web, App, HttpResponse, HttpServer, Responder};
use anyhow::{Context, bail, ensure};
use dozer_cache::cache::{expression::{QueryExpression, FilterExpression}, index, lmdb::cache::LmdbCache, Cache, query_helper::value_to_expression};
use dozer_types::{json_value_to_field, models::api_endpoint::ApiEndpoint, record_to_json};
use serde_json::Value;
use std::{collections::HashMap, sync::Arc};

use crate::rest_error::RestError;

fn get_record(
    cache: web::Data<Arc<LmdbCache>>,
    key: Value,
) -> anyhow::Result<HashMap<String, Value>> {
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
    cache: web::Data<Arc<LmdbCache>>,
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

async fn get(path: web::Path<(String,)>, cache: web::Data<Arc<LmdbCache>>) -> impl Responder {
    let key_json: Value = serde_json::from_str(&path.into_inner().0).unwrap();

    match get_record(cache, key_json) {
        Ok(map) => {
            let str = serde_json::to_string(&map).unwrap();
            HttpResponse::Ok().body(str)
        }
        Err(e) => HttpResponse::NotFound().body(e.to_string()),
    }
}

async fn list(filter_info: web::Json<Value> ,cache: web::Data<Arc<LmdbCache>>) -> Result<HttpResponse, RestError> {
    let filter_expression = value_to_expression(filter_info.0)
    .map_err(|e| RestError::Validation {
        message: Some(e.to_string()),
        details: None,
    }).map(|vec| -> Option<FilterExpression> {
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
                let app = app.app_data(web::Data::new(cache.clone()));
                endpoints.iter().fold(app, |app, endpoint| {
                    let list_route = &endpoint.path.clone();
                    let get_route = format!("{}/{}", list_route, "{id}");
                    app.route(list_route, web::post().to(list))
                        .route(&get_route, web::get().to(get))
                })
            })
            .bind(("0.0.0.0", self.port.to_owned()))?
            .shutdown_timeout(self.shutdown_timeout.to_owned())
            .run()
            .await
        })
    }
}
